use cosmrs::crypto::secp256k1::SigningKey;
use cosmrs::tx::{Body, Fee, SignDoc, SignerInfo};
use cosmrs::{AccountId, Any, Coin};

use crate::client::SourceHubClient;

/// Transaction signer for SourceHub (Cosmos SDK).
///
/// Uses cosmrs for standard Cosmos SDK tx building (TxBody, AuthInfo, SignDoc,
/// signing) and hand-encoded protobuf only for SourceHub-specific messages
/// (MsgCreatePolicy, MsgBearerPolicyCmd) that aren't in cosmos-sdk-proto.
pub(crate) struct TxSigner {
    signing_key: SigningKey,
    account_id: AccountId,
    chain_id: cosmrs::tendermint::chain::Id,
}

impl TxSigner {
    /// Create a signer from raw secp256k1 private key bytes.
    pub(crate) fn from_secp256k1_bytes(
        key_bytes: &[u8],
        chain_id: &str,
    ) -> Result<Self, TxSignerError> {
        let signing_key = SigningKey::from_slice(key_bytes)
            .map_err(|e| TxSignerError::Key(format!("invalid secp256k1 key: {}", e)))?;

        let public_key = signing_key.public_key();
        let account_id = public_key
            .account_id("source")
            .map_err(|e| TxSignerError::Key(format!("failed to derive address: {}", e)))?;

        let chain_id: cosmrs::tendermint::chain::Id = chain_id
            .parse()
            .map_err(|e| TxSignerError::Key(format!("invalid chain ID: {}", e)))?;

        Ok(Self {
            signing_key,
            account_id,
            chain_id,
        })
    }

    /// Get the bech32 account address (e.g., "source1...").
    pub(crate) fn address(&self) -> String {
        self.account_id.to_string()
    }

    /// Build, sign, and broadcast a MsgCreatePolicy transaction.
    /// Returns the policy ID extracted from the tx result.
    pub(crate) async fn create_policy(
        &self,
        client: &SourceHubClient,
        policy_yaml: &str,
    ) -> Result<String, TxSignerError> {
        let msg = build_msg_create_policy(&self.address(), policy_yaml);
        let tx_hash = self.sign_and_broadcast(client, vec![msg]).await?;
        tracing::debug!(tx_hash = %tx_hash, "create_policy: broadcast complete");
        let tx_result = client
            .await_tx(&tx_hash, 30_000)
            .await
            .map_err(|e| TxSignerError::Broadcast(e.to_string()))?;

        // Extract policy ID from tx_result.data (protobuf-encoded TxMsgData)
        if let Some(policy_id) = extract_policy_id_from_tx_data(&tx_result) {
            tracing::debug!(policy_id = %policy_id, "create_policy: extracted from tx data");
            return Ok(policy_id);
        }

        // Fallback: try to extract from events
        if let Some(policy_id) = extract_policy_id_from_events(&tx_result) {
            tracing::debug!(policy_id = %policy_id, "create_policy: extracted from events");
            return Ok(policy_id);
        }

        Err(TxSignerError::Broadcast(
            "could not extract policy ID from tx result".to_string(),
        ))
    }

    /// Build, sign, and broadcast a MsgBearerPolicyCmd transaction.
    /// Returns a JSON object with `tx_hash` and optionally `record_existed`/`record_found`.
    pub(crate) async fn bearer_policy_cmd(
        &self,
        client: &SourceHubClient,
        bearer_token: &str,
        policy_id: &str,
        cmd_json: serde_json::Value,
    ) -> Result<serde_json::Value, TxSignerError> {
        let msg = build_msg_bearer_policy_cmd(&self.address(), bearer_token, policy_id, cmd_json);
        let tx_hash = self.sign_and_broadcast(client, vec![msg]).await?;
        let tx_result = client
            .await_tx(&tx_hash, 30_000)
            .await
            .map_err(|e| TxSignerError::Broadcast(e.to_string()))?;

        let mut result = serde_json::json!({"tx_hash": tx_hash});

        // Extract RecordExisted/RecordFound from the tx result protobuf.
        // Path: TxMsgData.msg_responses[0].value → MsgBearerPolicyCmdResponse.result →
        //       PolicyCmdResult → SetRelationshipResult.record_existed (or DeleteRelationshipResult.record_found)
        if let Some(cmd_result) = extract_policy_cmd_result(&tx_result) {
            if let Some(existed) = cmd_result.record_existed {
                result["record_existed"] = serde_json::Value::Bool(existed);
            }
            if let Some(found) = cmd_result.record_found {
                result["record_found"] = serde_json::Value::Bool(found);
            }
        }

        Ok(result)
    }

    /// Sign and broadcast a transaction with cosmrs tx building.
    async fn sign_and_broadcast(
        &self,
        client: &SourceHubClient,
        messages: Vec<Any>,
    ) -> Result<String, TxSignerError> {
        let (account_number, sequence) = client
            .query_account(&self.address())
            .await
            .map_err(|e| TxSignerError::Broadcast(format!("account query: {}", e)))?;

        tracing::debug!(
            msg_count = messages.len(),
            account_number,
            sequence,
            "sign_and_broadcast"
        );

        let body = Body::new(messages, "", 0u32);

        let fee = Fee::from_amount_and_gas(
            Coin {
                denom: "uopen".parse().unwrap(),
                amount: 10_000u128,
            },
            400_000u64,
        );

        let auth_info =
            SignerInfo::single_direct(Some(self.signing_key.public_key()), sequence).auth_info(fee);

        let sign_doc = SignDoc::new(&body, &auth_info, &self.chain_id, account_number)
            .map_err(|e| TxSignerError::Sign(format!("SignDoc creation: {}", e)))?;

        let tx_raw = sign_doc
            .sign(&self.signing_key)
            .map_err(|e| TxSignerError::Sign(format!("signing: {}", e)))?;

        let tx_bytes = tx_raw
            .to_bytes()
            .map_err(|e| TxSignerError::Sign(format!("tx serialization: {}", e)))?;

        tracing::debug!(tx_bytes_len = tx_bytes.len(), "transaction serialized");

        client
            .broadcast_tx_sync(&tx_bytes)
            .await
            .map_err(|e| TxSignerError::Broadcast(e.to_string()))
    }
}

// === SourceHub-specific message builders ===
// These use hand-encoded protobuf because the SourceHub message types
// aren't in cosmos-sdk-proto. Long-term, these could be replaced with
// prost-build compiled types from SourceHub's proto definitions.

/// Build a protobuf Any for MsgCreatePolicy.
fn build_msg_create_policy(creator: &str, policy: &str) -> Any {
    // MsgCreatePolicy fields:
    // field 1 (string): creator
    // field 2 (string): policy (YAML text)
    // field 3 (enum): marshal_type = 1 (YAML)
    let mut buf = Vec::new();
    encode_string_field(&mut buf, 1, creator);
    encode_string_field(&mut buf, 2, policy);
    encode_varint_field(&mut buf, 3, 1);

    Any {
        type_url: "/sourcehub.acp.MsgCreatePolicy".to_string(),
        value: buf,
    }
}

/// Build a protobuf Any for MsgBearerPolicyCmd.
fn build_msg_bearer_policy_cmd(
    creator: &str,
    bearer_token: &str,
    policy_id: &str,
    cmd_json: serde_json::Value,
) -> Any {
    // MsgBearerPolicyCmd fields:
    // field 1 (string): creator
    // field 2 (string): bearer_token
    // field 3 (string): policy_id
    // field 4 (message): cmd (PolicyCmd)
    let cmd_bytes = encode_policy_cmd(&cmd_json);

    let mut buf = Vec::new();
    encode_string_field(&mut buf, 1, creator);
    encode_string_field(&mut buf, 2, bearer_token);
    encode_string_field(&mut buf, 3, policy_id);
    encode_bytes_field(&mut buf, 4, &cmd_bytes);

    Any {
        type_url: "/sourcehub.acp.MsgBearerPolicyCmd".to_string(),
        value: buf,
    }
}

/// Encode a PolicyCmd from JSON into protobuf bytes.
fn encode_policy_cmd(cmd: &serde_json::Value) -> Vec<u8> {
    let mut buf = Vec::new();

    if let Some(rel) = cmd.get("set_relationship_cmd") {
        let rel_bytes = encode_relationship_from_json(rel);
        let mut inner = Vec::new();
        encode_bytes_field(&mut inner, 1, &rel_bytes);
        encode_bytes_field(&mut buf, 1, &inner);
    } else if let Some(rel) = cmd.get("delete_relationship_cmd") {
        let rel_bytes = encode_relationship_from_json(rel);
        let mut inner = Vec::new();
        encode_bytes_field(&mut inner, 1, &rel_bytes);
        encode_bytes_field(&mut buf, 2, &inner);
    } else if let Some(obj) = cmd.get("register_object_cmd") {
        let obj_bytes = encode_object_from_json(obj);
        let mut inner = Vec::new();
        encode_bytes_field(&mut inner, 1, &obj_bytes);
        encode_bytes_field(&mut buf, 3, &inner);
    } else if let Some(obj) = cmd.get("archive_object_cmd") {
        let obj_bytes = encode_object_from_json(obj);
        let mut inner = Vec::new();
        encode_bytes_field(&mut inner, 1, &obj_bytes);
        encode_bytes_field(&mut buf, 4, &inner);
    }

    buf
}

/// Encode a Relationship message from JSON.
fn encode_relationship_from_json(json: &serde_json::Value) -> Vec<u8> {
    let rel = json.get("relationship").unwrap_or(json);
    let mut buf = Vec::new();

    if let Some(obj) = rel.get("object") {
        let obj_bytes = encode_object_from_json(obj);
        encode_bytes_field(&mut buf, 1, &obj_bytes);
    }
    if let Some(r) = rel.get("relation").and_then(|v| v.as_str()) {
        encode_string_field(&mut buf, 2, r);
    }
    if let Some(subj) = rel.get("subject") {
        let subj_bytes = encode_subject_from_json(subj);
        encode_bytes_field(&mut buf, 3, &subj_bytes);
    }

    buf
}

/// Encode an Object message from JSON.
fn encode_object_from_json(json: &serde_json::Value) -> Vec<u8> {
    let obj = json.get("object").unwrap_or(json);
    let mut buf = Vec::new();
    if let Some(r) = obj.get("resource").and_then(|v| v.as_str()) {
        encode_string_field(&mut buf, 1, r);
    }
    if let Some(id) = obj.get("id").and_then(|v| v.as_str()) {
        encode_string_field(&mut buf, 2, id);
    }
    buf
}

/// Encode a Subject message from JSON.
fn encode_subject_from_json(json: &serde_json::Value) -> Vec<u8> {
    let mut buf = Vec::new();
    if let Some(actor) = json.get("actor") {
        let mut actor_buf = Vec::new();
        if let Some(id) = actor.get("id").and_then(|v| v.as_str()) {
            encode_string_field(&mut actor_buf, 1, id);
        }
        encode_bytes_field(&mut buf, 1, &actor_buf);
    } else if json.get("all_actors").is_some() {
        encode_bytes_field(&mut buf, 3, &[]);
    }
    buf
}

// === Low-level protobuf encoding helpers ===

fn encode_varint(buf: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7F) as u8;
        value >>= 7;
        if value == 0 {
            buf.push(byte);
            break;
        }
        buf.push(byte | 0x80);
    }
}

fn encode_string_field(buf: &mut Vec<u8>, field_num: u32, value: &str) {
    if value.is_empty() {
        return;
    }
    let tag = (field_num << 3) | 2; // wire type 2 = length-delimited
    encode_varint(buf, tag as u64);
    encode_varint(buf, value.len() as u64);
    buf.extend_from_slice(value.as_bytes());
}

fn encode_bytes_field(buf: &mut Vec<u8>, field_num: u32, value: &[u8]) {
    let tag = (field_num << 3) | 2;
    encode_varint(buf, tag as u64);
    encode_varint(buf, value.len() as u64);
    buf.extend_from_slice(value);
}

fn encode_varint_field(buf: &mut Vec<u8>, field_num: u32, value: u64) {
    if value == 0 {
        return;
    }
    let tag = field_num << 3; // wire type 0 = varint
    encode_varint(buf, tag as u64);
    encode_varint(buf, value);
}

// === Policy ID extraction from tx results ===

/// Extract policy ID from tx_result.data (protobuf TxMsgData).
fn extract_policy_id_from_tx_data(tx_result: &serde_json::Value) -> Option<String> {
    let data_b64 = tx_result["result"]["tx_result"]["data"].as_str()?;
    let data_bytes =
        base64::Engine::decode(&base64::engine::general_purpose::STANDARD, data_b64).ok()?;

    // TxMsgData field 2 (msg_responses) → Any → MsgCreatePolicyResponse
    let msg_responses = decode_field_bytes(&data_bytes, 2)?;
    let response_bytes = decode_field_bytes(&msg_responses, 2)?;
    let record_bytes = decode_field_bytes(&response_bytes, 1)?;
    let policy_bytes = decode_field_bytes(&record_bytes, 1)?;
    let id_bytes = decode_field_bytes(&policy_bytes, 1)?;
    String::from_utf8(id_bytes).ok()
}

/// Extract policy ID from tx result events.
fn extract_policy_id_from_events(tx_result: &serde_json::Value) -> Option<String> {
    let events = tx_result["result"]["tx_result"]["events"].as_array()?;
    for event in events {
        let event_type = event["type"].as_str().unwrap_or("");
        if !event_type.contains("EventPolicyCreated") && !event_type.contains("PolicyCreated") {
            continue;
        }
        let attrs = event["attributes"].as_array()?;
        for attr in attrs {
            let key = decode_event_attr(attr, "key")?;
            if key == "policy_id" {
                let value = decode_event_attr(attr, "value")?;
                return Some(value.trim_matches('"').to_string());
            }
        }
    }
    None
}

/// Decode an event attribute value (handles both plain and base64-encoded).
fn decode_event_attr(attr: &serde_json::Value, field: &str) -> Option<String> {
    let raw = attr[field].as_str()?;
    if let Ok(bytes) = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, raw) {
        if let Ok(s) = String::from_utf8(bytes) {
            return Some(s);
        }
    }
    Some(raw.to_string())
}

// === Low-level protobuf decoding helpers ===

fn decode_field_bytes(data: &[u8], target_field: u32) -> Option<Vec<u8>> {
    let mut pos = 0;
    while pos < data.len() {
        let (tag, new_pos) = decode_varint_at(data, pos)?;
        pos = new_pos;
        let field_num = (tag >> 3) as u32;
        let wire_type = tag & 0x07;

        match wire_type {
            0 => {
                let (_, new_pos) = decode_varint_at(data, pos)?;
                pos = new_pos;
            }
            2 => {
                let (len, new_pos) = decode_varint_at(data, pos)?;
                pos = new_pos;
                let len = len as usize;
                if pos + len > data.len() {
                    return None;
                }
                if field_num == target_field {
                    return Some(data[pos..pos + len].to_vec());
                }
                pos += len;
            }
            1 => pos += 8,
            5 => pos += 4,
            _ => return None,
        }
    }
    None
}

fn decode_varint_at(data: &[u8], start: usize) -> Option<(u64, usize)> {
    let mut result: u64 = 0;
    let mut shift = 0;
    let mut pos = start;
    loop {
        if pos >= data.len() {
            return None;
        }
        let byte = data[pos];
        pos += 1;
        result |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            return Some((result, pos));
        }
        shift += 7;
        if shift >= 64 {
            return None;
        }
    }
}

// === PolicyCmd result extraction ===

struct PolicyCmdResult {
    record_existed: Option<bool>,
    record_found: Option<bool>,
}

/// Extract PolicyCmd result from a bearer policy cmd tx result.
///
/// Protobuf path: TxMsgData.msg_responses[0] (Any) → value →
///   MsgBearerPolicyCmdResponse.result (field 1) → PolicyCmdResult →
///     SetRelationshipResult (field 1) → record_existed (field 1, bool)
///     DeleteRelationshipResult (field 2) → record_found (field 1, bool)
fn extract_policy_cmd_result(tx_result: &serde_json::Value) -> Option<PolicyCmdResult> {
    let data_b64 = tx_result["result"]["tx_result"]["data"].as_str()?;
    let data_bytes =
        base64::Engine::decode(&base64::engine::general_purpose::STANDARD, data_b64).ok()?;

    // TxMsgData field 2 (msg_responses) → Any
    let msg_response = decode_field_bytes(&data_bytes, 2)?;
    // Any field 2 (value) → MsgBearerPolicyCmdResponse
    let response_bytes = decode_field_bytes(&msg_response, 2)?;
    // MsgBearerPolicyCmdResponse field 1 (result) → PolicyCmdResult
    let cmd_result_bytes = decode_field_bytes(&response_bytes, 1)?;

    let mut result = PolicyCmdResult {
        record_existed: None,
        record_found: None,
    };

    // SetRelationshipResult is field 1 of PolicyCmdResult
    if let Some(set_result) = decode_field_bytes(&cmd_result_bytes, 1) {
        // record_existed is field 1 (bool/varint) of SetRelationshipResult
        result.record_existed = Some(decode_bool_field(&set_result, 1));
    }

    // DeleteRelationshipResult is field 2 of PolicyCmdResult
    if let Some(del_result) = decode_field_bytes(&cmd_result_bytes, 2) {
        // record_found is field 1 (bool/varint) of DeleteRelationshipResult
        result.record_found = Some(decode_bool_field(&del_result, 1));
    }

    Some(result)
}

/// Decode a bool field from protobuf. Returns false if field not present (default).
fn decode_bool_field(data: &[u8], target_field: u32) -> bool {
    let mut pos = 0;
    while pos < data.len() {
        let (tag, new_pos) = match decode_varint_at(data, pos) {
            Some(v) => v,
            None => return false,
        };
        pos = new_pos;
        let field_num = (tag >> 3) as u32;
        let wire_type = tag & 0x07;

        match wire_type {
            0 => {
                // varint
                let (value, new_pos) = match decode_varint_at(data, pos) {
                    Some(v) => v,
                    None => return false,
                };
                pos = new_pos;
                if field_num == target_field {
                    return value != 0;
                }
            }
            2 => {
                let (len, new_pos) = match decode_varint_at(data, pos) {
                    Some(v) => v,
                    None => return false,
                };
                pos = new_pos + len as usize;
            }
            1 => pos += 8,
            5 => pos += 4,
            _ => return false,
        }
    }
    false
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum TxSignerError {
    #[error("key error: {0}")]
    Key(String),

    #[error("signing error: {0}")]
    Sign(String),

    #[error("broadcast error: {0}")]
    Broadcast(String),
}
