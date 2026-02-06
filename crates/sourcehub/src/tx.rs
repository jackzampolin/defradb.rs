use cosmrs::crypto::secp256k1::SigningKey;
use cosmrs::tx::{Body, Fee, SignDoc, SignerInfo};
use cosmrs::{AccountId, Any, Coin};

use crate::client::SourceHubClient;

/// Transaction signer for SourceHub (Cosmos SDK).
///
/// Wraps a secp256k1 signing key and provides methods to build,
/// sign, and broadcast transactions to the SourceHub chain.
pub struct TxSigner {
    signing_key: SigningKey,
    account_id: AccountId,
    chain_id: cosmrs::tendermint::chain::Id,
}

impl TxSigner {
    /// Create a signer from raw secp256k1 private key bytes.
    pub fn from_secp256k1_bytes(
        key_bytes: &[u8],
        chain_id: &str,
    ) -> Result<Self, TxSignerError> {
        let signing_key = SigningKey::from_slice(key_bytes)
            .map_err(|e| TxSignerError::Key(format!("invalid secp256k1 key: {}", e)))?;

        let public_key = signing_key.public_key();
        let account_id = public_key
            .account_id("sourcehub")
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

    /// Get the bech32 account address (e.g., "sourcehub1...").
    pub fn address(&self) -> String {
        self.account_id.to_string()
    }

    /// Build, sign, and broadcast a MsgCreatePolicy transaction.
    /// Returns the policy ID from the response events.
    pub async fn create_policy(
        &self,
        client: &SourceHubClient,
        policy_yaml: &str,
    ) -> Result<String, TxSignerError> {
        // Build the MsgCreatePolicy as a protobuf Any
        let msg = build_msg_create_policy(&self.address(), policy_yaml);
        let tx_hash = self.sign_and_broadcast(client, vec![msg]).await?;
        client
            .await_tx(&tx_hash, 30_000)
            .await
            .map_err(|e| TxSignerError::Broadcast(e.to_string()))?;

        // Query back the policy to get the ID
        // The policy ID is returned in tx events; for now, we wait and check
        // In a proper implementation we'd parse the tx result events.
        // For compatibility, we return the tx hash and let the caller handle it.
        Ok(tx_hash)
    }

    /// Build, sign, and broadcast a MsgBearerPolicyCmd transaction.
    pub async fn bearer_policy_cmd(
        &self,
        client: &SourceHubClient,
        bearer_token: &str,
        policy_id: &str,
        cmd_json: serde_json::Value,
    ) -> Result<serde_json::Value, TxSignerError> {
        let msg = build_msg_bearer_policy_cmd(
            &self.address(),
            bearer_token,
            policy_id,
            cmd_json,
        );
        let tx_hash = self.sign_and_broadcast(client, vec![msg]).await?;
        client
            .await_tx(&tx_hash, 30_000)
            .await
            .map_err(|e| TxSignerError::Broadcast(e.to_string()))?;
        Ok(serde_json::json!({"tx_hash": tx_hash}))
    }

    /// Sign and broadcast a transaction containing the given messages.
    async fn sign_and_broadcast(
        &self,
        client: &SourceHubClient,
        messages: Vec<Any>,
    ) -> Result<String, TxSignerError> {
        // Query account info for sequence number
        let (account_number, sequence) = client
            .query_account(&self.address())
            .await
            .map_err(|e| TxSignerError::Broadcast(format!("account query: {}", e)))?;

        // Build transaction body
        let body = Body::new(messages, "", 0u32);

        // Fee: 0uopen with 400000 gas (matching Go SDK)
        let fee = Fee::from_amount_and_gas(
            Coin {
                denom: "uopen".parse().unwrap(),
                amount: 0u128,
            },
            400_000u64,
        );

        let auth_info = SignerInfo::single_direct(
            Some(self.signing_key.public_key()),
            sequence,
        )
        .auth_info(fee);

        let sign_doc = SignDoc::new(
            &body,
            &auth_info,
            &self.chain_id,
            account_number,
        )
        .map_err(|e| TxSignerError::Sign(format!("SignDoc creation: {}", e)))?;

        let tx_raw = sign_doc
            .sign(&self.signing_key)
            .map_err(|e| TxSignerError::Sign(format!("signing: {}", e)))?;

        let tx_bytes = tx_raw
            .to_bytes()
            .map_err(|e| TxSignerError::Sign(format!("tx serialization: {}", e)))?;

        client
            .broadcast_tx_sync(&tx_bytes)
            .await
            .map_err(|e| TxSignerError::Broadcast(e.to_string()))
    }
}

/// Build a protobuf Any for MsgCreatePolicy.
///
/// The type URL and encoding must match what SourceHub's Cosmos SDK module expects.
fn build_msg_create_policy(creator: &str, policy: &str) -> Any {
    // MsgCreatePolicy protobuf encoding:
    // field 1 (string): creator
    // field 2 (string): policy (YAML text)
    // field 3 (enum): marshal_type = 1 (YAML)
    let mut buf = Vec::new();
    // field 1: creator (tag = 0x0a, length-delimited)
    encode_string_field(&mut buf, 1, creator);
    // field 2: policy
    encode_string_field(&mut buf, 2, policy);
    // field 3: marshal_type = 1 (YAML)
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
    // MsgBearerPolicyCmd protobuf encoding:
    // field 1 (string): creator
    // field 2 (string): bearer_token
    // field 3 (string): policy_id
    // field 4 (message): cmd (PolicyCmd)
    let cmd_bytes = encode_policy_cmd(&cmd_json);

    let mut buf = Vec::new();
    encode_string_field(&mut buf, 1, creator);
    encode_string_field(&mut buf, 2, bearer_token);
    encode_string_field(&mut buf, 3, policy_id);
    // field 4: cmd (length-delimited message)
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
        // PolicyCmd field 1: set_relationship_cmd
        let mut inner = Vec::new();
        encode_bytes_field(&mut inner, 1, &rel_bytes); // relationship field 1
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

    // field 1: object (message)
    if let Some(obj) = rel.get("object") {
        let obj_bytes = encode_object_from_json(obj);
        encode_bytes_field(&mut buf, 1, &obj_bytes);
    }
    // field 2: relation (string)
    if let Some(r) = rel.get("relation").and_then(|v| v.as_str()) {
        encode_string_field(&mut buf, 2, r);
    }
    // field 3: subject (message)
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
    // Subject is a oneof: actor (field 1), actor_set (field 2), all_actors (field 3), object (field 4)
    if let Some(actor) = json.get("actor") {
        let mut actor_buf = Vec::new();
        if let Some(id) = actor.get("id").and_then(|v| v.as_str()) {
            encode_string_field(&mut actor_buf, 1, id);
        }
        encode_bytes_field(&mut buf, 1, &actor_buf);
    } else if json.get("all_actors").is_some() {
        // AllActors is an empty message
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

#[derive(Debug, thiserror::Error)]
pub enum TxSignerError {
    #[error("key error: {0}")]
    Key(String),

    #[error("signing error: {0}")]
    Sign(String),

    #[error("broadcast error: {0}")]
    Broadcast(String),
}
