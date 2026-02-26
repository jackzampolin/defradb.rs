use std::sync::Mutex;

use alloy_primitives::{Bytes, FixedBytes};
use alloy_sol_types::SolCall;
use async_trait::async_trait;
use identity::Did;
use k256::ecdsa::SigningKey;

use acp::{DocumentACP, DocumentPermission, Identity, Result};

use super::abi::{IAcp, ACP_ADDRESS};
use super::bearer;
use super::client::{ClientError, HubRsClient};
use super::signer::EvmSigner;

pub struct HubRsDocumentACP {
    client: HubRsClient,
    signer: EvmSigner,
    signing_key: SigningKey,
    nonce: Mutex<u64>,
}

impl HubRsDocumentACP {
    pub async fn new(rpc_url: String, private_key: &[u8]) -> std::result::Result<Self, HubRsError> {
        let client = HubRsClient::new(rpc_url);
        let chain_id = client
            .chain_id()
            .await
            .map_err(|e| HubRsError::Init(format!("failed to get chain ID: {}", e)))?;
        let signer = EvmSigner::new(private_key, chain_id)
            .map_err(|e| HubRsError::Init(format!("signer: {}", e)))?;
        let signing_key = SigningKey::from_slice(private_key)
            .map_err(|e| HubRsError::Init(format!("k256 key: {}", e)))?;
        let nonce = client
            .get_nonce(signer.address())
            .await
            .map_err(|e| HubRsError::Init(format!("nonce: {}", e)))?;
        Ok(Self {
            client,
            signer,
            signing_key,
            nonce: Mutex::new(nonce),
        })
    }

    pub async fn add_policy(
        &self,
        _creator_did: &str,
        policy_yaml: &str,
    ) -> std::result::Result<String, HubRsError> {
        // createPolicy(bytes policy, uint8 marshalType=1 for YAML)
        let call = IAcp::createPolicyCall {
            policy: Bytes::from(policy_yaml.as_bytes().to_vec()),
            marshalType: 1,
        };
        let calldata = Bytes::from(call.abi_encode());
        let receipt = self.send_tx(calldata).await?;

        // Extract policy ID from logs (first log, first topic after event sig)
        let logs = receipt["logs"].as_array();
        if let Some(logs) = logs {
            for log in logs {
                let topics = log["topics"].as_array();
                if let Some(topics) = topics {
                    if topics.len() >= 2 {
                        let topic_hex = topics[1].as_str().unwrap_or("");
                        let topic = topic_hex.strip_prefix("0x").unwrap_or(topic_hex);
                        return Ok(format!("0x{}", topic));
                    }
                }
            }
        }

        Err(HubRsError::Transaction(
            "could not extract policy ID from receipt".into(),
        ))
    }

    async fn send_tx(&self, data: Bytes) -> std::result::Result<serde_json::Value, HubRsError> {
        let nonce = {
            let mut n = self
                .nonce
                .lock()
                .map_err(|_| HubRsError::Transaction("nonce lock poisoned".into()))?;
            let current = *n;
            *n += 1;
            current
        };

        let raw = self
            .signer
            .sign_tx(nonce, ACP_ADDRESS, data)
            .map_err(|e| HubRsError::Transaction(format!("sign: {}", e)))?;
        let tx_hash = self
            .client
            .send_raw_transaction(raw)
            .await
            .map_err(|e| HubRsError::Transaction(format!("send: {}", e)))?;
        self.client
            .wait_for_receipt(tx_hash)
            .await
            .map_err(|e| HubRsError::Transaction(format!("receipt: {}", e)))
    }

    fn create_bearer_token(&self, did: &str) -> std::result::Result<String, acp::Error> {
        let signing_config = defra_core::signing::get_identity(did).ok_or_else(|| {
            tracing::warn!(
                did,
                "hub.rs bearer token creation failed: no signing config for DID"
            );
            acp::Error::PermissionDenied(format!("no signing config found for DID: {}", did))
        })?;

        let key = SigningKey::from_slice(&signing_config.private_key_bytes)
            .map_err(|e| acp::Error::PermissionDenied(format!("invalid signing key: {}", e)))?;

        bearer::create_bearer_token(&key, did, 300).map_err(|e| {
            acp::Error::PermissionDenied(format!("bearer token creation failed: {}", e))
        })
    }

    fn create_node_bearer_token(&self) -> std::result::Result<String, acp::Error> {
        let did = self.signer.did();
        bearer::create_bearer_token(&self.signing_key, &did, 300).map_err(|e| {
            acp::Error::PermissionDenied(format!("node bearer token creation failed: {}", e))
        })
    }

    fn policy_id_to_bytes32(policy_id: &str) -> FixedBytes<32> {
        let hex_str = policy_id.strip_prefix("0x").unwrap_or(policy_id);
        let bytes = hex::decode(hex_str).unwrap_or_default();
        let mut arr = [0u8; 32];
        let start = 32usize.saturating_sub(bytes.len());
        arr[start..].copy_from_slice(&bytes[..bytes.len().min(32)]);
        FixedBytes::from(arr)
    }
}

fn hub_err(e: ClientError) -> acp::Error {
    acp::Error::Storage(format!("hub.rs: {}", e))
}

fn hub_tx_err(e: HubRsError) -> acp::Error {
    acp::Error::Storage(format!("hub.rs: {}", e))
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl DocumentACP for HubRsDocumentACP {
    async fn register_doc_object(
        &self,
        identity: &Did,
        policy_id: &str,
        resource_name: &str,
        doc_id: &str,
    ) -> Result<()> {
        let bearer_token = self.create_bearer_token(identity.as_str())?;
        let pid = Self::policy_id_to_bytes32(policy_id);
        let cmd = encode_register_object_cmd(resource_name, doc_id);
        let call = IAcp::bearerPolicyCmdCall {
            bearerToken: bearer_token,
            policyId: pid,
            cmd: Bytes::from(cmd),
        };
        let calldata = Bytes::from(call.abi_encode());
        self.send_tx(calldata).await.map_err(hub_tx_err)?;
        Ok(())
    }

    async fn is_doc_registered(
        &self,
        policy_id: &str,
        resource_name: &str,
        doc_id: &str,
    ) -> Result<bool> {
        let pid = Self::policy_id_to_bytes32(policy_id);
        let call = IAcp::getObjectOwnerCall {
            policyId: pid,
            resource: resource_name.to_string(),
            objectId: doc_id.to_string(),
        };
        let calldata = Bytes::from(call.abi_encode());
        let result = self
            .client
            .eth_call(ACP_ADDRESS, calldata)
            .await
            .map_err(hub_err)?;
        let decoded = IAcp::getObjectOwnerCall::abi_decode_returns(&result)
            .map_err(|e| acp::Error::Storage(format!("ABI decode: {}", e)))?;
        Ok(decoded.registered)
    }

    async fn check_doc_access(
        &self,
        identity: &Identity,
        permission: DocumentPermission,
        policy_id: &str,
        resource_name: &str,
        doc_id: &str,
    ) -> Result<bool> {
        let pid = Self::policy_id_to_bytes32(policy_id);

        // Check registration
        let owner_call = IAcp::getObjectOwnerCall {
            policyId: pid,
            resource: resource_name.to_string(),
            objectId: doc_id.to_string(),
        };
        let owner_data = self
            .client
            .eth_call(ACP_ADDRESS, Bytes::from(owner_call.abi_encode()))
            .await
            .map_err(hub_err)?;
        let owner_result = IAcp::getObjectOwnerCall::abi_decode_returns(&owner_data)
            .map_err(|e| acp::Error::Storage(format!("ABI decode: {}", e)))?;

        if !owner_result.registered {
            return Ok(true);
        }

        let actor_did = match identity.did() {
            Some(did) => did.as_str().to_string(),
            None => "did:key:anonymous".to_string(),
        };

        let permissions_to_check = if permission == DocumentPermission::Read {
            vec![
                DocumentPermission::Read,
                DocumentPermission::Update,
                DocumentPermission::Delete,
            ]
        } else {
            vec![permission]
        };

        for perm in permissions_to_check {
            let call = IAcp::verifyAccessRequestCall {
                policyId: pid,
                resources: vec![resource_name.to_string()],
                objectIds: vec![doc_id.to_string()],
                permissions: vec![perm.as_str().to_string()],
                actor: actor_did.clone(),
            };
            let calldata = Bytes::from(call.abi_encode());
            let result = self
                .client
                .eth_call(ACP_ADDRESS, calldata)
                .await
                .map_err(hub_err)?;
            let has_access = IAcp::verifyAccessRequestCall::abi_decode_returns(&result)
                .map_err(|e| acp::Error::Storage(format!("ABI decode: {}", e)))?;
            if has_access {
                return Ok(true);
            }
        }

        Ok(false)
    }

    async fn add_actor_relationship(
        &self,
        requestor: &Did,
        target: &Did,
        policy_id: &str,
        resource_name: &str,
        doc_id: &str,
        relation: &str,
        _managing_relations: &[String],
    ) -> Result<bool> {
        let bearer_token = self.create_bearer_token(requestor.as_str())?;
        let pid = Self::policy_id_to_bytes32(policy_id);
        let cmd = encode_set_relationship_cmd(resource_name, doc_id, relation, target.as_str());
        let call = IAcp::bearerPolicyCmdCall {
            bearerToken: bearer_token,
            policyId: pid,
            cmd: Bytes::from(cmd),
        };
        let calldata = Bytes::from(call.abi_encode());
        self.send_tx(calldata).await.map_err(hub_tx_err)?;
        Ok(true)
    }

    async fn delete_actor_relationship(
        &self,
        requestor: &Did,
        target: &Did,
        policy_id: &str,
        resource_name: &str,
        doc_id: &str,
        relation: &str,
        _managing_relations: &[String],
    ) -> Result<bool> {
        let bearer_token = self.create_bearer_token(requestor.as_str())?;
        let pid = Self::policy_id_to_bytes32(policy_id);
        let cmd = encode_delete_relationship_cmd(resource_name, doc_id, relation, target.as_str());
        let call = IAcp::bearerPolicyCmdCall {
            bearerToken: bearer_token,
            policyId: pid,
            cmd: Bytes::from(cmd),
        };
        let calldata = Bytes::from(call.abi_encode());
        self.send_tx(calldata).await.map_err(hub_tx_err)?;
        Ok(true)
    }

    async fn unregister_doc_object(
        &self,
        policy_id: &str,
        resource_name: &str,
        doc_id: &str,
    ) -> Result<()> {
        let pid = Self::policy_id_to_bytes32(policy_id);

        // Query owner
        let owner_call = IAcp::getObjectOwnerCall {
            policyId: pid,
            resource: resource_name.to_string(),
            objectId: doc_id.to_string(),
        };
        let owner_data = self
            .client
            .eth_call(ACP_ADDRESS, Bytes::from(owner_call.abi_encode()))
            .await
            .map_err(hub_err)?;
        let owner_result = IAcp::getObjectOwnerCall::abi_decode_returns(&owner_data)
            .map_err(|e| acp::Error::Storage(format!("ABI decode: {}", e)))?;

        if !owner_result.registered {
            return Ok(());
        }

        // Use node's bearer token for archive (node is the tx sender)
        let bearer_token = self.create_node_bearer_token()?;
        let cmd = encode_archive_object_cmd(resource_name, doc_id);
        let call = IAcp::bearerPolicyCmdCall {
            bearerToken: bearer_token,
            policyId: pid,
            cmd: Bytes::from(cmd),
        };
        let calldata = Bytes::from(call.abi_encode());
        self.send_tx(calldata).await.map_err(hub_tx_err)?;
        Ok(())
    }
}

// Bearer policy command encoding.
// Commands are JSON-encoded bytes passed to bearerPolicyCmd.

fn encode_register_object_cmd(resource: &str, object_id: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "register_object_cmd": {
            "object": { "resource": resource, "id": object_id }
        }
    }))
    .unwrap_or_default()
}

fn encode_set_relationship_cmd(
    resource: &str,
    object_id: &str,
    relation: &str,
    target_did: &str,
) -> Vec<u8> {
    let subject = if target_did == "*" {
        serde_json::json!({ "all_actors": {} })
    } else {
        serde_json::json!({ "actor": { "id": target_did } })
    };
    serde_json::to_vec(&serde_json::json!({
        "set_relationship_cmd": {
            "relationship": {
                "object": { "resource": resource, "id": object_id },
                "relation": relation,
                "subject": subject,
            }
        }
    }))
    .unwrap_or_default()
}

fn encode_delete_relationship_cmd(
    resource: &str,
    object_id: &str,
    relation: &str,
    target_did: &str,
) -> Vec<u8> {
    let subject = if target_did == "*" {
        serde_json::json!({ "all_actors": {} })
    } else {
        serde_json::json!({ "actor": { "id": target_did } })
    };
    serde_json::to_vec(&serde_json::json!({
        "delete_relationship_cmd": {
            "relationship": {
                "object": { "resource": resource, "id": object_id },
                "relation": relation,
                "subject": subject,
            }
        }
    }))
    .unwrap_or_default()
}

fn encode_archive_object_cmd(resource: &str, object_id: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "archive_object_cmd": {
            "object": { "resource": resource, "id": object_id }
        }
    }))
    .unwrap_or_default()
}

#[derive(Debug, thiserror::Error)]
pub enum HubRsError {
    #[error("initialization error: {0}")]
    Init(String),

    #[error("transaction error: {0}")]
    Transaction(String),
}
