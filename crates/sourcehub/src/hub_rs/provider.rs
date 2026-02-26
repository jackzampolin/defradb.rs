use std::sync::Mutex;

use alloy_primitives::{Bytes, FixedBytes};
use alloy_sol_types::SolCall;
use async_trait::async_trait;
use k256::ecdsa::SigningKey;

use crate::circuit_breaker::CircuitBreaker;
use crate::policy_cache::PolicyCache;
use crate::provider::{ProviderError, ProviderPolicyInfo, SourceHubProvider, SubjectRef};

use super::abi::{IAcp, ACP_ADDRESS};
use super::bearer;
use super::client::{ClientError, HubRsClient};
use super::signer::EvmSigner;

pub struct HubRsProvider {
    client: HubRsClient,
    signer: EvmSigner,
    signing_key: SigningKey,
    nonce: Mutex<u64>,
    circuit_breaker: CircuitBreaker,
    policy_cache: PolicyCache,
}

impl HubRsProvider {
    pub async fn new(rpc_url: String, private_key: &[u8]) -> Result<Self, ProviderError> {
        let client = HubRsClient::new(rpc_url);
        let chain_id = client
            .chain_id()
            .await
            .map_err(|e| ProviderError::Config(format!("failed to get chain ID: {}", e)))?;
        let signer = EvmSigner::new(private_key, chain_id)
            .map_err(|e| ProviderError::Config(format!("signer: {}", e)))?;
        let signing_key = SigningKey::from_slice(private_key)
            .map_err(|e| ProviderError::Config(format!("k256 key: {}", e)))?;
        let nonce = client
            .get_nonce(signer.address())
            .await
            .map_err(|e| ProviderError::Config(format!("nonce: {}", e)))?;
        Ok(Self {
            client,
            signer,
            signing_key,
            nonce: Mutex::new(nonce),
            circuit_breaker: CircuitBreaker::new(),
            policy_cache: PolicyCache::new(),
        })
    }

    async fn send_tx(&self, data: Bytes) -> Result<serde_json::Value, ProviderError> {
        let nonce = {
            let mut n = self
                .nonce
                .lock()
                .map_err(|_| ProviderError::Transaction("nonce lock poisoned".into()))?;
            let current = *n;
            *n += 1;
            current
        };

        let raw = self
            .signer
            .sign_tx(nonce, ACP_ADDRESS, data)
            .map_err(|e| ProviderError::Transaction(format!("sign: {}", e)))?;
        let tx_hash = self
            .client
            .send_raw_transaction(raw)
            .await
            .map_err(|e| ProviderError::Transaction(format!("send: {}", e)))?;
        self.client
            .wait_for_receipt(tx_hash)
            .await
            .map_err(|e| ProviderError::Transaction(format!("receipt: {}", e)))
    }

    async fn with_circuit_breaker<F, Fut, T>(&self, op: F) -> Result<T, ProviderError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, ClientError>>,
    {
        if !self.circuit_breaker.allow_request() {
            tracing::warn!("hub.rs circuit breaker open: denying access (fail-closed)");
            return Err(ProviderError::Unavailable(
                "hub.rs unreachable; circuit breaker open".to_string(),
            ));
        }

        match op().await {
            Ok(value) => {
                self.circuit_breaker.record_success();
                Ok(value)
            }
            Err(ClientError::Timeout(msg)) => {
                self.circuit_breaker.record_failure();
                Err(ProviderError::Unavailable(format!("timeout: {}", msg)))
            }
            Err(e) => {
                self.circuit_breaker.record_failure();
                Err(ProviderError::Query(e.to_string()))
            }
        }
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

fn subject_to_cmd_json(subject: &SubjectRef) -> serde_json::Value {
    match subject {
        SubjectRef::Actor(did) => serde_json::json!({ "actor": { "id": did } }),
        SubjectRef::AllActors => serde_json::json!({ "all_actors": {} }),
    }
}

fn encode_register_object_cmd(resource: &str, object_id: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "register_object_cmd": {
            "object": { "resource": resource, "id": object_id }
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

fn encode_set_relationship_cmd(
    resource: &str,
    object_id: &str,
    relation: &str,
    subject: &SubjectRef,
) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "set_relationship_cmd": {
            "relationship": {
                "object": { "resource": resource, "id": object_id },
                "relation": relation,
                "subject": subject_to_cmd_json(subject),
            }
        }
    }))
    .unwrap_or_default()
}

fn encode_delete_relationship_cmd(
    resource: &str,
    object_id: &str,
    relation: &str,
    subject: &SubjectRef,
) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "delete_relationship_cmd": {
            "relationship": {
                "object": { "resource": resource, "id": object_id },
                "relation": relation,
                "subject": subject_to_cmd_json(subject),
            }
        }
    }))
    .unwrap_or_default()
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl SourceHubProvider for HubRsProvider {
    fn authorized_account(&self) -> String {
        format!("{:?}", self.signer.address())
    }

    async fn create_bearer_token(&self, did: &str) -> Result<String, ProviderError> {
        // For the node's own DID, use the stored signing key directly
        // (the node identity may not be in the global signing store).
        if Some(did.to_string()) == self.self_did() {
            return bearer::create_bearer_token(&self.signing_key, did, 300).map_err(|e| {
                ProviderError::Config(format!("node bearer token creation failed: {}", e))
            });
        }

        let signing_config = defra_core::signing::get_identity(did).ok_or_else(|| {
            tracing::warn!(
                did,
                "hub.rs bearer token creation failed: no signing config for DID"
            );
            ProviderError::Config(format!("no signing config found for DID: {}", did))
        })?;

        let key = SigningKey::from_slice(&signing_config.private_key_bytes)
            .map_err(|e| ProviderError::Config(format!("invalid signing key: {}", e)))?;

        bearer::create_bearer_token(&key, did, 300)
            .map_err(|e| ProviderError::Config(format!("bearer token creation failed: {}", e)))
    }

    fn self_did(&self) -> Option<String> {
        Some(self.signer.did())
    }

    async fn create_policy(&self, policy_yaml: &str) -> Result<String, ProviderError> {
        let call = IAcp::createPolicyCall {
            policy: Bytes::from(policy_yaml.as_bytes().to_vec()),
            marshalType: 1,
        };
        let calldata = Bytes::from(call.abi_encode());
        let receipt = self.send_tx(calldata).await?;

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

        Err(ProviderError::Transaction(
            "could not extract policy ID from receipt".into(),
        ))
    }

    async fn register_object(
        &self,
        bearer_token: &str,
        policy_id: &str,
        resource: &str,
        object_id: &str,
    ) -> Result<(), ProviderError> {
        let pid = Self::policy_id_to_bytes32(policy_id);
        let cmd = encode_register_object_cmd(resource, object_id);
        let call = IAcp::bearerPolicyCmdCall {
            bearerToken: bearer_token.to_string(),
            policyId: pid,
            cmd: Bytes::from(cmd),
        };
        let calldata = Bytes::from(call.abi_encode());
        self.send_tx(calldata).await?;
        Ok(())
    }

    async fn archive_object(
        &self,
        bearer_token: &str,
        policy_id: &str,
        resource: &str,
        object_id: &str,
    ) -> Result<(), ProviderError> {
        let pid = Self::policy_id_to_bytes32(policy_id);
        let cmd = encode_archive_object_cmd(resource, object_id);
        let call = IAcp::bearerPolicyCmdCall {
            bearerToken: bearer_token.to_string(),
            policyId: pid,
            cmd: Bytes::from(cmd),
        };
        let calldata = Bytes::from(call.abi_encode());
        self.send_tx(calldata).await?;
        Ok(())
    }

    async fn set_relationship(
        &self,
        bearer_token: &str,
        policy_id: &str,
        resource: &str,
        object_id: &str,
        relation: &str,
        subject: &SubjectRef,
    ) -> Result<bool, ProviderError> {
        let pid = Self::policy_id_to_bytes32(policy_id);
        let cmd = encode_set_relationship_cmd(resource, object_id, relation, subject);
        let call = IAcp::bearerPolicyCmdCall {
            bearerToken: bearer_token.to_string(),
            policyId: pid,
            cmd: Bytes::from(cmd),
        };
        let calldata = Bytes::from(call.abi_encode());
        self.send_tx(calldata).await?;
        Ok(true)
    }

    async fn delete_relationship(
        &self,
        bearer_token: &str,
        policy_id: &str,
        resource: &str,
        object_id: &str,
        relation: &str,
        subject: &SubjectRef,
    ) -> Result<bool, ProviderError> {
        let pid = Self::policy_id_to_bytes32(policy_id);
        let cmd = encode_delete_relationship_cmd(resource, object_id, relation, subject);
        let call = IAcp::bearerPolicyCmdCall {
            bearerToken: bearer_token.to_string(),
            policyId: pid,
            cmd: Bytes::from(cmd),
        };
        let calldata = Bytes::from(call.abi_encode());
        self.send_tx(calldata).await?;
        Ok(true)
    }

    async fn query_policy(
        &self,
        policy_id: &str,
    ) -> Result<Option<ProviderPolicyInfo>, ProviderError> {
        if let Some(cached) = self.policy_cache.get(policy_id) {
            return Ok(Some(ProviderPolicyInfo {
                id: cached.id,
                name: cached.name,
            }));
        }

        tracing::debug!(policy_id, "policy cache miss; querying on-chain");
        let pid = Self::policy_id_to_bytes32(policy_id);
        let call = IAcp::getPolicyCall { policyId: pid };
        let calldata = Bytes::from(call.abi_encode());

        let result = self
            .with_circuit_breaker(|| self.client.eth_call(ACP_ADDRESS, calldata.clone()))
            .await?;

        if result.is_empty() {
            return Ok(None);
        }

        let info = ProviderPolicyInfo {
            id: policy_id.to_string(),
            name: policy_id.to_string(),
        };
        self.policy_cache.insert(policy_id, info.name.clone());
        Ok(Some(info))
    }

    async fn query_object_owner(
        &self,
        policy_id: &str,
        resource: &str,
        object_id: &str,
    ) -> Result<(bool, String), ProviderError> {
        let pid = Self::policy_id_to_bytes32(policy_id);
        let call = IAcp::getObjectOwnerCall {
            policyId: pid,
            resource: resource.to_string(),
            objectId: object_id.to_string(),
        };
        let calldata = Bytes::from(call.abi_encode());

        let result = self
            .with_circuit_breaker(|| self.client.eth_call(ACP_ADDRESS, calldata.clone()))
            .await?;

        let decoded = IAcp::getObjectOwnerCall::abi_decode_returns(&result)
            .map_err(|e| ProviderError::Query(format!("ABI decode: {}", e)))?;

        // Extract owner DID from the record bytes if registered
        let owner = if decoded.registered {
            String::from_utf8(decoded.record.to_vec()).unwrap_or_default()
        } else {
            String::new()
        };

        Ok((decoded.registered, owner))
    }

    async fn verify_access(
        &self,
        policy_id: &str,
        resource: &str,
        object_id: &str,
        permission: &str,
        actor_did: &str,
    ) -> Result<bool, ProviderError> {
        let pid = Self::policy_id_to_bytes32(policy_id);
        let call = IAcp::verifyAccessRequestCall {
            policyId: pid,
            resources: vec![resource.to_string()],
            objectIds: vec![object_id.to_string()],
            permissions: vec![permission.to_string()],
            actor: actor_did.to_string(),
        };
        let calldata = Bytes::from(call.abi_encode());

        let result = self
            .with_circuit_breaker(|| self.client.eth_call(ACP_ADDRESS, calldata.clone()))
            .await?;

        let has_access = IAcp::verifyAccessRequestCall::abi_decode_returns(&result)
            .map_err(|e| ProviderError::Query(format!("ABI decode: {}", e)))?;

        Ok(has_access)
    }
}
