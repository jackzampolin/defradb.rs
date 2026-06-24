use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::provider::{
    AcpLightClientStatus, ProviderError, ProviderPolicyInfo, SourceHubProvider, SubjectRef,
};
use crate::tuning::AcpTuning;
use acp_light_client::AcpLightClient;
use alloy_primitives::{Bytes, FixedBytes};
use alloy_sol_types::SolCall;
use async_trait::async_trait;
use events::{AcpCacheInvalidatedData, AcpHeightAdvancedData, Bus, Message};
use k256::ecdsa::SigningKey;
use serde::Deserialize;
use sha2::{Digest, Sha256};

use super::abi::{IAcp, ACP_ADDRESS};
use super::bearer;
use super::client::{ClientError, HubRsClient};
use super::provider_commands::{
    encode_archive_object_cmd, encode_delete_relationship_cmd, encode_register_object_cmd,
    encode_set_relationship_cmd, resolve_registered_or_passthrough_bearer_token,
};
use super::signer::EvmSigner;

const MAX_NONCE_RETRIES: usize = 5;
const MAX_NONCE_RESERVE_ATTEMPTS: usize = 16;

pub struct HubRsProvider {
    light_client: Arc<AcpLightClient>,
    client: HubRsClient,
    signer: EvmSigner,
    signing_key: SigningKey,
    nonce: AtomicU64,
    light_client_observability: Arc<AtomicU64>,
    light_client_observer_handle: tokio::task::JoinHandle<()>,
}

fn derive_ws_url(rpc_url: &str) -> String {
    rpc_url
        .replacen("http://", "ws://", 1)
        .replacen("https://", "wss://", 1)
}

impl HubRsProvider {
    pub async fn new(
        rpc_url: String,
        private_key: &[u8],
        tuning: &AcpTuning,
        event_bus: Option<Arc<dyn Bus>>,
    ) -> Result<Self, ProviderError> {
        let ws_url = derive_ws_url(&rpc_url);
        let light_client = Arc::new(
            AcpLightClient::new(&rpc_url, &ws_url, 10)
                .await
                .map_err(|e| ProviderError::Config(format!("light client: {}", e)))?,
        );
        let light_client_observability = Arc::new(AtomicU64::new(0));

        let client = HubRsClient::new(rpc_url, tuning.request_timeout, tuning.receipt_timeout)
            .map_err(|e| ProviderError::Config(format!("HTTP client: {}", e)))?;
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

        let light_client_observer_handle = tokio::spawn(run_light_client_observer(
            light_client.clone(),
            light_client_observability.clone(),
            event_bus,
        ));

        Ok(Self {
            light_client,
            client,
            signer,
            signing_key,
            nonce: AtomicU64::new(nonce),
            light_client_observability,
            light_client_observer_handle,
        })
    }

    async fn send_tx(&self, data: Bytes) -> Result<serde_json::Value, ProviderError> {
        let mut nonce_retries = 0;
        let tx_hash = loop {
            let nonce = if nonce_retries == 0 {
                self.nonce.fetch_add(1, Ordering::Relaxed)
            } else {
                let chain_nonce = self
                    .client
                    .get_nonce(self.signer.address())
                    .await
                    .map_err(|e| ProviderError::Config(format!("nonce: {}", e)))?;
                self.reserve_nonce_at_or_after(chain_nonce)
            };

            let raw = self
                .signer
                .sign_tx(nonce, ACP_ADDRESS, data.clone())
                .map_err(|e| ProviderError::Transaction(format!("sign: {}", e)))?;

            match self.client.send_raw_transaction(raw).await {
                Ok(tx_hash) => break tx_hash,
                Err(e) if nonce_retries < MAX_NONCE_RETRIES && is_nonce_error(&e) => {
                    nonce_retries += 1;
                    tracing::debug!(error = %e, "hub.rs transaction nonce stale; refreshing");
                    tokio::time::sleep(Duration::from_millis(100 * nonce_retries as u64)).await;
                }
                Err(e) => return Err(ProviderError::Transaction(format!("send: {}", e))),
            }
        };
        self.client
            .wait_for_receipt(tx_hash)
            .await
            .map_err(|e| ProviderError::Transaction(format!("receipt: {}", e)))
    }

    fn reserve_nonce_at_or_after(&self, chain_nonce: u64) -> u64 {
        for _ in 0..MAX_NONCE_RESERVE_ATTEMPTS {
            let current = self.nonce.load(Ordering::Relaxed);
            let nonce = current.max(chain_nonce);
            if self
                .nonce
                .compare_exchange(current, nonce + 1, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                return nonce;
            }
        }

        let _ = self.nonce.fetch_max(chain_nonce, Ordering::Relaxed);
        self.nonce.fetch_add(1, Ordering::Relaxed).max(chain_nonce)
    }

    async fn guarded_eth_call<F, Fut, T>(&self, op: F) -> Result<T, ProviderError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, ClientError>>,
    {
        match op().await {
            Ok(value) => Ok(value),
            Err(ClientError::Timeout(msg)) => {
                Err(ProviderError::Unavailable(format!("timeout: {}", msg)))
            }
            Err(e) => Err(ProviderError::Query(e.to_string())),
        }
    }

    async fn query_policy_ids(&self) -> Result<Vec<String>, ProviderError> {
        let call = IAcp::getPolicyIdsCall {};
        let calldata = Bytes::from(call.abi_encode());
        let result = self
            .guarded_eth_call(|| self.client.eth_call(ACP_ADDRESS, calldata.clone()))
            .await?;
        IAcp::getPolicyIdsCall::abi_decode_returns(&result)
            .map_err(|e| ProviderError::Query(format!("ABI decode: {}", e)))
    }

    async fn query_policy_raw(&self, policy_id: &str) -> Result<Option<String>, ProviderError> {
        let call = IAcp::getPolicyCall {
            policyId: Self::policy_id_to_bytes32(policy_id),
        };
        let calldata = Bytes::from(call.abi_encode());
        let result = self
            .guarded_eth_call(|| self.client.eth_call(ACP_ADDRESS, calldata.clone()))
            .await?;
        let bytes = IAcp::getPolicyCall::abi_decode_returns(&result)
            .map_err(|e| ProviderError::Query(format!("ABI decode: {}", e)))?;
        if bytes.is_empty() {
            return Ok(None);
        }
        let record: HubRsPolicyRecord = serde_json::from_slice(&bytes)
            .map_err(|e| ProviderError::Query(format!("policy JSON: {}", e)))?;
        Ok(record.raw_policy)
    }

    fn policy_id_to_bytes32(policy_id: &str) -> FixedBytes<32> {
        let hex_str = policy_id.strip_prefix("0x").unwrap_or(policy_id);
        let bytes = hex::decode(hex_str).unwrap_or_default();
        let mut arr = [0u8; 32];
        let start = 32usize.saturating_sub(bytes.len());
        arr[start..].copy_from_slice(&bytes[..bytes.len().min(32)]);
        FixedBytes::from(arr)
    }

    fn compute_access_decision_id(
        policy_id: &str,
        creator_did: &str,
        actor_did: &str,
        resource: &str,
        object_id: &str,
        permission: &str,
    ) -> String {
        let mut hasher = Sha256::new();
        hasher.update(policy_id.as_bytes());
        hasher.update(creator_did.as_bytes());
        hasher.update(actor_did.as_bytes());
        hasher.update(resource.as_bytes());
        hasher.update(object_id.as_bytes());
        hasher.update(permission.as_bytes());
        hex::encode(hasher.finalize())
    }

    fn relations_for_permission(permission: &str) -> Vec<&str> {
        match permission {
            "read" => vec!["owner", "writer", "reader"],
            "update" => vec!["owner", "writer"],
            "delete" => vec!["owner"],
            _ => vec![permission],
        }
    }

    async fn verify_access_request_live(
        &self,
        policy_id: &str,
        resource: &str,
        object_id: &str,
        permission: &str,
        actor_did: &str,
    ) -> Result<bool, ProviderError> {
        let call = IAcp::verifyAccessRequestCall {
            policyId: Self::policy_id_to_bytes32(policy_id),
            resources: vec![resource.to_string()],
            objectIds: vec![object_id.to_string()],
            permissions: vec![permission.to_string()],
            actor: actor_did.to_string(),
        };
        let calldata = Bytes::from(call.abi_encode());
        let result = self
            .guarded_eth_call(|| self.client.eth_call(ACP_ADDRESS, calldata.clone()))
            .await?;

        let allowed = IAcp::verifyAccessRequestCall::abi_decode_returns(&result)
            .map_err(|e| ProviderError::Query(format!("ABI decode: {}", e)))?;
        tracing::info!(
            creator_did = %self.signer.did(),
            policy_id = %policy_id,
            resource = %resource,
            object_id = %object_id,
            permission = %permission,
            actor_did = %actor_did,
            allowed,
            "hub.rs verifyAccessRequest result"
        );
        Ok(allowed)
    }
}

fn is_nonce_error(error: &ClientError) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    message.contains("nonce") || message.contains("duplicate transaction")
}

#[derive(Deserialize)]
struct HubRsPolicyRecord {
    raw_policy: Option<String>,
}

impl Drop for HubRsProvider {
    fn drop(&mut self) {
        self.light_client_observer_handle.abort();
    }
}

fn format_root(root: alloy_primitives::B256) -> String {
    format!("0x{}", hex::encode(root))
}

async fn run_light_client_observer(
    light_client: Arc<AcpLightClient>,
    last_invalidation_height: Arc<AtomicU64>,
    event_bus: Option<Arc<dyn Bus>>,
) {
    let mut next_height = 1u64;
    let mut previous_root = None;

    loop {
        let sync = match light_client
            .wait_for_height(next_height, Duration::from_secs(24 * 60 * 60))
            .await
        {
            Ok(sync) => sync,
            Err(error) => {
                tracing::debug!(error = %error, "ACP light client observer wait_for_height failed");
                next_height = light_client
                    .header_chain()
                    .latest_height()
                    .saturating_add(1);
                continue;
            }
        };

        let module_state_root = format_root(sync.module_state_root);
        if let Some(ref bus) = event_bus {
            bus.publish(Message::acp_height_advanced(AcpHeightAdvancedData {
                height: sync.height,
                module_state_root: module_state_root.clone(),
            }));
        }

        if let Some(previous_root_value) = previous_root {
            if previous_root_value != sync.module_state_root {
                let entries_invalidated = light_client
                    .cache()
                    .invalidate_stale(sync.module_state_root);
                last_invalidation_height.store(sync.height, Ordering::Relaxed);

                if let Some(ref bus) = event_bus {
                    bus.publish(Message::acp_cache_invalidated(AcpCacheInvalidatedData {
                        height: sync.height,
                        module_state_root: module_state_root.clone(),
                        previous_root: format_root(previous_root_value),
                        entries_invalidated,
                    }));
                }
            }
        }

        previous_root = Some(sync.module_state_root);
        next_height = sync.height.saturating_add(1);
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl SourceHubProvider for HubRsProvider {
    fn authorized_account(&self) -> String {
        format!("{:?}", self.signer.address())
    }

    async fn create_bearer_token(&self, did: &str) -> Result<String, ProviderError> {
        if Some(did.to_string()) == self.self_did() {
            return bearer::create_bearer_token(&self.signing_key, did, 300).map_err(|e| {
                ProviderError::Config(format!("node bearer token creation failed: {}", e))
            });
        }

        if let Some(token) = resolve_registered_or_passthrough_bearer_token(did)? {
            return Ok(token);
        }

        tracing::warn!(
            did,
            "hub.rs bearer token creation failed: no signing config for DID and no request token"
        );
        Err(ProviderError::Config(format!(
            "no signing config found for DID: {}",
            did
        )))
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
        self.send_tx(calldata).await?;

        let ids = self.query_policy_ids().await?;
        for id in ids.iter().rev() {
            if let Some(raw_policy) = self.query_policy_raw(id).await? {
                if raw_policy.trim() == policy_yaml.trim() {
                    return Ok(id.clone());
                }
            }
        }

        Err(ProviderError::Query("created policy ID not found".into()))
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
        let send_tx_start = Instant::now();
        self.send_tx(calldata).await?;
        tracing::info!(
            doc_id = %object_id,
            elapsed = ?send_tx_start.elapsed(),
            "hub.rs register_object send_tx completed"
        );

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

    async fn set_relationship_subject(
        &self,
        _bearer_token: &str,
        policy_id: &str,
        resource: &str,
        object_id: &str,
        relation: &str,
        kind: u8,
        subject_resource: &str,
        subject_object_id: &str,
        subject_relation: &str,
    ) -> Result<bool, ProviderError> {
        let pid = Self::policy_id_to_bytes32(policy_id);
        let call = IAcp::setRelationshipSubjectCall {
            policyId: pid,
            resource: resource.to_string(),
            objectId: object_id.to_string(),
            relation: relation.to_string(),
            subjectKind: kind,
            subjectResource: subject_resource.to_string(),
            subjectObjectId: subject_object_id.to_string(),
            subjectRelation: subject_relation.to_string(),
        };
        let calldata = Bytes::from(call.abi_encode());
        self.send_tx(calldata).await?;
        Ok(true)
    }

    async fn delete_relationship_subject(
        &self,
        _bearer_token: &str,
        policy_id: &str,
        resource: &str,
        object_id: &str,
        relation: &str,
        kind: u8,
        subject_resource: &str,
        subject_object_id: &str,
        subject_relation: &str,
    ) -> Result<bool, ProviderError> {
        let pid = Self::policy_id_to_bytes32(policy_id);
        let call = IAcp::deleteRelationshipSubjectCall {
            policyId: pid,
            resource: resource.to_string(),
            objectId: object_id.to_string(),
            relation: relation.to_string(),
            subjectKind: kind,
            subjectResource: subject_resource.to_string(),
            subjectObjectId: subject_object_id.to_string(),
            subjectRelation: subject_relation.to_string(),
        };
        let calldata = Bytes::from(call.abi_encode());
        self.send_tx(calldata).await?;
        Ok(true)
    }

    async fn query_policy(
        &self,
        policy_id: &str,
    ) -> Result<Option<ProviderPolicyInfo>, ProviderError> {
        if let Some(raw_policy) = self.query_policy_raw(policy_id).await? {
            Ok(Some(ProviderPolicyInfo {
                id: policy_id.to_string(),
                name: policy_id.to_string(),
                raw_policy: Some(raw_policy),
            }))
        } else {
            Ok(None)
        }
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
            .guarded_eth_call(|| self.client.eth_call(ACP_ADDRESS, calldata.clone()))
            .await?;

        let decoded = IAcp::getObjectOwnerCall::abi_decode_returns(&result)
            .map_err(|e| ProviderError::Query(format!("ABI decode: {}", e)))?;

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
        let actor_subject =
            zanzibar::Subject::Entity(zanzibar::Did::new_unchecked(actor_did.to_string()));
        let actor_hash = actor_subject.storage_hash();
        let wildcard_hash = zanzibar::Subject::Wildcard.storage_hash();

        for relation in Self::relations_for_permission(permission) {
            let storage_key = format!(
                "/rel/{}/{}/{}/{}",
                resource, object_id, relation, actor_hash
            );
            let result = self
                .light_client
                .check_access(policy_id, &storage_key)
                .await
                .map_err(|e| ProviderError::Query(format!("light client: {}", e)))?;
            if result.allowed {
                return Ok(true);
            }

            let wildcard_key = format!(
                "/rel/{}/{}/{}/{}",
                resource, object_id, relation, wildcard_hash
            );
            let result = self
                .light_client
                .check_access(policy_id, &wildcard_key)
                .await
                .map_err(|e| ProviderError::Query(format!("light client: {}", e)))?;
            if result.allowed {
                return Ok(true);
            }
        }

        Ok(false)
    }

    async fn create_access_decision(
        &self,
        policy_id: &str,
        resource: &str,
        object_id: &str,
        permission: &str,
        actor_did: &str,
    ) -> Result<Option<String>, ProviderError> {
        tracing::info!(
            creator_did = %self.signer.did(),
            policy_id = %policy_id,
            resource = %resource,
            object_id = %object_id,
            permission = %permission,
            actor_did = %actor_did,
            "hub.rs create_access_decision start"
        );
        let currently_allowed = self
            .verify_access_request_live(policy_id, resource, object_id, permission, actor_did)
            .await?;
        if !currently_allowed {
            tracing::warn!(
                creator_did = %self.signer.did(),
                policy_id = %policy_id,
                resource = %resource,
                object_id = %object_id,
                permission = %permission,
                actor_did = %actor_did,
                "hub.rs create_access_decision denied by verifyAccessRequest"
            );
            return Err(ProviderError::Query(format!(
                "actor {} denied {} on {}:{}",
                actor_did, permission, resource, object_id
            )));
        }

        let call = IAcp::checkAccessCall {
            policyId: Self::policy_id_to_bytes32(policy_id),
            resources: vec![resource.to_string()],
            objectIds: vec![object_id.to_string()],
            permissions: vec![permission.to_string()],
            actor: actor_did.to_string(),
        };
        let calldata = Bytes::from(call.abi_encode());
        let receipt = self.send_tx(calldata).await?;

        let decision_id = Self::compute_access_decision_id(
            policy_id,
            &self.signer.did(),
            actor_did,
            resource,
            object_id,
            permission,
        );
        tracing::info!(
            creator_did = %self.signer.did(),
            policy_id = %policy_id,
            resource = %resource,
            object_id = %object_id,
            permission = %permission,
            actor_did = %actor_did,
            decision_id = %decision_id,
            receipt_block_number = ?receipt["blockNumber"].as_str(),
            "hub.rs checkAccess transaction confirmed"
        );

        if let Some(block_number_hex) = receipt["blockNumber"].as_str() {
            let block_number = u64::from_str_radix(
                block_number_hex
                    .strip_prefix("0x")
                    .unwrap_or(block_number_hex),
                16,
            )
            .unwrap_or_default();
            if block_number > 0 {
                self.light_client
                    .wait_for_height(block_number, Duration::from_secs(5))
                    .await
                    .map_err(|e| {
                        ProviderError::Unavailable(format!(
                            "access decision sync at height {}: {}",
                            block_number, e
                        ))
                    })?;
            }
        }

        let decision = self
            .light_client
            .check_access_decision(&decision_id)
            .await
            .map_err(|e| ProviderError::Query(format!("light client decision check: {}", e)))?;
        tracing::info!(
            decision_id = %decision_id,
            allowed = decision.allowed,
            verified_height = decision.verified_at_height,
            "hub.rs light client access decision lookup result"
        );
        if !decision.allowed {
            return Err(ProviderError::Query(format!(
                "access decision {} not visible after confirmation",
                decision_id
            )));
        }

        Ok(Some(decision_id))
    }

    fn acp_light_client_status(&self) -> Result<AcpLightClientStatus, ProviderError> {
        let sync = self.light_client.header_chain().state();
        let last_invalidation_height = self.light_client_observability.load(Ordering::Relaxed);

        Ok(AcpLightClientStatus {
            height: sync.as_ref().map_or(0, |state| state.height),
            module_state_root: sync
                .as_ref()
                .map_or_else(String::new, |state| format_root(state.module_state_root)),
            cache_entries: self.light_client.cache().len(),
            last_invalidation_height,
            connected: sync.is_some(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crypto::PrivateKey;
    use identity::Identity;

    fn store_remote_secp256r1_identity(did: &str) {
        let private_key = crypto::generate_secp256r1().expect("should generate secp256r1 key");
        let public_key = private_key.public_key();
        let public_key_bytes = public_key.raw_owned();
        let public_key_hex = hex::encode(&public_key_bytes);
        defra_core::signing::store_identity(
            did,
            defra_core::signing::SigningConfig {
                key_type: defra_core::signing::SigningKeyType::Secp256r1,
                private_key_bytes: Vec::new(),
                public_key_bytes,
                public_key_hex,
                remote_signer: None,
                signing_authorization: None,
            },
        );
    }

    #[test]
    fn resolve_registered_or_passthrough_bearer_token_uses_request_token_for_remote_identity() {
        let _guard = crate::signing_state_test_guard();
        let did = "did:key:zRemoteHubRsToken";
        let token = "device.jwt.token".to_string();
        defra_core::signing::clear_identity_store();
        defra_core::signing::clear_request_bearer_token(did);

        store_remote_secp256r1_identity(did);
        defra_core::signing::set_request_bearer_token(did, token.clone());

        let resolved = resolve_registered_or_passthrough_bearer_token(did)
            .expect("resolution should succeed")
            .expect("token should resolve");
        assert_eq!(resolved, token);

        defra_core::signing::clear_request_bearer_token(did);
        defra_core::signing::clear_identity_store();
    }

    #[test]
    fn resolve_registered_or_passthrough_bearer_token_builds_local_secp256k1_token() {
        let _guard = crate::signing_state_test_guard();
        let private_key = crypto::generate_secp256k1().expect("should generate secp256k1 key");
        let raw_identity =
            identity::RawIdentity::from_secp256k1(private_key).expect("identity should build");
        let did = raw_identity.did().expect("did should derive").to_string();

        defra_core::signing::clear_identity_store();
        defra_core::signing::store_identity(
            &did,
            defra_core::signing::SigningConfig {
                key_type: defra_core::signing::SigningKeyType::Secp256k1,
                private_key_bytes: defra_core::signing::SigningConfig::private_key_bytes_from_vec(
                    raw_identity.private_key_bytes().to_vec(),
                ),
                public_key_bytes: raw_identity.public_key_bytes().to_vec(),
                public_key_hex: hex::encode(raw_identity.public_key_bytes()),
                remote_signer: None,
                signing_authorization: None,
            },
        );

        let resolved = resolve_registered_or_passthrough_bearer_token(&did)
            .expect("resolution should succeed")
            .expect("token should resolve");
        assert_eq!(resolved.matches('.').count(), 2);

        defra_core::signing::clear_request_bearer_token(&did);
        defra_core::signing::clear_identity_store();
    }

    #[test]
    fn relations_for_permission_expands_standard_permissions() {
        assert_eq!(
            HubRsProvider::relations_for_permission("read"),
            vec!["owner", "writer", "reader"]
        );
        assert_eq!(
            HubRsProvider::relations_for_permission("update"),
            vec!["owner", "writer"]
        );
        assert_eq!(
            HubRsProvider::relations_for_permission("delete"),
            vec!["owner"]
        );
    }

    #[test]
    fn relations_for_permission_preserves_relation_style_checks() {
        assert_eq!(
            HubRsProvider::relations_for_permission("writer"),
            vec!["writer"]
        );
        assert_eq!(
            HubRsProvider::relations_for_permission("signer"),
            vec!["signer"]
        );
    }

    #[test]
    fn nonce_errors_include_hubrs_duplicate_transaction_response() {
        assert!(is_nonce_error(&ClientError::Rpc("nonce too low".into())));
        assert!(is_nonce_error(&ClientError::Rpc(
            "code -32000: duplicate transaction".into()
        )));
        assert!(!is_nonce_error(&ClientError::Rpc(
            "execution reverted".into()
        )));
    }
}
