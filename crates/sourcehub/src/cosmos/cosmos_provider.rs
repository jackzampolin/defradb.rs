use std::time::Duration;

use async_trait::async_trait;

use crate::circuit_breaker::CircuitBreaker;
use crate::policy_cache::PolicyCache;
use crate::provider::{ProviderError, ProviderPolicyInfo, SourceHubProvider, SubjectRef};
use crate::tuning::AcpTuning;

use super::client::{ClientError, SourceHubClient};
use super::tx::TxSigner;

/// SourceHub provider backed by Cosmos SDK (REST/LCD + CometBFT).
///
/// Wraps all network calls with a circuit breaker to prevent fail-open
/// behaviour when SourceHub is unreachable. Policy metadata is cached
/// locally; cache misses always fall back to an on-chain query.
pub struct CosmosProvider {
    client: SourceHubClient,
    signer: TxSigner,
    circuit_breaker: CircuitBreaker,
    policy_cache: PolicyCache,
}

impl CosmosProvider {
    pub fn new(
        grpc_address: String,
        comet_address: String,
        signer_key: &[u8],
        chain_id: &str,
        tuning: &AcpTuning,
    ) -> Result<Self, ProviderError> {
        let client = SourceHubClient::new(grpc_address, comet_address, tuning.request_timeout)
            .map_err(|e| ProviderError::Config(format!("HTTP client: {}", e)))?;
        let signer = TxSigner::from_secp256k1_bytes(signer_key, chain_id)
            .map_err(|e| ProviderError::Config(e.to_string()))?;
        Ok(Self {
            client,
            signer,
            circuit_breaker: CircuitBreaker::new(
                tuning.circuit_breaker_threshold,
                tuning.circuit_breaker_reset_timeout,
            ),
            policy_cache: PolicyCache::new(tuning.cache_ttl),
        })
    }

    /// Wrap a fallible SourceHub network call with circuit breaker logic.
    ///
    /// If the circuit is open, returns `ProviderError::Unavailable` immediately
    /// rather than attempting the network call. On success the circuit records a
    /// success; on failure it records a failure and may trip the circuit.
    async fn with_circuit_breaker<F, Fut, T>(&self, op: F) -> Result<T, ProviderError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, ClientError>>,
    {
        if !self.circuit_breaker.allow_request() {
            tracing::warn!("SourceHub circuit breaker open: denying access (fail-closed)");
            return Err(ProviderError::Unavailable(
                "SourceHub unreachable; circuit breaker open".to_string(),
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
}

fn subject_to_json(subject: &SubjectRef) -> serde_json::Value {
    match subject {
        SubjectRef::Actor(did) => serde_json::json!({ "actor": { "id": did } }),
        SubjectRef::AllActors => serde_json::json!({ "all_actors": {} }),
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl SourceHubProvider for CosmosProvider {
    fn authorized_account(&self) -> String {
        self.signer.address()
    }

    async fn create_bearer_token(&self, did: &str) -> Result<String, ProviderError> {
        let signing_config = match defra_core::signing::get_identity(did) {
            Some(config) => config,
            None => {
                // Fall back to the original JWT from the HTTP request.
                // Go DefraDB's BearerToken() pattern: the user's JWT is already signed
                // by their private key, so we can forward it to SourceHub as-is.
                if let Some(token) = defra_core::signing::get_request_bearer_token(did) {
                    return Ok(token);
                }
                tracing::warn!(
                    did,
                    "SourceHub bearer token creation failed: no signing config for DID \
                     and no request token. The identity may have been unregistered."
                );
                return Err(ProviderError::Config(format!(
                    "no signing config found for DID: {}",
                    did
                )));
            }
        };

        let key_type: crypto::KeyType = match signing_config.key_type.as_str() {
            "ed25519" => crypto::KeyType::Ed25519,
            "secp256k1" => crypto::KeyType::Secp256k1,
            other => {
                return Err(ProviderError::Config(format!(
                    "unsupported key type: {}",
                    other
                )))
            }
        };

        let raw_identity =
            identity::RawIdentity::from_bytes(key_type, &signing_config.private_key_bytes)
                .map_err(|e| ProviderError::Config(format!("failed to create identity: {}", e)))?;

        let token_bytes = identity::new_token(
            &raw_identity,
            Duration::from_secs(300),
            None,
            Some(self.signer.address()),
        )
        .map_err(|e| ProviderError::Config(format!("failed to create bearer token: {}", e)))?;

        String::from_utf8(token_bytes)
            .map_err(|e| ProviderError::Config(format!("bearer token is not valid UTF-8: {}", e)))
    }

    fn self_did(&self) -> Option<String> {
        None
    }

    async fn create_policy(&self, policy_yaml: &str) -> Result<String, ProviderError> {
        self.signer
            .create_policy(&self.client, policy_yaml)
            .await
            .map_err(|e| ProviderError::Transaction(e.to_string()))
    }

    async fn register_object(
        &self,
        bearer_token: &str,
        policy_id: &str,
        resource: &str,
        object_id: &str,
    ) -> Result<(), ProviderError> {
        let cmd = serde_json::json!({
            "register_object_cmd": {
                "object": { "resource": resource, "id": object_id }
            }
        });
        self.signer
            .bearer_policy_cmd(&self.client, bearer_token, policy_id, cmd)
            .await
            .map_err(|e| ProviderError::Transaction(e.to_string()))?;
        Ok(())
    }

    async fn archive_object(
        &self,
        bearer_token: &str,
        policy_id: &str,
        resource: &str,
        object_id: &str,
    ) -> Result<(), ProviderError> {
        let cmd = serde_json::json!({
            "archive_object_cmd": {
                "object": { "resource": resource, "id": object_id }
            }
        });
        self.signer
            .bearer_policy_cmd(&self.client, bearer_token, policy_id, cmd)
            .await
            .map_err(|e| ProviderError::Transaction(e.to_string()))?;
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
        let cmd = serde_json::json!({
            "set_relationship_cmd": {
                "relationship": {
                    "object": { "resource": resource, "id": object_id },
                    "relation": relation,
                    "subject": subject_to_json(subject),
                }
            }
        });
        let result = self
            .signer
            .bearer_policy_cmd(&self.client, bearer_token, policy_id, cmd)
            .await
            .map_err(|e| ProviderError::Transaction(e.to_string()))?;

        let record_existed = result
            .get("record_existed")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        Ok(!record_existed)
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
        let cmd = serde_json::json!({
            "delete_relationship_cmd": {
                "relationship": {
                    "object": { "resource": resource, "id": object_id },
                    "relation": relation,
                    "subject": subject_to_json(subject),
                }
            }
        });
        let result = self
            .signer
            .bearer_policy_cmd(&self.client, bearer_token, policy_id, cmd)
            .await
            .map_err(|e| ProviderError::Transaction(e.to_string()))?;

        let record_found = result
            .get("record_found")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        Ok(record_found)
    }

    /// Query a policy by ID, using the local cache when available.
    ///
    /// A cache miss (absent or expired entry) always falls back to an on-chain
    /// query so that stale cache state never silently misrepresents policy
    /// existence. The result is cached on success for future calls.
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
        let result = self
            .with_circuit_breaker(|| self.client.query_policy(policy_id))
            .await?;

        if let Some(ref p) = result {
            self.policy_cache.insert(policy_id, p.name.clone());
        }

        Ok(result.map(|p| ProviderPolicyInfo {
            id: p.id,
            name: p.name,
        }))
    }

    async fn query_object_owner(
        &self,
        policy_id: &str,
        resource: &str,
        object_id: &str,
    ) -> Result<(bool, String), ProviderError> {
        self.with_circuit_breaker(|| {
            self.client
                .query_object_owner(policy_id, resource, object_id)
        })
        .await
    }

    async fn verify_access(
        &self,
        policy_id: &str,
        resource: &str,
        object_id: &str,
        permission: &str,
        actor_did: &str,
    ) -> Result<bool, ProviderError> {
        self.with_circuit_breaker(|| {
            self.client
                .verify_access(policy_id, resource, object_id, permission, actor_did)
        })
        .await
    }
}
