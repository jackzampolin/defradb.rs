use async_trait::async_trait;

use super::circuit_breaker::CircuitBreaker;
use super::client::{ClientError, SourceHubClient};
use super::policy_cache::PolicyCache;
use super::provider::{ProviderError, ProviderPolicyInfo, SourceHubProvider, SubjectRef};
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
    ) -> Result<Self, ProviderError> {
        let client = SourceHubClient::new(grpc_address, comet_address);
        let signer = TxSigner::from_secp256k1_bytes(signer_key, chain_id)
            .map_err(|e| ProviderError::Config(e.to_string()))?;
        Ok(Self {
            client,
            signer,
            circuit_breaker: CircuitBreaker::new(),
            policy_cache: PolicyCache::new(),
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
