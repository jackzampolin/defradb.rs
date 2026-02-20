use async_trait::async_trait;

use crate::client::SourceHubClient;
use crate::provider::{ProviderError, ProviderPolicyInfo, SourceHubProvider, SubjectRef};
use crate::tx::TxSigner;

/// SourceHub provider backed by Cosmos SDK (REST/LCD + CometBFT).
pub struct CosmosProvider {
    client: SourceHubClient,
    signer: TxSigner,
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
        Ok(Self { client, signer })
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

    async fn query_policy(
        &self,
        policy_id: &str,
    ) -> Result<Option<ProviderPolicyInfo>, ProviderError> {
        self.client
            .query_policy(policy_id)
            .await
            .map(|opt| {
                opt.map(|p| ProviderPolicyInfo {
                    id: p.id,
                    name: p.name,
                })
            })
            .map_err(|e| ProviderError::Query(e.to_string()))
    }

    async fn query_object_owner(
        &self,
        policy_id: &str,
        resource: &str,
        object_id: &str,
    ) -> Result<(bool, String), ProviderError> {
        self.client
            .query_object_owner(policy_id, resource, object_id)
            .await
            .map_err(|e| ProviderError::Query(e.to_string()))
    }

    async fn verify_access(
        &self,
        policy_id: &str,
        resource: &str,
        object_id: &str,
        permission: &str,
        actor_did: &str,
    ) -> Result<bool, ProviderError> {
        self.client
            .verify_access(policy_id, resource, object_id, permission, actor_did)
            .await
            .map_err(|e| ProviderError::Query(e.to_string()))
    }
}
