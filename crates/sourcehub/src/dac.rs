//! SourceHub DocumentACP implementation.
//!
//! Implements the `DocumentACP` trait by communicating with a SourceHub
//! blockchain node via REST/LCD queries and CometBFT transaction broadcast.

use std::time::Duration;

use async_trait::async_trait;
use identity::Did;

use acp::{DocumentACP, DocumentPermission, Identity, Result};

use crate::client::SourceHubClient;
use crate::tx::TxSigner;

/// DocumentACP backed by SourceHub's on-chain x/acp module.
///
/// Write operations (register, relationship changes) are submitted as
/// Cosmos SDK transactions using bearer token authentication.
/// Read operations (is_registered, check_access) use REST/LCD queries.
pub struct SourceHubDocumentACP {
    client: SourceHubClient,
    signer: TxSigner,
}

impl SourceHubDocumentACP {
    pub fn new(client: SourceHubClient, signer: TxSigner) -> Self {
        Self { client, signer }
    }

    /// Create a policy on SourceHub. Returns the tx hash.
    pub async fn add_policy(
        &self,
        _creator_did: &str,
        policy_yaml: &str,
    ) -> std::result::Result<String, crate::tx::TxSignerError> {
        self.signer.create_policy(&self.client, policy_yaml).await
    }

    /// Generate a bearer token for the given DID.
    ///
    /// Looks up the identity's signing config from the global store,
    /// creates a JWT with `authorized_account` set to the validator address.
    fn create_bearer_token(&self, did: &str) -> std::result::Result<String, acp::Error> {
        let signing_config = defra_core::signing::get_identity(did).ok_or_else(|| {
            acp::Error::PermissionDenied(format!(
                "no signing config found for DID: {}",
                did
            ))
        })?;

        let key_type: crypto::KeyType = match signing_config.key_type.as_str() {
            "ed25519" => crypto::KeyType::Ed25519,
            "secp256k1" => crypto::KeyType::Secp256k1,
            other => {
                return Err(acp::Error::PermissionDenied(format!(
                    "unsupported key type: {}",
                    other
                )))
            }
        };

        let raw_identity = identity::RawIdentity::from_bytes(
            key_type,
            &signing_config.private_key_bytes,
        )
        .map_err(|e| acp::Error::PermissionDenied(format!("failed to create identity: {}", e)))?;

        let token_bytes = identity::new_token(
            &raw_identity,
            Duration::from_secs(300), // 5 minute validity
            None,
            Some(self.signer.address()),
        )
        .map_err(|e| {
            acp::Error::PermissionDenied(format!("failed to create bearer token: {}", e))
        })?;

        String::from_utf8(token_bytes).map_err(|e| {
            acp::Error::PermissionDenied(format!("bearer token is not valid UTF-8: {}", e))
        })
    }

    /// Execute a bearer policy command transaction.
    async fn bearer_cmd(
        &self,
        did: &str,
        policy_id: &str,
        cmd: serde_json::Value,
    ) -> Result<serde_json::Value> {
        let bearer_token = self.create_bearer_token(did)?;
        self.signer
            .bearer_policy_cmd(&self.client, &bearer_token, policy_id, cmd)
            .await
            .map_err(|e| acp::Error::Storage(format!("SourceHub tx failed: {}", e)))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl DocumentACP for SourceHubDocumentACP {
    async fn register_doc_object(
        &self,
        identity: &Did,
        policy_id: &str,
        resource_name: &str,
        doc_id: &str,
    ) -> Result<()> {
        let cmd = serde_json::json!({
            "register_object_cmd": {
                "object": {
                    "resource": resource_name,
                    "id": doc_id,
                }
            }
        });
        self.bearer_cmd(identity.as_str(), policy_id, cmd).await?;
        Ok(())
    }

    async fn is_doc_registered(
        &self,
        policy_id: &str,
        resource_name: &str,
        doc_id: &str,
    ) -> Result<bool> {
        let (is_registered, _owner) = self
            .client
            .query_object_owner(policy_id, resource_name, doc_id)
            .await
            .map_err(|e| acp::Error::Storage(format!("SourceHub query failed: {}", e)))?;
        Ok(is_registered)
    }

    async fn check_doc_access(
        &self,
        identity: &Identity,
        permission: DocumentPermission,
        policy_id: &str,
        resource_name: &str,
        doc_id: &str,
    ) -> Result<bool> {
        // Unregistered docs are public
        let (is_registered, _owner) = self
            .client
            .query_object_owner(policy_id, resource_name, doc_id)
            .await
            .map_err(|e| acp::Error::Storage(format!("SourceHub query failed: {}", e)))?;

        if !is_registered {
            return Ok(true);
        }

        // Anonymous cannot access registered docs
        let did = match identity.did() {
            Some(did) => did,
            None => return Ok(false),
        };

        self.client
            .verify_access(
                policy_id,
                resource_name,
                doc_id,
                permission.as_str(),
                did.as_str(),
            )
            .await
            .map_err(|e| acp::Error::Storage(format!("SourceHub query failed: {}", e)))
    }

    async fn add_actor_relationship(
        &self,
        requestor: &Did,
        target: &Did,
        collection_id: &str,
        doc_id: &str,
        relation: &str,
        _managing_relations: &[String],
    ) -> Result<bool> {
        let cmd = serde_json::json!({
            "set_relationship_cmd": {
                "relationship": {
                    "object": {
                        "resource": collection_id,
                        "id": doc_id,
                    },
                    "relation": relation,
                    "subject": {
                        "actor": {
                            "id": target.as_str(),
                        }
                    }
                }
            }
        });
        self.bearer_cmd(requestor.as_str(), collection_id, cmd)
            .await?;
        Ok(true)
    }

    async fn delete_actor_relationship(
        &self,
        requestor: &Did,
        target: &Did,
        collection_id: &str,
        doc_id: &str,
        relation: &str,
        _managing_relations: &[String],
    ) -> Result<bool> {
        let cmd = serde_json::json!({
            "delete_relationship_cmd": {
                "relationship": {
                    "object": {
                        "resource": collection_id,
                        "id": doc_id,
                    },
                    "relation": relation,
                    "subject": {
                        "actor": {
                            "id": target.as_str(),
                        }
                    }
                }
            }
        });
        self.bearer_cmd(requestor.as_str(), collection_id, cmd)
            .await?;
        Ok(true)
    }

    async fn unregister_doc_object(
        &self,
        policy_id: &str,
        resource_name: &str,
        doc_id: &str,
    ) -> Result<()> {
        // Unregister needs a bearer token from the owner.
        // Query the owner first, then use their identity.
        let (_is_registered, owner_did) = self
            .client
            .query_object_owner(policy_id, resource_name, doc_id)
            .await
            .map_err(|e| acp::Error::Storage(format!("SourceHub query failed: {}", e)))?;

        if owner_did.is_empty() {
            return Ok(());
        }

        let cmd = serde_json::json!({
            "archive_object_cmd": {
                "object": {
                    "resource": resource_name,
                    "id": doc_id,
                }
            }
        });
        self.bearer_cmd(&owner_did, policy_id, cmd).await?;
        Ok(())
    }
}
