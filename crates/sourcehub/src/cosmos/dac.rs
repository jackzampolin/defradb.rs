//! SourceHub DocumentACP implementation.
//!
//! Implements the `DocumentACP` trait by delegating to a `SourceHubProvider`
//! for all on-chain communication. The provider abstraction allows swapping
//! between Cosmos SDK and EVM backends.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use identity::Did;

use acp::{DocumentACP, DocumentPermission, Identity, Result};

use super::provider::{ProviderError, SourceHubProvider, SubjectRef};

/// DocumentACP backed by SourceHub's on-chain x/acp module.
///
/// Delegates write and read operations to a `SourceHubProvider` implementation.
pub struct SourceHubDocumentACP {
    provider: Arc<dyn SourceHubProvider>,
}

impl SourceHubDocumentACP {
    pub fn new(provider: Arc<dyn SourceHubProvider>) -> Self {
        Self { provider }
    }

    /// Create a policy on SourceHub. Returns the policy ID.
    pub async fn add_policy(
        &self,
        _creator_did: &str,
        policy_yaml: &str,
    ) -> std::result::Result<String, ProviderError> {
        self.provider.create_policy(policy_yaml).await
    }

    /// Generate a bearer token for the given DID.
    ///
    /// Looks up the identity's signing config from the global store,
    /// creates a JWT with `authorized_account` set to the provider's account.
    ///
    /// If no signing config is found for the DID, the identity may have been
    /// unregistered remotely. A clear warning is logged and access is denied
    /// rather than proceeding with an invalid or absent identity.
    fn create_bearer_token(&self, did: &str) -> std::result::Result<String, acp::Error> {
        let signing_config = defra_core::signing::get_identity(did).ok_or_else(|| {
            tracing::warn!(
                did,
                "SourceHub bearer token creation failed: no signing config for DID. \
                 The identity may have been unregistered. Denying access."
            );
            acp::Error::PermissionDenied(format!("no signing config found for DID: {}", did))
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

        let raw_identity =
            identity::RawIdentity::from_bytes(key_type, &signing_config.private_key_bytes)
                .map_err(|e| {
                    acp::Error::PermissionDenied(format!("failed to create identity: {}", e))
                })?;

        let token_bytes = identity::new_token(
            &raw_identity,
            Duration::from_secs(300), // 5 minute validity
            None,
            Some(self.provider.authorized_account()),
        )
        .map_err(|e| {
            acp::Error::PermissionDenied(format!("failed to create bearer token: {}", e))
        })?;

        String::from_utf8(token_bytes).map_err(|e| {
            acp::Error::PermissionDenied(format!("bearer token is not valid UTF-8: {}", e))
        })
    }
}

fn did_to_subject(target: &Did) -> SubjectRef {
    if target.as_str() == "*" {
        SubjectRef::AllActors
    } else {
        SubjectRef::Actor(target.as_str().to_string())
    }
}

fn provider_err(e: ProviderError) -> acp::Error {
    acp::Error::Storage(format!("SourceHub: {}", e))
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
        let bearer_token = self.create_bearer_token(identity.as_str())?;
        self.provider
            .register_object(&bearer_token, policy_id, resource_name, doc_id)
            .await
            .map_err(provider_err)
    }

    async fn is_doc_registered(
        &self,
        policy_id: &str,
        resource_name: &str,
        doc_id: &str,
    ) -> Result<bool> {
        let (is_registered, _owner) = self
            .provider
            .query_object_owner(policy_id, resource_name, doc_id)
            .await
            .map_err(provider_err)?;
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
            .provider
            .query_object_owner(policy_id, resource_name, doc_id)
            .await
            .map_err(provider_err)?;

        if !is_registered {
            return Ok(true);
        }

        let actor_did = match identity.did() {
            Some(did) => did.as_str().to_string(),
            None => {
                // Anonymous user: use a synthetic DID to check if all_actors grants access.
                // If all_actors was set, SourceHub returns true for any valid DID.
                "did:key:anonymous".to_string()
            }
        };

        // For read permission, also check update and delete (implied read access).
        // Go's bridge.go: "if identity has access to any write permission,
        // they don't need to explicitly have read permission to read."
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
            let has_access = self
                .provider
                .verify_access(policy_id, resource_name, doc_id, perm.as_str(), &actor_did)
                .await
                .map_err(provider_err)?;

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
        let subject = did_to_subject(target);
        self.provider
            .set_relationship(
                &bearer_token,
                policy_id,
                resource_name,
                doc_id,
                relation,
                &subject,
            )
            .await
            .map_err(provider_err)
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
        let subject = did_to_subject(target);
        self.provider
            .delete_relationship(
                &bearer_token,
                policy_id,
                resource_name,
                doc_id,
                relation,
                &subject,
            )
            .await
            .map_err(provider_err)
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
            .provider
            .query_object_owner(policy_id, resource_name, doc_id)
            .await
            .map_err(provider_err)?;

        if owner_did.is_empty() {
            return Ok(());
        }

        let bearer_token = self.create_bearer_token(&owner_did)?;
        self.provider
            .archive_object(&bearer_token, policy_id, resource_name, doc_id)
            .await
            .map_err(provider_err)
    }
}
