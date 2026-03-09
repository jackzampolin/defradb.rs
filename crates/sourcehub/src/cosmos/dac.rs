//! SourceHub DocumentACP implementation.
//!
//! Implements the `DocumentACP` trait by delegating to a `SourceHubProvider`
//! for all on-chain communication. The provider abstraction allows swapping
//! between Cosmos SDK and EVM backends.

use std::sync::Arc;

use async_trait::async_trait;
use identity::Did;

use acp::{DocumentACP, DocumentPermission, Identity, Result};

use crate::provider::{ProviderError, SourceHubProvider, SubjectRef};

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

    async fn create_bearer_token(&self, did: &str) -> std::result::Result<String, acp::Error> {
        self.provider
            .create_bearer_token(did)
            .await
            .map_err(provider_err)
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
        let bearer_token = self.create_bearer_token(identity.as_str()).await?;
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

        self.provider
            .verify_access(
                policy_id,
                resource_name,
                doc_id,
                permission.as_str(),
                &actor_did,
            )
            .await
            .map_err(provider_err)
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
        let bearer_token = self.create_bearer_token(requestor.as_str()).await?;
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
        let bearer_token = self.create_bearer_token(requestor.as_str()).await?;
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
        let (_is_registered, owner_did) = self
            .provider
            .query_object_owner(policy_id, resource_name, doc_id)
            .await
            .map_err(provider_err)?;

        if owner_did.is_empty() {
            return Ok(());
        }

        // hub.rs uses node's DID for archive; Cosmos uses the owner's DID
        let archive_did = self
            .provider
            .self_did()
            .unwrap_or_else(|| owner_did.clone());
        let bearer_token = self.create_bearer_token(&archive_did).await?;
        self.provider
            .archive_object(&bearer_token, policy_id, resource_name, doc_id)
            .await
            .map_err(provider_err)
    }
}
