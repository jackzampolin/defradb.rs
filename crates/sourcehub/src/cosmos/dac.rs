//! SourceHub DocumentACP implementation.
//!
//! Implements the `DocumentACP` trait by delegating to a `SourceHubProvider`
//! for all on-chain communication. The provider abstraction allows swapping
//! between Cosmos SDK and EVM backends.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use identity::Did;

use acp::{DocumentACP, DocumentPermission, Identity, Result, Subject};

use crate::access_cache::AccessCache;
use crate::provider::{AcpLightClientStatus, ProviderError, SourceHubProvider, SubjectRef};

/// DocumentACP backed by SourceHub's on-chain x/acp module.
///
/// Delegates write and read operations to a `SourceHubProvider` implementation.
/// By default it caches `verify_access` results to avoid redundant network
/// roundtrips. hub.rs can opt out because remote ACP state may change outside
/// DefraDB's mutation path.
pub struct SourceHubDocumentACP {
    provider: Arc<dyn SourceHubProvider>,
    access_cache: Option<Arc<AccessCache>>,
    #[cfg(not(target_arch = "wasm32"))]
    event_subscriber: Option<super::event_subscriber::CosmosEventSubscriber>,
}

impl SourceHubDocumentACP {
    pub fn new(provider: Arc<dyn SourceHubProvider>, cache_ttl: Duration) -> Self {
        Self {
            provider,
            access_cache: Some(Arc::new(AccessCache::new(cache_ttl))),
            #[cfg(not(target_arch = "wasm32"))]
            event_subscriber: None,
        }
    }

    pub fn without_access_cache(provider: Arc<dyn SourceHubProvider>) -> Self {
        Self {
            provider,
            access_cache: None,
            #[cfg(not(target_arch = "wasm32"))]
            event_subscriber: None,
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn with_cosmos_event_invalidation(
        mut self,
        websocket_url: impl Into<String>,
    ) -> std::result::Result<Self, ProviderError> {
        let cache = self.access_cache.as_ref().ok_or_else(|| {
            ProviderError::Config(
                "Cosmos event invalidation requires the access cache to be enabled".into(),
            )
        })?;
        self.event_subscriber = Some(super::event_subscriber::CosmosEventSubscriber::start(
            websocket_url.into(),
            Arc::clone(cache),
        )?);
        Ok(self)
    }

    fn invalidate_cached_access(&self, policy_id: &str, resource_name: &str, doc_id: &str) {
        if let Some(cache) = &self.access_cache {
            cache.invalidate_object(policy_id, resource_name, doc_id);
        }
    }

    /// Create a policy on SourceHub. Returns the policy ID.
    pub async fn add_policy(
        &self,
        _creator_did: &str,
        policy_yaml: &str,
    ) -> std::result::Result<String, ProviderError> {
        self.provider.create_policy(policy_yaml).await
    }

    pub async fn get_policy(
        &self,
        policy_id: &str,
    ) -> std::result::Result<Option<acp::Policy>, ProviderError> {
        let Some(info) = self.provider.query_policy(policy_id).await? else {
            return Ok(None);
        };

        let Some(raw_policy) = info.raw_policy else {
            return Err(ProviderError::Query(format!(
                "policy '{}' is not available from the configured provider",
                policy_id
            )));
        };

        acp::policy_yaml::check_duplicate_yaml_keys(&raw_policy)
            .map_err(|e| ProviderError::Query(format!("policy parse: {}", e)))?;
        let parsed = acp::policy_yaml::parse_policy_yaml(&raw_policy)
            .map_err(|e| ProviderError::Query(format!("policy parse: {}", e)))?;
        let mut policy = acp::policy_yaml::build_policy(&parsed, 1)
            .map_err(|e| ProviderError::Query(format!("policy parse: {}", e)))?;
        policy.id = info.id;
        Ok(Some(policy))
    }

    async fn create_bearer_token(&self, did: &str) -> std::result::Result<String, acp::Error> {
        self.provider
            .create_bearer_token(did)
            .await
            .map_err(provider_err)
    }

    pub fn acp_light_client_status(
        &self,
    ) -> std::result::Result<AcpLightClientStatus, ProviderError> {
        self.provider.acp_light_client_status()
    }
}

fn did_to_subject(target: &Did) -> SubjectRef {
    if target.as_str() == "*" {
        SubjectRef::AllActors
    } else {
        SubjectRef::Actor(target.as_str().to_string())
    }
}

/// Lowers an actor [`Subject`] (`Entity`/`Wildcard`) to the bare-DID actor the
/// existing relationship path operates on. Non-actor subjects must be routed to
/// the structured `*_relationship_subject` provider methods instead.
fn subject_to_actor_did(target: &Subject) -> Did {
    match target {
        Subject::Wildcard => Did::wildcard(),
        Subject::Entity(did) => Did::new_unchecked(did.to_string()),
        _ => Did::new_unchecked(String::new()),
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

    async fn get_doc_owner(
        &self,
        policy_id: &str,
        resource_name: &str,
        doc_id: &str,
    ) -> Result<Option<Did>> {
        let (is_registered, owner) = self
            .provider
            .query_object_owner(policy_id, resource_name, doc_id)
            .await
            .map_err(provider_err)?;

        if !is_registered {
            return Ok(None);
        }

        Did::new(&owner)
            .map(Some)
            .map_err(|error| acp::Error::Storage(format!("invalid owner DID: {error}")))
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
            None => "did:key:anonymous".to_string(),
        };

        if let Some(cache) = &self.access_cache {
            if let Some(cached) = cache.get(
                &actor_did,
                policy_id,
                resource_name,
                doc_id,
                permission.as_str(),
            ) {
                return Ok(cached);
            }
        }

        let result = self
            .provider
            .verify_access(
                policy_id,
                resource_name,
                doc_id,
                permission.as_str(),
                &actor_did,
            )
            .await
            .map_err(provider_err)?;

        if result {
            if let Some(cache) = &self.access_cache {
                cache.set(
                    &actor_did,
                    policy_id,
                    resource_name,
                    doc_id,
                    permission.as_str(),
                    result,
                );
            }
        }

        Ok(result)
    }

    async fn create_access_decision(
        &self,
        identity: &Identity,
        policy_id: &str,
        resource_name: &str,
        object_id: &str,
        permission: &str,
    ) -> Result<Option<String>> {
        let Some(actor_did) = identity.did() else {
            return Ok(None);
        };

        self.provider
            .create_access_decision(
                policy_id,
                resource_name,
                object_id,
                permission,
                actor_did.as_str(),
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
        let result = self
            .provider
            .set_relationship(
                &bearer_token,
                policy_id,
                resource_name,
                doc_id,
                relation,
                &subject,
            )
            .await
            .map_err(provider_err)?;

        self.invalidate_cached_access(policy_id, resource_name, doc_id);

        Ok(result)
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
        let result = self
            .provider
            .delete_relationship(
                &bearer_token,
                policy_id,
                resource_name,
                doc_id,
                relation,
                &subject,
            )
            .await
            .map_err(provider_err)?;

        self.invalidate_cached_access(policy_id, resource_name, doc_id);

        Ok(result)
    }

    async fn add_relationship(
        &self,
        requestor: &Did,
        target: Subject,
        policy_id: &str,
        resource_name: &str,
        doc_id: &str,
        relation: &str,
        managing_relations: &[String],
    ) -> Result<bool> {
        match target {
            Subject::Entity(_) | Subject::Wildcard => {
                let actor = subject_to_actor_did(&target);
                self.add_actor_relationship(
                    requestor,
                    &actor,
                    policy_id,
                    resource_name,
                    doc_id,
                    relation,
                    managing_relations,
                )
                .await
            }
            Subject::EntitySet { .. } => {
                let (kind, sr, so, srel) =
                    zanzibar::encode_subject(&target).map_err(acp::Error::from)?;
                let result = self
                    .provider
                    .set_relationship_subject(
                        policy_id,
                        resource_name,
                        doc_id,
                        relation,
                        kind,
                        &sr,
                        &so,
                        &srel,
                    )
                    .await
                    .map_err(provider_err)?;
                self.invalidate_cached_access(policy_id, resource_name, doc_id);
                Ok(result)
            }
            other => Err(acp::Error::UnsupportedSubject(other.to_string())),
        }
    }

    async fn delete_relationship(
        &self,
        requestor: &Did,
        target: Subject,
        policy_id: &str,
        resource_name: &str,
        doc_id: &str,
        relation: &str,
        managing_relations: &[String],
    ) -> Result<bool> {
        match target {
            Subject::Entity(_) | Subject::Wildcard => {
                let actor = subject_to_actor_did(&target);
                self.delete_actor_relationship(
                    requestor,
                    &actor,
                    policy_id,
                    resource_name,
                    doc_id,
                    relation,
                    managing_relations,
                )
                .await
            }
            Subject::EntitySet { .. } => {
                let (kind, sr, so, srel) =
                    zanzibar::encode_subject(&target).map_err(acp::Error::from)?;
                let result = self
                    .provider
                    .delete_relationship_subject(
                        policy_id,
                        resource_name,
                        doc_id,
                        relation,
                        kind,
                        &sr,
                        &so,
                        &srel,
                    )
                    .await
                    .map_err(provider_err)?;
                self.invalidate_cached_access(policy_id, resource_name, doc_id);
                Ok(result)
            }
            other => Err(acp::Error::UnsupportedSubject(other.to_string())),
        }
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
            .map_err(provider_err)?;

        self.invalidate_cached_access(policy_id, resource_name, doc_id);

        Ok(())
    }
}

#[cfg(test)]
mod tests;
