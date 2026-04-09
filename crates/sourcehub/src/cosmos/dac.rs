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
    access_cache: Option<AccessCache>,
}

impl SourceHubDocumentACP {
    pub fn new(provider: Arc<dyn SourceHubProvider>, cache_ttl: Duration) -> Self {
        Self {
            provider,
            access_cache: Some(AccessCache::new(cache_ttl)),
        }
    }

    pub fn without_access_cache(provider: Arc<dyn SourceHubProvider>) -> Self {
        Self {
            provider,
            access_cache: None,
        }
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
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use super::*;

    struct MockProvider {
        decisions: Mutex<VecDeque<bool>>,
        created_decision: Mutex<Option<String>>,
        verify_calls: Mutex<usize>,
    }

    impl MockProvider {
        fn new(decisions: Vec<bool>) -> Self {
            Self {
                decisions: Mutex::new(decisions.into()),
                created_decision: Mutex::new(None),
                verify_calls: Mutex::new(0),
            }
        }

        fn verify_calls(&self) -> usize {
            *self.verify_calls.lock().unwrap()
        }

        fn created_decision(&self) -> Option<String> {
            self.created_decision.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl SourceHubProvider for MockProvider {
        fn authorized_account(&self) -> String {
            "0x0".to_string()
        }

        async fn create_bearer_token(
            &self,
            _did: &str,
        ) -> std::result::Result<String, ProviderError> {
            unreachable!("create_bearer_token is not used in this test")
        }

        fn self_did(&self) -> Option<String> {
            None
        }

        async fn create_policy(
            &self,
            _policy_yaml: &str,
        ) -> std::result::Result<String, ProviderError> {
            unreachable!("create_policy is not used in this test")
        }

        async fn register_object(
            &self,
            _bearer_token: &str,
            _policy_id: &str,
            _resource: &str,
            _object_id: &str,
        ) -> std::result::Result<(), ProviderError> {
            unreachable!("register_object is not used in this test")
        }

        async fn archive_object(
            &self,
            _bearer_token: &str,
            _policy_id: &str,
            _resource: &str,
            _object_id: &str,
        ) -> std::result::Result<(), ProviderError> {
            unreachable!("archive_object is not used in this test")
        }

        async fn set_relationship(
            &self,
            _bearer_token: &str,
            _policy_id: &str,
            _resource: &str,
            _object_id: &str,
            _relation: &str,
            _subject: &SubjectRef,
        ) -> std::result::Result<bool, ProviderError> {
            unreachable!("set_relationship is not used in this test")
        }

        async fn delete_relationship(
            &self,
            _bearer_token: &str,
            _policy_id: &str,
            _resource: &str,
            _object_id: &str,
            _relation: &str,
            _subject: &SubjectRef,
        ) -> std::result::Result<bool, ProviderError> {
            unreachable!("delete_relationship is not used in this test")
        }

        async fn query_policy(
            &self,
            _policy_id: &str,
        ) -> std::result::Result<Option<crate::provider::ProviderPolicyInfo>, ProviderError>
        {
            unreachable!("query_policy is not used in this test")
        }

        async fn query_object_owner(
            &self,
            _policy_id: &str,
            _resource: &str,
            _object_id: &str,
        ) -> std::result::Result<(bool, String), ProviderError> {
            Ok((
                true,
                "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK".to_string(),
            ))
        }

        async fn verify_access(
            &self,
            _policy_id: &str,
            _resource: &str,
            _object_id: &str,
            _permission: &str,
            _actor_did: &str,
        ) -> std::result::Result<bool, ProviderError> {
            *self.verify_calls.lock().unwrap() += 1;
            let decision = self
                .decisions
                .lock()
                .unwrap()
                .pop_front()
                .expect("mock verify_access decision");
            Ok(decision)
        }

        async fn create_access_decision(
            &self,
            _policy_id: &str,
            _resource: &str,
            _object_id: &str,
            _permission: &str,
            actor_did: &str,
        ) -> std::result::Result<Option<String>, ProviderError> {
            let decision_id = format!("decision-for-{actor_did}");
            *self.created_decision.lock().unwrap() = Some(decision_id.clone());
            Ok(Some(decision_id))
        }
    }

    #[tokio::test]
    async fn check_doc_access_does_not_cache_remote_denials() {
        let provider = Arc::new(MockProvider::new(vec![false, true]));
        let acp = SourceHubDocumentACP::without_access_cache(provider.clone());
        let did = Did::new("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK").unwrap();
        let identity = Identity::from(did);

        let denied = acp
            .check_doc_access(
                &identity,
                DocumentPermission::Read,
                "policy-1",
                "users",
                "doc-1",
            )
            .await
            .expect("initial denial");
        assert!(!denied);

        let allowed = acp
            .check_doc_access(
                &identity,
                DocumentPermission::Read,
                "policy-1",
                "users",
                "doc-1",
            )
            .await
            .expect("fresh remote decision");
        assert!(allowed);
        assert_eq!(provider.verify_calls(), 2);
    }

    #[tokio::test]
    async fn check_doc_access_does_not_cache_denials_when_enabled() {
        let provider = Arc::new(MockProvider::new(vec![false, true]));
        let acp = SourceHubDocumentACP::new(provider.clone(), Duration::from_secs(300));
        let did = Did::new("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK").unwrap();
        let identity = Identity::from(did);

        let denied = acp
            .check_doc_access(
                &identity,
                DocumentPermission::Read,
                "policy-1",
                "users",
                "doc-1",
            )
            .await
            .expect("initial denial");
        assert!(!denied);

        let allowed = acp
            .check_doc_access(
                &identity,
                DocumentPermission::Read,
                "policy-1",
                "users",
                "doc-1",
            )
            .await
            .expect("fresh remote decision");
        assert!(allowed);
        assert_eq!(provider.verify_calls(), 2);
    }

    #[tokio::test]
    async fn create_access_decision_delegates_to_provider() {
        let provider = Arc::new(MockProvider::new(vec![]));
        let acp = SourceHubDocumentACP::without_access_cache(provider.clone());
        let did = Did::new("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK").unwrap();
        let identity = Identity::from(did.clone());

        let decision_id = acp
            .create_access_decision(&identity, "policy-1", "transcript", "transcript", "writer")
            .await
            .expect("create access decision should succeed");

        assert_eq!(decision_id, Some(format!("decision-for-{}", did.as_str())));
        assert_eq!(provider.created_decision(), decision_id);
    }
}
