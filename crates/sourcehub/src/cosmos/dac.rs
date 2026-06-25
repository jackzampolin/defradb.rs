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
                let bearer_token = self.create_bearer_token(requestor.as_str()).await?;
                let (kind, sr, so, srel) =
                    zanzibar::encode_subject(&target).map_err(acp::Error::from)?;
                let result = self
                    .provider
                    .set_relationship_subject(
                        &bearer_token,
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
                let bearer_token = self.create_bearer_token(requestor.as_str()).await?;
                let (kind, sr, so, srel) =
                    zanzibar::encode_subject(&target).map_err(acp::Error::from)?;
                let result = self
                    .provider
                    .delete_relationship_subject(
                        &bearer_token,
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
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use super::*;

    /// Records which relationship-emitting provider method a routing test drove,
    /// plus the structured-subject codec tuple for the EntitySet path.
    #[derive(Default)]
    struct RelationshipCalls {
        set_relationship: bool,
        delete_relationship: bool,
        set_subject: Option<(String, u8, String, String, String)>,
        delete_subject: Option<(String, u8, String, String, String)>,
    }

    struct MockProvider {
        decisions: Mutex<VecDeque<bool>>,
        created_decision: Mutex<Option<String>>,
        verify_calls: Mutex<usize>,
        rel_calls: Mutex<RelationshipCalls>,
    }

    impl MockProvider {
        fn new(decisions: Vec<bool>) -> Self {
            Self {
                decisions: Mutex::new(decisions.into()),
                created_decision: Mutex::new(None),
                verify_calls: Mutex::new(0),
                rel_calls: Mutex::new(RelationshipCalls::default()),
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
            Ok("mock.bearer.token".to_string())
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
            self.rel_calls.lock().unwrap().set_relationship = true;
            Ok(true)
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
            self.rel_calls.lock().unwrap().delete_relationship = true;
            Ok(true)
        }

        async fn set_relationship_subject(
            &self,
            bearer_token: &str,
            _policy_id: &str,
            _resource: &str,
            _object_id: &str,
            _relation: &str,
            kind: u8,
            subject_resource: &str,
            subject_object_id: &str,
            subject_relation: &str,
        ) -> std::result::Result<bool, ProviderError> {
            self.rel_calls.lock().unwrap().set_subject = Some((
                bearer_token.to_string(),
                kind,
                subject_resource.to_string(),
                subject_object_id.to_string(),
                subject_relation.to_string(),
            ));
            Ok(true)
        }

        async fn delete_relationship_subject(
            &self,
            bearer_token: &str,
            _policy_id: &str,
            _resource: &str,
            _object_id: &str,
            _relation: &str,
            kind: u8,
            subject_resource: &str,
            subject_object_id: &str,
            subject_relation: &str,
        ) -> std::result::Result<bool, ProviderError> {
            self.rel_calls.lock().unwrap().delete_subject = Some((
                bearer_token.to_string(),
                kind,
                subject_resource.to_string(),
                subject_object_id.to_string(),
                subject_relation.to_string(),
            ));
            Ok(true)
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

    fn requestor() -> Did {
        Did::new("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK").unwrap()
    }

    #[tokio::test]
    async fn add_relationship_routes_actor_to_set_relationship() {
        let provider = Arc::new(MockProvider::new(vec![]));
        let acp = SourceHubDocumentACP::without_access_cache(provider.clone());
        let target = Subject::Entity(zanzibar::Did::new_unchecked(
            "did:key:zActorTarget".to_string(),
        ));

        let result = acp
            .add_relationship(
                &requestor(),
                target,
                "policy-1",
                "users",
                "doc-1",
                "reader",
                &[],
            )
            .await
            .expect("actor relationship should route to set_relationship");
        assert!(result);

        let calls = provider.rel_calls.lock().unwrap();
        assert!(calls.set_relationship, "actor must hit set_relationship");
        assert!(
            calls.set_subject.is_none(),
            "actor must not hit subject path"
        );
    }

    #[tokio::test]
    async fn add_relationship_routes_object_edge_to_subject_kind_2() {
        let provider = Arc::new(MockProvider::new(vec![]));
        let acp = SourceHubDocumentACP::without_access_cache(provider.clone());
        let target = Subject::entity_set("directory", "d1", "");

        let result = acp
            .add_relationship(
                &requestor(),
                target,
                "policy-1",
                "users",
                "doc-1",
                "reader",
                &[],
            )
            .await
            .expect("object edge should route to set_relationship_subject");
        assert!(result);

        let calls = provider.rel_calls.lock().unwrap();
        assert!(
            !calls.set_relationship,
            "object edge must not hit actor path"
        );
        assert_eq!(
            calls.set_subject,
            Some((
                "mock.bearer.token".to_string(),
                2,
                "directory".to_string(),
                "d1".to_string(),
                String::new()
            ))
        );
    }

    #[tokio::test]
    async fn add_relationship_routes_userset_to_subject_kind_3() {
        let provider = Arc::new(MockProvider::new(vec![]));
        let acp = SourceHubDocumentACP::without_access_cache(provider.clone());
        let target = Subject::entity_set("directory", "d1", "member");

        acp.add_relationship(
            &requestor(),
            target,
            "policy-1",
            "users",
            "doc-1",
            "reader",
            &[],
        )
        .await
        .expect("userset should route to set_relationship_subject");

        let calls = provider.rel_calls.lock().unwrap();
        assert_eq!(
            calls.set_subject,
            Some((
                "mock.bearer.token".to_string(),
                3,
                "directory".to_string(),
                "d1".to_string(),
                "member".to_string()
            ))
        );
    }

    #[tokio::test]
    async fn delete_relationship_routes_object_edge_to_subject_kind_2() {
        let provider = Arc::new(MockProvider::new(vec![]));
        let acp = SourceHubDocumentACP::without_access_cache(provider.clone());
        let target = Subject::entity_set("directory", "d1", "");

        acp.delete_relationship(
            &requestor(),
            target,
            "policy-1",
            "users",
            "doc-1",
            "reader",
            &[],
        )
        .await
        .expect("object edge delete should route to delete_relationship_subject");

        let calls = provider.rel_calls.lock().unwrap();
        assert!(!calls.delete_relationship);
        assert_eq!(
            calls.delete_subject,
            Some((
                "mock.bearer.token".to_string(),
                2,
                "directory".to_string(),
                "d1".to_string(),
                String::new()
            ))
        );
    }

    /// A provider that omits the structured-subject overrides, so the trait
    /// default (`Unsupported`) is exercised on the EntitySet path.
    struct NoSubjectProvider;

    #[async_trait]
    impl SourceHubProvider for NoSubjectProvider {
        fn authorized_account(&self) -> String {
            "0x0".to_string()
        }
        async fn create_bearer_token(
            &self,
            _did: &str,
        ) -> std::result::Result<String, ProviderError> {
            Ok("mock.bearer.token".to_string())
        }
        fn self_did(&self) -> Option<String> {
            None
        }
        async fn create_policy(
            &self,
            _policy_yaml: &str,
        ) -> std::result::Result<String, ProviderError> {
            unreachable!()
        }
        async fn register_object(
            &self,
            _bearer_token: &str,
            _policy_id: &str,
            _resource: &str,
            _object_id: &str,
        ) -> std::result::Result<(), ProviderError> {
            unreachable!()
        }
        async fn archive_object(
            &self,
            _bearer_token: &str,
            _policy_id: &str,
            _resource: &str,
            _object_id: &str,
        ) -> std::result::Result<(), ProviderError> {
            unreachable!()
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
            unreachable!()
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
            unreachable!()
        }
        async fn query_policy(
            &self,
            _policy_id: &str,
        ) -> std::result::Result<Option<crate::provider::ProviderPolicyInfo>, ProviderError>
        {
            unreachable!()
        }
        async fn query_object_owner(
            &self,
            _policy_id: &str,
            _resource: &str,
            _object_id: &str,
        ) -> std::result::Result<(bool, String), ProviderError> {
            unreachable!()
        }
        async fn verify_access(
            &self,
            _policy_id: &str,
            _resource: &str,
            _object_id: &str,
            _permission: &str,
            _actor_did: &str,
        ) -> std::result::Result<bool, ProviderError> {
            unreachable!()
        }
    }

    #[tokio::test]
    async fn add_relationship_subject_default_is_unsupported() {
        let provider = Arc::new(NoSubjectProvider);
        let acp = SourceHubDocumentACP::without_access_cache(provider);
        let target = Subject::entity_set("directory", "d1", "");

        let err = acp
            .add_relationship(
                &requestor(),
                target,
                "policy-1",
                "users",
                "doc-1",
                "reader",
                &[],
            )
            .await
            .expect_err("provider without subject support must fail");
        let message = err.to_string();
        assert!(
            message.contains("unsupported"),
            "expected Unsupported error, got: {message}"
        );
    }
}
