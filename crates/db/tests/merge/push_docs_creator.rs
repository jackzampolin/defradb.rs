use acp::DocumentACP;
use acp::DocumentPermission;
use acp::Identity;
use acp::LocalDocumentACP;
use acp::MemoryAcpStore;
use db::merge::push_docs_creator::*;
use db::Collection;
use identity::Did;
use schema::CollectionVersion;
use schema::PolicyDescription;
use std::sync::Arc;

#[tokio::test]
async fn resolve_push_creator_uses_owner_for_protected_document() {
    let acp = LocalDocumentACP::new(Arc::new(MemoryAcpStore::new()));
    let owner = Did::new("did:key:z6Mkowner").unwrap();
    acp.register_doc_object(&owner, "policy-1", "Users", "doc1")
        .await
        .unwrap();

    let creator = resolve_push_creator(Some(&acp), &protected_collection(), "doc1", "local-peer")
        .await
        .unwrap();

    assert_eq!(creator, owner.to_string());
}

#[tokio::test]
async fn resolve_push_creator_falls_back_only_without_collection_policy() {
    let collection = Collection::new(CollectionVersion::new("Users", "v1", "col1", vec![]));

    let creator = resolve_push_creator(None, &collection, "doc1", "local-peer")
        .await
        .unwrap();

    assert_eq!(creator, "local-peer");
}

#[tokio::test]
async fn resolve_push_creator_errors_when_owner_is_missing() {
    let acp = LocalDocumentACP::new(Arc::new(MemoryAcpStore::new()));

    let error = resolve_push_creator(Some(&acp), &protected_collection(), "doc1", "local-peer")
        .await
        .unwrap_err();

    assert!(matches!(error, PushCreatorError::OwnerMissing { .. }));
}

#[tokio::test]
async fn resolve_push_creator_errors_when_acp_lookup_fails() {
    let error = resolve_push_creator(
        Some(&FailingAcp),
        &protected_collection(),
        "doc1",
        "local-peer",
    )
    .await
    .unwrap_err();

    assert!(matches!(error, PushCreatorError::LookupFailed { .. }));
}

fn protected_collection() -> Collection {
    Collection::new(
        CollectionVersion::new("Users", "v1", "col1", vec![])
            .with_policy(PolicyDescription::new("policy-1", "Users")),
    )
}

struct FailingAcp;

#[async_trait::async_trait]
impl DocumentACP for FailingAcp {
    async fn register_doc_object(
        &self,
        _identity: &Did,
        _policy_id: &str,
        _resource_name: &str,
        _doc_id: &str,
    ) -> acp::Result<()> {
        Err(acp::Error::Storage("boom".to_string()))
    }

    async fn is_doc_registered(
        &self,
        _policy_id: &str,
        _resource_name: &str,
        _doc_id: &str,
    ) -> acp::Result<bool> {
        Err(acp::Error::Storage("boom".to_string()))
    }

    async fn get_doc_owner(
        &self,
        _policy_id: &str,
        _resource_name: &str,
        _doc_id: &str,
    ) -> acp::Result<Option<Did>> {
        Err(acp::Error::Storage("boom".to_string()))
    }

    async fn check_doc_access(
        &self,
        _identity: &Identity,
        _permission: DocumentPermission,
        _policy_id: &str,
        _resource_name: &str,
        _doc_id: &str,
    ) -> acp::Result<bool> {
        Err(acp::Error::Storage("boom".to_string()))
    }

    async fn add_actor_relationship(
        &self,
        _requestor: &Did,
        _target: &Did,
        _policy_id: &str,
        _collection_id: &str,
        _doc_id: &str,
        _relation: &str,
        _managing_relations: &[String],
    ) -> acp::Result<bool> {
        Err(acp::Error::Storage("boom".to_string()))
    }

    async fn delete_actor_relationship(
        &self,
        _requestor: &Did,
        _target: &Did,
        _policy_id: &str,
        _collection_id: &str,
        _doc_id: &str,
        _relation: &str,
        _managing_relations: &[String],
    ) -> acp::Result<bool> {
        Err(acp::Error::Storage("boom".to_string()))
    }

    async fn unregister_doc_object(
        &self,
        _policy_id: &str,
        _resource_name: &str,
        _doc_id: &str,
    ) -> acp::Result<()> {
        Err(acp::Error::Storage("boom".to_string()))
    }
}
