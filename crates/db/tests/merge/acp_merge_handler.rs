use acp::LocalDocumentACP;
use acp::MemoryAcpStore;
use db::merge::acp_merge_handler::*;
use db::merge::merge_handler::hook::CompositeMergeHook;
use defra_core::merge::BlockMetadata;
use defra_core::merge::MergeOutcome;
use schema::CollectionVersion;
use schema::PolicyDescription;
use std::sync::Arc;

fn protected_collection() -> CollectionVersion {
    CollectionVersion::new("Users", "v1", "col1", vec![])
        .with_policy(PolicyDescription::new("policy-1", "users"))
}

fn hook(strict: bool) -> AcpCompositeMergeHook {
    let hook = AcpCompositeMergeHook::new(None);
    hook.set_document_acp(Arc::new(LocalDocumentACP::new(Arc::new(
        MemoryAcpStore::new(),
    ))));
    hook.set_strict_replicated_doc_access(strict);
    hook
}

#[tokio::test]
async fn local_acp_allows_unregistered_encrypted_document() {
    let result = hook(false)
        .on_encrypted_link(
            "doc1",
            &protected_collection(),
            &BlockMetadata::normal("doc1", "col1", "creator", Some("peer"), false),
        )
        .await
        .unwrap();

    assert_eq!(result, None);
}

#[tokio::test]
async fn strict_acp_retries_unregistered_encrypted_document() {
    let result = hook(true)
        .on_encrypted_link(
            "doc1",
            &protected_collection(),
            &BlockMetadata::normal("doc1", "col1", "creator", Some("peer"), false),
        )
        .await
        .unwrap();

    assert_eq!(
        result,
        Some(MergeOutcome::retryable_skip(
            "encrypted replicated document is not yet registered in local ACP",
        ))
    );
}
