use async_lock::Mutex as TokioMutex;
use db::read::versioned::*;
use std::sync::Arc;

#[test]
fn test_looks_like_cidv1() {
    assert!(
        VersionedFetcher::<storage::backends::memory::MemoryStore>::looks_like_cidv1(
            "bafyreiajq6jmyblg2b6vupjdapzkaodbt7kkwqp4fijekdvydnyxvr4y7q"
        )
    );
    assert!(
        VersionedFetcher::<storage::backends::memory::MemoryStore>::looks_like_cidv1(
            "bafybeid57gpbwi4i6bg7g35hhhhhhhhhhhhhhhhhhhhhhhdoesnotexist"
        )
    );
    assert!(
        !VersionedFetcher::<storage::backends::memory::MemoryStore>::looks_like_cidv1(
            "fhbnjfahfhfhanfhga"
        )
    );
    assert!(!VersionedFetcher::<storage::backends::memory::MemoryStore>::looks_like_cidv1("short"));
}
#[tokio::test]
async fn kms_identity_prefers_caller_then_task_then_thread() {
    let txn = Arc::new(TokioMutex::new(None));
    let caller = identity::Did::new("did:key:caller").unwrap();
    let fetcher = VersionedFetcher::<storage::backends::memory::MemoryStore>::with_kms(
        txn.clone(),
        None,
        Some(caller.clone()),
    );
    let ambient =
        VersionedFetcher::<storage::backends::memory::MemoryStore>::with_kms(txn, None, None);
    let _thread =
        defra_core::current_identity::scoped_current_identity(Some("did:key:thread".into()));

    defra_core::current_identity::with_scoped_identity(Some("did:key:task".into()), async {
        assert_eq!(fetcher.kms_request_context().user_identity(), Some(&caller));
        assert_eq!(
            ambient.kms_request_context().user_identity(),
            Some(&identity::Did::new("did:key:task").unwrap())
        );
    })
    .await;

    assert_eq!(
        ambient.kms_request_context().user_identity(),
        Some(&identity::Did::new("did:key:thread").unwrap())
    );
}
