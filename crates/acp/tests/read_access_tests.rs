use std::sync::{Arc, Mutex};

use acp::read_access::{check_doc_read_access, DocAccess, ObjectAccessChecker};
use async_trait::async_trait;

struct FakeChecker {
    doc_access: DocAccess,
    collection_access: DocAccess,
    calls: Arc<Mutex<Vec<String>>>,
}

impl FakeChecker {
    fn new(doc_access: DocAccess, collection_access: DocAccess) -> Self {
        Self {
            doc_access,
            collection_access,
            calls: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }
}

#[async_trait]
impl ObjectAccessChecker for FakeChecker {
    async fn object_access(
        &self,
        _policy_id: &str,
        _resource_name: &str,
        object_id: &str,
    ) -> acp::Result<DocAccess> {
        self.calls.lock().unwrap().push(object_id.to_string());
        Ok(if object_id == "col1" {
            self.collection_access
        } else {
            self.doc_access
        })
    }
}

#[tokio::test]
async fn branchable_public_doc_requires_collection_read() {
    let checker = FakeChecker::new(
        DocAccess {
            has_access: true,
            explicit: false,
        },
        DocAccess {
            has_access: false,
            explicit: true,
        },
    );

    let allowed = check_doc_read_access(&checker, "policy1", "resource1", "col1", true, "doc1")
        .await
        .unwrap();

    assert!(!allowed);
    assert_eq!(checker.calls(), vec!["doc1", "col1"]);
}

#[tokio::test]
async fn branchable_explicit_doc_grant_wins_over_collection_denial() {
    let checker = FakeChecker::new(
        DocAccess {
            has_access: true,
            explicit: true,
        },
        DocAccess {
            has_access: false,
            explicit: true,
        },
    );

    let allowed = check_doc_read_access(&checker, "policy1", "resource1", "col1", true, "doc1")
        .await
        .unwrap();

    assert!(allowed);
    assert_eq!(checker.calls(), vec!["doc1"]);
}

#[tokio::test]
async fn non_branchable_public_doc_does_not_check_collection() {
    let checker = FakeChecker::new(
        DocAccess {
            has_access: true,
            explicit: false,
        },
        DocAccess {
            has_access: false,
            explicit: true,
        },
    );

    let allowed = check_doc_read_access(&checker, "policy1", "resource1", "col1", false, "doc1")
        .await
        .unwrap();

    assert!(allowed);
    assert_eq!(checker.calls(), vec!["doc1"]);
}

#[tokio::test]
async fn branchable_collection_level_commit_checks_collection_object() {
    let checker = FakeChecker::new(
        DocAccess {
            has_access: false,
            explicit: true,
        },
        DocAccess {
            has_access: true,
            explicit: true,
        },
    );

    let allowed = check_doc_read_access(&checker, "policy1", "resource1", "col1", true, "")
        .await
        .unwrap();

    assert!(allowed);
    assert_eq!(checker.calls(), vec!["col1"]);
}
