use std::collections::HashSet;
use std::sync::Arc;

use acp::{DocumentACP, LocalDocumentACP, MemoryAcpStore};
use defra_core::browser_sync::{BrowserSyncPull, BrowserSyncRequest};
use document::{DocID, Document, NormalValue};
use query::mutator::DocMutator;
use schema::{CollectionVersion, FieldDescription, FieldKind, PolicyDescription};
use storage::backends::MemoryStore;

use crate::browser_sync_adapter::BrowserSyncAdapter;

fn users_schema(policy: bool) -> CollectionVersion {
    let mut schema = CollectionVersion::new(
        "Users",
        "users-version",
        "users-collection",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            FieldDescription::new("3", "email", FieldKind::string()),
        ],
    );
    if policy {
        schema.policy = Some(PolicyDescription::new("users-policy", "users"));
    }
    schema
}

async fn update_document(
    database: &Arc<db::DB<MemoryStore>>,
    doc_id: &str,
    field: &str,
    value: &str,
) {
    let mut document = Document::new();
    document.set_id(DocID::from_string(doc_id).unwrap());
    document.set(field, value);
    db::AutoCommitMutator::new(database.clone())
        .update("Users", document, HashSet::from([field.to_string()]))
        .await
        .unwrap();
}

async fn read_document(database: &Arc<db::DB<MemoryStore>>, doc_id: &str) -> Document {
    let txn = database.new_txn(true).await.unwrap();
    database
        .get_collection("Users")
        .unwrap()
        .unwrap()
        .get_by_doc_id(
            &txn.datastore().unwrap(),
            &txn.systemstore().unwrap(),
            &DocID::from_string(doc_id).unwrap(),
        )
        .await
        .unwrap()
        .unwrap()
}

async fn create_document(
    database: &Arc<db::DB<MemoryStore>>,
    name: &str,
) -> defra_core::browser_sync::BrowserSyncDocument {
    let mut document = Document::new();
    document.set("name", name);
    let created = db::AutoCommitMutator::new(database.clone())
        .create("Users", document)
        .await
        .unwrap();
    let doc_id = created.doc_id.to_string();
    let engine = db_merge::BrowserSyncEngine::new(database.clone());
    let document_ref = engine.document_ref(&doc_id).await.unwrap().unwrap();
    engine.load_document(&document_ref).await.unwrap().unwrap()
}

#[tokio::test]
async fn pull_uses_advancing_cursor_pages() {
    let database = Arc::new(db::DB::new(MemoryStore::new()).unwrap());
    database
        .create_collection(users_schema(false))
        .await
        .unwrap();
    create_document(&database, "Alice").await;
    create_document(&database, "Bob").await;

    let acp = Arc::new(LocalDocumentACP::new(Arc::new(MemoryAcpStore::new())));
    let adapter = BrowserSyncAdapter::new_arc(database, acp);
    let first = adapter
        .sync(
            BrowserSyncRequest {
                documents: Vec::new(),
                pull: Some(BrowserSyncPull {
                    doc_ids: Vec::new(),
                    cursor: None,
                    limit: Some(1),
                }),
            },
            None,
            false,
        )
        .await
        .unwrap();
    assert_eq!(first.documents.len(), 1);
    let cursor = first.next_cursor.expect("first page has a continuation");

    let second = adapter
        .sync(
            BrowserSyncRequest {
                documents: Vec::new(),
                pull: Some(BrowserSyncPull {
                    doc_ids: Vec::new(),
                    cursor: Some(cursor),
                    limit: Some(1),
                }),
            },
            None,
            false,
        )
        .await
        .unwrap();
    assert_eq!(second.documents.len(), 1);
    assert!(second.next_cursor.is_none());
}

#[tokio::test]
async fn sync_registers_only_new_authenticated_documents() {
    let source = Arc::new(db::DB::new(MemoryStore::new()).unwrap());
    let target = Arc::new(db::DB::new(MemoryStore::new()).unwrap());
    source.create_collection(users_schema(true)).await.unwrap();
    target.create_collection(users_schema(true)).await.unwrap();
    let public_document = create_document(&source, "Public").await;
    let protected_document = create_document(&source, "Protected").await;
    let public_doc_id = public_document.doc_id.clone();
    let protected_doc_id = protected_document.doc_id.clone();

    let acp = Arc::new(LocalDocumentACP::new(Arc::new(MemoryAcpStore::new())));
    let adapter = BrowserSyncAdapter::new_arc(target, acp.clone());
    adapter
        .sync(
            BrowserSyncRequest {
                documents: vec![public_document.clone()],
                pull: None,
            },
            None,
            false,
        )
        .await
        .unwrap();

    let owner = "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK";
    adapter
        .sync(
            BrowserSyncRequest {
                documents: vec![public_document, protected_document.clone()],
                pull: None,
            },
            Some(owner),
            false,
        )
        .await
        .unwrap();

    assert!(!acp
        .is_doc_registered("users-policy", "users", &public_doc_id)
        .await
        .unwrap());
    assert!(acp
        .is_doc_registered("users-policy", "users", &protected_doc_id)
        .await
        .unwrap());
    assert_eq!(
        acp.get_doc_owner("users-policy", "users", &protected_doc_id)
            .await
            .unwrap()
            .map(|did| did.to_string()),
        Some(owner.to_string())
    );
}

#[tokio::test]
async fn invalid_batch_is_rejected_before_any_document_is_written() {
    let source = Arc::new(db::DB::new(MemoryStore::new()).unwrap());
    let target = Arc::new(db::DB::new(MemoryStore::new()).unwrap());
    source.create_collection(users_schema(false)).await.unwrap();
    target.create_collection(users_schema(false)).await.unwrap();
    let valid = create_document(&source, "Valid").await;
    let valid_doc_id = valid.doc_id.clone();
    let mut invalid = create_document(&source, "Invalid").await;
    invalid.doc_id = "bae-forged".into();

    let acp = Arc::new(LocalDocumentACP::new(Arc::new(MemoryAcpStore::new())));
    let adapter = BrowserSyncAdapter::new_arc(target.clone(), acp);
    assert!(adapter
        .sync(
            BrowserSyncRequest {
                documents: vec![valid, invalid],
                pull: None,
            },
            None,
            false,
        )
        .await
        .is_err());

    assert!(db_merge::BrowserSyncEngine::new(target)
        .document_ref(&valid_doc_id)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn duplicate_document_batch_is_rejected_before_merge() {
    let source = Arc::new(db::DB::new(MemoryStore::new()).unwrap());
    let target = Arc::new(db::DB::new(MemoryStore::new()).unwrap());
    source.create_collection(users_schema(false)).await.unwrap();
    target.create_collection(users_schema(false)).await.unwrap();
    let document = create_document(&source, "Alice").await;
    let doc_id = document.doc_id.clone();

    let acp = Arc::new(LocalDocumentACP::new(Arc::new(MemoryAcpStore::new())));
    let adapter = BrowserSyncAdapter::new_arc(target.clone(), acp);
    assert!(adapter
        .sync(
            BrowserSyncRequest {
                documents: vec![document.clone(), document],
                pull: None,
            },
            None,
            false,
        )
        .await
        .is_err());

    assert!(db_merge::BrowserSyncEngine::new(target)
        .document_ref(&doc_id)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn protected_document_rejects_updates_from_another_identity() {
    let source = Arc::new(db::DB::new(MemoryStore::new()).unwrap());
    let target = Arc::new(db::DB::new(MemoryStore::new()).unwrap());
    source.create_collection(users_schema(true)).await.unwrap();
    target.create_collection(users_schema(true)).await.unwrap();
    let document = create_document(&source, "Protected").await;

    let acp = Arc::new(LocalDocumentACP::new(Arc::new(MemoryAcpStore::new())));
    let adapter = BrowserSyncAdapter::new_arc(target, acp);
    let owner = "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK";
    adapter
        .sync(
            BrowserSyncRequest {
                documents: vec![document.clone()],
                pull: None,
            },
            Some(owner),
            false,
        )
        .await
        .unwrap();

    let other = "did:key:z6MkpTHR8VNsBxYAAWHut2Geadd9jSwuBV8xRoAnwWsdvktH";
    let error = adapter
        .sync(
            BrowserSyncRequest {
                documents: vec![document.clone()],
                pull: None,
            },
            Some(other),
            false,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        defra_http::router::BrowserSyncError::Forbidden(_)
    ));

    adapter
        .sync(
            BrowserSyncRequest {
                documents: vec![document],
                pull: None,
            },
            Some(other),
            true,
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn concurrent_changes_converge_through_push_pull_exchange() {
    let browser = Arc::new(db::DB::new(MemoryStore::new()).unwrap());
    let server = Arc::new(db::DB::new(MemoryStore::new()).unwrap());
    browser
        .create_collection(users_schema(false))
        .await
        .unwrap();
    server.create_collection(users_schema(false)).await.unwrap();
    let initial = create_document(&browser, "Alice").await;
    let doc_id = initial.doc_id.clone();

    let acp = Arc::new(LocalDocumentACP::new(Arc::new(MemoryAcpStore::new())));
    let adapter = BrowserSyncAdapter::new_arc(server.clone(), acp);
    adapter
        .sync(
            BrowserSyncRequest {
                documents: vec![initial],
                pull: None,
            },
            None,
            false,
        )
        .await
        .unwrap();

    update_document(&browser, &doc_id, "name", "Alice Browser").await;
    update_document(&server, &doc_id, "email", "alice@example.com").await;
    let browser_engine = db_merge::BrowserSyncEngine::new(browser.clone());
    let browser_ref = browser_engine.document_ref(&doc_id).await.unwrap().unwrap();
    let browser_update = browser_engine
        .load_document(&browser_ref)
        .await
        .unwrap()
        .unwrap();

    let response = adapter
        .sync(
            BrowserSyncRequest {
                documents: vec![browser_update],
                pull: Some(BrowserSyncPull {
                    doc_ids: vec![doc_id.clone()],
                    cursor: None,
                    limit: Some(1),
                }),
            },
            None,
            false,
        )
        .await
        .unwrap();
    assert_eq!(response.documents.len(), 1);
    browser_engine
        .apply_document(&response.documents[0], "server")
        .await
        .unwrap();

    for document in [
        read_document(&browser, &doc_id).await,
        read_document(&server, &doc_id).await,
    ] {
        assert_eq!(
            document.get("name"),
            Some(&NormalValue::String("Alice Browser".into()))
        );
        assert_eq!(
            document.get("email"),
            Some(&NormalValue::String("alice@example.com".into()))
        );
    }
}
