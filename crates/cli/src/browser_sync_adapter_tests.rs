use std::collections::HashSet;
use std::sync::Arc;

use acp::{DocumentACP, LocalDocumentACP, MemoryAcpStore};
use crypto::{Key as _, PrivateKey as _};
use defra_core::browser_sync::{BrowserSyncPull, BrowserSyncRequest};
use defra_core::signing::{set_signing_config, SigningConfig, SigningKeyType};
use document::{DocID, Document, NormalValue};
use query::mutator::DocMutator;
use schema::{CollectionVersion, FieldDescription, FieldKind, PolicyDescription};
use storage::backends::MemoryStore;

use crate::browser_sync_adapter::BrowserSyncAdapter;

/// Generate an Ed25519 identity and install it as the thread-local block
/// signing config, so documents created afterwards carry genesis blocks
/// signed by this identity. Returns the signer's DID.
fn install_signing_identity() -> String {
    let private_key = crypto::generate_ed25519().unwrap();
    let public_key = private_key.public_key();
    let did = public_key.did().unwrap();
    set_signing_config(Some(SigningConfig {
        key_type: SigningKeyType::Ed25519,
        private_key_bytes: SigningConfig::private_key_bytes_from_slice(private_key.raw()),
        public_key_bytes: public_key.raw().to_vec(),
        public_key_hex: hex::encode(public_key.raw()),
        remote_signer: None,
        signing_authorization: None,
    }));
    did
}

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

/// Create a document whose DAG cannot fit in a sync payload. Returns its
/// doc id; it deliberately cannot be loaded through `load_document`.
async fn create_oversized_document(database: &Arc<db::DB<MemoryStore>>) -> String {
    let mut document = Document::new();
    document.set(
        "name",
        "x".repeat(defra_core::browser_sync::MAX_SYNC_PAYLOAD_BYTES + 1024),
    );
    db::AutoCommitMutator::new(database.clone())
        .create("Users", document)
        .await
        .unwrap()
        .doc_id
        .to_string()
}

/// A document too large to load must not fail the page it lands on. Before the
/// per-document skip, every pull covering it returned 422 forever and the
/// cursor could never advance past it, wedging the whole sync.
#[tokio::test]
async fn pull_skips_a_document_that_exceeds_the_payload_limit() {
    let database = Arc::new(db::DB::new(MemoryStore::new()).unwrap());
    database
        .create_collection(users_schema(false))
        .await
        .unwrap();
    let oversized = create_oversized_document(&database).await;
    create_document(&database, "Alice").await;

    let acp = Arc::new(LocalDocumentACP::new(Arc::new(MemoryAcpStore::new())));
    let adapter = BrowserSyncAdapter::new_arc(database, acp);

    let mut cursor = None;
    let mut seen: Vec<String> = Vec::new();
    for _ in 0..8 {
        let page = adapter
            .sync(
                BrowserSyncRequest {
                    documents: Vec::new(),
                    pull: Some(BrowserSyncPull {
                        doc_ids: Vec::new(),
                        cursor: cursor.clone(),
                        limit: Some(1),
                    }),
                },
                None,
                false,
            )
            .await
            .expect("an oversized document must not fail the pull");
        seen.extend(page.documents.iter().map(|doc| doc.doc_id.clone()));
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }

    assert!(
        !seen.contains(&oversized),
        "oversized document should have been skipped, got {seen:?}"
    );
    assert_eq!(
        seen.len(),
        1,
        "the loadable document should still be served, got {seen:?}"
    );
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
async fn sync_registers_only_new_signed_documents() {
    let source = Arc::new(db::DB::new(MemoryStore::new()).unwrap());
    let target = Arc::new(db::DB::new(MemoryStore::new()).unwrap());
    source.create_collection(users_schema(true)).await.unwrap();
    target.create_collection(users_schema(true)).await.unwrap();
    let public_document = create_document(&source, "Public").await;
    let owner = install_signing_identity();
    let protected_document = create_document(&source, "Protected").await;
    set_signing_config(None);
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

    adapter
        .sync(
            BrowserSyncRequest {
                documents: vec![public_document, protected_document.clone()],
                pull: None,
            },
            Some(&owner),
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
        Some(owner)
    );
}

#[tokio::test]
async fn foreign_signed_document_cannot_be_squatted_by_pushing_caller() {
    let source = Arc::new(db::DB::new(MemoryStore::new()).unwrap());
    let target = Arc::new(db::DB::new(MemoryStore::new()).unwrap());
    source.create_collection(users_schema(true)).await.unwrap();
    target.create_collection(users_schema(true)).await.unwrap();

    // Alice authors and signs the document in her browser node.
    let alice = install_signing_identity();
    let document = create_document(&source, "Alice").await;
    set_signing_config(None);
    let doc_id = document.doc_id.clone();

    // Bob obtains Alice's DAG and pushes it to a server that has never seen
    // the document. Bob must not become the registered ACP owner.
    let acp = Arc::new(LocalDocumentACP::new(Arc::new(MemoryAcpStore::new())));
    let adapter = BrowserSyncAdapter::new_arc(target, acp.clone());
    let bob = "did:key:z6MkpTHR8VNsBxYAAWHut2Geadd9jSwuBV8xRoAnwWsdvktH";
    adapter
        .sync(
            BrowserSyncRequest {
                documents: vec![document],
                pull: None,
            },
            Some(bob),
            false,
        )
        .await
        .unwrap();

    let owner = acp
        .get_doc_owner("users-policy", "users", &doc_id)
        .await
        .unwrap()
        .map(|did| did.to_string());
    assert_ne!(
        owner.as_deref(),
        Some(bob),
        "pushing caller must not squat ownership of a foreign-signed document"
    );
    assert_eq!(
        owner.as_deref(),
        Some(alice.as_str()),
        "ownership must be registered to the verified genesis creator"
    );
}

#[tokio::test]
async fn unsigned_document_stays_unregistered_for_authenticated_caller() {
    let source = Arc::new(db::DB::new(MemoryStore::new()).unwrap());
    let target = Arc::new(db::DB::new(MemoryStore::new()).unwrap());
    source.create_collection(users_schema(true)).await.unwrap();
    target.create_collection(users_schema(true)).await.unwrap();

    // No signing config: the genesis block carries no verifiable creator.
    let document = create_document(&source, "Unsigned").await;
    let doc_id = document.doc_id.clone();

    let acp = Arc::new(LocalDocumentACP::new(Arc::new(MemoryAcpStore::new())));
    let adapter = BrowserSyncAdapter::new_arc(target, acp.clone());
    let caller = "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK";
    adapter
        .sync(
            BrowserSyncRequest {
                documents: vec![document],
                pull: None,
            },
            Some(caller),
            false,
        )
        .await
        .unwrap();

    // Matches the Local ACP replication convention: without a verified
    // creator the document is not registered (unregistered == public).
    assert!(!acp
        .is_doc_registered("users-policy", "users", &doc_id)
        .await
        .unwrap());
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
    let owner = install_signing_identity();
    let document = create_document(&source, "Protected").await;
    set_signing_config(None);

    let acp = Arc::new(LocalDocumentACP::new(Arc::new(MemoryAcpStore::new())));
    let adapter = BrowserSyncAdapter::new_arc(target, acp);
    adapter
        .sync(
            BrowserSyncRequest {
                documents: vec![document.clone()],
                pull: None,
            },
            Some(&owner),
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

/// #1188 skips the oversized document, but the property that matters is that
/// pagination still reaches documents ordered *after* it. Doc IDs are
/// content-derived, so this fixture asserts the oversized document actually
/// sits between two loadable ones before checking that both are served.
#[tokio::test]
async fn pull_reaches_documents_ordered_after_an_oversized_document() {
    let database = Arc::new(db::DB::new(MemoryStore::new()).unwrap());
    database
        .create_collection(users_schema(false))
        .await
        .unwrap();

    let oversized = create_oversized_document(&database).await;
    let mut loadable = Vec::new();
    for name in ["Alice", "Bob", "Carol", "Dave", "Erin", "Frank"] {
        loadable.push(create_document(&database, name).await.doc_id);
    }
    let before: Vec<_> = loadable.iter().filter(|id| **id < oversized).collect();
    let after: Vec<_> = loadable.iter().filter(|id| **id > oversized).collect();
    assert!(
        !before.is_empty() && !after.is_empty(),
        "fixture must straddle the oversized document: before={before:?} after={after:?}"
    );

    let acp = Arc::new(LocalDocumentACP::new(Arc::new(MemoryAcpStore::new())));
    let adapter = BrowserSyncAdapter::new_arc(database, acp);

    let mut cursor = None;
    let mut seen: Vec<String> = Vec::new();
    for _ in 0..32 {
        let page = adapter
            .sync(
                BrowserSyncRequest {
                    documents: Vec::new(),
                    pull: Some(BrowserSyncPull {
                        doc_ids: Vec::new(),
                        cursor: cursor.clone(),
                        limit: Some(1),
                    }),
                },
                None,
                false,
            )
            .await
            .expect("an oversized document must not fail the pull");
        seen.extend(page.documents.iter().map(|doc| doc.doc_id.clone()));
        match page.next_cursor {
            Some(next) => {
                assert_ne!(Some(&next), cursor.as_ref(), "cursor must advance");
                cursor = Some(next);
            }
            None => break,
        }
    }

    seen.sort();
    let mut expected = loadable.clone();
    expected.sort();
    assert_eq!(
        seen, expected,
        "every loadable document must be served, including those after the oversized one"
    );
    assert!(!seen.contains(&oversized));
}

/// A DAG exceeding `MAX_SYNC_BLOCKS_PER_DOCUMENT` must be classified as
/// `TooLarge` so the permanent-document skip handles it instead of wedging the
/// page. Block count grows at 2 blocks per single-field update, so the 4096
/// limit is reached after ~2047 updates while the payload is still under 1 MB
/// — a more reachable trigger than the 16 MiB cap.
///
/// Ignored by default: building the DAG takes ~50s. Run with
/// `cargo test -p cli --lib block_heavy_document -- --ignored`.
#[tokio::test]
#[ignore]
async fn pull_skips_a_block_heavy_document_and_keeps_paginating() {
    let database = Arc::new(db::DB::new(MemoryStore::new()).unwrap());
    database
        .create_collection(users_schema(false))
        .await
        .unwrap();

    let mut document = Document::new();
    document.set("name", "seed");
    let heavy = db::AutoCommitMutator::new(database.clone())
        .create("Users", document)
        .await
        .unwrap()
        .doc_id
        .to_string();
    for index in 0..2_100 {
        update_document(&database, &heavy, "name", &format!("v{index}")).await;
    }

    let engine = db_merge::BrowserSyncEngine::new(database.clone());
    let heavy_ref = engine.document_ref(&heavy).await.unwrap().unwrap();
    let load_error = engine.load_document(&heavy_ref).await.unwrap_err();
    assert!(
        matches!(load_error, db_merge::BrowserSyncError::TooLarge(ref m) if m.contains("block count")),
        "fixture must trip the block-count limit, got {load_error:?}"
    );

    let mut loadable = Vec::new();
    for name in ["Alice", "Bob", "Carol", "Dave", "Erin", "Frank"] {
        loadable.push(create_document(&database, name).await.doc_id);
    }
    assert!(
        loadable.iter().any(|id| *id > heavy),
        "fixture needs a document ordered after the block-heavy one"
    );

    let acp = Arc::new(LocalDocumentACP::new(Arc::new(MemoryAcpStore::new())));
    let adapter = BrowserSyncAdapter::new_arc(database, acp);

    let mut cursor: Option<String> = None;
    let mut seen: Vec<String> = Vec::new();
    for _ in 0..32 {
        let page = adapter
            .sync(
                BrowserSyncRequest {
                    documents: Vec::new(),
                    pull: Some(BrowserSyncPull {
                        doc_ids: Vec::new(),
                        cursor: cursor.clone(),
                        limit: Some(1),
                    }),
                },
                None,
                false,
            )
            .await
            .expect("a block-heavy document must not fail the pull");
        seen.extend(page.documents.iter().map(|doc| doc.doc_id.clone()));
        match page.next_cursor {
            Some(next) => {
                assert_ne!(Some(&next), cursor.as_ref(), "cursor must advance");
                cursor = Some(next);
            }
            None => break,
        }
    }

    seen.sort();
    let mut expected = loadable.clone();
    expected.sort();
    assert_eq!(
        seen, expected,
        "every loadable document must be served, including those after the block-heavy one"
    );
    assert!(!seen.contains(&heavy));
}
