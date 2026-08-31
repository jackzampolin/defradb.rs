//! Collection-level head coverage for the P2P head provider.
//!
//! `BranchableSync` serves whatever `get_collection_heads` returns. A
//! branchable collection that has committed documents must therefore expose
//! its collection heads — otherwise every sync response is empty and a peer
//! can never pull the collection.

use db::merge::head_provider::DbHeadProvider;
use db::AutoCommitMutator;
use db::DB;
use document::Document;
use p2p::sync::DocumentHeadProvider;
use query::mutator::DocMutator;
use schema::{CollectionVersion, FieldDescription, FieldKind};
use std::sync::Arc;
use storage::corekv::{IterOptions, Store};
use storage::RegolithStore;

fn branchable_schema() -> CollectionVersion {
    CollectionVersion::new(
        "Transcript",
        "v1",
        "col-transcript",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "body", FieldKind::string()),
        ],
    )
    .as_branchable()
}

/// Every raw store key that textually contains a collection-head segment, so
/// a failure shows where heads actually landed (or that none were written).
async fn raw_collection_head_keys(store: &Arc<RegolithStore>) -> Vec<String> {
    let txn = store.new_txn(true).await.unwrap();
    let mut iter = txn.iterator(IterOptions::new()).await.unwrap();
    let mut keys = Vec::new();
    while let Some(pair) = iter.next().await.unwrap() {
        let key = String::from_utf8_lossy(&pair.key).into_owned();
        if key.contains("/c/") {
            keys.push(key);
        }
    }
    keys
}

#[tokio::test]
async fn branchable_collection_writes_expose_collection_heads() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let db = Arc::new(DB::from_arc(store.clone()).unwrap());
    db.create_collection(branchable_schema()).await.unwrap();

    let mutator = AutoCommitMutator::new(db.clone());
    let mut doc = Document::new();
    doc.set("body", "first".to_string());
    mutator.create("Transcript", doc).await.unwrap();

    let raw = raw_collection_head_keys(&store).await;
    let provider = DbHeadProvider::new(db);
    let heads = provider
        .get_collection_heads("col-transcript")
        .await
        .unwrap();
    assert!(
        !heads.is_empty(),
        "a branchable collection with one committed document must expose \
         collection heads to BranchableSync; raw '/c/' keys in the store: {raw:?}"
    );
}
