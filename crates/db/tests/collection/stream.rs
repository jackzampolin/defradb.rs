use crate::common::schema::test_schema;
use async_lock::Mutex;
use db::database::DB;
use db::read::doc::DbDocFetcher;
use db::write::doc::DbDocMutator;
use document::Document;
use document::NormalValue;
use query::mutator::DocMutator;
use query::runner::DocFetcher;
use std::sync::Arc;
use storage::backends::MemoryStore;

/// Create a DB with a `Users` collection and `n` committed documents,
/// named `user-0`..`user-{n-1}` in insertion order.
async fn fixture_with_docs(n: usize) -> (Arc<DB<MemoryStore>>, String) {
    let db = Arc::new(DB::new(MemoryStore::new()).unwrap());
    db.create_collection(test_schema()).await.unwrap();

    for i in 0..n {
        let txn = db.new_txn(false).await.unwrap();
        let mutator = DbDocMutator::new(db.clone(), txn);
        let mut doc = Document::new();
        doc.set("name", NormalValue::String(format!("user-{i}")));
        mutator.create("Users", doc).await.unwrap();
        let txn = mutator.take_txn().await.unwrap();
        txn.commit().await.unwrap();
    }

    (db, "Users".to_string())
}

async fn fetcher(db: &Arc<DB<MemoryStore>>) -> DbDocFetcher<MemoryStore> {
    DbDocFetcher::new(db.new_txn(true).await.unwrap())
}

/// Delete the `n`th document in insertion order.
async fn delete_nth_document(db: &Arc<DB<MemoryStore>>, collection_name: &str, n: usize) {
    let doc_id = {
        let f = fetcher(db).await;
        let docs = f.get_all(collection_name).await.unwrap();
        docs[n].id().unwrap().clone()
    };

    let txn = db.new_txn(false).await.unwrap();
    let mutator = DbDocMutator::new(db.clone(), txn);
    mutator.delete(collection_name, &doc_id).await.unwrap();
    let txn = mutator.take_txn().await.unwrap();
    txn.commit().await.unwrap();
}

/// Overwrite the `n`th document's blob (in insertion order) with bytes
/// that fail to decode as CBOR.
async fn corrupt_nth_document(db: &Arc<DB<MemoryStore>>, collection_name: &str, n: usize) {
    let txn = db.new_txn(false).await.unwrap();
    let shared = Arc::new(Mutex::new(Some(txn)));
    {
        let (collection, datastore, systemstore) =
            db::collection::loader::get_collection_with_lazy_load(&shared, collection_name)
                .await
                .unwrap();
        let entries = collection
            .get_all_with_datastore_short_ids(&datastore, &systemstore, false)
            .await
            .unwrap();
        let doc_short_id = entries[n].0;
        datastore
            .set(&collection.doc_key(doc_short_id), b"not valid cbor")
            .await
            .unwrap();
    }

    let txn = shared.lock().await.take().unwrap();
    txn.commit().await.unwrap();
}

/// The stream must be observationally identical to the eager path.
#[tokio::test]
async fn stream_matches_get_all_with_deleted_ordering_and_content() {
    let (db, collection_name) = fixture_with_docs(5).await;
    let fetcher = fetcher(&db).await;

    let eager = fetcher
        .get_all_with_deleted(&collection_name, false)
        .await
        .unwrap();

    let mut streamed = Vec::new();
    let mut stream = fetcher
        .stream_all_with_deleted(&collection_name, false)
        .await
        .unwrap();
    while let Some(pair) = stream.next().await.unwrap() {
        streamed.push(pair);
    }

    assert_eq!(streamed.len(), eager.len());
    for (s, e) in streamed.iter().zip(eager.iter()) {
        assert_eq!(s.0.id(), e.0.id());
        assert_eq!(s.1, e.1);
    }
}

/// Deleted documents interleaved must be skipped identically.
#[tokio::test]
async fn stream_skips_deleted_when_not_showing_deleted() {
    let (db, collection_name) = fixture_with_docs(5).await;
    delete_nth_document(&db, &collection_name, 1).await;
    delete_nth_document(&db, &collection_name, 3).await;
    let fetcher = fetcher(&db).await;

    let mut streamed = Vec::new();
    let mut stream = fetcher
        .stream_all_with_deleted(&collection_name, false)
        .await
        .unwrap();
    while let Some(pair) = stream.next().await.unwrap() {
        streamed.push(pair);
    }

    assert_eq!(streamed.len(), 3);
    assert!(streamed.iter().all(|(_, deleted)| !deleted));
}

/// Partial consumption must not error and must not require draining.
#[tokio::test]
async fn stream_may_be_dropped_after_partial_consumption() {
    let (db, collection_name) = fixture_with_docs(100).await;
    let fetcher = fetcher(&db).await;

    let mut stream = fetcher
        .stream_all_with_deleted(&collection_name, false)
        .await
        .unwrap();
    for _ in 0..3 {
        assert!(stream.next().await.unwrap().is_some());
    }
    drop(stream);
}

/// A resolution error (not just a raw iterator error) must fuse the
/// stream: polling again after the error must not silently resume from
/// the next entry.
#[tokio::test]
async fn stream_fuses_after_resolution_error() {
    let (db, collection_name) = fixture_with_docs(3).await;
    corrupt_nth_document(&db, &collection_name, 0).await;
    let fetcher = fetcher(&db).await;

    let mut stream = fetcher
        .stream_all_with_deleted(&collection_name, false)
        .await
        .unwrap();

    assert!(stream.next().await.is_err());
    assert!(stream.next().await.unwrap().is_none());
}
