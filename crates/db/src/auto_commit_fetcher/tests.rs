use super::*;
use document::NormalValue;
use query::mutator::DocMutator;
use schema::{CollectionVersion, FieldDescription, FieldKind};
use storage::backends::MemoryStore;

use crate::doc_mutator::DbDocMutator;

fn test_schema() -> CollectionVersion {
    CollectionVersion::new(
        "Users",
        "v1",
        "col-users",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
        ],
    )
}

async fn fixture_with_docs(n: usize) -> Arc<DB<MemoryStore>> {
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

    db
}

/// The stream must be observationally identical to the eager path.
#[tokio::test]
async fn stream_matches_get_all_with_deleted_ordering_and_content() {
    let db = fixture_with_docs(5).await;
    let fetcher = AutoCommitFetcher::new(db);

    let eager = fetcher.get_all_with_deleted("Users", false).await.unwrap();

    let mut streamed = Vec::new();
    let mut stream = fetcher
        .stream_all_with_deleted("Users", false)
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

/// Partial consumption must not error and must not require draining -
/// `AutoCommitDocStream` owns its read transaction, unlike
/// `CollectionDocStream`'s own tests where the transaction outlives the
/// stream, so this exercises the `Drop` discard path specifically.
#[tokio::test]
async fn stream_may_be_dropped_after_partial_consumption() {
    let db = fixture_with_docs(20).await;
    let fetcher = AutoCommitFetcher::new(db);

    let mut stream = fetcher
        .stream_all_with_deleted("Users", false)
        .await
        .unwrap();
    for _ in 0..3 {
        assert!(stream.next().await.unwrap().is_some());
    }
    drop(stream);
}
