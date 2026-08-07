//! A limited query must read keys proportional to the limit, not to the
//! collection.
//!
//! `docFetches` in `explain(execute)` cannot show this: it counts documents
//! yielded by `ScanNode`, which a `LimitNode` already capped before the scan
//! became lazy. A document-level counter sitting between the plan and the
//! fetcher can't show it either: a fetcher that materializes the whole
//! collection internally and hands back a `LIMIT`-sized slice still looks
//! proportional from up there. [`CountingStore`] sits below the fetcher, at
//! the `Store` API boundary, and counts keys pulled through that trait.
//! Whatever a backend does to refill its own window (chunked snapshots,
//! buffered scans, ...) happens inside its `Iterator` implementation, below
//! this boundary, and is not observed here.

use document::Document;
use query::{DocMutator, QueryExecutor, QueryRequest};
use schema::{CollectionVersion, FieldDescription, FieldKind};
use std::sync::Arc;
use storage::corekv::Store;
use storage::MemoryStore;

use crate::counting_store::CountingStore;
use crate::{AutoCommitMutator, LensedAutoCommitFetcher, DB};

const COLLECTION_SIZE: usize = 2000;
const LIMIT: usize = 10;

fn users_schema() -> CollectionVersion {
    let mut schema = CollectionVersion::new(
        "Users",
        "users-v1",
        "users-collection",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
        ],
    );
    schema.is_materialized = true;
    schema
}

async fn seeded_db<S: Store + 'static>(store: S) -> Arc<DB<CountingStore<S>>> {
    let db = Arc::new(DB::new(CountingStore::new(store)).unwrap());
    db.create_collection(users_schema()).await.unwrap();

    let docs = (0..COLLECTION_SIZE)
        .map(|i| {
            let mut doc = Document::new();
            doc.set("name", document::NormalValue::String(format!("User{i}")));
            doc
        })
        .collect();
    AutoCommitMutator::new(db.clone())
        .create_many("Users", docs)
        .await
        .unwrap();
    db
}

/// A `limit` query must not read the whole collection from storage.
///
/// `keys_read` bounds the scan: a lazy iterator stops pulling keys once the
/// limit is satisfied, well short of the rest of a
/// `COLLECTION_SIZE`-document collection. `point_gets` bounds document
/// assembly, which does a couple of point lookups (`is_deleted`,
/// `load_version`) per document.
async fn assert_limit_query_reads_keys_proportional_to_the_limit<S: Store + 'static>(store: S) {
    let db = seeded_db(store).await;
    let keys_before = db.store().keys_read();
    let gets_before = db.store().point_gets();

    // `with_provider` rather than a registry: an implicit read transaction
    // would route the query to the registry's own fetcher, past the counter.
    let runner = query::QueryRunner::with_provider(
        LensedAutoCommitFetcher::new(db.clone()),
        crate::DbCollectionProvider::new_arc(db.clone()),
    );

    let response = runner
        .execute(QueryRequest::new(format!(
            "query {{ Users(limit: {LIMIT}) {{ name }} }}"
        )))
        .await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);

    let returned = response.data.as_ref().unwrap()["Users"]
        .as_array()
        .unwrap()
        .len();
    assert_eq!(returned, LIMIT);

    let keys = db.store().keys_read() - keys_before;
    assert!(
        keys <= LIMIT * 2,
        "a limit-{LIMIT} query read {keys} keys from storage across a \
         {COLLECTION_SIZE}-document collection (observed: 11 on all four backends); a lazy scan reads a \
         number of keys proportional to the limit"
    );

    let gets = db.store().point_gets() - gets_before;
    assert!(
        gets <= LIMIT * 3,
        "a limit-{LIMIT} query performed {gets} point lookups (observed: 23 \
         on all four backends); document assembly must scale with the \
         limit, not the collection"
    );
}

#[tokio::test]
async fn limit_query_reads_keys_proportional_to_the_limit() {
    assert_limit_query_reads_keys_proportional_to_the_limit(MemoryStore::new()).await;
}

#[cfg(feature = "redb")]
#[tokio::test]
async fn limit_query_reads_keys_proportional_to_the_limit_redb() {
    let dir = tempfile::tempdir().unwrap();
    assert_limit_query_reads_keys_proportional_to_the_limit(
        storage::RedbStore::open(dir.path()).unwrap(),
    )
    .await;
}

#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn limit_query_reads_keys_proportional_to_the_limit_rocksdb() {
    let dir = tempfile::tempdir().unwrap();
    assert_limit_query_reads_keys_proportional_to_the_limit(
        storage::RocksDbStore::open(dir.path()).unwrap(),
    )
    .await;
}

#[cfg(feature = "fjall")]
#[tokio::test]
async fn limit_query_reads_keys_proportional_to_the_limit_fjall() {
    let dir = tempfile::tempdir().unwrap();
    assert_limit_query_reads_keys_proportional_to_the_limit(
        storage::FjallStore::open(dir.path()).unwrap(),
    )
    .await;
}

/// `explain(execute)` must not materialize the collection from storage
/// either.
///
/// `docFetches` cannot show this: it counts documents yielded by `ScanNode`,
/// which `LimitNode` already capped, so it reads the same whether the source
/// streams or was pre-materialized. `keys_read` and `point_gets` sit below
/// the fetcher, at the `Store` API boundary, so they observe what explain
/// actually pulled through that trait — not what any backend read from
/// disk internally.
async fn assert_explain_execute_reads_keys_proportional_to_the_limit<S: Store + 'static>(store: S) {
    let db = seeded_db(store).await;
    let keys_before = db.store().keys_read();
    let gets_before = db.store().point_gets();

    let runner = query::QueryRunner::with_provider(
        LensedAutoCommitFetcher::new(db.clone()),
        crate::DbCollectionProvider::new_arc(db.clone()),
    );

    let response = runner
        .execute(QueryRequest::new(format!(
            "query @explain(type: execute) {{ Users(limit: {LIMIT}) {{ name }} }}"
        )))
        .await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);

    let keys = db.store().keys_read() - keys_before;
    assert!(
        keys <= LIMIT * 2,
        "explain(execute) on a limit-{LIMIT} query read {keys} keys from \
         storage across a {COLLECTION_SIZE}-document collection (observed: \
         11 on all four backends); a lazy \
         scan reads a number of keys proportional to the limit"
    );

    let gets = db.store().point_gets() - gets_before;
    assert!(
        gets <= LIMIT * 3,
        "explain(execute) on a limit-{LIMIT} query performed {gets} point \
         lookups (observed: 23 on all four backends); document assembly must scale \
         with the limit, not the collection"
    );
}

#[tokio::test]
async fn explain_execute_reads_keys_proportional_to_the_limit() {
    assert_explain_execute_reads_keys_proportional_to_the_limit(MemoryStore::new()).await;
}

#[cfg(feature = "redb")]
#[tokio::test]
async fn explain_execute_reads_keys_proportional_to_the_limit_redb() {
    let dir = tempfile::tempdir().unwrap();
    assert_explain_execute_reads_keys_proportional_to_the_limit(
        storage::RedbStore::open(dir.path()).unwrap(),
    )
    .await;
}

#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn explain_execute_reads_keys_proportional_to_the_limit_rocksdb() {
    let dir = tempfile::tempdir().unwrap();
    assert_explain_execute_reads_keys_proportional_to_the_limit(
        storage::RocksDbStore::open(dir.path()).unwrap(),
    )
    .await;
}

#[cfg(feature = "fjall")]
#[tokio::test]
async fn explain_execute_reads_keys_proportional_to_the_limit_fjall() {
    let dir = tempfile::tempdir().unwrap();
    assert_explain_execute_reads_keys_proportional_to_the_limit(
        storage::FjallStore::open(dir.path()).unwrap(),
    )
    .await;
}
