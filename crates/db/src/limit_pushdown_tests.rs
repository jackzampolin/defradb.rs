//! A limited query must read keys proportional to the limit, not to the
//! collection.
//!
//! `docFetches` in `explain(execute)` cannot show this: it counts documents
//! yielded by `ScanNode`, which a `LimitNode` already capped before the scan
//! became lazy. A document-level counter sitting between the plan and the
//! fetcher can't show it either: a fetcher that materializes the whole
//! collection internally and hands back a `LIMIT`-sized slice still looks
//! proportional from up there. [`CountingStore`] sits below the fetcher, at
//! the storage layer, and counts what the query actually pulled off disk.

use document::Document;
use query::{DocMutator, QueryExecutor, QueryRequest};
use schema::{CollectionVersion, FieldDescription, FieldKind};
use std::sync::Arc;
use storage::MemoryStore;

use crate::counting_store::CountingStore;
use crate::{AutoCommitMutator, LensedAutoCommitFetcher, DB};

const COLLECTION_SIZE: usize = 2000;
const LIMIT: usize = 10;
/// `chunked::DEFAULT_CHUNK_SIZE`. A lazy scan refills in chunks, so a
/// limit-10 query reads one chunk, not one document.
const CHUNK_SIZE: usize = 256;

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

async fn seeded_db() -> Arc<DB<CountingStore<MemoryStore>>> {
    let db = Arc::new(DB::new(CountingStore::new(MemoryStore::new())).unwrap());
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
/// limit is satisfied, well short of even one `CHUNK_SIZE` refill, let alone
/// the rest of a `COLLECTION_SIZE`-document collection. `point_gets` bounds
/// document assembly, which does a couple of point lookups (`is_deleted`,
/// `load_version`) per document.
#[tokio::test]
async fn limit_query_reads_keys_proportional_to_the_limit() {
    let db = seeded_db().await;
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
         {COLLECTION_SIZE}-document collection (observed: 11, far short of \
         even one {CHUNK_SIZE}-key chunk); a lazy scan reads a number of \
         keys proportional to the limit"
    );

    let gets = db.store().point_gets() - gets_before;
    assert!(
        gets <= LIMIT * 3,
        "a limit-{LIMIT} query performed {gets} point lookups (observed: 23); \
         document assembly must scale with the limit, not the collection"
    );
}

/// `explain(execute)` must not materialize the collection from storage
/// either.
///
/// `docFetches` cannot show this: it counts documents yielded by `ScanNode`,
/// which `LimitNode` already capped, so it reads the same whether the source
/// streams or was pre-materialized. `keys_read` and `point_gets` sit below
/// the fetcher, at the storage layer, so they observe what explain actually
/// pulled off disk.
#[tokio::test]
async fn explain_execute_reads_keys_proportional_to_the_limit() {
    let db = seeded_db().await;
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
         11, far short of even one {CHUNK_SIZE}-key chunk); a lazy scan \
         reads a number of keys proportional to the limit"
    );

    let gets = db.store().point_gets() - gets_before;
    assert!(
        gets <= LIMIT * 3,
        "explain(execute) on a limit-{LIMIT} query performed {gets} point \
         lookups (observed: 23); document assembly must scale with the \
         limit, not the collection"
    );
}
