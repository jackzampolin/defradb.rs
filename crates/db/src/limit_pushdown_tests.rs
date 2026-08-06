//! A limited query must read documents proportional to the limit, not to the
//! collection.
//!
//! `docFetches` in `explain(execute)` cannot show this: it counts documents
//! yielded by `ScanNode`, which a `LimitNode` already capped before the scan
//! became lazy. The counter here sits between the plan and the storage-backed
//! fetcher instead, so it observes what the query actually pulled out of the
//! store.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use document::Document;
use query::doc_stream::DocStream;
use query::error::Result;
use query::fetcher::CommitsQueryOptions;
use query::planner::index_selection::IndexScanParams;
use query::{DocFetcher, DocMutator, FetchByIdsResult, QueryExecutor, QueryRequest};
use schema::{CollectionVersion, FieldDescription, FieldKind};
use storage::MemoryStore;

use crate::{AutoCommitMutator, LensedAutoCommitFetcher, DB};

const COLLECTION_SIZE: usize = 300;
const LIMIT: usize = 10;

/// Counts the documents a query pulls out of the inner storage-backed fetcher,
/// whether it streams them or materializes them.
struct CountingFetcher {
    inner: LensedAutoCommitFetcher<MemoryStore>,
    documents_read: Arc<AtomicUsize>,
}

/// Counts documents as the query pulls them, one per `next`.
struct CountingStream {
    inner: Box<dyn DocStream>,
    documents_read: Arc<AtomicUsize>,
}

#[async_trait]
impl DocStream for CountingStream {
    async fn next(&mut self) -> Result<Option<(Document, bool)>> {
        let pulled = self.inner.next().await?;
        if pulled.is_some() {
            self.documents_read.fetch_add(1, Ordering::SeqCst);
        }
        Ok(pulled)
    }

    async fn close(&mut self) -> Result<()> {
        self.inner.close().await
    }
}

#[async_trait]
impl DocFetcher for CountingFetcher {
    async fn get_all(&self, collection_name: &str) -> Result<Vec<Document>> {
        let docs = self.inner.get_all(collection_name).await?;
        self.documents_read.fetch_add(docs.len(), Ordering::SeqCst);
        Ok(docs)
    }

    async fn get_all_with_deleted(
        &self,
        collection_name: &str,
        show_deleted: bool,
    ) -> Result<Vec<(Document, bool)>> {
        let docs = self
            .inner
            .get_all_with_deleted(collection_name, show_deleted)
            .await?;
        self.documents_read.fetch_add(docs.len(), Ordering::SeqCst);
        Ok(docs)
    }

    async fn stream_all_with_deleted(
        &self,
        collection_name: &str,
        show_deleted: bool,
    ) -> Result<Box<dyn DocStream>> {
        Ok(Box::new(CountingStream {
            inner: self
                .inner
                .stream_all_with_deleted(collection_name, show_deleted)
                .await?,
            documents_read: self.documents_read.clone(),
        }))
    }

    async fn get_by_ids(
        &self,
        collection_name: &str,
        doc_ids: &[String],
    ) -> Result<FetchByIdsResult> {
        let result = self.inner.get_by_ids(collection_name, doc_ids).await?;
        self.documents_read
            .fetch_add(result.docs().len(), Ordering::SeqCst);
        Ok(result)
    }

    async fn get_by_field_value(
        &self,
        collection_name: &str,
        field_name: &str,
        value: &str,
    ) -> Result<Vec<Document>> {
        let docs = self
            .inner
            .get_by_field_value(collection_name, field_name, value)
            .await?;
        self.documents_read.fetch_add(docs.len(), Ordering::SeqCst);
        Ok(docs)
    }

    async fn get_commits(&self, options: &CommitsQueryOptions) -> Result<Vec<Document>> {
        self.inner.get_commits(options).await
    }

    async fn get_by_index_scan(
        &self,
        collection_name: &str,
        params: &IndexScanParams,
    ) -> Result<query::fetcher::IndexScanResult> {
        self.inner.get_by_index_scan(collection_name, params).await
    }

    fn supports_index_queries(&self) -> bool {
        self.inner.supports_index_queries()
    }
}

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

async fn seeded_db() -> Arc<DB<MemoryStore>> {
    let db = Arc::new(DB::new(MemoryStore::new()).unwrap());
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

/// A `limit` query must not read the whole collection.
///
/// The bound is deliberately loose (twice the limit): the point is the gap
/// between "proportional to the limit" and "proportional to the collection",
/// not an exact read count, which chunked storage reads may round up.
#[tokio::test]
async fn limit_query_reads_documents_proportional_to_the_limit() {
    let db = seeded_db().await;
    let documents_read = Arc::new(AtomicUsize::new(0));

    // `with_provider` rather than a registry: an implicit read transaction
    // would route the query to the registry's own fetcher, past the counter.
    let runner = query::QueryRunner::with_provider(
        CountingFetcher {
            inner: LensedAutoCommitFetcher::new(db.clone()),
            documents_read: documents_read.clone(),
        },
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

    let read = documents_read.load(Ordering::SeqCst);
    assert!(
        read <= LIMIT * 2,
        "a limit-{LIMIT} query read {read} of {COLLECTION_SIZE} documents; \
         it must read a number proportional to the limit, not to the collection"
    );
}
