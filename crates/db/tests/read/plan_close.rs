//! A query that errors mid-plan must still close its document stream.
//!
//! `DocStream::close` is the only point at which a stream can flush work it
//! deferred while yielding documents, because `Drop` cannot await. A plan
//! whose pull loop returns early on `?` skips it, and the deferred lens
//! migration write-backs go with it.

use crate::common::schema::users_schema;
use async_trait::async_trait;
use db::AutoCommitMutator;
use db::LensedAutoCommitFetcher;
use db::DB;
use document::Document;
use query::doc_stream::DocStream;
use query::error::QueryError;
use query::error::Result;
use query::fetcher::CommitsQueryOptions;
use query::planner::index_selection::IndexScanParams;
use query::DocFetcher;
use query::DocMutator;
use query::FetchByIdsResult;
use query::QueryExecutor;
use query::QueryRequest;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use storage::MemoryStore;

const COLLECTION_SIZE: usize = 50;
const FAIL_AFTER: usize = 3;

/// Yields `FAIL_AFTER` documents, then errors. Records whether it was closed.
struct FailingStream {
    inner: Box<dyn DocStream>,
    yielded: usize,
    closed: Arc<AtomicBool>,
}

#[async_trait]
impl DocStream for FailingStream {
    async fn next(&mut self) -> Result<Option<(Document, bool)>> {
        if self.yielded == FAIL_AFTER {
            return Err(QueryError::execution("injected mid-plan failure"));
        }
        self.yielded += 1;
        self.inner.next().await
    }

    async fn close(&mut self) -> Result<()> {
        self.closed.store(true, Ordering::SeqCst);
        self.inner.close().await
    }
}

struct FailingFetcher {
    inner: LensedAutoCommitFetcher<MemoryStore>,
    closed: Arc<AtomicBool>,
}

#[async_trait]
impl DocFetcher for FailingFetcher {
    /// Fails like every other read here, so a seeking caller sees the same
    /// failure a scanning one would.
    async fn stream_by_doc_short_ids(
        &self,
        collection_name: &str,
        _doc_short_ids: &[u64],
        show_deleted: bool,
    ) -> query::error::Result<Box<dyn query::doc_stream::DocStream>> {
        self.stream_all_with_deleted(collection_name, show_deleted)
            .await
    }

    async fn stream_all_with_deleted(
        &self,
        collection_name: &str,
        show_deleted: bool,
    ) -> Result<Box<dyn DocStream>> {
        let inner = self
            .inner
            .stream_all_with_deleted(collection_name, show_deleted)
            .await?;
        Ok(Box::new(FailingStream {
            inner,
            yielded: 0,
            closed: self.closed.clone(),
        }))
    }

    async fn get_all(&self, collection_name: &str) -> Result<Vec<Document>> {
        self.inner.get_all(collection_name).await
    }

    async fn get_all_with_deleted(
        &self,
        collection_name: &str,
        show_deleted: bool,
    ) -> Result<Vec<(Document, bool)>> {
        self.inner
            .get_all_with_deleted(collection_name, show_deleted)
            .await
    }

    async fn get_by_ids(&self, collection_name: &str, ids: &[String]) -> Result<FetchByIdsResult> {
        self.inner.get_by_ids(collection_name, ids).await
    }

    async fn get_by_field_value(
        &self,
        collection_name: &str,
        field_name: &str,
        value: &str,
    ) -> Result<Vec<Document>> {
        self.inner
            .get_by_field_value(collection_name, field_name, value)
            .await
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

/// A query that fails partway through its scan must still close the stream.
#[tokio::test]
async fn stream_is_closed_when_the_query_errors_mid_plan() {
    let db = seeded_db().await;
    let closed = Arc::new(AtomicBool::new(false));

    let runner = query::QueryRunner::with_provider(
        FailingFetcher {
            inner: LensedAutoCommitFetcher::new(db.clone()),
            closed: closed.clone(),
        },
        db::DbCollectionProvider::new_arc(db.clone()),
    );

    let response = runner
        .execute(QueryRequest::new("query { Users { name } }".to_string()))
        .await;

    assert!(
        !response.errors.is_empty(),
        "the injected failure should have surfaced as a query error"
    );
    assert!(
        closed.load(Ordering::SeqCst),
        "the plan errored without closing its stream, so anything the stream \
         deferred was dropped"
    );
}

/// A cursor query routes through the planner (`nested.rs`), not the simple
/// path — `select.is_cursor` is one of `needs_planner`'s conditions (see
/// `select.rs`). `first` is set well above `FAIL_AFTER` so the CursorNode
/// pulls past the injected failure instead of being satisfied first.
#[tokio::test]
async fn cursor_stream_is_closed_when_the_query_errors_mid_plan() {
    let db = seeded_db().await;
    let closed = Arc::new(AtomicBool::new(false));

    let runner = query::QueryRunner::with_provider(
        FailingFetcher {
            inner: LensedAutoCommitFetcher::new(db.clone()),
            closed: closed.clone(),
        },
        db::DbCollectionProvider::new_arc(db.clone()),
    );

    let response = runner
        .execute(QueryRequest::new(
            "query { _cursor { Users(first: 10) { name } } }".to_string(),
        ))
        .await;

    assert!(
        !response.errors.is_empty(),
        "the injected failure should have surfaced as a query error"
    );
    assert!(
        closed.load(Ordering::SeqCst),
        "the plan errored without closing its stream, so anything the stream \
         deferred was dropped"
    );
}
