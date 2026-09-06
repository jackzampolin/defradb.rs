use std::sync::Arc;

use db::{DbCollectionProvider, DbTransactionRegistry, LensedAutoCommitFetcher, DB};
use query::TransactionError;
use query::{QueryExecutor, QueryRequest, TransactionGuard, TransactionHandle};
use storage::corekv::Store;
use storage::RegolithStore;

struct Fixture {
    runner: Arc<dyn QueryExecutor>,
    registry: Arc<DbTransactionRegistry<RegolithStore>>,
}

impl Fixture {
    async fn new() -> Self {
        let db = Arc::new(DB::new(RegolithStore::in_memory().unwrap()).unwrap());
        for schema in query::parse_sdl("type Users { name: String }").unwrap() {
            db.create_collection(schema).await.unwrap();
        }
        let registry = Arc::new(DbTransactionRegistry::new(db.clone()));
        let runner = Arc::new(query::QueryRunner::with_arc_registry_and_provider(
            LensedAutoCommitFetcher::new(db.clone()),
            DbCollectionProvider::new_arc(db),
            registry.clone(),
        ));
        Self { runner, registry }
    }

    async fn assert_discarded(&self, handle: &TransactionHandle) {
        assert_eq!(self.registry.active_transaction_count().unwrap(), 0);
        let response = self.runner.execute_in_txn(read(), handle).await;
        assert!(
            response.has_errors(),
            "orphaned transaction is still accessible"
        );
        let response = self.runner.execute(read()).await;
        assert!(!response.has_errors(), "{:?}", response.errors);
        assert_eq!(response.data.unwrap()["Users"], serde_json::json!([]));
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            self.registry.db().store().close(),
        )
        .await
        .expect("abandoned transaction still holds the store open")
        .unwrap();
    }
}

fn read() -> QueryRequest {
    QueryRequest::new("{ Users { name } }")
}

async fn create(guard: &TransactionGuard<'_, dyn QueryExecutor>) {
    let response = guard
        .execute(QueryRequest::new(
            r#"mutation { add_Users(input: {name: "pending"}) { _docID } }"#,
        ))
        .await;
    assert!(!response.has_errors(), "{:?}", response.errors);
}

#[tokio::test]
async fn dropping_guard_discards_writes_despite_saved_handle() {
    let fixture = Fixture::new().await;
    let guard = TransactionGuard::begin(fixture.runner.as_ref(), false)
        .await
        .unwrap();
    create(&guard).await;
    let handle = guard.handle().unwrap().clone();
    assert!(!guard.execute(read()).await.has_errors());
    drop(guard);
    fixture.assert_discarded(&handle).await;
}

#[tokio::test]
async fn aborting_owner_removes_transaction() {
    let fixture = Fixture::new().await;
    let executor = fixture.runner.clone();
    let (ready, handle) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let guard = TransactionGuard::begin(executor.as_ref(), false)
            .await
            .unwrap();
        create(&guard).await;
        ready.send(guard.handle().unwrap().clone()).unwrap();
        std::future::pending::<()>().await;
        drop(guard);
    });
    let handle = handle.await.unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    fixture.assert_discarded(&handle).await;
}

#[test]
fn guard_cleanup_does_not_require_a_running_runtime() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let fixture = runtime.block_on(Fixture::new());
    let guard = runtime
        .block_on(TransactionGuard::begin(fixture.runner.as_ref(), false))
        .unwrap();
    runtime.block_on(create(&guard));
    let handle = guard.handle().unwrap().clone();
    drop(runtime);
    drop(guard);
    assert_eq!(fixture.registry.active_transaction_count().unwrap(), 0);
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(fixture.assert_discarded(&handle));
}

#[derive(Clone, Copy, PartialEq)]
enum PauseAt {
    Begin,
    Execute,
    Commit,
    Rollback,
    AfterCommit,
}

#[tokio::test]
async fn cancellation_while_finalization_waits_for_an_active_request_releases_the_transaction() {
    use query::TransactionRegistry;

    for commit in [false, true] {
        let fixture = Fixture::new().await;
        let guard = TransactionGuard::begin(fixture.runner.as_ref(), false)
            .await
            .unwrap();
        create(&guard).await;
        let handle = guard.handle().unwrap().clone();
        let ctx = fixture
            .registry
            .get(&handle)
            .into_result()
            .unwrap()
            .unwrap();
        let action_lock = ctx.action_lock().unwrap();
        let action = action_lock.lock().await;
        let mut finalize = Box::pin(async move {
            if commit {
                guard.commit().await
            } else {
                guard.rollback().await
            }
        });
        assert!(futures::poll!(&mut finalize).is_pending());
        assert_eq!(fixture.registry.active_transaction_count().unwrap(), 0);
        drop(finalize);
        drop(action);
        drop(ctx);
        fixture.assert_discarded(&handle).await;
    }
}

struct PausedExecutor {
    inner: Arc<dyn QueryExecutor>,
    at: PauseAt,
}

impl PausedExecutor {
    async fn pause(&self, at: PauseAt) {
        if self.at == at {
            std::future::pending::<()>().await;
        }
    }
}

#[async_trait::async_trait]
impl QueryExecutor for PausedExecutor {
    async fn execute(&self, request: QueryRequest) -> query::QueryResponse {
        self.inner.execute(request).await
    }

    async fn execute_in_txn(
        &self,
        request: QueryRequest,
        handle: &TransactionHandle,
    ) -> query::QueryResponse {
        self.pause(PauseAt::Execute).await;
        self.inner.execute_in_txn(request, handle).await
    }

    async fn begin_txn(&self, readonly: bool) -> Result<TransactionHandle, TransactionError> {
        self.pause(PauseAt::Begin).await;
        self.inner.begin_txn(readonly).await
    }

    async fn commit_txn(&self, handle: &TransactionHandle) -> Result<(), TransactionError> {
        self.pause(PauseAt::Commit).await;
        self.inner.commit_txn(handle).await?;
        self.pause(PauseAt::AfterCommit).await;
        Ok(())
    }

    async fn rollback_txn(&self, handle: &TransactionHandle) -> Result<(), TransactionError> {
        self.pause(PauseAt::Rollback).await;
        self.inner.rollback_txn(handle).await
    }

    fn abandon_txn(&self, handle: &TransactionHandle) {
        self.inner.abandon_txn(handle);
    }

    async fn schema(&self) -> query::Result<String> {
        self.inner.schema().await
    }
}

#[tokio::test]
async fn cancellation_at_each_guard_boundary_discards_uncommitted_state() {
    for at in [
        PauseAt::Begin,
        PauseAt::Execute,
        PauseAt::Commit,
        PauseAt::Rollback,
    ] {
        let fixture = Fixture::new().await;
        let executor = PausedExecutor {
            inner: fixture.runner.clone(),
            at,
        };
        if at == PauseAt::Begin {
            let mut begin = Box::pin(TransactionGuard::begin(&executor, false));
            assert!(futures::poll!(&mut begin).is_pending());
            drop(begin);
            assert_eq!(fixture.registry.active_transaction_count().unwrap(), 0);
            continue;
        }
        let guard = TransactionGuard::begin(&executor, false).await.unwrap();
        let handle = guard.handle().unwrap().clone();
        let response = fixture
            .runner
            .execute_in_txn(
                QueryRequest::new(r#"mutation { add_Users(input: {name: "pending"}) { _docID } }"#),
                &handle,
            )
            .await;
        assert!(!response.has_errors(), "{:?}", response.errors);
        let mut operation = Box::pin(async move {
            match at {
                PauseAt::Execute => {
                    guard.execute(read()).await;
                }
                PauseAt::Commit => {
                    guard.commit().await.unwrap();
                }
                PauseAt::Rollback => {
                    guard.rollback().await.unwrap();
                }
                _ => unreachable!(),
            }
        });
        assert!(futures::poll!(&mut operation).is_pending());
        drop(operation);
        fixture.assert_discarded(&handle).await;
    }
}

#[tokio::test]
async fn cancellation_after_durable_commit_cannot_undo_it_or_discard_another_transaction() {
    let fixture = Fixture::new().await;
    let executor = PausedExecutor {
        inner: fixture.runner.clone(),
        at: PauseAt::AfterCommit,
    };
    let guard = TransactionGuard::begin(&executor, false).await.unwrap();
    let response = guard
        .execute(QueryRequest::new(
            r#"mutation { add_Users(input: {name: "committed"}) { _docID } }"#,
        ))
        .await;
    assert!(!response.has_errors(), "{:?}", response.errors);
    let mut commit = Box::pin(guard.commit());
    assert!(futures::poll!(&mut commit).is_pending());
    assert_eq!(fixture.registry.active_transaction_count().unwrap(), 0);
    let other = fixture.runner.begin_txn(false).await.unwrap();
    drop(commit);
    assert_eq!(fixture.registry.active_transaction_count().unwrap(), 1);
    fixture.runner.rollback_txn(&other).await.unwrap();
    let response = fixture.runner.execute(read()).await;
    assert!(!response.has_errors(), "{:?}", response.errors);
    assert_eq!(
        response.data.unwrap()["Users"],
        serde_json::json!([{"name": "committed"}])
    );
}
