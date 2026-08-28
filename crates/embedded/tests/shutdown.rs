//! Regression tests for the EmbeddedNode::shutdown API (#813).
//!
//! Verifies:
//! 1. `shutdown()` can be called and returns
//! 2. After `shutdown()`, `is_shutdown()` reports true
//! 3. `shutdown()` is idempotent — repeated calls are safe no-ops
//! 4. After `shutdown()`, the database reports `is_closed() == true`
//! 5. Concurrent `shutdown()` callers block until teardown completes

use anyhow::Result;
use async_trait::async_trait;
use embedded::{EmbeddedNodeConfig, Libp2pConfig, NodeBuilder, Persistence, TransportConfig};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_reports_closed_database() -> Result<()> {
    let node = NodeBuilder::default().build().await?;
    assert!(!node.is_shutdown(), "fresh node should not report shutdown");
    assert!(
        !node.database.is_closed(),
        "fresh database should not report closed"
    );

    node.shutdown().await;

    assert!(
        node.is_shutdown(),
        "after shutdown(), is_shutdown() should return true"
    );
    assert!(
        node.database.is_closed(),
        "after shutdown(), database should report closed"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_is_idempotent() -> Result<()> {
    let node = NodeBuilder::default().build().await?;

    // First call does real work.
    node.shutdown().await;
    assert!(node.is_shutdown());
    assert!(node.database.is_closed());

    // Second and third calls must be safe no-ops.
    node.shutdown().await;
    node.shutdown().await;
    assert!(node.is_shutdown());
    assert!(node.database.is_closed());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_concurrent_calls_are_safe() -> Result<()> {
    let node = Arc::new(NodeBuilder::default().build().await?);

    // Fire several concurrent shutdown() calls. They should all return
    // without panicking, and the node should end up fully shut down.
    let handles: Vec<_> = (0..8)
        .map(|_| {
            let n = node.clone();
            tokio::spawn(async move { n.shutdown().await })
        })
        .collect();

    for h in handles {
        h.await.expect("shutdown task should not panic");
    }

    assert!(node.is_shutdown());
    assert!(node.database.is_closed());

    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn p2p_shutdown_releases_persistent_store() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("data.regolith");
    let config = EmbeddedNodeConfig {
        persistence: Persistence::Persistent,
        transport: TransportConfig::Libp2p(Libp2pConfig {
            listen_addr: "/ip4/127.0.0.1/tcp/0".to_string(),
        }),
        ..Default::default()
    };
    let node =
        embedded::build_with_store(Arc::new(storage::RegolithStore::open(&path)?), config).await?;

    node.shutdown().await;
    drop(node);

    let reopened = storage::RegolithStore::open(&path)?;
    storage::Store::close(&reopened).await?;

    Ok(())
}

#[derive(Clone)]
struct SlowCloseStore {
    inner: storage::RegolithStore,
    close_calls: Arc<AtomicUsize>,
    close_started: Arc<Notify>,
    close_finished: Arc<AtomicBool>,
    allow_close: Arc<Notify>,
}

impl SlowCloseStore {
    fn new() -> Self {
        Self {
            inner: storage::RegolithStore::in_memory().expect("in-memory regolith"),
            close_calls: Arc::new(AtomicUsize::new(0)),
            close_started: Arc::new(Notify::new()),
            close_finished: Arc::new(AtomicBool::new(false)),
            allow_close: Arc::new(Notify::new()),
        }
    }
}

impl SlowCloseStore {
    async fn wait_until_close_started(&self) {
        loop {
            let notified = self.close_started.notified();
            if self.close_calls.load(Ordering::SeqCst) > 0 {
                return;
            }
            notified.await;
        }
    }
}

impl storage::corekv::private::Sealed for SlowCloseStore {}

#[async_trait]
impl storage::Store for SlowCloseStore {
    async fn new_txn(&self, readonly: bool) -> storage::Result<Box<dyn storage::Txn>> {
        self.inner.new_txn(readonly).await
    }

    async fn close(&self) -> storage::Result<()> {
        self.close_calls.fetch_add(1, Ordering::SeqCst);
        self.close_started.notify_waiters();
        self.allow_close.notified().await;
        let result = self.inner.close().await;
        self.close_finished.store(true, Ordering::SeqCst);
        result
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_shutdown_callers_wait_for_teardown_completion() -> Result<()> {
    let store = Arc::new(SlowCloseStore::new());
    let node =
        Arc::new(embedded::build_with_store(store.clone(), EmbeddedNodeConfig::default()).await?);

    let first = {
        let node = node.clone();
        tokio::spawn(async move {
            node.shutdown().await;
        })
    };

    tokio::time::timeout(Duration::from_secs(5), store.wait_until_close_started())
        .await
        .expect("database close did not start");

    let second = {
        let node = node.clone();
        let store = store.clone();
        tokio::spawn(async move {
            node.shutdown().await;
            assert!(
                store.close_finished.load(Ordering::SeqCst),
                "shutdown() returned before store close completed"
            );
        })
    };

    tokio::task::yield_now().await;
    assert!(
        !second.is_finished(),
        "concurrent shutdown caller returned before teardown completed"
    );

    store.allow_close.notify_one();

    tokio::time::timeout(Duration::from_secs(5), async {
        first.await.expect("first shutdown task should not panic");
        second.await.expect("second shutdown task should not panic");
    })
    .await
    .expect("shutdown tasks did not finish");

    assert_eq!(
        store.close_calls.load(Ordering::SeqCst),
        1,
        "shutdown should only close the store once"
    );
    assert!(store.close_finished.load(Ordering::SeqCst));

    Ok(())
}
