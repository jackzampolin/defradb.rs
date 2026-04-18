//! Regression tests for the EmbeddedNode::shutdown API (#813).
//!
//! Verifies:
//! 1. `shutdown()` can be called and returns
//! 2. After `shutdown()`, `is_shutdown()` reports true
//! 3. `shutdown()` is idempotent — repeated calls are safe no-ops
//! 4. After `shutdown()`, the database reports `is_closed() == true`

use anyhow::Result;
use embedded::NodeBuilder;

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
    use std::sync::Arc;

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
