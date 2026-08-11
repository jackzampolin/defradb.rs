use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use query::QueryRequest;

mod executors;

use executors::{
    context_observing_executor, slow_signing_executor, spawning_signing_executor,
    test_signing_config,
};

#[test]
fn cancelled_caller_runtime_does_not_own_signed_query_runtime() {
    let started = Arc::new(AtomicBool::new(false));
    let (completed_tx, completed_rx) = std::sync::mpsc::channel();
    let executor = slow_signing_executor(started.clone(), completed_tx);
    let caller_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("caller runtime");
    let signed_runtime = super::SignedQueryRuntime::new().expect("signed runtime");
    let signed_runtime_handle = signed_runtime.handle();
    let signed_query_permit = signed_runtime.admit().expect("query admission");

    caller_runtime.block_on(async {
        let task = tokio::spawn(async move {
            super::execute_with_signing_context(
                executor,
                QueryRequest::new("{ delayed }"),
                None,
                test_signing_config(),
                "did:key:zCancellationTest".to_string(),
                signed_runtime_handle,
                signed_query_permit,
            )
            .await
        });
        while !started.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
        task.abort();
        let _ = task.await;
    });
    drop(caller_runtime);
    drop(signed_runtime);

    completed_rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .expect("signed query must complete after its caller and node runtimes are dropped");
}

#[test]
fn signed_query_runtime_keeps_spawned_work_alive_after_query_returns() {
    let (completed_tx, completed_rx) = std::sync::mpsc::channel();
    let executor = spawning_signing_executor(completed_tx);
    let caller_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("caller runtime");
    let signed_runtime = super::SignedQueryRuntime::new().expect("signed runtime");

    let response = caller_runtime.block_on(super::execute_with_signing_context(
        executor,
        QueryRequest::new("{ spawnBackgroundWork }"),
        None,
        test_signing_config(),
        "did:key:zSpawnTest".to_string(),
        signed_runtime.handle(),
        signed_runtime.admit().expect("query admission"),
    ));
    assert!(
        !response.has_errors(),
        "query failed: {:?}",
        response.errors
    );
    drop(caller_runtime);

    completed_rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .expect("work spawned by a signed query must outlive query completion");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn signed_query_context_survives_awaits_on_node_owned_runtime() {
    let expected_did = "did:key:zContextTest".to_string();
    let signing_config = test_signing_config();
    let executor =
        context_observing_executor(expected_did.clone(), signing_config.public_key_hex.clone());
    let signed_runtime = super::SignedQueryRuntime::new().expect("signed runtime");

    let response = super::execute_with_signing_context(
        executor,
        QueryRequest::new("{ observeContext }"),
        None,
        signing_config,
        expected_did,
        signed_runtime.handle(),
        signed_runtime.admit().expect("query admission"),
    )
    .await;

    assert!(
        !response.has_errors(),
        "signed query context failed: {:?}",
        response.errors
    );
    assert!(
        signed_runtime
            .close_admission_and_wait_for(std::time::Duration::from_secs(2))
            .await,
        "signed query context permit did not drain"
    );
    signed_runtime.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn signed_query_runtime_closes_admission_and_drains_in_flight_queries() {
    let runtime = Arc::new(super::SignedQueryRuntime::new().expect("signed runtime"));
    let permit = runtime.admit().expect("initial query admission");
    let closing_runtime = runtime.clone();
    let mut close_task = tokio::spawn(async move {
        closing_runtime
            .close_admission_and_wait_for(std::time::Duration::from_secs(2))
            .await
    });

    while !runtime.state.closing.load(Ordering::Acquire) {
        tokio::task::yield_now().await;
    }
    assert!(
        runtime.admit().is_none(),
        "queries that race shutdown must fail admission"
    );
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(25), &mut close_task)
            .await
            .is_err(),
        "shutdown admission drain returned while a query permit was live"
    );

    drop(permit);
    let drained = tokio::time::timeout(std::time::Duration::from_secs(2), close_task)
        .await
        .expect("admission drain timed out")
        .expect("admission drain task panicked");
    assert!(drained, "admission drain reported a timeout");
    runtime.shutdown().await;
}

#[tokio::test]
async fn signed_query_runtime_admission_drain_has_a_deadline() {
    let runtime = super::SignedQueryRuntime::new().expect("signed runtime");
    let permit = runtime.admit().expect("query admission");

    assert!(
        !runtime
            .close_admission_and_wait_for(std::time::Duration::from_millis(25))
            .await,
        "admission drain ignored its deadline"
    );

    drop(permit);
    runtime.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn signed_query_runtime_shutdown_is_idempotent_and_node_scoped() {
    let runtime_a = Arc::new(super::SignedQueryRuntime::new().expect("runtime A"));
    let runtime_b = Arc::new(super::SignedQueryRuntime::new().expect("runtime B"));
    assert!(
        runtime_a
            .close_admission_and_wait_for(std::time::Duration::from_secs(2))
            .await
    );

    let first = {
        let runtime = runtime_a.clone();
        tokio::spawn(async move { runtime.shutdown().await })
    };
    let second = {
        let runtime = runtime_a.clone();
        tokio::spawn(async move { runtime.shutdown().await })
    };
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        first.await.expect("first shutdown caller panicked");
        second.await.expect("second shutdown caller panicked");
    })
    .await
    .expect("concurrent shutdown callers did not converge");

    let permit_b = runtime_b
        .admit()
        .expect("shutting down runtime A must not close runtime B");
    drop(permit_b);
    assert!(
        runtime_b
            .close_admission_and_wait_for(std::time::Duration::from_secs(2))
            .await
    );
    runtime_b.shutdown().await;
}

#[tokio::test]
async fn signed_query_runtime_drop_is_nonblocking_inside_async_context() {
    let runtime = super::SignedQueryRuntime::new().expect("signed runtime");
    let state = runtime.state.clone();
    let permit = runtime.admit().expect("query admission");
    drop(runtime);
    assert!(
        state.closing.load(Ordering::Acquire),
        "drop must close query admission"
    );
    assert!(
        !state.closed.load(Ordering::Acquire),
        "runtime closed while an admitted query was still live"
    );
    drop(permit);
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            let notified = state.closed_notify.notified();
            if state.closed.load(Ordering::Acquire) {
                break;
            }
            notified.await;
        }
    })
    .await
    .expect("dropped signed runtime did not close after its query drained");
}
