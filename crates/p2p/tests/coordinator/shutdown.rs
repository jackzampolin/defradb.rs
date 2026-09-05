use super::SyncShutdownHandle;
use std::sync::Arc;
use tokio::sync::oneshot;

#[tokio::test(start_paused = true)]
async fn concurrent_shutdown_callers_wait_for_task_completion() {
    let shutdown = SyncShutdownHandle::new(1);
    let (release, wait) = oneshot::channel();
    shutdown.spawn_task(async move {
        let _ = wait.await;
    });

    let first = tokio::spawn({
        let shutdown = shutdown.clone();
        async move { shutdown.shutdown().await }
    });
    shutdown.cancelled().await;

    let second = shutdown.shutdown();
    tokio::pin!(second);
    tokio::select! {
        biased;
        _ = &mut second => panic!("shutdown returned while a registered task was still running"),
        _ = tokio::task::yield_now() => {}
    }

    release.send(()).unwrap();
    second.await;
    first.await.unwrap();
    shutdown.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn shutdown_joins_aborted_tasks_before_returning() {
    let shutdown = SyncShutdownHandle::new(1);
    let resource = Arc::new(());
    let retained = Arc::downgrade(&resource);
    let (started, running) = oneshot::channel();

    shutdown.spawn_task(async {});
    shutdown.spawn_task(async move {
        let _resource = resource;
        started.send(()).unwrap();
        std::future::pending::<()>().await;
    });
    running.await.unwrap();

    shutdown.shutdown().await;

    assert!(
        retained.upgrade().is_none(),
        "aborted task still owns its resource"
    );
}

#[tokio::test(start_paused = true)]
async fn cancelling_the_first_shutdown_caller_does_not_detach_tasks() {
    let shutdown = SyncShutdownHandle::new(1);
    let resource = Arc::new(());
    let retained = Arc::downgrade(&resource);
    let (started, running) = oneshot::channel();
    shutdown.spawn_task(async move {
        let _resource = resource;
        started.send(()).unwrap();
        std::future::pending::<()>().await;
    });
    running.await.unwrap();

    let first = tokio::spawn({
        let shutdown = shutdown.clone();
        async move { shutdown.shutdown().await }
    });
    shutdown.cancelled().await;
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());

    shutdown.shutdown().await;

    assert!(
        retained.upgrade().is_none(),
        "cancelling shutdown detached the task"
    );
}

#[tokio::test]
async fn shutdown_rejects_new_tasks_without_spawning_them() {
    let shutdown = SyncShutdownHandle::new(1);
    shutdown.shutdown().await;
    let resource = Arc::new(());
    let retained = Arc::downgrade(&resource);

    assert!(!shutdown.spawn_task(async move {
        let _resource = resource;
        panic!("task started after shutdown");
    }));

    assert!(
        retained.upgrade().is_none(),
        "rejected task was detached instead of dropped"
    );
    assert_eq!(shutdown.retained_task_count(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn task_registration_racing_shutdown_does_not_leave_resources_alive() {
    for _ in 0..32 {
        let shutdown = SyncShutdownHandle::new(1);
        let resource = Arc::new(());
        let retained = Arc::downgrade(&resource);
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let registration = tokio::spawn({
            let shutdown = shutdown.clone();
            let barrier = barrier.clone();
            async move {
                barrier.wait().await;
                let task_shutdown = shutdown.clone();
                shutdown.spawn_task(async move {
                    let _resource = resource;
                    task_shutdown.cancelled().await;
                });
            }
        });

        barrier.wait().await;
        shutdown.shutdown().await;
        registration.await.unwrap();

        assert!(
            retained.upgrade().is_none(),
            "registration raced past the drain"
        );
    }
}
