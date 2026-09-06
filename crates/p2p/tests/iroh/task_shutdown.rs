use std::future::{pending, Future};
use std::sync::Arc;

use super::{shutdown_tracked_tasks, spawn_task, SpawnedTasks};

fn registry() -> SpawnedTasks {
    Arc::new(parking_lot::Mutex::new(Some(tokio::task::JoinSet::new())))
}

fn spawn(tasks: &SpawnedTasks, future: impl Future<Output = ()> + Send + 'static) {
    let _ = spawn_task(tasks, future);
}

#[tokio::test]
async fn shutdown_rejects_new_work_without_polling_it() {
    let tasks = registry();
    shutdown_tracked_tasks(tasks.clone()).await;
    let resource = Arc::new(());
    let retained = Arc::downgrade(&resource);
    spawn(&tasks, async move {
        let _resource = resource;
        panic!("work started after shutdown");
    });
    assert!(
        retained.upgrade().is_none(),
        "rejected work retained its resources"
    );
}

struct SpawnOnDrop {
    tasks: SpawnedTasks,
    resource: Arc<()>,
}

impl Drop for SpawnOnDrop {
    fn drop(&mut self) {
        let resource = self.resource.clone();
        spawn(&self.tasks, async move {
            let _resource = resource;
            pending::<()>().await;
        });
    }
}

#[tokio::test]
async fn shutdown_joins_tasks_and_rejects_work_spawned_during_cleanup() {
    let tasks = registry();
    let resource = Arc::new(());
    let retained = Arc::downgrade(&resource);
    let guard = SpawnOnDrop {
        tasks: tasks.clone(),
        resource,
    };
    let (started, ready) = tokio::sync::oneshot::channel();
    spawn(&tasks, async move {
        let _guard = guard;
        started.send(()).unwrap();
        pending::<()>().await;
    });
    ready.await.unwrap();
    shutdown_tracked_tasks(tasks).await;
    assert!(
        retained.upgrade().is_none(),
        "late-spawned task escaped shutdown"
    );
}

#[tokio::test]
async fn spawning_reaps_completed_tasks_and_preserves_individual_cancellation() {
    let tasks = registry();
    let completed = spawn_task(&tasks, async {}).unwrap();
    while !completed.is_finished() {
        tokio::task::yield_now().await;
    }
    let resource = Arc::new(());
    let retained = Arc::downgrade(&resource);
    let running = spawn_task(&tasks, async move {
        let _resource = resource;
        pending::<()>().await;
    })
    .unwrap();
    assert_eq!(tasks.lock().as_ref().unwrap().len(), 1);
    running.abort();
    shutdown_tracked_tasks(tasks.clone()).await;
    assert!(retained.upgrade().is_none());
    assert!(tasks.lock().is_none());
    shutdown_tracked_tasks(tasks).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_spawn_and_shutdown_release_all_resources() {
    let tasks = registry();
    let resource = Arc::new(());
    let retained = Arc::downgrade(&resource);
    let barrier = Arc::new(tokio::sync::Barrier::new(65));
    let mut callers = tokio::task::JoinSet::new();
    for _ in 0..64 {
        let tasks = tasks.clone();
        let resource = resource.clone();
        let barrier = barrier.clone();
        callers.spawn(async move {
            barrier.wait().await;
            spawn(&tasks, async move {
                let _resource = resource;
                pending::<()>().await;
            });
        });
    }
    drop(resource);
    barrier.wait().await;
    shutdown_tracked_tasks(tasks.clone()).await;
    while let Some(result) = callers.join_next().await {
        result.unwrap();
    }
    assert!(tasks.lock().is_none());
    assert!(retained.upgrade().is_none());
}
