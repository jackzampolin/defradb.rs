# Finding 53: Replication Loop Has Proper Concurrency Control

**Severity**: GREEN
**Category**: Resource Management
**Session**: 5 (Resource Limits & Edge Cases)

## Summary

The replication loop runner properly limits concurrent merge operations using a tokio Semaphore. This is the one place in the P2P stack with explicit concurrency control for processing work.

## Evidence

**Semaphore-based worker pool** (`sync/replication/loop_runner.rs:136-176`):
```rust
pub async fn run_parallel<B, H, F>(
    coordinator: Arc<SyncCoordinator<B>>,
    mut events: mpsc::Receiver<SyncEvent>,
    handler: Arc<H>,
    config: ReplicationConfig,
    on_result: F,
) {
    let semaphore = Arc::new(Semaphore::new(config.max_workers));
    // ...
    let permit = semaphore.clone().acquire_owned().await.unwrap();
    tokio::spawn(async move {
        let result = process_event(&coord, event, h.as_ref(), &c).await;
        cb(result);
        drop(permit); // Release permit after processing
    });
}
```

**Default config** (`sync/replication/config.rs:16-25`):
```rust
impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            continue_on_error: true,
            rebroadcast_on_merge: false,
            batch_size: 50,
            max_workers: 32,
        }
    }
}
```

**Batch processing** (`loop_runner.rs:210-262`):
- Sequential loop: waits for first event, then drains up to `batch_size - 1` more
- Merge-eligible events are batched for efficient processing
- Non-merge events processed individually

## What's Good

1. Semaphore prevents unbounded task spawning in the merge path
2. `max_workers: 32` is a reasonable default
3. Batch processing reduces per-event overhead
4. `continue_on_error: true` prevents a single bad block from stopping replication
5. Owned permits ensure cleanup even on task panic

## Assessment

This is a good pattern. The concern is that the semaphore only controls the *merge* path — inbound message processing and DAG fetching (Findings 30, 33) still have unbounded spawning upstream of this point.
