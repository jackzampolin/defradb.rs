# Async Patterns Audit Findings

## Summary
- Total findings: 10
- Critical: 0 | High: 3 | Medium: 4 | Low: 3

## Findings

### Finding 1
- **severity:** high
- **category:** anti-pattern
- **crate:** embedded
- **file:** crates/embedded/src/node.rs
- **line:** 156
- **pattern:** blocking-in-async
- **description:** `std::fs::create_dir_all` is called inside `async fn build()`. This blocks the executor thread while the OS performs directory creation. While this only runs at startup, it sets a bad precedent and could be problematic if the filesystem is slow (network mount, encrypted disk with passphrase prompt).
- **training_ref:** async-book ch12 "Blocking the Executor"
- **suggested_fix:** Use `tokio::fs::create_dir_all(parent).await` or wrap in `tokio::task::spawn_blocking`.

### Finding 2
- **severity:** high
- **category:** anti-pattern
- **crate:** embedded
- **file:** crates/embedded/src/node.rs
- **line:** 1236-1267
- **pattern:** blocking-in-async
- **description:** `load_or_generate_iroh_secret_key` performs multiple blocking filesystem operations (`std::fs::read`, `std::fs::create_dir_all`, `std::fs::write`, `std::fs::set_permissions`) and is called from `async fn setup_iroh()` at line 719. All of these block the executor thread. The function is sync (`fn`, not `async fn`), so it blocks the calling async task's executor thread for the entire duration.
- **training_ref:** async-book ch12 "Blocking the Executor"
- **suggested_fix:** Either convert to `async fn` using `tokio::fs::*`, or wrap the call site: `let secret_key = tokio::task::spawn_blocking(move || load_or_generate_iroh_secret_key(path)).await??;`

### Finding 3
- **severity:** high
- **category:** anti-pattern
- **crate:** defra-node
- **file:** crates/defra-node/src/lib.rs
- **line:** 566, 891-915
- **pattern:** blocking-in-async
- **description:** `async fn build()` calls `std::fs::create_dir_all` at line 566. Additionally, `load_or_generate_secret_key` (lines 891-915) performs `std::fs::read`, `std::fs::create_dir_all`, `std::fs::write`, and `std::fs::set_permissions`, called from `async fn setup_p2p` at line 759. Same issue as Finding 2 in a different crate.
- **training_ref:** async-book ch12 "Blocking the Executor"
- **suggested_fix:** Same as Finding 2: use `tokio::fs` or `spawn_blocking`. Since both crates have the identical pattern, consider extracting a shared `async fn load_or_generate_secret_key` utility.

### Finding 4
- **severity:** medium
- **category:** anti-pattern
- **crate:** db
- **file:** crates/db/src/migration/mod.rs
- **line:** 50
- **pattern:** blocking-in-async
- **description:** `std::fs::read(path)` is called inside `async fn add_lens()` to load WASM bytes from disk. WASM modules can be large (multiple MB), making this a potentially significant block of the executor thread.
- **training_ref:** async-book ch12 "Blocking the Executor"
- **suggested_fix:** Use `tokio::fs::read(path).await` or `tokio::task::spawn_blocking(move || std::fs::read(path)).await?`.

### Finding 5
- **severity:** medium
- **category:** anti-pattern
- **crate:** embedded
- **file:** crates/embedded/src/node.rs
- **line:** 633, 749
- **pattern:** unbounded-channel
- **description:** `tokio::sync::mpsc::unbounded_channel::<PushFailure>()` is used for the failure reporting channel in both the libp2p path (line 633) and iroh path (line 749). Under sustained push failure conditions (e.g., a peer is unreachable but keeps being targeted), failures could accumulate without bound, growing memory until the node OOMs.
- **training_ref:** async-book ch13 "Backpressure with Bounded Channels"
- **suggested_fix:** Replace with a bounded channel: `mpsc::channel::<PushFailure>(1024)`. The sender side should use `try_send` and log/drop excess failures rather than applying backpressure to the sync coordinator.

### Finding 6
- **severity:** medium
- **category:** improvement
- **crate:** embedded
- **file:** crates/embedded/src/node.rs
- **line:** 594-596
- **pattern:** untracked-spawn
- **description:** The P2P host task is spawned with `tokio::spawn` but the `JoinHandle` is immediately dropped. While the host shuts down via the command channel (`HostCommand::Shutdown`), there is no way to await completion of the host task. During shutdown (lines 684-692), the abort list includes `host_event_task`, `replication_task`, `failure_recorder_task`, and `retry_loop_task`, but NOT the host task itself. If the host task is slow to exit (e.g., flushing a large gossip queue), shutdown will not wait for it.
- **training_ref:** async-book ch13 "Structured Concurrency: JoinSet and TaskTracker"
- **suggested_fix:** Capture the JoinHandle: `let host_task = tokio::spawn(async move { host.run().await; });` and include `host_task.abort_handle()` in the `ShutdownHandle::libp2p` abort list, or better yet, await it during shutdown with a timeout.

### Finding 7
- **severity:** medium
- **category:** anti-pattern
- **crate:** p2p
- **file:** crates/p2p/src/host/p2p_host/mod.rs
- **line:** 326-354
- **pattern:** select-starvation
- **description:** The P2P host event loop uses `biased` select with swarm events as the highest priority. The comment at line 327 explains this is intentional for ordering guarantees. However, under heavy peer activity (many connections, frequent gossip), the swarm branch will always be ready, starving the command channel. This means `HostCommand::Shutdown` cannot be delivered, potentially causing the node to hang during shutdown. The two-stream events channel has the same starvation risk as commands.
- **training_ref:** async-book ch12 "select! Fairness and Starvation"
- **suggested_fix:** Process swarm events in a batch (drain up to N events per iteration) then always poll the command channel once per iteration. Alternatively, add a `CancellationToken` that is checked at the top of the loop, independent of `select!`: `if self.cancel_token.is_cancelled() { break; }`.

### Finding 8
- **severity:** low
- **category:** anti-pattern
- **crate:** cli
- **file:** crates/cli/src/commands/client/backup.rs
- **line:** 81, 94
- **pattern:** blocking-in-async
- **description:** `std::fs::write` (line 81) and `std::fs::read_to_string` (line 94) are used in `async fn execute()` methods. Backup files can be large, making these significant blocking calls. However, the CLI context has minimal concurrent async work, so the practical impact is low.
- **training_ref:** async-book ch12 "Blocking the Executor"
- **suggested_fix:** Use `tokio::fs::write` and `tokio::fs::read_to_string`. For CLI commands this is low priority since the process is typically single-purpose.

### Finding 9
- **severity:** low
- **category:** anti-pattern
- **crate:** cli
- **file:** crates/cli/src/commands/client/view.rs
- **line:** 79, 90
- **pattern:** blocking-in-async
- **description:** `std::fs::read_to_string` is called twice in `async fn execute()` to read query and SDL files. Files are typically small (a few KB), so the blocking duration is minimal. Same low-impact pattern as Finding 8.
- **training_ref:** async-book ch12 "Blocking the Executor"
- **suggested_fix:** Use `tokio::fs::read_to_string`. Low priority.

### Finding 10
- **severity:** low
- **category:** improvement
- **crate:** p2p
- **file:** crates/p2p/src/iroh/endpoint.rs
- **line:** 246, 267, 873
- **pattern:** untracked-spawn
- **description:** Per-connection and per-stream handler tasks are spawned via `tokio::spawn` without collecting JoinHandles. These are fire-and-forget tasks that naturally terminate when connections close. During shutdown (lines 173-179), subscription reader tasks and active sync tasks are properly aborted, but in-flight connection handler tasks are not. In practice, these tasks will exit when the endpoint drops and connections reset, but there is a brief window where they continue running after the event loop has exited.
- **training_ref:** async-book ch13 "Structured Concurrency: JoinSet and TaskTracker"
- **suggested_fix:** Use a `JoinSet` or `CancellationToken` shared with connection handler tasks so they can be signaled during shutdown. Given these tasks self-terminate on connection close, this is a robustness improvement rather than a bug fix.
