# Concurrency Audit Findings

## Summary
- Total findings: 10
- Critical: 0 | High: 2 | Medium: 5 | Low: 3

## Findings

### Finding 1
- **severity:** high
- **category:** anti-pattern
- **crate:** p2p
- **file:** crates/p2p/src/host/command_handler/messaging.rs
- **line:** 119-143
- **pattern:** lock-held-across-await
- **description:** Multiple handler functions (`handle_send_doc_sync_request`, `handle_send_doc_sync_response`, `handle_send_branchable_sync_request`, `handle_send_branchable_sync_response`, `handle_send_se_artifacts`, `handle_send_car_request`, `handle_send_car_response`) acquire the `tokio::sync::Mutex` on `two_stream_handler` and then call `.await` on network I/O while still holding the lock guard. For example, at line 137: `let mut h = handler.lock().await;` followed by `h.send_doc_sync_request_fire_and_forget(peer_id, request).await;`. This serializes all outbound two-stream operations through a single mutex, creating a bottleneck. If a network write stalls, all other two-stream operations are blocked. Contrast with `handle_send_two_stream_request` (line 64-111) which correctly releases the lock between sending and waiting for the response.
- **training_ref:** async-book ch8 "Tokio Sync Primitives" -- "don't use std::sync::Mutex across .await points"
- **suggested_fix:** Follow the pattern already used in `handle_send_two_stream_request`: acquire the lock, perform the minimal stream-opening operation, release the lock, then do any I/O outside the lock scope. For fire-and-forget sends, the handler methods could return a `Future` or stream handle that completes the write without the lock.

### Finding 2
- **severity:** high
- **category:** anti-pattern
- **crate:** storage (all backends)
- **file:** crates/storage/src/backends/redb/store.rs, crates/storage/src/backends/rocksdb/store.rs, crates/storage/src/backends/fjall/store.rs, crates/storage/src/backends/memory/store.rs
- **line:** redb:28, rocksdb:18, fjall:26, memory:19
- **pattern:** rwlock-for-bool-flag
- **description:** All four storage backends use `Arc<RwLock<bool>>` for the `closed` flag. This is a tokio async `RwLock` wrapping a single boolean, which means every `new_txn()` call must `.await` on a read lock acquisition just to check a flag. In redb/rocksdb/fjall, the read lock is held while incrementing `active_txn_count` to prevent a TOCTOU race with `close()`. However, this entire protocol can be implemented with `AtomicBool` + a state machine: atomically check closed and increment count using `compare_exchange` on a combined state, or use the existing `active_txn_count` as the coordination mechanism (set to `usize::MAX` on close). The current approach adds unnecessary async contention on every transaction creation in a hot path.
- **training_ref:** rust-patterns-book ch6 "Shared State: Arc, Mutex, RwLock, Atomics" -- "Atomics: Lock-free for simple values"
- **suggested_fix:** Replace `Arc<RwLock<bool>>` with `AtomicBool`. For the TOCTOU protection in redb/rocksdb/fjall, use a CAS loop: (1) load `closed`, (2) if false, `fetch_add(1)` on `active_txn_count`, (3) re-check `closed`, (4) if now true, `fetch_sub(1)` and return error. In `close()`, set `closed=true` then wait for `active_txn_count==0`. This is the standard "reference-counted close" pattern used in production databases.

### Finding 3
- **severity:** medium
- **category:** improvement
- **crate:** blockstore
- **file:** crates/blockstore/src/lib.rs
- **line:** 94, 123, 188, 203, 361
- **pattern:** relaxed-ordering-for-config-flag
- **description:** The `rehash` (`AtomicBool`) flag uses `Ordering::Relaxed` for both loads and stores. This flag controls whether hash verification is performed on block reads. When one thread calls `hash_on_read(true)`, other threads using `Ordering::Relaxed` may not see the update for an arbitrarily long time on weakly-ordered architectures (e.g., ARM). On x86, this happens to work due to strong TSO guarantees, but is not portable. In practice, a delay in enabling hash verification could allow unverified reads after the caller believes verification is active.
- **training_ref:** rust-patterns-book ch6 "Lock-Free Patterns" -- Acquire/Release semantics for publishing data
- **suggested_fix:** Use `Ordering::Release` for the `store` in `hash_on_read()` and `Ordering::Acquire` for the `load` in `get()`. This ensures that when hash verification is enabled, subsequent reads on other threads see the updated value promptly. The `Debug` impl and `rehash_enabled()` accessor can remain `Relaxed` since they are informational.

### Finding 4
- **severity:** medium
- **category:** improvement
- **crate:** events
- **file:** crates/events/src/channel_bus.rs
- **line:** 197
- **pattern:** seqcst-for-id-counter
- **description:** The `next_id` counter in `ChannelBus` uses `Ordering::SeqCst` for `fetch_add`. This counter generates unique subscription IDs and only needs monotonicity (no two callers get the same ID). `SeqCst` establishes a total ordering across all atomic operations on all variables, which is far stronger than needed for a simple counter. The same pattern appears in `ffi/src/state/registry.rs` (lines 30, 108) where `SeqCst` is used for handle generation counters.
- **training_ref:** rust-patterns-book ch6 "Shared State: Arc, Mutex, RwLock, Atomics" -- "Atomics: Lock-free for simple values" uses `Ordering::Relaxed` for counters
- **suggested_fix:** Use `Ordering::Relaxed` for monotonic ID/handle counters where the only invariant is uniqueness. `fetch_add` with `Relaxed` still guarantees atomicity (no two threads get the same value). Reserve `SeqCst` for cases requiring global ordering guarantees across multiple atomics.

### Finding 5
- **severity:** medium
- **category:** improvement
- **crate:** events
- **file:** crates/events/src/channel_bus.rs
- **line:** 136
- **pattern:** relaxed-ordering-for-observable-counter
- **description:** The `dropped_count` per-subscriber uses `Ordering::Relaxed` for `fetch_add` in `publish()`, but `Ordering::SeqCst` for `swap` and `load` in `Subscription::check_and_reset_dropped()` and `dropped_count()` (in `subscription.rs` lines 101, 106). The mixed orderings are inconsistent but technically safe here because the counter is a simple "at least N messages were dropped" signal. However, the `SeqCst` on the read side buys nothing when the write side is `Relaxed` -- the counter could still be stale on read. Either make both sides `Relaxed` (acceptable for an advisory counter) or both sides `AcqRel`/`Acquire` for consistency.
- **training_ref:** rust-patterns-book ch6 "Lock-Free Patterns" -- consistent ordering pairs
- **suggested_fix:** Use `Ordering::Relaxed` on both sides since this is an advisory counter, or use `Release` on the write side and `Acquire` on the read side for prompt visibility. The current asymmetry (`Relaxed` write, `SeqCst` read) pays the cost of `SeqCst` without the benefit.

### Finding 6
- **severity:** medium
- **category:** improvement
- **crate:** sourcehub
- **file:** crates/sourcehub/src/hub_rs/provider.rs
- **line:** 26, 90-99
- **pattern:** std-mutex-in-async-context
- **description:** `HubRsProvider` uses `std::sync::Mutex<u64>` for the nonce counter. The `send_tx` method (line 90) acquires this lock inside an `async fn`. While the lock is released before the first `.await` point (line 101), this is fragile -- any future refactoring that moves the `await` inside the lock scope would silently create a blocking hold. More importantly, `std::sync::Mutex` will block the tokio worker thread if contended, potentially causing deadlocks if all worker threads are blocked. Given that `send_tx` is called from async contexts and performs network I/O, using `tokio::sync::Mutex` or an `AtomicU64` with `fetch_add` would be safer.
- **training_ref:** async-book ch8 "Tokio Sync Primitives" -- "don't use std::sync::Mutex across .await points"
- **suggested_fix:** Replace `Mutex<u64>` with `AtomicU64` and use `fetch_add(1, Ordering::Relaxed)` to get the next nonce. This is lock-free, cannot block the tokio runtime, and is semantically correct for a monotonic counter. Alternatively, use `tokio::sync::Mutex` if the nonce must be coordinated with other state.

### Finding 7
- **severity:** medium
- **category:** improvement
- **crate:** sourcehub
- **file:** crates/sourcehub/src/hub_rs/provider.rs
- **line:** 27, 214, 249
- **pattern:** std-mutex-in-async-observer
- **description:** `Arc<Mutex<HubRsLightClientObservability>>` uses `std::sync::Mutex` in the `run_light_client_observer` async function. At line 249, the lock is acquired inside a `loop` that calls `.await`. Although the lock is released before the next `await` (the lock scope is a short `if let Ok(mut state) = ...` block), the `std::sync::Mutex` in a long-running async loop is a code smell. If the light client observer blocks on the mutex while a synchronous method like `acp_light_client_status()` (line 702) also holds it, the tokio worker thread is blocked until the synchronous caller finishes.
- **training_ref:** async-book ch8 "Tokio Sync Primitives" -- sync primitives in async code
- **suggested_fix:** Since `HubRsLightClientObservability` is a tiny struct (single `Option<u64>`), consider using `AtomicU64` for `last_invalidation_height` (with 0 meaning "none"). This eliminates the mutex entirely. Alternatively, use `parking_lot::Mutex` which never poisons and has shorter critical sections.

### Finding 8
- **severity:** low
- **category:** improvement
- **crate:** storage (redb, rocksdb, fjall)
- **file:** crates/storage/src/backends/redb/store.rs, crates/storage/src/backends/rocksdb/store.rs, crates/storage/src/backends/fjall/store.rs
- **line:** redb:175,289,298,341,352,354; rocksdb:153,174,184,186; fjall:137,158,166,203,213,215
- **pattern:** seqcst-for-txn-counter
- **description:** The `active_txn_count` uses `Ordering::SeqCst` for all operations (`load`, `fetch_add`, `fetch_sub`). This counter tracks active transactions for graceful shutdown. Since it coordinates with only the `closed` flag (a single other variable), `Acquire`/`Release` semantics would be sufficient and cheaper on weakly-ordered architectures. `SeqCst` provides a total order across *all* atomics, which is unnecessary when only two variables interact.
- **training_ref:** rust-patterns-book ch6 "Lock-Free Patterns" -- use Acquire/Release for paired atomics
- **suggested_fix:** Use `Ordering::AcqRel` for `fetch_add`/`fetch_sub` and `Ordering::Acquire` for `load`. This provides the necessary happens-before relationship (increment before use, decrement after done) without the overhead of sequential consistency. On x86 this makes no performance difference, but on ARM it avoids unnecessary memory barriers.

### Finding 9
- **severity:** low
- **category:** improvement
- **crate:** crypto
- **file:** crates/crypto/src/encryption/nonce.rs
- **line:** 26, 63-64
- **pattern:** relaxed-ordering-for-config-flag
- **description:** `USE_DETERMINISTIC_NONCE` uses `Ordering::Relaxed` for both the store (in `ffi/src/lib.rs:196`) and the load (line 26). This is a test-only flag that switches between secure random nonces and deterministic nonces. If the flag is set by one thread and read by another, `Relaxed` could theoretically cause the reader to miss the update. In practice, this flag is set once during initialization (before any encryption operations), so the risk is academic. However, the flag guards a security-critical code path (nonce generation).
- **training_ref:** rust-patterns-book ch6 "Lock-Free Patterns" -- Acquire/Release for flag publishing
- **suggested_fix:** Use `Ordering::Release` for the store and `Ordering::Acquire` for the load. This has zero cost on x86 and ensures the flag is visible promptly on ARM. Given this is test-only infrastructure, the severity is low.

### Finding 10
- **severity:** low
- **category:** improvement
- **crate:** sourcehub
- **file:** crates/sourcehub/src/hub_rs/client.rs
- **line:** 34
- **pattern:** relaxed-ordering-for-id-counter
- **description:** `HubRsClient::next_id()` uses `Ordering::Relaxed` for a request ID counter. This is correct -- the counter only needs uniqueness (no two calls return the same value), and `fetch_add` provides that guarantee even with `Relaxed`. This is noted here as a positive example for comparison with Finding 4, where similar counters unnecessarily use `SeqCst`.
- **training_ref:** rust-patterns-book ch6 "Shared State: Arc, Mutex, RwLock, Atomics"
- **suggested_fix:** No change needed. This is the correct pattern for ID generation counters.
