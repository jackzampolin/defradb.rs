# Finding: Pending DAGs HashMap Has Unbounded Growth

**Stream**: 03 - P2P Network Security
**Session**: 4 — Replication Protocol Security
**Severity**: MEDIUM
**Category**: Memory Leak / Resource Exhaustion

## Summary

The `pending_dags` map (`HashMap<Cid, PendingDag>`) in `SyncManager` grows without bound. DocSync and BranchableSync replies can add entries that never resolve (if referenced blocks don't exist), causing a slow memory leak. There is no eviction policy, TTL, or capacity limit.

## Affected Files

| File | Lines | Issue |
|------|-------|-------|
| `crates/p2p/src/sync/manager/process/mod.rs` | 79 | `pending_dags: Arc<RwLock<HashMap<Cid, PendingDag>>>` — no capacity limit |
| `crates/p2p/src/sync/manager/process/pending_dag.rs` | 53-74 | `register_docsync_dag` — inserts without checking capacity |
| `crates/p2p/src/sync/manager/process/pending_dag.rs` | 81-104 | `register_branchable_dag` — inserts without checking capacity |
| `crates/p2p/src/sync/manager/process/pending_dag.rs` | 111-206 | `retry_pending_dag` — only removes on completion, never on timeout |

## Details

### No Eviction

```rust
// process/mod.rs:79
pub(super) pending_dags: Arc<RwLock<HashMap<Cid, PendingDag>>>,
```

Entries are added by `register_docsync_dag` and `register_branchable_dag`. They are only removed when `retry_pending_dag` finds the DAG is complete (all missing blocks fetched). If blocks never arrive:

- The pending entry stays forever
- The `missing` HashSet inside PendingDag also grows if blocks reference more missing blocks
- No background cleanup task scans for stale entries

### Attack Scenario

1. Attacker sends DocSyncReplies referencing thousands of fake CIDs
2. Each CID creates a pending DAG entry
3. DAG fetcher tasks (Finding 30) attempt Bitswap fetches that time out (30s each)
4. Pending entries remain after fetcher tasks give up
5. Over time, pending_dags map grows monotonically
6. Node's memory slowly leaks

### PendingDag Size

Each `PendingDag` contains:
- `doc_id: String` — variable length
- `collection_id: String` — variable length
- `creator: String` — variable length
- `missing: HashSet<Cid>` — grows as DAG traversal discovers more missing blocks
- `source_peer: Option<PeerId>`

With thousands of entries, each containing a HashSet of missing CIDs, this can consume significant memory.

### Also: query_to_root Map

```rust
// process/mod.rs:82
pub(super) query_to_root: Arc<RwLock<HashMap<QueryId, Cid>>>,
```

This map is similarly unbounded but is less of a concern since QueryId is generated locally and corresponds to actual Bitswap operations.

## Remediation

1. Add a `MAX_PENDING_DAGS` capacity limit (e.g., 1000) — reject new registrations when full
2. Add a TTL per pending DAG (e.g., 5 minutes) — a background task evicts stale entries
3. Log pending_dags size periodically for monitoring

## Test Gap

No test verifies that pending_dags entries are cleaned up after Bitswap fetch failures.
