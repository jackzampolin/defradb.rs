# p2p/ — Peer-to-peer networking tests

```
cargo test -p integration-test --test p2p
```

## Files

| File | Tests | What it covers |
|------|-------|----------------|
| `document.rs` | 3 | Document replication across topologies |
| `management.rs` | 3 | P2P collection/replicator management |
| `sync.rs` | 10 | Sync protocol (document sync, versions, branchable, invalid CID) |
| `trust_boundary.rs` | 3 | ACP enforcement at P2P trust boundaries |
| `replication.rs` | 3 | Basic replication lifecycle |
| `replication_advanced.rs` | 3 | Multi-collection and bidirectional replication |
| `stubs.rs` | 9 | Stress tests (ignored by default) |

**24 active tests, 9 ignored.** All active tests pass across Go/Rust/mixed topologies.

## Stress Tests

The 9 ignored tests in `stubs.rs` are resource-intensive stress tests:

- **car_bomb_protection** — 50-doc burst replication
- **rate_limiter_saturation** — flooding one peer while another sends
- **dag_semaphore_exhaustion** — DAG fetch pipeline saturation

Run them explicitly:

```
cargo test -p integration-test --test p2p -- --ignored --test-threads=1
```
