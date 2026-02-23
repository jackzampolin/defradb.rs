# p2p/ — Peer-to-peer networking tests

```
cargo test -p integration-test --test p2p
```

## Files

| File | Tests | What it covers |
|------|-------|----------------|
| `document.rs` | 2 | Document replication across runtimes |
| `management.rs` | 3 | P2P collection/replicator management |
| `sync.rs` | 10 | Sync protocol (document sync, versions, branchable, invalid CID) |
| `trust_boundary.rs` | 3 | ACP enforcement at P2P trust boundaries |
| `replication.rs` | 3 | Basic replication lifecycle |
| `replication_advanced.rs` | 3 | Multi-collection and bidirectional replication |
| `resilience.rs` | 9 | P2P stress tests (ignored by default) |

**22 active tests, 11 ignored.**

### Ignored

| Test | Reason |
|------|--------|
| `go_go_p2p_trust_boundary` | Go does not carry owner DID in PushLog Creator field |
| `go_rust_p2p_trust_boundary` | Go does not carry owner DID in PushLog Creator field |
| `rust_rust_car_bomb_protection` | Stress test — run with `--ignored` |
| `go_go_car_bomb_protection` | Stress test |
| `go_rust_car_bomb_protection` | Stress test |
| `rust_rust_rate_limiter_saturation` | Stress test |
| `go_go_rate_limiter_saturation` | Stress test |
| `go_rust_rate_limiter_saturation` | Stress test |
| `rust_rust_dag_semaphore_exhaustion` | Stress test |
| `go_go_dag_semaphore_exhaustion` | Stress test |
| `go_rust_dag_semaphore_exhaustion` | Stress test |

## Stress Tests

The 9 ignored stress tests are resource-intensive:

- **car_bomb_protection** — 50-doc burst replication
- **rate_limiter_saturation** — flooding one peer while another sends
- **dag_semaphore_exhaustion** — DAG fetch pipeline saturation

Run them explicitly:

```
cargo test -p integration-test --test p2p -- --ignored --test-threads=1
```
