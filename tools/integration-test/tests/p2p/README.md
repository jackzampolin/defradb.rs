# p2p/ — Peer-to-peer networking tests

```
cargo test -p integration-test --test p2p
```

## Files

| File | Tests | What it covers |
|------|-------|----------------|
| `connection_manager.rs` | 1 | Connection manager pruning behavior |
| `document.rs` | 2 | Document replication across runtimes |
| `idempotent_replay.rs` | 3 | Idempotent reconnect/replay behavior |
| `management.rs` | 3 | P2P collection/replicator management |
| `quarantine.rs` | 1 | #1126 x #1128 composition fence: canonical-pick convergence under fan-in with the quarantine guard staying silent, across a hub restart |
| `receiver_pull.rs` | 1 | Paced receiver-pull convergence fence (#1116 stage 2 retry clock, storm bound) |
| `sync.rs` | 11 | Sync protocol (document sync, versions, branchable, invalid CID) |
| `trust_boundary.rs` | 3 | ACP enforcement at P2P trust boundaries |
| `transports.rs` | 3 | TCP, QUIC, and WebSocket listen address coverage; Rust↔Rust QUIC/WS and Rust↔Go QUIC dialing |
| `replication.rs` | 4 | Basic replication lifecycle |
| `replication_advanced.rs` | 3 | Multi-collection and bidirectional replication |
| `resilience.rs` | 9 | P2P stress tests (ignored by default) |
| `write_contention.rs` | 9 | Concurrent write behavior across P2P topologies |

**53 total tests: 42 active, 11 ignored.**

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
