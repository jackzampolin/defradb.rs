# Finding 47: Timeout Map — Complete Audit of All Async Operations

**Severity**: MEDIUM
**Category**: Availability / Hangs
**Session**: 5 (Resource Limits & Edge Cases)

## Summary

Comprehensive audit of all timeout values and async operations that could hang indefinitely. Most request-response patterns have proper timeouts. The main gaps are the two-stream `read_to_end` calls (Finding 44) and the CAR fetch polling loop.

## Complete Timeout Map

### Timeouts Present (GREEN)

| Component | Timeout | Location |
|-----------|---------|----------|
| Request-response protocol | 30s | `behaviour.rs:49` (`REQUEST_TIMEOUT`) |
| Two-stream response wait | 30s | `two_stream/handler/mod.rs:32` (`RESPONSE_TIMEOUT`) |
| Two-stream PushLog send | 30s | `host/command_handler/messaging.rs:87-88` |
| DocSync response wait | 30s | `two_stream/handler/doc_sync.rs:64` |
| Bitswap per-block fetch | 10s | `host/command_handler/bitswap.rs:74` |
| Bitswap blockstore poll | 30s | `sync/coordinator/dag_fetcher.rs:192` |
| CAR fetch blockstore poll | 10s | `sync/coordinator/dag_fetcher.rs:160` |
| DAG sync block fetch | 30s | `sync/dag_sync/config.rs:96` |
| Idle connection | 60s | `host/p2p_host/mod.rs:36` |
| Shutdown grace period | 5s | `host/command_handler/mod.rs:197` |
| GossipSub heartbeat | 1s | `behaviour.rs:193` |
| Peer TTL (disconnected) | 1h | `sync/peer_state/tracker/mod.rs:114` |

### Timeouts Missing (FINDING)

| Operation | Location | Risk |
|-----------|----------|------|
| Two-stream `read_to_end` (5 sites) | `handler/inbound.rs:34,100`, `handler/car.rs:15`, `runner.rs:149,165` | HIGH — Slowloris (Finding 44) |
| PushLogCodec `read_message` | `codec.rs:46` | LOW — has `take(16MB)` size limit but no read timeout. Yamux stream timeout provides implicit bound |
| `collect_dag_blocks` (CAR response) | `sync/car.rs:72-79` | LOW — reads from local blockstore, not network |
| `find_all_missing_links` | `sync/manager/links.rs:73` | LOW — iterative local blockstore reads |

### Implicit Timeouts (via tokio channel close or peer disconnect)

| Operation | Mechanism |
|-----------|-----------|
| Command channel recv | Channel close → `None` → shutdown |
| Event channel recv | Channel close → `None` → loop exit |
| Bitswap session | Task abort handle on cancellation |

## DAG Fetcher Timeout Analysis

The `poll_fetch_dag` function (`dag_fetcher.rs:23-141`) has a bounded lifecycle:
- CAR fetch: 10s timeout → falls through to Bitswap
- Per-block Bitswap fetch: 30s timeout per block
- Maximum 20 iterations of DAG walking
- Each iteration: `join_all` on N parallel fetches (each with 30s timeout)

**Worst case**: 20 iterations × N blocks × 30s = potentially long-running but bounded. The 20-iteration cap (Finding 37) ensures this terminates.

## Recommendation

The critical gap is Finding 44 (two-stream read timeouts). The remaining items are low risk because they operate on local data or have implicit bounds from libp2p's transport layer.
