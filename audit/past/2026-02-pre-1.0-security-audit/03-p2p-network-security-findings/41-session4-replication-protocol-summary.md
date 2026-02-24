# Session 4 Summary: Replication Protocol Security

**Stream**: 03 - P2P Network Security
**Session**: 4 — Replication Protocol Security (CRITICAL)
**Date**: 2026-02-21

## Scope

Deep-dive into the two-stream replication protocol, message deserialization, sync coordinator event handling, DAG fetch lifecycle, Bitswap retry logic, and resource accounting. This session validated and expanded Finding 00 (unbounded read), then traced the complete data flow from stream read through CBOR deserialization, event dispatch, access checks, CID parsing, DAG registration, Bitswap fetch, and merge.

## Findings

### New Findings (30-40)

| # | Title | Severity | Category |
|---|-------|----------|----------|
| 30 | Unbounded tokio task spawning per peer | HIGH | DoS / Resource Exhaustion |
| 31 | DocSyncRequest.doc_ids unbounded array | HIGH | DoS / Amplification |
| 32 | Pending DAGs HashMap unbounded growth | MEDIUM | Memory Leak |
| 33 | DAG fetcher unbounded task fan-out | MEDIUM | Resource Exhaustion |
| 34 | CBOR triple-try deserialization amplification | LOW | Performance |
| 35 | CAR response blocks stored without origin verification | MEDIUM | Data Integrity |
| 36 | Channel backpressure memory accumulation | LOW | Resource Exhaustion |
| 37 | DAG depth capped at 20 iterations | GREEN | Defense in Depth |
| 38 | CID parsing graceful error handling | GREEN | Error Handling |
| 39 | PushLog response always sent | GREEN | Protocol Correctness |
| 40 | Bitswap retry bounded, no infinite loop | GREEN | Defense in Depth |

### Updated Findings

| # | Title | Change |
|---|-------|--------|
| 00 | Two-stream no message size limit | Confirmed 5 code sites (7 call paths), added amplification analysis |

## Tally

| Severity | Count | Findings |
|----------|-------|----------|
| HIGH | 2 | 30, 31 |
| MEDIUM | 3 | 32, 33, 35 |
| LOW | 2 | 34, 36 |
| GREEN | 4 | 37, 38, 39, 40 |

## Checklist Status

- [x] Confirm finding 00: all 5 `read_to_end` locations lack `take()`. No additional instances found.
- [x] Two-stream: each stream spawns a new tokio task — no limit on concurrent tasks per peer. **HIGH** (Finding 30)
- [x] Two-stream: no per-peer memory accounting — concurrent streams accumulate independently. **Part of Finding 30**
- [x] CBOR deserialization happens AFTER full read into memory — malformed CBOR still consumes all memory. **Part of Finding 00 update**
- [x] Message type validation is post-deserialization only — triple-try pattern. **LOW** (Finding 34)
- [x] CID parsing: errors handled gracefully (returns error, sends response). **GREEN** (Finding 38)
- [x] DocSyncRequest.doc_ids: unbounded array, can contain millions of entries. **HIGH** (Finding 31)
- [x] Block data in PushLogRequest: no field-level size limit. **Part of Finding 31**
- [x] pending_dag_missing: unbounded growth, no eviction. **MEDIUM** (Finding 32)
- [x] DAG depth: capped at 20 iterations in poll_fetch_dag. **GREEN** (Finding 37)
- [x] Bitswap retry logic: bounded by timeouts (10s per block, 30s per poll). **GREEN** (Finding 40)
- [x] Channel sends in error paths: bounded channels, tasks block but no deadlock. **LOW** (Finding 36)
- [x] Response always sent even on error (pushlog): yes, all 4 paths send response. **GREEN** (Finding 39)

## Attack Surface Summary

The two-stream protocol has a **compound DoS vulnerability** where three independent weaknesses reinforce each other:

1. **No message size limit** (Finding 00) — each stream can allocate unbounded memory
2. **No task spawning limit** (Finding 30) — unlimited concurrent streams, each with its own allocation
3. **No field validation** (Finding 31) — even correctly-sized messages can trigger unbounded work

A single attacker peer can simultaneously:
- Open 1000 request streams (1000 tokio tasks, each reading unbounded data)
- Send DocSyncRequests with 100,000 doc_ids (100,000 database lookups per request)
- Trigger DAG fetcher fan-out (1000+ additional tasks from reply processing)

The correct implementation pattern already exists in `codec.rs` (`read_message` with `take()`) — it just isn't used by the two-stream path.

## Key Defenses Already Present

- DAG depth capped at 20 iterations (prevents DAG bombs)
- CID parsing never panics (graceful error handling everywhere)
- PushLog always sends response (no peer left hanging)
- Bitswap retry bounded by timeouts (no infinite loops)
- PeerStateTracker has per-peer CID tracking with LRU bounds
- Replication loop uses Semaphore for outbound concurrency

## Remaining for Session 5

Session 5 (Resource Limits & Edge Cases) should address:
- Confirming no per-peer rate limiting exists (referenced but not deep-dived)
- GossipSub mesh size defaults
- Interaction between connection limits (Finding 01) and task spawning (Finding 30)
- Whether fixing Findings 00 + 01 + 30 together provides adequate protection
