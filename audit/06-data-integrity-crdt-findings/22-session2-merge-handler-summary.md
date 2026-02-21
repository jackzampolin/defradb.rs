# Session 2 Summary: Merge Handler Deep-Dive

**Date:** 2026-02-21
**Auditor:** Claude Opus 4.6
**Scope:** Merge handler — 3,126 LOC across 7 files (mod.rs, batch.rs, composite.rs, collection.rs, lww.rs, counter.rs, definition.rs)
**Cross-References:** blockstore/src/lib.rs, blockstore/src/traits.rs, p2p/src/sync/merge.rs, p2p/src/sync/replication/*, defra-core/src/block.rs

## Executive Summary

The merge handler is the most security-critical component in defradb.rs. It receives untrusted blocks from P2P peers, traverses DAG structures recursively, and applies CRDT merges that permanently alter document state. The overall design is sound — transactions provide atomicity, CRDT idempotency provides safety against duplicate processing, and the binary-split batch retry is correct. However, the implementation has **one high-severity denial-of-service vulnerability** (unbounded recursive DAG traversal), **three medium-severity issues** (unbounded dedup growth, missing CID verification, no per-document locking), and several low-severity concerns.

## Findings by Severity

### High (1)

| # | Finding | Impact |
|---|---------|--------|
| 11 | [Recursive DAG traversal no depth limit](11-recursive-dag-traversal-no-depth-limit.md) | Attacker crafts deep block chain → stack overflow → node crash |

### Medium (3)

| # | Finding | Impact |
|---|---------|--------|
| 12 | [merged_composites unbounded growth](12-merged-composites-unbounded-growth.md) | Memory grows monotonically with node uptime, no eviction |
| 14 | [No per-document merge locking](14-no-per-document-merge-locking.md) | Concurrent merges for same document can lose field updates in document cache |
| 18 | [Block CID not verified before merge](18-block-cid-not-verified-before-merge.md) | Content substitution possible if blockstore corrupted or hash_on_read disabled |

### Low (3)

| # | Finding | Impact |
|---|---------|--------|
| 13 | [Parent block missing silently skipped](13-parent-block-missing-silently-skipped.md) | Known trade-off for availability; could cause temporary divergence |
| 15 | [Decryption failure falls back to ciphertext](15-decryption-failure-falls-back-to-ciphertext.md) | Ciphertext passed to CBOR decoder → merge abort, not corruption |
| 16 | [Collection delta no dedup guard](16-collection-delta-no-dedup-guard.md) | Redundant processing on dual broadcast, no state corruption |
| 17 | [Composite dedup TOCTOU race](17-composite-dedup-toctou-race.md) | Double processing possible but CRDT idempotency prevents corruption |

### Informational (3)

| # | Finding | Impact |
|---|---------|--------|
| 19 | [Batch merge rollback correctness](19-batch-merge-partial-rollback-correctness.md) | Verified clean — transaction discard, binary-split, base case all correct |
| 20 | [Field iteration order deterministic](20-field-iteration-order-deterministic.md) | Verified clean — Vec<DAGLink> sorted by CID, CRDT commutativity guarantees convergence |
| 21 | [Encryption key plaintext in blockstore](21-encryption-block-key-plaintext-in-blockstore.md) | Matches Go architecture — KMS needed for true key isolation |

## Security Checklist Results

| Check | Result |
|-------|--------|
| Recursive DAG traversal — stack safety | **FAIL** — no depth limit |
| Composite CID deduplication — bounded | **FAIL** — unbounded HashSet |
| Composite CID deduplication — thread-safe | PARTIAL — TOCTOU race, mitigated by CRDT idempotency |
| Parent load failure handling | PASS — skip design is correct for availability |
| Batch merge rollback correctness | PASS — transaction discard on failure |
| Binary-split base case | PASS — single block → individual processing |
| Binary-split exponential retry | PASS — O(N) worst case, not exponential |
| Field merge ordering | PASS — deterministic Vec iteration, order-independent CRDTs |
| Encryption-aware merge (ACP skip) | PASS — correct behavior |
| Encryption-aware merge (decrypt fail) | PARTIAL — fallback to ciphertext, usually causes abort |
| Head tracking (composite) | PASS — proper head merging, only superseded heads deleted |
| Head tracking (field) | PASS — per-field head from block.heads |
| Head regression prevention | PASS — new head added, old removed atomically in transaction |
| Block decode safety (CBOR) | PASS — `serde_ipld_dagcbor::from_slice` has no known amplification vectors |
| Block CID verification | **FAIL** — hash_on_read disabled by default |
| Concurrent merge safety | **FAIL** — no per-document locking in parallel mode |
| P2P vs local merge path | PASS — same merge handler, same validation |
| Transaction commit/discard lifecycle | PASS — clean lifecycle in all paths |
| Event bus emission timing | PASS — events emitted after commit, not before |

## Root Cause Analysis

The high-severity finding (#11) stems from a fundamental mismatch between Go and Rust runtime characteristics. Go goroutines have dynamically-growing stacks (8 KB → 1 GB), so recursive DAG traversal in Go never hits a stack limit in practice. Rust's tokio threads have fixed 2 MB stacks. The recursive `Box::pin` pattern heap-allocates futures but doesn't prevent stack growth from the async call chain. This is a Rust-specific concern that doesn't exist in the Go reference implementation.

The medium-severity findings (#12, #14, #18) share a common theme: **the merge handler was designed for sequential processing and later extended with concurrency/batching without adding the corresponding safety mechanisms**. Go's merge handler processes events sequentially per-collection via a channel queue, which implicitly provides document-level serialization and bounded dedup scope. The Rust port added `run_parallel` and node-lifetime dedup without matching these safety properties.

## What Was Checked

- All 7 merge handler files read in full (3,126 LOC)
- Blockstore implementation and traits (443 + 139 LOC)
- P2P merge handler trait and replication loop (294 + 377 + 185 LOC)
- Block type definitions and CBOR serialization (729 LOC)
- Replication config (25 LOC)
- Cross-referenced with Session 1 CRDT findings

## What Was NOT Checked (Deferred to Future Sessions)

- Priority generation correctness (how priority values are computed from DAG height)
- Document state reconstruction completeness (whether all field values are captured)
- Head tracking correctness under complex branching scenarios
- CRDT interaction with lens transforms during merge
- Merge handler behavior during crash recovery (is_recovery = true path)
- Merge behavior for schema version mismatches (cross-version sync)
