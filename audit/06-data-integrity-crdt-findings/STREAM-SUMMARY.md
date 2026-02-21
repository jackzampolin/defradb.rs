# Stream 06: Data Integrity & CRDT Correctness — Full Audit Summary

**Stream:** 06 — Data Integrity & CRDT Correctness
**Sessions:** 6 of 6 (complete)
**Date:** 2026-02-21
**Auditor:** Claude Opus 4.6
**Total Findings:** 59 (excluding 5 session summaries)

## Executive Summary

The data integrity and CRDT correctness subsystems of defradb.rs are **fundamentally sound**. The core CRDT properties — commutativity, idempotency, and convergence — hold for all supported types (LWW, Counter, Composite). Priority-based conflict resolution is deterministic, partition healing converges correctly, and deletion/resurrection semantics are well-defined.

However, the audit identified **3 high-severity issues** (all in the searchable encryption subsystem, which is partially unimplemented), **18 medium-severity issues** spanning CRDT composite gaps, CID verification, merge handler safety, searchable encryption, transaction limits, index consistency, and resource exhaustion, and **14 low-severity issues** across floating-point precision, resource management, and defense-in-depth gaps.

The most systemic risk is the **lack of CID verification on P2P ingestion paths** (Findings 18, 23, 24, 29), which undermines the content-addressed integrity model. The most impactful functional gap is the **incomplete searchable encryption pipeline** (Findings 34, 37), which blocks SE functionality in Rust nodes.

## Severity Overview

| Severity | Count | Key Themes |
|----------|-------|------------|
| **High** | 3 | SE receiver not implemented, SE query not integrated, recursive DAG no depth limit |
| **Medium** | 18 | Composite counter gaps (3), CID verification (4), merge handler (2), SE subsystem (5), transactions (2), index consistency (1), nonce storage (1) |
| **Low** | 14 | Float precision (3), resource management (4), defense-in-depth (4), schema evolution (1), backup integrity (1), document size (1) |
| **Test Gap** | 1 | Property test coverage gaps |
| **Informational** | 12 | Architecture analysis, design trade-offs, privacy analysis |
| **Verified Clean** | 11 | LWW correctness, counter idempotency, snapshot isolation, partition healing, deletion semantics |

## All Findings by Session

### Session 1: CRDT Correctness (Findings 00–09)

**Scope:** LWW, Counter, Composite CRDT implementations (~5,000 LOC)

| # | Severity | Title |
|---|----------|-------|
| 00 | Medium | [Composite counter nonce ordering unsafe](00-composite-counter-nonce-ordering-unsafe.md) |
| 01 | Medium | [Composite counter missing allow_decrement](01-composite-counter-missing-allow-decrement.md) |
| 02 | Medium | [Composite counter missing Float64 support](02-composite-counter-missing-float64-support.md) |
| 03 | Low | [Float64 counter non-associative divergence](03-float64-counter-non-associative-divergence.md) |
| 04 | Test Gap | [Property test coverage gaps](04-property-test-coverage-gaps.md) |
| 05 | Informational | [Priority ceiling u64::MAX permanent immutability](05-priority-ceiling-u64max-permanent-immutability.md) |
| 06 | Low | [Counter nonce storage unbounded growth](06-counter-nonce-storage-unbounded-growth.md) |
| 07 | Verified Clean | [LWW tie-breaking correctness verified](07-lww-tie-breaking-correctness-verified.md) |
| 08 | Verified Clean | [Counter nonce idempotency verified](08-counter-nonce-idempotency-verified.md) |
| 09 | Informational | [Composite pre-validation atomicity analysis](09-composite-pre-validation-atomicity-analysis.md) |

**Root Cause:** Findings 00–02 share a common root cause — the Composite CRDT reimplements counter logic inline instead of delegating to the standalone Counter, omitting nonce ordering, allow_decrement, and Float64 support.

### Session 2: Merge Handler (Findings 11–21)

**Scope:** Merge handler subsystem (~3,126 LOC across 7 files)

| # | Severity | Title |
|---|----------|-------|
| 11 | **High** | [Recursive DAG traversal no depth limit](11-recursive-dag-traversal-no-depth-limit.md) |
| 12 | Medium | [merged_composites unbounded growth](12-merged-composites-unbounded-growth.md) |
| 13 | Low | [Parent block missing silently skipped](13-parent-block-missing-silently-skipped.md) |
| 14 | Medium | [No per-document merge locking](14-no-per-document-merge-locking.md) |
| 15 | Low | [Decryption failure falls back to ciphertext](15-decryption-failure-falls-back-to-ciphertext.md) |
| 16 | Low | [Collection delta no dedup guard](16-collection-delta-no-dedup-guard.md) |
| 17 | Low | [Composite dedup TOCTOU race](17-composite-dedup-toctou-race.md) |
| 18 | Medium | [Block CID not verified before merge](18-block-cid-not-verified-before-merge.md) |
| 19 | Informational | [Batch merge partial rollback correctness](19-batch-merge-partial-rollback-correctness.md) |
| 20 | Informational | [Field iteration order deterministic](20-field-iteration-order-deterministic.md) |
| 21 | Informational | [Encryption block key plaintext in blockstore](21-encryption-block-key-plaintext-in-blockstore.md) |

**Root Cause:** The high-severity DAG recursion issue (11) is Rust-specific — Go goroutines have dynamic stacks, Rust's tokio threads have fixed 2 MB stacks. Findings 12, 14, 18 stem from the merge handler being designed for sequential processing and later extended with concurrency without matching safety mechanisms.

### Session 3: Block Integrity & CID Determinism (Findings 23–29)

**Scope:** Blockstore, P2P ingestion, backup/restore, CID computation

| # | Severity | Title |
|---|----------|-------|
| 23 | Medium | [No CID verification on put()](23-no-cid-verification-on-put.md) |
| 24 | Medium | [Unsupported hash algorithm bypass](24-unsupported-hash-algorithm-bypass.md) |
| 25 | Informational | [CID determinism dual CBOR verified](25-cid-determinism-dual-cbor-verified.md) |
| 26 | Informational | [Time encoding RFC3339 Go-compatible](26-time-encoding-rfc3339-go-compatible.md) |
| 27 | Low | [Backup no block-level integrity](27-backup-no-block-level-integrity.md) |
| 28 | Informational | [Block construction CID from serialized bytes](28-block-construction-cid-from-serialized-bytes.md) |
| 29 | Medium | [P2P PushLog no CID verification before storage](29-p2p-pushlog-no-cid-verification-before-storage.md) |

**Key Finding:** CID computation is fully deterministic and Go-compatible (25, 26, 28). But CID verification is disabled or missing across all push-based P2P paths (18, 23, 24, 29), undermining content-addressed integrity.

### Session 4: Searchable Encryption (Findings 31–39)

**Scope:** SE tag computation, key management, artifact storage, P2P distribution, query integration

| # | Severity | Title |
|---|----------|-------|
| 31 | Informational | [SE tag computation sound equality only](31-se-tag-computation-sound-equality-only.md) |
| 32 | Medium | [SE push docs no identity isolation](32-se-push-docs-no-identity-isolation.md) |
| 33 | Medium | [SE artifact storage key reveals document-tag association](33-se-artifact-storage-key-reveals-document-tag-association.md) |
| 34 | **High** | [SE receiver not implemented — artifacts discarded](34-se-receiver-not-implemented-artifacts-discarded.md) |
| 35 | Medium | [No SE artifact validation on receive](35-no-artifact-validation-on-receive.md) |
| 36 | Medium | [SE enc_key not zeroized Vec\<u8\>](36-se-enc-key-not-zeroized-vec-u8.md) |
| 37 | **High** | [SE query evaluation not in Rust planner/runner](37-se-no-query-evaluation-in-rust-planner.md) |
| 38 | Informational | [SE replicator query leakage analysis](38-se-replicator-query-leakage-analysis.md) |
| 39 | Medium | [SE merge handler no artifact generation](39-se-merge-handler-no-artifact-generation.md) |

**Key Finding:** The SE cryptographic primitives are correct (31), but the end-to-end pipeline is incomplete. The send path works (Rust → Go), but receive (34) and query evaluation (37) are not implemented. These are 1.0 blockers if SE is in scope.

### Session 5: Transaction System (Findings 41–52)

**Scope:** MVCC snapshot isolation, conflict detection, transaction lifecycle across all 4 backends

| # | Severity | Title |
|---|----------|-------|
| 41 | Medium | [ConflictTracker GC misses long-running transactions](41-conflict-tracker-gc-misses-long-running-txns.md) |
| 42 | Low | [Memory backend committed before apply](42-memory-backend-committed-before-apply.md) |
| 43 | Low | [Conflict check not atomic with storage write](43-conflict-check-not-atomic-with-storage-write.md) |
| 44 | Medium | [No transaction timeout or concurrent limit](44-no-transaction-timeout-or-limit.md) |
| 45 | Low | [Drop does not execute discard callbacks](45-drop-does-not-execute-discard-callbacks.md) |
| 46 | Informational | [Write skew possible — documented trade-off](46-write-skew-possible-documented-tradeoff.md) |
| 47 | Informational | [RocksDB OwnedSnapshot transmute sound](47-rocksdb-owned-snapshot-transmute-sound.md) |
| 48 | Verified Clean | [Snapshot isolation verified all backends](48-snapshot-isolation-verified-all-backends.md) |
| 49 | Verified Clean | [Index-document atomicity verified](49-index-document-atomicity-verified.md) |
| 50 | Verified Clean | [Group commit conflict detection correct](50-group-commit-conflict-detection-correct.md) |
| 51 | Verified Clean | [Callback panic safety verified](51-callback-panic-safety-verified.md) |
| 52 | Verified Clean | [Cross-backend consistency verified](52-cross-backend-consistency-verified.md) |

**Key Strength:** The transaction system is architecturally strong — uniform ConflictTracker across all backends, comprehensive concurrency test suite, panic-safe callbacks, and verified snapshot isolation. The two medium issues (41, 44) are resource management gaps, not correctness bugs.

### Session 6: Edge Cases & Convergence (Findings 54–63)

**Scope:** Unbounded nonce storage, float precision, partition healing, deletion/resurrection, schema evolution, index consistency

| # | Severity | Title |
|---|----------|-------|
| 54 | Verified Clean | [Counter nonces survive deletion — resurrection correct](54-counter-nonces-survive-deletion-resurrection-correct.md) |
| 55 | Low | [Float64 running-sum divergence confirmed](55-float64-running-sum-divergence-confirmed.md) |
| 56 | Medium | [Index update failure non-blocking — stale indexes](56-index-update-failure-non-blocking-stale-indexes.md) |
| 57 | Low | [Schema evolution unknown fields silently discarded](57-schema-evolution-unknown-fields-silently-discarded.md) |
| 58 | Verified Clean | [Priority from DAG height not user-controlled](58-priority-from-dag-height-not-user-controlled.md) |
| 59 | Low | [No document size limit](59-no-document-size-limit.md) |
| 60 | Verified Clean | [Partition healing convergence — DAG ordering correct](60-partition-healing-convergence-dag-ordering-correct.md) |
| 61 | Medium | [Nonce storage cost quantified — P2P amplification](61-nonce-storage-cost-quantified.md) |
| 62 | Verified Clean | [LWW deletion and resurrection deterministic](62-lww-deletion-resurrection-deterministic.md) |
| 63 | Low | [Float equality epsilon comparison in queries](63-float-equality-epsilon-comparison-in-queries.md) |

**Key Finding:** The CRDT convergence guarantees hold under all tested edge cases. Partition healing (60), deletion/resurrection (54, 62), and priority generation (58) are all verified correct. The two medium issues are index consistency (56) and nonce storage growth (61).

## Severity Breakdown — All 59 Findings

### High (3)

| # | Session | Title | 1.0 Blocker? |
|---|---------|-------|-------------|
| 11 | 2 | Recursive DAG traversal no depth limit | Yes — stack overflow DoS |
| 34 | 4 | SE receiver not implemented | Yes — if SE in scope |
| 37 | 4 | SE query evaluation not in Rust planner | Yes — if SE in scope |

### Medium (18)

| # | Session | Title | Category |
|---|---------|-------|----------|
| 00 | 1 | Composite counter nonce ordering unsafe | CRDT correctness |
| 01 | 1 | Composite counter missing allow_decrement | CRDT policy |
| 02 | 1 | Composite counter missing Float64 support | CRDT correctness |
| 12 | 2 | merged_composites unbounded growth | Resource exhaustion |
| 14 | 2 | No per-document merge locking | Concurrency |
| 18 | 2 | Block CID not verified before merge | Integrity |
| 23 | 3 | No CID verification on put() | Integrity |
| 24 | 3 | Unsupported hash algorithm bypass | Integrity |
| 29 | 3 | PushLog no CID verification | Integrity |
| 32 | 4 | SE push docs no identity isolation | SE security |
| 33 | 4 | SE artifact storage key leakage | SE privacy |
| 35 | 4 | No SE artifact validation on receive | SE security |
| 36 | 4 | SE enc_key not zeroized | Key management |
| 39 | 4 | SE merge handler no artifact generation | SE completeness |
| 41 | 5 | ConflictTracker GC misses long txns | Conflict detection |
| 44 | 5 | No transaction timeout or limit | Resource exhaustion |
| 56 | 6 | Index update failure non-blocking | Index consistency |
| 61 | 6 | Nonce storage cost quantified | Resource exhaustion |

### Low (14)

| # | Session | Title |
|---|---------|-------|
| 03 | 1 | Float64 counter non-associative divergence |
| 06 | 1 | Counter nonce storage unbounded growth |
| 13 | 2 | Parent block missing silently skipped |
| 15 | 2 | Decryption failure falls back to ciphertext |
| 16 | 2 | Collection delta no dedup guard |
| 17 | 2 | Composite dedup TOCTOU race |
| 27 | 3 | Backup no block-level integrity |
| 42 | 5 | Memory backend committed before apply |
| 43 | 5 | Conflict check not atomic with storage write |
| 45 | 5 | Drop does not execute discard callbacks |
| 55 | 6 | Float64 running-sum divergence confirmed |
| 57 | 6 | Schema evolution unknown fields silently discarded |
| 59 | 6 | No document size limit |
| 63 | 6 | Float equality epsilon comparison in queries |

### Verified Clean (11)

| # | Session | Title |
|---|---------|-------|
| 07 | 1 | LWW tie-breaking correctness |
| 08 | 1 | Counter nonce idempotency |
| 48 | 5 | Snapshot isolation — all backends |
| 49 | 5 | Index-document atomicity |
| 50 | 5 | Group commit conflict detection |
| 51 | 5 | Callback panic safety |
| 52 | 5 | Cross-backend consistency |
| 54 | 6 | Counter nonces survive deletion |
| 58 | 6 | Priority from DAG height |
| 60 | 6 | Partition healing convergence |
| 62 | 6 | LWW deletion/resurrection deterministic |

## Top 5 Recommendations for 1.0

### 1. Enable CID Verification on P2P Ingestion (Findings 18, 23, 24, 29)

The content-addressed integrity model is currently unenforced on push-based P2P paths. A compromised peer can inject blocks with mismatched CIDs, enabling content substitution, priority forgery, and CRDT state corruption.

**Action:** Enable `hash_on_read` by default. Add CID verification on `put()`. Reject unsupported hash algorithms. Verify CIDs on PushLog receive.

### 2. Add DAG Recursion Depth Limit (Finding 11)

The recursive DAG traversal in the merge handler has no depth limit. A malicious peer can craft a deep block chain that causes stack overflow on Rust's fixed-size tokio stacks.

**Action:** Add a configurable depth limit (e.g., 1000) with clear error when exceeded. Convert recursion to iteration with an explicit stack for defense-in-depth.

### 3. Fix Composite Counter Inconsistencies (Findings 00, 01, 02)

The Composite CRDT reimplements counter logic inline, omitting crash-safe nonce ordering, allow_decrement policy, and Float64 support. This creates three distinct correctness gaps.

**Action:** Refactor Composite to delegate counter merges to the standalone Counter CRDT.

### 4. Make Index Updates Blocking (Finding 56)

Index update failures during P2P merge are logged but don't prevent transaction commit. This silently creates stale indexes that never self-heal.

**Action:** Set `process_error` on index update failure so the transaction rolls back. Documents and indexes stay atomically consistent.

### 5. Complete Searchable Encryption Pipeline (Findings 34, 37)

The SE receiver and query evaluator are not implemented. Rust nodes cannot serve SE queries or process received SE artifacts.

**Action:** Implement SE receiver handler and integrate SE evaluation into the query planner/runner. (Only if SE is in 1.0 scope.)

## Architectural Assessment

### Strengths

1. **CRDT Mathematical Properties**: LWW commutativity, counter nonce idempotency, and convergence across all permutations are proven via property tests. The standalone CRDT implementations are correct.

2. **Deterministic CID Computation**: CID generation is fully deterministic and Go-compatible across document encoding, block construction, DAG-CBOR serialization, and DocID generation.

3. **Transaction System**: Uniform ConflictTracker, comprehensive cross-backend test suite, panic-safe callbacks, and verified snapshot isolation across all 4 backends.

4. **Priority-Based Conflict Resolution**: Using DAG height instead of wall-clock time eliminates clock-skew attacks and ensures deterministic ordering.

5. **Deletion/Resurrection Semantics**: Soft deletion with nonce preservation ensures CRDT invariants hold through delete-resurrect cycles.

### Weaknesses

1. **CID Verification Gaps**: The content-addressed integrity model is the foundation of trustless P2P replication, but verification is disabled or missing on all push-based paths. This is the single most important security gap.

2. **Composite Counter Code Duplication**: The Composite CRDT's inline counter implementation diverges from the standalone Counter in 3 ways, creating correctness bugs that only manifest during composite merges (the P2P path).

3. **Searchable Encryption Completeness**: The SE primitives are cryptographically sound but the end-to-end pipeline is incomplete — send works, receive and query don't.

4. **Resource Exhaustion Surfaces**: No limits on nonce accumulation, document size, transaction count, or merged_composites growth. These are DoS vectors that need defense-in-depth for production deployments.

5. **Index Consistency**: Index updates are within the same transaction as document storage (good), but failures don't block commit (bad). This creates a silent consistency gap.

## Overall Security Posture

**Rating: Moderate — suitable for controlled deployments, requires hardening for adversarial environments.**

The core data model is sound. CRDTs converge correctly, transactions provide proper isolation, CIDs are deterministic, and the merge handler's design is architecturally correct. The gaps are primarily in **enforcement** (CID verification disabled), **completeness** (SE pipeline), and **resource management** (unbounded growth patterns). None of the identified issues cause silent data corruption under normal operation — they require either adversarial P2P peers, extreme edge cases (float precision at scale), or sustained resource pressure (nonce/transaction exhaustion).

For 1.0: Fix the 3 high-severity issues, address the CID verification cluster (4 medium findings), and fix the composite counter delegation (3 medium findings). The remaining medium/low findings can be addressed post-1.0 as hardening work.
