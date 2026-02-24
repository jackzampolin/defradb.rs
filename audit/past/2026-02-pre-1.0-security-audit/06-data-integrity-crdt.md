# Audit Stream 6: Data Integrity & CRDT Correctness

## Scope

Data integrity guarantees and CRDT merge correctness. Audit covers:
- CID computation (Rust-Go parity, collision resistance)
- CRDT merge semantics under adversarial inputs
- Searchable encryption correctness and security properties
- Block verification and tamper detection
- Backup/restore integrity (round-trip fidelity)
- Transaction isolation and consistency
- Document lifecycle invariants
- Index consistency after mutations

## Key Questions

- Can a crafted CRDT delta corrupt the merge state?
- Are there CRDT merge paths that don't validate inputs?
- Is the searchable encryption scheme IND-CPA secure?
- Can encrypted indexes leak ordering or frequency information?
- Do transactions properly isolate concurrent operations?
- Can a partial backup restore leave the DB in an inconsistent state?
- Are there invariants that can be violated by specific operation sequences?

## Crates of Interest

- `crdt/`
- `blockstore/`
- `document/`
- `defra-core/` (core types and invariants)
- `storage/` (transaction implementation)
- `query/` (index usage)

## Recon Findings

### Surface Area
- **CRDT crate**: 1,749 LOC (src) + 3,021 LOC (tests) - LWW, Counter, Composite, Priority
- **Merge handler**: 3,126 LOC across 7 files (VERY HIGH complexity)
- **Blockstore**: 424 LOC (CID verification, LRU cache, merge tracking)
- **Searchable encryption**: 725 LOC in db/src/se/ (coordinator, artifact_gen, storage)
- **Transaction impl**: Across 4 storage backends (redb, fjall, rocksdb, memory)

### CRDT Implementation
- **LWW**: Priority-based with lexicographic tie-breaking for deterministic convergence
- **Counter**: Nonce-based idempotency, wrapping Int64 semantics, overflow rejection for Float64
- **Composite**: Pre-validates ALL field types before applying ANY changes (atomic)
- All field names, schema versions, doc IDs validated before merge

### CID Computation
- DAG-CBOR serialization (codec 0x71) + SHA2-256 (code 0x12)
- Sorted heads/links (lexicographic by CID) for determinism
- Hash-on-read verification available (SHA2-256 digest comparison)

### Merge Handler (Most Complex Component)
- Recursive DAG traversal (parents before children)
- Composite CID deduplication (prevents re-processing from dual broadcast)
- Encryption-aware merge (skip decryption if document not in local ACP)
- Batch merge with binary-split strategy

### Transaction Isolation
- MVCC snapshot isolation with buffered writes (BTreeMap)
- Write-write conflict detection (ConflictTracker)
- Drop safety: warns if uncommitted
- Write skew possible (documented trade-off)

### Red Flags
- **MEDIUM: Nonce storage unbounded** - Counter nonces stored permanently without GC (intentional trade-off)
- **MEDIUM: Write skew possible** - Snapshot isolation, not serializable
- **LOW: Hash verification optional** - `hash_on_read()` flag, not always enabled
- **LOW: Unsupported hash algorithms** - Logged warning, verification skipped (permissive read)

### Green Strengths
- Mathematically sound CRDT implementations with comprehensive tests (3K LOC)
- Deterministic convergence via lexicographic tie-breaking
- Transaction-level atomicity for document + index updates
- CID computation is standard and collision-resistant

## Estimated Scope

**MEDIUM: 3-6 sessions**

### Session 1: CRDT Merge Semantics (CRITICAL)

| File | Lines | Focus |
|------|-------|-------|
| `crates/crdt/src/lww.rs` | 208-222 | LWW tie-breaking (lexicographic `<=`), empty value edge case |
| `crates/crdt/src/counter.rs` | 281-308, 376-481 | Nonce idempotency, Int64 wrapping, Float64 overflow rejection |
| `crates/crdt/src/composite.rs` | 406-461 | Pre-validation atomicity, field type mismatch handling |
| `crates/crdt/src/traits.rs` | 10-40, 91-119 | MergeResult semantics, ReplicatedData contract |
| `crates/crdt/tests/property_tests.rs` | all | Property-based convergence tests |

**Checklist**: LWW tie-breaker direction, counter allow-decrement validation, composite atomicity, adversarial deltas (schema_version mismatch, wrong field_name)

### Session 2: Merge Handler & DAG Convergence (CRITICAL)

| File | Lines | Focus |
|------|-------|-------|
| `crates/db/src/merge_handler/mod.rs` | 139-300 | Block decode/dispatch, encryption-aware merge |
| `crates/db/src/merge_handler/batch.rs` | 35-137 | Batch merge with rollback, binary-split retry |
| `crates/db/src/merge_handler/composite.rs` | 90-161, 443-451 | **Recursive parent processing** (Box::pin), merged_composites dedup, field merge ordering |
| `crates/db/src/merge_handler/lww.rs` | 1-231 | Field-level LWW merges |
| `crates/db/src/merge_handler/counter.rs` | 1-338 | Field-level counter merges |

**Checklist**: DAG traversal stack safety (100+ parents), dedup guard races, parent load failure (continue vs error), batch rollback correctness, binary-split base case

### Session 3: Block Integrity & CID Determinism (HIGH) — COMPLETE

| # | Severity | Title |
|---|----------|-------|
| 23 | Medium | No CID verification on blockstore put() — blocks stored without hash validation |
| 24 | Medium | Unsupported hash algorithm (non-SHA2-256) bypasses ALL integrity verification |
| 25 | Informational | CID determinism verified — dual CBOR codecs both produce deterministic Go-compatible output |
| 26 | Informational | Time encoding (RFC3339 nano) matches Go, deterministic across platforms |
| 27 | Low | Backup file has no integrity checksum; import uses GraphQL (safe, CIDs regenerated) |
| 28 | Informational | Block construction correct — CID computed from serialized bytes atomically |
| 29 | Medium | P2P PushLog stores blocks without CID content verification |

**Key findings**: Three Medium-severity verification gaps form a connected attack surface — PushLog sends unverified data (29) → blockstore stores without verification (23) → unsupported hash codes bypass verification even when enabled (24). CID determinism is sound — CBOR encoding, time formatting, key ordering, and DocID computation all match Go.

### Session 4: Searchable Encryption & Privacy (HIGH) — COMPLETE

| # | Severity | Title |
|---|----------|-------|
| 31 | Green | SE tag computation sound for equality search — HMAC-SHA256 construction verified |
| 32 | Medium | Push docs creates coordinator without identity — tag isolation broken for all pushes |
| 33 | Medium | Artifact storage key reveals document-tag associations in plaintext |
| 34 | High | SE receiver not implemented — incoming artifacts silently discarded |
| 35 | Medium | No SE artifact validation on P2P receive path (blocked on #34) |
| 36 | Medium | SE enc_key stored as plain Vec<u8> — no zeroization on drop |
| 37 | High | SE query evaluation not integrated into Rust query planner/runner |
| 38 | Informational | Replicator query leakage — complete access pattern visibility (by design) |
| 39 | Medium | Merge handler does not generate SE artifacts for replicated documents |

**Key findings**: The SE cryptographic primitives are sound (HMAC-SHA256, proper domain separation, 128-bit tags). However, the end-to-end SE pipeline is incomplete: the receive path discards artifacts (#34), the query planner has no SE integration (#37), and push_docs omits identity for tag isolation (#32). Two HIGH-severity items are 1.0 blockers (plus the Stream 1 UTF-8 lossy finding). Deterministic tags enable frequency analysis by design (shared with Go).

### Session 5: Transaction Isolation & Consistency (MEDIUM) — COMPLETE

| # | Severity | Title |
|---|----------|-------|
| 41 | Medium | ConflictTracker GC misses conflicts for long-running transactions |
| 42 | Low | Memory backend marks committed before applying changes |
| 43 | Low | Conflict check not atomic with storage write (direct commit) |
| 44 | Medium | No transaction timeout or concurrent transaction limit |
| 45 | Low | Drop does not execute discard callbacks (by design) |
| 46 | Informational | Write skew possible — documented trade-off |
| 47 | Informational | RocksDB OwnedSnapshot transmute is sound |
| 48 | Informational | Snapshot isolation verified across all backends |
| 49 | Informational | Index-document atomicity verified |
| 50 | Informational | Group commit conflict detection correctly atomic |
| 51 | Informational | Callback panic safety verified |
| 52 | Informational | Cross-backend consistency verified |

**Key findings**: Two Medium-severity issues. The ConflictTracker GC (#41) prunes committed write sets at a hard cap of 1000 entries without awareness of active transaction read_versions, allowing write-write conflicts to go undetected under sustained high throughput with long-running transactions. No transaction timeout or concurrent transaction limit (#44) creates a DoS vector via the HTTP API. Architectural strengths: uniform ConflictTracker across all four backends, group commit optimization with correctly serialized conflict detection, catch_unwind callback panic safety, and atomic document+index operations within single transactions.

### Session 6: Edge Cases & Convergence (MEDIUM) — COMPLETE

| # | Severity | Title |
|---|----------|-------|
| 54 | Verified Clean | Counter nonces survive deletion — resurrection semantics correct |
| 55 | Low | Float64 running-sum architecture causes order-dependent divergence (extends #03) |
| 56 | Medium | Index update failure does not block transaction commit — stale indexes |
| 57 | Low | Schema evolution: unknown fields silently discarded during cross-version merge |
| 58 | Verified Clean | Priority values derived from DAG height — not user-controlled |
| 59 | Low | No document size limit — single field can be multi-GB |
| 60 | Verified Clean | Partition healing convergence — DAG ordering ensures correctness |
| 61 | Medium | Nonce storage cost quantified — P2P amplification vector (extends #06) |
| 62 | Verified Clean | LWW deletion and resurrection fully deterministic |
| 63 | Low | Float equality uses f64::EPSILON comparison — CRDT/query inconsistency |

**Key findings**: The CRDT convergence guarantees hold under all tested edge cases. Partition healing (60), deletion/resurrection (54, 62), and priority generation (58) are all verified correct. Two Medium-severity issues: index update failures during P2P merge don't block commit, leaving documents stored without index entries (#56); nonce storage grows permanently at ~180 bytes per counter increment with no GC, quantifying the DoS surface at ~1.5 GB/day for 100 ops/sec (#61).

## Stream Summary

See [STREAM-SUMMARY.md](06-data-integrity-crdt-findings/STREAM-SUMMARY.md) for the comprehensive summary of all 59 findings across 6 sessions, severity tables, top 5 recommendations, and overall security assessment.
