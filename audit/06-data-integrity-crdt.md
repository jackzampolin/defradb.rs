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

### Session 3: Block Integrity & CID Determinism (HIGH)

| File | Lines | Focus |
|------|-------|-------|
| `crates/blockstore/src/lib.rs` | 143-175, 184-207 | Hash verification optional, cache bypass on rehash, permissive read |
| `crates/document/src/encoding.rs` | 17-25 | RFC3339 time encoding (affects CID) |
| `crates/document/src/doc_id.rs` | 36-46 | DocID from UUID v5 + content CID |

**Checklist**: Hash mismatch handling, unsupported algorithms silently pass, CID determinism, sorted heads/links

### Session 4: Searchable Encryption & Privacy (HIGH)

| File | Lines | Focus |
|------|-------|-------|
| `crates/db/src/se/coordinator.rs` | 44-150 | SE coordinator, enc_key as Vec<u8> (zeroized?), identity_pubkey tag isolation |
| `crates/db/src/se/artifact_gen.rs` | all | Artifact generation (IND-CPA?) |
| `crates/db/src/se/storage.rs` | all | Artifact storage and retrieval |

**Checklist**: Frequency analysis, deterministic vs randomized encryption, field name leakage, tag isolation, replicator query leakage

### Session 5: Transaction Isolation & Consistency (MEDIUM)

| File | Lines | Focus |
|------|-------|-------|
| `crates/storage/src/backends/redb/transaction.rs` | 17-44, 108-114 | Snapshot isolation, pending writes BTreeMap |
| `crates/storage/src/backends/shared.rs` | 183-259 | ConflictTracker: version counter, write-write detection, GC at 1000 entries |

**Checklist**: Write skew possible (documented), ConflictTracker GC pruning, drop safety (uncommitted txn), callback execution

### Session 6: Edge Cases (MEDIUM)

| File | Lines | Focus |
|------|-------|-------|
| `crates/crdt/src/counter.rs` | 292-300, 424, 459-465 | Nonce storage unbounded (8B/increment), Int64 wrapping, Float64 NaN/infinity rejection |

**Checklist**: Nonce GC strategy, float rounding accumulation, partition healing DAG order, counter deletion resurrection
