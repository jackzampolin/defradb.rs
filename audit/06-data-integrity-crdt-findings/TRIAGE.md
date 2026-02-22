# Stream 06: Data Integrity & CRDT Correctness — Triage Report

**Date:** 2026-02-21
**Triaged by:** Claude Opus 4.6
**Total findings reviewed:** 54 (excluding 5 session summaries and STREAM-SUMMARY)

---

## 1. Findings Table

Sorted by severity (HIGH first, GREEN last).

| # | Severity | Title | Status | One-Line Summary |
|---|----------|-------|--------|------------------|
| 11 | HIGH | Recursive DAG traversal no depth limit | CONFIRMED | Merge handler recurses into parent blocks without depth limit; attacker-crafted deep DAG chains overflow tokio's fixed 2 MB stack. |
| 34 | HIGH | SE receiver not implemented — artifacts discarded | CONFIRMED | Rust nodes read and discard incoming SE artifacts; Rust-as-replicator cannot serve SE queries. |
| 37 | HIGH | SE query evaluation not in Rust planner/runner | CONFIRMED | Query planner has no awareness of encrypted indexes; SE equality queries fall back to full scan or return empty on replicators. |
| 00 | MEDIUM | Composite counter nonce ordering unsafe | CONFIRMED | Composite writes counter value before marking nonce; crash between the two causes double-counting on replay. |
| 01 | MEDIUM | Composite counter missing allow_decrement | CONFIRMED | Composite counter merge path has no allow_decrement check; negative increments bypass PCounter policy via P2P. |
| 02 | MEDIUM | Composite counter missing Float64 support | CONFIRMED | Composite hardcodes Int64 interpretation; f64 bytes reinterpreted as i64 produce silently wrong values. |
| 12 | MEDIUM | merged_composites unbounded growth | CONFIRMED | Node-lifetime HashSet of every merged composite CID never cleared; 100K docs x 100 mutations = ~840 MB. |
| 14 | MEDIUM | No per-document merge locking | CONFIRMED | Concurrent merges for same document in run_parallel mode can interleave read-modify-write, losing field updates in denormalized doc. |
| 18 | MEDIUM | Block CID not verified before merge | CONFIRMED | Merge handler loads blocks from blockstore without verifying content hashes to CID; hash_on_read disabled by default. |
| 23 | MEDIUM | No CID verification on put() | CONFIRMED | Blockstore put() and put_many() accept arbitrary (CID, data) pairs without hash verification; poisoned data also cached. |
| 24 | MEDIUM | Unsupported hash algorithm bypass | CONFIRMED | verify_hash() returns Ok(()) for non-SHA2-256 CIDs; attacker uses unsupported hash code to bypass all integrity checks. |
| 29 | MEDIUM | P2P PushLog no CID verification before storage | CONFIRMED | PushLog handler stores peer-supplied block under peer-supplied CID with zero verification; primary P2P attack vector. |
| 32 | MEDIUM | SE push docs no identity isolation | CONFIRMED | SECoordinator created with_key() sets identity to None; all users' tags are identical for same values, enabling cross-user correlation. |
| 33 | MEDIUM | SE artifact storage key reveals document-tag association | CONFIRMED | Storage keys `/se/{col}/{idx}/{tag}/{docID}` expose full document-value graph to storage-level adversary; shared with Go by design. |
| 35 | MEDIUM | No SE artifact validation on receive | CONFIRMED | SE artifacts sent unsigned and fire-and-forget; when receiver is implemented, no validation framework exists to prevent index corruption. |
| 36 | MEDIUM | SE enc_key not zeroized Vec\<u8\> | CONFIRMED | SE encryption key stored as plain Vec\<u8\>; persists in heap after coordinator drop, cloned without zeroization in push path. |
| 39 | MEDIUM | SE merge handler no artifact generation | CONFIRMED | Merge handler does not generate SE artifacts for replicated docs; SE artifact chain breaks at first replication hop. |
| 41 | MEDIUM | ConflictTracker GC misses long-running transactions | CONFIRMED | Hardcoded 1000-entry cap prunes committed write sets without checking active transaction read_versions; long-running txns miss conflicts. |
| 44 | MEDIUM | No transaction timeout or concurrent limit | CONFIRMED | No max duration, no concurrent txn limit; attacker opens thousands of HTTP transactions holding snapshots, preventing compaction. |
| 56 | MEDIUM | Index update failure non-blocking — stale indexes | CONFIRMED | Index update failure during P2P merge logged as warning but does not prevent commit; documents stored without index entries permanently. |
| 61 | MEDIUM | Nonce storage cost quantified — P2P amplification | CONFIRMED | Each counter increment permanently consumes ~180 bytes of nonce storage; at 100/sec one counter grows ~1.5 GB/day with no GC. |
| 03 | LOW | Float64 counter non-associative divergence | CONFIRMED | IEEE 754 non-associativity causes sub-ULP divergence when 3+ float deltas applied in different orders across nodes. |
| 04 | LOW | Property test coverage gaps | CONFIRMED | No Composite property tests, no Float64 3-delta convergence test, no LWW delete commutativity, no adversarial priority ranges. |
| 06 | LOW | Counter nonce storage unbounded growth | CONFIRMED | Nonces stored permanently, never garbage collected; documented in code but no GC implemented. |
| 13 | LOW | Parent block missing silently skipped | CONFIRMED | Missing parent blocks during merge skipped with debug log; deliberate availability-over-consistency trade-off matching Go. |
| 15 | LOW | Decryption failure falls back to ciphertext | CONFIRMED | Failed decryption passes raw ciphertext to CRDT merge; almost always fails CBOR decode but theoretically could store garbage. |
| 16 | LOW | Collection delta no dedup guard | CONFIRMED | Collection delta handler has no dedup set unlike composite handler; redundant processing on dual-broadcast but CRDT state correct. |
| 17 | LOW | Composite dedup TOCTOU race | CONFIRMED | Check-then-insert in merged_composites separated by entire merge; concurrent tasks can both process same CID; CRDT idempotency saves. |
| 27 | LOW | Backup no block-level integrity | CONFIRMED | Backup JSON has no checksum or signature; tampered backups import successfully with modified field values and new CIDs. |
| 42 | LOW | Memory backend committed before apply | CONFIRMED | Memory backend sets committed=true before applying changes; panic at await point loses changes without warning. |
| 43 | LOW | Conflict check not atomic with storage write | CONFIRMED | Direct commit path records write set in ConflictTracker before storage write; failed write leaves phantom conflict entries. |
| 45 | LOW | Drop does not execute discard callbacks | CONFIRMED | Transaction Drop logs warning but does not run on_discard callbacks; by design due to async/panic risks in Drop. |
| 55 | LOW | Float64 running-sum divergence confirmed | CONFIRMED | Running-sum architecture confirmed as root cause of Float64 non-associativity; same behavior as Go. |
| 57 | LOW | Schema evolution unknown fields silently discarded | CONFIRMED | Cross-version merge strips unknown fields from document layer; CRDT blocks preserved in DAG for later schema upgrade. |
| 59 | LOW | No document size limit | CONFIRMED | No field value or document size limit at CRDT/merge layer; multi-GB fields possible up to backend limits (redb 3 GiB). |
| 63 | LOW | Float equality epsilon comparison in queries | CONFIRMED | Query filter uses f64::EPSILON tolerance while CRDT uses byte-exact comparison; inconsistency at sub-ULP scale. |
| 05 | INFO | Priority ceiling u64::MAX permanent immutability | INFORMATIONAL | Delta with priority u64::MAX permanently freezes a field; inherent to LWW CRDTs, mitigated by DAG-height priority generation. |
| 09 | INFO | Composite pre-validation atomicity analysis | INFORMATIONAL | Pre-validation catches type mismatches before writes; post-validation relies on transaction rollback; design is sound with caveats. |
| 19 | INFO | Batch merge partial rollback correctness | INFORMATIONAL | Binary-split retry correctly discards failed batches and retries halves; O(N) worst case, correct rollback semantics. |
| 20 | INFO | Field iteration order deterministic | INFORMATIONAL | Field merges iterate sorted Vec\<DAGLink\>, not HashMap; order is deterministic and irrelevant due to CRDT commutativity. |
| 21 | INFO | Encryption block key plaintext in blockstore | INFORMATIONAL | AES key stored in plaintext in blockstore and synced via Bitswap; ACP gate is advisory only; matches Go architecture gap. |
| 25 | INFO | CID determinism dual CBOR verified | INFORMATIONAL | Both CBOR paths (ciborium for DocID, serde_ipld_dagcbor for blocks) produce deterministic, Go-compatible output. |
| 26 | INFO | Time encoding RFC3339 Go-compatible | INFORMATIONAL | Time formatting matches Go's RFC3339Nano exactly; nanosecond precision, timezone preservation, Z for UTC. |
| 28 | INFO | Block construction CID from serialized bytes | INFORMATIONAL | CID always computed from serialized bytes, not in-memory structs; no double-serialization, no race conditions. |
| 31 | GREEN | SE tag computation sound equality only | GREEN | HMAC-SHA256 tag with domain separator is cryptographically sound; cross-field/collection/identity isolation verified. |
| 38 | INFO | SE replicator query leakage analysis | INFORMATIONAL | Replicator has full visibility into schema, document IDs, value equality, query patterns; inherent to D-SSE design, shared with Go. |
| 46 | INFO | Write skew possible — documented trade-off | INFORMATIONAL | Snapshot isolation with write-write conflict detection only; write skew possible but acceptable for CRDT-based architecture. |
| 47 | INFO | RocksDB OwnedSnapshot transmute sound | INFORMATIONAL | Unsafe transmute for self-referential snapshot struct is sound; Arc\<DB\> guarantees lifetime, Send/Sync correct. |
| 54 | GREEN | Counter nonces survive deletion — resurrection correct | GREEN | Nonces preserved through delete-resurrect cycles; prevents duplicate counter application on resurrection. |
| 58 | GREEN | Priority from DAG height not user-controlled | GREEN | Priority derived from Merkle DAG height, not timestamps or user input; u64::MAX unreachable through normal operation. |
| 60 | GREEN | Partition healing convergence — DAG ordering correct | GREEN | LWW priority-based resolution + counter nonce idempotency ensure convergence regardless of merge order after partition heal. |
| 62 | GREEN | LWW deletion and resurrection deterministic | GREEN | Delete (empty data) at higher priority wins; same priority loses to non-empty (existence preferred); fully deterministic. |
| 07 | GREEN | LWW tie-breaking correctness verified | GREEN | Commutativity, associativity, idempotency all proven; lexicographic byte comparison is correct and platform-independent. |
| 08 | GREEN | Counter nonce idempotency verified | GREEN | Duplicate detection correct; nonce scoping prevents cross-field/document collisions; overflow handling matches Go. |
| 48 | GREEN | Snapshot isolation verified all backends | GREEN | All 4 backends take snapshot at new_txn time; pending writes overlay correctly; tombstones handled; comprehensive test suite. |
| 49 | GREEN | Index-document atomicity verified | GREEN | Document mutations and index updates share single transaction; crash safety guaranteed by backend WAL. |
| 50 | GREEN | Group commit conflict detection correct | GREEN | Flush loop serializes conflict check + storage write; inter-batch conflicts detected; 500-commit batch cap. |
| 51 | GREEN | Callback panic safety verified | GREEN | catch_unwind on sync and async callbacks; panicking callback does not prevent other callbacks or corrupt state. |
| 52 | GREEN | Cross-backend consistency verified | GREEN | All 4 backends use shared ConflictTracker; identical conflict detection semantics; shared test suite confirms parity. |

---

## 2. Themes

### Theme A: CID / Block Integrity Verification (Findings 18, 23, 24, 29)

The content-addressed integrity model — the foundation of trustless P2P replication — is **unenforced on all push-based paths**. The blockstore accepts unverified (CID, data) pairs on put(), hash_on_read is disabled by default, PushLog stores peer-supplied blocks without verification, and unsupported hash algorithms silently pass verification. These four findings form a single systemic vulnerability: a compromised peer can inject blocks with fabricated content.

### Theme B: Composite Counter Code Duplication (Findings 00, 01, 02)

The Composite CRDT reimplements counter logic inline instead of delegating to the standalone Counter. This duplication omits three features the standalone Counter has: crash-safe nonce ordering, allow_decrement policy enforcement, and Float64 numeric kind support. All three findings share a single root cause and a single fix (delegate to Counter).

### Theme C: Searchable Encryption Pipeline Completeness (Findings 32, 33, 34, 35, 36, 37, 39)

SE cryptographic primitives are sound (Finding 31, GREEN), but the end-to-end pipeline is incomplete. The send path works (Rust to Go/Rust), but receiving artifacts (34), querying with SE (37), identity isolation (32), artifact validation (35), key zeroization (36), and merge-time artifact generation (39) are all missing or incomplete. If SE is in 1.0 scope, this is the largest functional gap.

### Theme D: Merge Handler Safety (Findings 11, 12, 14, 15, 16, 17)

The merge handler was designed for sequential processing and later extended with concurrency (`run_parallel`). This created: unbounded recursion depth (11), unbounded dedup set growth (12), missing per-document locking (14), decryption fallback to ciphertext (15), missing collection dedup (16), and TOCTOU in composite dedup (17). Only Finding 11 is high-severity; the others are mitigated by CRDT idempotency.

### Theme E: Transaction & Resource Management (Findings 41, 44, 06, 59, 61)

No limits on transaction count, transaction duration, nonce accumulation, or document size. The ConflictTracker GC has a hardcoded 1000-entry cap that ignores active transactions. These are DoS vectors that require defense-in-depth for production but are not correctness bugs.

### Theme F: CRDT Correctness (Findings 07, 08, 54, 60, 62 -- all GREEN)

The core CRDT properties are mathematically sound. LWW commutativity/associativity/idempotency, counter nonce idempotency, partition healing convergence, and deletion/resurrection semantics are all verified correct with property tests and edge-case coverage.

### Theme G: Float64 Precision (Findings 03, 55, 63)

IEEE 754 non-associativity causes sub-ULP divergence for Float64 counters when 3+ deltas are applied in different orders. The running-sum architecture is the root cause. Query-layer epsilon comparison adds a minor inconsistency. All shared with Go; acceptable for 1.0.

### Theme H: Index Consistency (Finding 56)

Index update failures during P2P merge are logged but do not block the transaction commit. Documents are stored without index entries, creating permanently stale indexes. This is a single finding but has outsized impact on query correctness.

---

## 3. Actionable vs Informational

### Must Fix (1.0 Blockers)

These are CRITICAL/HIGH findings and confirmed HIGH-impact MEDIUM findings that can cause node crashes, data corruption, or fundamental feature gaps.

| # | Severity | Title | Why It Blocks 1.0 |
|---|----------|-------|--------------------|
| 11 | HIGH | Recursive DAG traversal no depth limit | Attacker crashes node via stack overflow; trivial to exploit via P2P |
| 34 | HIGH | SE receiver not implemented | Rust replicator nodes cannot serve SE queries (if SE in scope) |
| 37 | HIGH | SE query evaluation not in planner | Encrypted index queries don't use the index (if SE in scope) |
| 18 | MEDIUM | Block CID not verified before merge | Content substitution via P2P undermines content-addressed integrity |
| 23 | MEDIUM | No CID verification on put() | Fabricated blocks stored and cached indefinitely |
| 24 | MEDIUM | Unsupported hash algorithm bypass | Verification silently skipped for non-SHA256 CIDs |
| 29 | MEDIUM | PushLog no CID verification | Primary P2P attack vector for block injection |
| 00 | MEDIUM | Composite counter nonce ordering | Crash during composite counter merge causes double-counting |
| 01 | MEDIUM | Composite counter missing allow_decrement | PCounter policy bypass via P2P composite path |
| 02 | MEDIUM | Composite counter missing Float64 | Silent data corruption if Float64 counters go through composite merge |
| 56 | MEDIUM | Index update failure non-blocking | Permanently stale indexes after P2P merge; silent query incorrectness |

### Should Fix (Pre-1.0)

MEDIUM findings with real exploit potential or meaningful user impact, but not immediate crash/corruption risks.

| # | Severity | Title | Risk |
|---|----------|-------|------|
| 12 | MEDIUM | merged_composites unbounded growth | Memory exhaustion on long-running nodes (~840 MB at 10M merges) |
| 14 | MEDIUM | No per-document merge locking | Lost updates in denormalized doc view under parallel merge |
| 41 | MEDIUM | ConflictTracker GC misses long txns | Silent write-write conflict bypass under high throughput |
| 44 | MEDIUM | No transaction timeout or limit | HTTP API DoS via leaked transactions |
| 32 | MEDIUM | SE push docs no identity isolation | Cross-user tag correlation on replicators |
| 36 | MEDIUM | SE enc_key not zeroized | Key material persists in heap after drop |
| 61 | MEDIUM | Nonce storage cost quantified | P2P-amplified permanent storage exhaustion |

### Accept Risk / Backlog

LOW/INFO findings that represent design trade-offs, defense-in-depth improvements, or documented limitations.

| # | Severity | Title | Rationale |
|---|----------|-------|-----------|
| 03 | LOW | Float64 non-associative divergence | Sub-ULP; same as Go; document as limitation |
| 04 | LOW | Property test coverage gaps | Test quality; not a runtime risk |
| 06 | LOW | Counter nonce unbounded growth | Documented in code; negligible at typical usage |
| 13 | LOW | Parent block missing silently skipped | Deliberate availability trade-off matching Go |
| 15 | LOW | Decryption failure falls back to ciphertext | Almost always fails CBOR decode; ACP gate prevents in practice |
| 16 | LOW | Collection delta no dedup guard | Redundant work only; CRDT correctness maintained |
| 17 | LOW | Composite dedup TOCTOU race | CRDT idempotency mitigates; wasted work only |
| 27 | LOW | Backup no block-level integrity | Document-level import regenerates CIDs; low priority |
| 42 | LOW | Memory backend committed before apply | Testing-only backend; low practical risk |
| 43 | LOW | Conflict check not atomic with storage write | False positives (unnecessary retries), not false negatives |
| 45 | LOW | Drop does not execute discard callbacks | By design; async/panic safety constraint |
| 55 | LOW | Float64 running-sum divergence confirmed | Extension of Finding 03; same Go parity rationale |
| 57 | LOW | Schema evolution unknown fields discarded | Correct behavior; CRDT blocks preserved in DAG |
| 59 | LOW | No document size limit | P2P message size provides partial protection |
| 63 | LOW | Float equality epsilon comparison | Standard float practice; sub-ULP inconsistency |
| 05 | INFO | Priority ceiling u64::MAX | Inherent to LWW; mitigated by DAG-height generation |
| 09 | INFO | Composite pre-validation atomicity | Sound design with documented caveats |
| 19 | INFO | Batch merge rollback correctness | Verified correct |
| 20 | INFO | Field iteration order deterministic | Verified correct |
| 21 | INFO | Encryption key plaintext in blockstore | Architectural gap shared with Go; requires KMS for fix |
| 25 | INFO | CID determinism dual CBOR verified | Verified correct |
| 26 | INFO | Time encoding Go-compatible | Verified correct |
| 28 | INFO | Block construction CID from bytes | Verified correct |
| 33 | MEDIUM | SE artifact storage key leakage | Inherent to SE design; shared with Go; no code change needed |
| 35 | MEDIUM | No SE artifact validation on receive | Blocked on Finding 34; must be addressed when receiver is built |
| 38 | INFO | SE replicator query leakage | Inherent to D-SSE; documented privacy trade-off |
| 39 | MEDIUM | SE merge handler no artifact generation | Matches Go design; document for now |
| 46 | INFO | Write skew possible | Accepted trade-off; CRDT handles concurrent mods |
| 47 | INFO | RocksDB transmute sound | Verified correct |

### No Action (GREEN)

Confirmed safe. No changes needed.

| # | Title |
|---|-------|
| 07 | LWW tie-breaking correctness verified |
| 08 | Counter nonce idempotency verified |
| 31 | SE tag computation sound for equality search |
| 48 | Snapshot isolation verified all backends |
| 49 | Index-document atomicity verified |
| 50 | Group commit conflict detection correct |
| 51 | Callback panic safety verified |
| 52 | Cross-backend consistency verified |
| 54 | Counter nonces survive deletion — resurrection correct |
| 58 | Priority from DAG height not user-controlled |
| 60 | Partition healing convergence correct |
| 62 | LWW deletion and resurrection deterministic |

---

## 4. Recommended Fix Order

The ordering prioritizes: (1) crash/DoS prevention, (2) data integrity enforcement, (3) correctness bugs, (4) feature completeness, (5) resource management.

### Phase 1: Prevent Node Crashes and Data Corruption (Week 1)

**1. Add DAG recursion depth limit (Finding 11)**
- Why first: Trivially exploitable DoS that crashes the node via stack overflow. Single function change with a depth counter parameter.
- Effort: Small (add depth parameter, propagate through 4 call sites).
- Validation: Unit test with 1000+ deep chain; integration test for P2P rejection.

**2. Enable CID verification on P2P ingestion (Findings 29, 23, 18, 24)**
- Why second: These four findings form a single vulnerability cluster. Without CID verification, the content-addressed trust model is broken. Fix in this order:
  - 29: Add `verify_block_cid()` before `put()` in PushLog handler (highest attack surface).
  - 23: Add optional verify-on-put in blockstore (defense-in-depth).
  - 18: Enable `hash_on_read` by default for P2P blockstores.
  - 24: Reject unsupported hash algorithms in `verify_hash()` (match ProofNode behavior).
- Effort: Medium (4 targeted changes, all in the same verification layer).
- Validation: Unit tests with mismatched CID/data; integration test with malicious PushLog.

**3. Fix index update failure handling (Finding 56)**
- Why third: Silent query incorrectness from stale indexes is hard to diagnose in production. Simple fix (set process_error on index failure).
- Effort: Small (change warning to error assignment in 2-4 locations).
- Validation: Integration test that verifies index entries exist after P2P merge.

### Phase 2: Fix CRDT Correctness (Week 2)

**4. Fix Composite counter delegation (Findings 00, 01, 02)**
- Why grouped: Single root cause (inline reimplementation). Fix all three by delegating to standalone Counter.
  - 00: Swap nonce/value write ordering in composite.rs.
  - 01: Add allow_decrement to FieldCrdtType::Counter and check in apply_field_delta.
  - 02: Add NumericKind to FieldCrdtType::Counter and dispatch Float64 handling.
- Effort: Medium (refactor FieldCrdtType enum, update apply_field_delta).
- Validation: Unit tests for negative increment rejection, Float64 through composite, crash ordering.

### Phase 3: Resource Management (Week 3)

**5. Replace merged_composites HashSet with bounded LRU (Finding 12)**
- Effort: Small (swap HashSet for LruCache, match existing blockstore pattern).

**6. Add transaction limits and automatic cleanup (Finding 44)**
- Effort: Medium (concurrent txn cap in new_txn, periodic cleanup task in HTTP server).

**7. Fix ConflictTracker GC to respect active transactions (Finding 41)**
- Effort: Medium (track min active read_version, prune only safe entries).

**8. Add per-document merge locking (Finding 14)**
- Effort: Medium (DashMap-based lock manager or per-collection channel serialization).

### Phase 4: Searchable Encryption (Week 4, if in scope)

**9. Implement SE artifact receiver (Finding 34)**
- Effort: Large (CBOR deserialization, validation, storage integration, size limits).

**10. Integrate SE into query planner/runner (Finding 37)**
- Effort: Large (planner index selection, runner evaluation, coordinator wiring).

**11. Add identity isolation to SE coordinator (Finding 32)**
- Effort: Small (thread identity pubkey through to push_existing_docs).

**12. Add SE key zeroization (Finding 36)**
- Effort: Small (use `zeroize::Zeroizing<Vec<u8>>` for enc_key).

### Phase 5: Backlog (Post-1.0)

- Findings 03/55: Float64 precision documentation
- Findings 06/61: Nonce GC implementation
- Finding 15: Explicit decryption failure handling
- Finding 27: Backup file integrity checksums
- Finding 42: Memory backend committed flag ordering
- Finding 44: Per-transaction timeouts
- Finding 59: Document size limits
- Finding 04: Property test expansion

---

## Summary Statistics

| Category | Count |
|----------|-------|
| Total findings (non-summary) | 54 |
| HIGH | 3 |
| MEDIUM | 18 |
| LOW | 14 |
| INFO | 9 |
| GREEN (verified safe) | 12 |
| Must-fix for 1.0 | 11 |
| Should-fix pre-1.0 | 7 |
| Accept risk / backlog | 24 |
| No action needed | 12 |
