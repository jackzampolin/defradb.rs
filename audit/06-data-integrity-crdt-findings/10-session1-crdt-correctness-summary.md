# Session 1 Summary: Data Integrity & CRDT Correctness

**Date:** 2026-02-21
**Auditor:** Claude Opus 4.6
**Scope:** CRDT crate - LWW, Counter, Composite implementations
**Files Reviewed:** 12 files, ~5,000 LOC (src + tests)

## Executive Summary

The CRDT implementations are fundamentally sound. The LWW and standalone Counter CRDTs satisfy the required mathematical properties (commutativity, associativity, idempotency) and are well-tested with property-based tests covering convergence across all orderings. However, the Composite CRDT has three medium-severity gaps where it diverges from the standalone Counter's behavior, creating inconsistencies that could cause data corruption or convergence failures during P2P replication.

## Findings by Severity

### Medium (3)

| # | Finding | Impact |
|---|---------|--------|
| 00 | [Composite counter nonce ordering unsafe](00-composite-counter-nonce-ordering-unsafe.md) | Crash during composite counter merge can cause double-counting, violating idempotency |
| 01 | [Composite counter missing allow_decrement](01-composite-counter-missing-allow-decrement.md) | Negative increments bypass schema policy when routed through Composite merge path |
| 02 | [Composite counter missing Float64 support](02-composite-counter-missing-float64-support.md) | Float64 counter values silently reinterpreted as Int64, causing data corruption |

### Low (2)

| # | Finding | Impact |
|---|---------|--------|
| 03 | [Float64 counter non-associative divergence](03-float64-counter-non-associative-divergence.md) | IEEE 754 rounding causes ULP-level divergence for 3+ deltas in different orders |
| 06 | [Counter nonce storage unbounded growth](06-counter-nonce-storage-unbounded-growth.md) | Nonces accumulate without GC; ~430 MB/day for 100 ops/second counter |

### Informational (2)

| # | Finding | Impact |
|---|---------|--------|
| 05 | [Priority ceiling u64::MAX](05-priority-ceiling-u64max-permanent-immutability.md) | Inherent to LWW CRDTs; defense is at priority generation layer |
| 09 | [Composite pre-validation atomicity](09-composite-pre-validation-atomicity-analysis.md) | Design is sound given proper transaction usage |

### Test Gaps (1)

| # | Finding | Impact |
|---|---------|--------|
| 04 | [Property test coverage gaps](04-property-test-coverage-gaps.md) | Missing Float64 convergence, Composite convergence, delete commutativity tests |

### Verified Clean (2)

| # | Finding |
|---|---------|
| 07 | [LWW tie-breaking correctness](07-lww-tie-breaking-correctness-verified.md) |
| 08 | [Counter nonce idempotency](08-counter-nonce-idempotency-verified.md) |

## Root Cause Analysis

The three medium-severity findings share a common root cause: **the Composite CRDT reimplements counter logic inline instead of delegating to the standalone Counter**. The Composite has its own counter merge code (`composite.rs:263-327`) that duplicates the Counter's behavior but omits:

1. Nonce-before-value write ordering (crash safety)
2. The `allow_decrement` check (schema policy enforcement)
3. Float64 type support and overflow protection

**Recommended fix:** Refactor the Composite to delegate counter merges to the standalone `Counter` CRDT rather than reimplementing the logic inline.

## Security Checklist Results

| Check | Result |
|-------|--------|
| LWW commutativity | PASS |
| LWW associativity | PASS |
| LWW idempotency | PASS |
| LWW delete semantics | PASS |
| LWW tie-breaking determinism | PASS (lexicographic on raw bytes) |
| Counter nonce idempotency | PASS (standalone), NONCE-ORDER-FAIL (composite) |
| Counter overflow Int64 | PASS (wrapping, matches Go) |
| Counter overflow Float64 | PASS (standalone rejects infinity/NaN) |
| Counter allow_decrement enforcement | PASS (standalone), FAIL (composite) |
| Composite pre-validation | PASS |
| Composite atomicity | PASS (transaction-based) |
| Schema version validation | PASS (all three CRDT types) |
| Field name validation | PASS (all three CRDT types) |
| Unknown field rejection | PASS |
| Cross-type delta rejection | PASS |
| Property test coverage | PARTIAL (gaps documented) |

## What Was NOT Checked (Deferred to Future Sessions)

- How priority values are generated (Merkle-DAG height counter logic)
- How CRDTs are instantiated and routed during P2P merge (merge handler code)
- Whether the Composite merge path is actually used in production (may be standalone CRDTs only)
- CRDT interaction with searchable encryption
- CRDT interaction with ACP (access control filtering of deltas)
