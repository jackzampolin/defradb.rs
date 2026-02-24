# FFI Integration Test Coverage Metrics

- **Severity:** INFO
- **Category:** Test Coverage / FFI Compatibility
- **Status:** Informational — comprehensive metrics from `ffi-test status`

## Summary

The `ffi-test` tool (at `tools/ffi-test/`) provides a complete view of Go integration test pass rates when running against the Rust FFI implementation. As of the latest test runs (Feb 10-17, 2026), **2202 of 2290 tests pass (96% pass rate)** across 102 test packages. 3 tests fail and 85 are skipped. 14 query packages have been run but show no individual test data (common-patterns branch runs).

## Details

### Overall Status

| Metric | Value |
|--------|-------|
| Total packages | 102 |
| Total tests | 2290 |
| Passed | 2202 (96%) |
| Failed | 3 |
| Skipped | 85 |

### Top-Level Package Breakdown

| Package | Pass | Fail | Skip | Total | Rate |
|---------|------|------|------|-------|------|
| acp | 345 | 0 | 48 | 393 | 87% |
| backup | 22 | 0 | 0 | 22 | 100% |
| collection | 19 | 0 | 2 | 21 | 90% |
| collection_version | 406 | 0 | 2 | 408 | 99% |
| encryption | 32 | 0 | 6 | 38 | 84% |
| explain | 249 | 0 | 0 | 249 | 100% |
| index | 365 | 0 | 0 | 365 | 100% |
| issues | 0 | 0 | 5 | 5 | 0% |
| mutation | 201 | 2 | 20 | 223 | 90% |
| net | 70 | 0 | 0 | 70 | 100% |
| node | 0 | 1 | 0 | 1 | 0% |
| query | 435 | 0 | 0 | 435 | 100% |
| searchable_encryption | 26 | 0 | 0 | 26 | 100% |
| signature | 18 | 0 | 2 | 20 | 90% |
| subscription | 13 | 0 | 0 | 13 | 100% |
| view | 1 | 0 | 0 | 1 | 100% |

### Failing Tests (3)

1. `mutation/create/field_kinds/one_to_one` — 1 failure (9 pass, 2 skip)
2. `mutation/create/field_kinds/one_to_one_to_one` — 1 failure (1 pass)
3. `node` — 1 failure (0 pass)

### Packages with No Data (14)

These query packages were run on the `common-patterns` branch but produced no individual test results (likely build failures or empty runs):
- `query/commits`, `query/commits/branchables`, `query/inline_array`, `query/json`
- `query/many_to_many`, `query/one_to_many`, `query/one_to_many_multiple`
- `query/one_to_many_to_many`, `query/one_to_many_to_one`
- `query/one_to_one`, `query/one_to_one_multiple`
- `query/one_to_one_to_many`, `query/one_to_one_to_one`, `query/one_to_two_many`

### Skipped Tests (85)

Most skipped tests are in:
- `acp` (48 skips) — likely tests requiring SourceHub or unimplemented features
- `mutation` (20 skips) — embedding tests, some CRDT/field_kinds tests
- `encryption` (6 skips) — encrypted feature tests
- `issues` (5 skips) — known issue reproductions

### Security Implication

The 96% pass rate confirms strong functional compatibility. The failing tests are in relationship mutations (one_to_one, one_to_one_to_one), suggesting potential edge cases in foreign key handling across the FFI boundary. The 14 query packages with no data indicate that relational query patterns (joins) may not yet be fully exercised.

## Test Gap

The metrics confirm that the FFI layer is functionally correct for the vast majority of operations. The primary gaps are:
1. **Relational mutations** — 3 failures in one_to_one relationship creation
2. **Relational queries** — 14 packages with no test data
3. **Node lifecycle** — 1 failure in basic node test
