# INFO Findings

20 findings across 5 streams. These are informational findings that document design trade-offs, verified behaviors, or meta-observations that do not require code changes.

## Stream 01 -- Cryptographic Inventory (1)

| # | Finding File | Title | Status |
|---|-------------|-------|--------|
| 01-17 | `audit/01-cryptographic-inventory-findings/17-se-deterministic-tags-frequency-analysis.md` | SE Deterministic Tags Frequency Analysis | ACKNOWLEDGED |

## Stream 02 -- Access Control Policy (4)

| # | Finding File | Title | Status |
|---|-------------|-------|--------|
| 02-07 | `audit/02-access-control-policy-findings/07-dac-checklist-verification.md` | DAC implementation checklist verification | VERIFIED |
| 02-13 | `audit/02-access-control-policy-findings/13-nac-disabled-state-behavior-analysis.md` | NAC disabled state behavior analysis | VERIFIED CORRECT |
| 02-17 | `audit/02-access-control-policy-findings/17-policy-id-not-content-hash-of-yaml.md` | Policy ID is not a simple content hash of YAML | VERIFIED CORRECT |
| 02-37 | `audit/02-access-control-policy-findings/37-sourcehub-all-session1-4-findings-apply.md` | All Session 1-4 findings apply to SourceHub mode | CONFIRMED |

## Stream 04 -- Identity & Key Management (4)

| # | Finding File | Title | Status |
|---|-------------|-------|--------|
| 04-04 | `audit/04-identity-key-management-findings/04-identity-context-no-public-only-state.md` | IdentityContext has no public-key-only state | CONFIRMED |
| 04-11 | `audit/04-identity-key-management-findings/11-no-token-replay-protection.md` | No token replay protection | CONFIRMED |
| 04-39 | `audit/04-identity-key-management-findings/39-403-not-401-for-invalid-credentials.md` | 403 not 401 for invalid credentials | CONFIRMED |
| 04-62 | `audit/04-identity-key-management-findings/62-no-key-rotation-test.md` | No key rotation test | CONFIRMED |

## Stream 06 -- Data Integrity & CRDT (9)

| # | Finding File | Title | Status |
|---|-------------|-------|--------|
| 06-05 | `audit/06-data-integrity-crdt-findings/05-priority-ceiling-u64max-permanent-immutability.md` | Priority ceiling u64::MAX permanent immutability | INFORMATIONAL |
| 06-09 | `audit/06-data-integrity-crdt-findings/09-composite-pre-validation-atomicity-analysis.md` | Composite pre-validation atomicity analysis | INFORMATIONAL |
| 06-19 | `audit/06-data-integrity-crdt-findings/19-batch-merge-partial-rollback-correctness.md` | Batch merge partial rollback correctness | INFORMATIONAL |
| 06-20 | `audit/06-data-integrity-crdt-findings/20-field-iteration-order-deterministic.md` | Field iteration order deterministic | INFORMATIONAL |
| 06-21 | `audit/06-data-integrity-crdt-findings/21-encryption-block-key-plaintext-in-blockstore.md` | Encryption block key plaintext in blockstore | INFORMATIONAL |
| 06-25 | `audit/06-data-integrity-crdt-findings/25-cid-determinism-dual-cbor-verified.md` | CID determinism dual CBOR verified | INFORMATIONAL |
| 06-26 | `audit/06-data-integrity-crdt-findings/26-time-encoding-rfc3339-go-compatible.md` | Time encoding RFC3339 Go-compatible | INFORMATIONAL |
| 06-28 | `audit/06-data-integrity-crdt-findings/28-block-construction-cid-from-serialized-bytes.md` | Block construction CID from serialized bytes | INFORMATIONAL |
| 06-38 | `audit/06-data-integrity-crdt-findings/38-se-replicator-query-leakage-analysis.md` | SE replicator query leakage analysis | INFORMATIONAL |
| 06-46 | `audit/06-data-integrity-crdt-findings/46-write-skew-possible-documented-tradeoff.md` | Write skew possible -- documented trade-off | INFORMATIONAL |
| 06-47 | `audit/06-data-integrity-crdt-findings/47-rocksdb-owned-snapshot-transmute-sound.md` | RocksDB OwnedSnapshot transmute sound | INFORMATIONAL |

## Stream 07 -- Dependency & Unsafe Code (2)

| # | Finding File | Title | Status |
|---|-------------|-------|--------|
| 07-36 | `audit/07-dependency-unsafe-code-findings/36-dependency-inventory.md` | Comprehensive Dependency Inventory | INFORMATIONAL |
| 07-53 | `audit/07-dependency-unsafe-code-findings/53-ffi-test-coverage-metrics.md` | FFI Test Coverage Metrics | INFORMATIONAL |
