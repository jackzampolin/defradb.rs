# Finding: Replicator Query Leakage — Complete Access Pattern Visibility

**Stream**: 06 - Data Integrity & CRDT Correctness
**Session**: 4 - Searchable Encryption Deep-Dive
**Severity**: INFORMATIONAL (inherent to SE design, shared with Go)
**Category**: Searchable Encryption / Privacy Analysis
**Status**: ACKNOWLEDGED

## Summary

A replicator node storing SE artifacts has complete visibility into query access patterns, document-value relationships, and schema structure. This analysis consolidates the information leakage from the SE subsystem to characterize the total information available to an honest-but-curious replicator.

## Replicator's Observable Information

### From SE Artifact Storage

| Observable | Source | Information Gained |
|---|---|---|
| Field names | `index_id` in artifacts (= field name) | Complete schema of encrypted-indexed fields |
| Document IDs | `doc_id` in artifacts | Document existence and lifecycle |
| Value equality | Same `search_tag` across documents | Which documents share the same value |
| Value frequency | Tag count distribution | Statistical distribution of values per field |
| Value cardinality | Distinct tag count per field | Number of unique values per field |
| Value changes | New artifacts replacing old | When document field values change |

### From SE Queries

| Observable | Source | Information Gained |
|---|---|---|
| Query predicates | `search_tag` in query | Which value is being searched for |
| Query frequency | Repeated queries | How often specific values are searched |
| Result set | Documents matching a query | Which documents have the queried value |
| Multi-field queries | Multiple tags in one request | AND conjunctions over multiple fields |
| Temporal patterns | Query timing | When queries happen, access patterns over time |

### From Document Correlation

| Analysis | Method | Information Gained |
|---|---|---|
| Boolean field values | 2-tag field, count each tag | Exact value of every document (majority/minority) |
| Low-cardinality enums | Few tags, known distribution | Statistical inference of field values |
| Known-plaintext attack | If replicator knows one doc's value, tag reveals all docs with same value | Full equality matching without key |
| Cross-query correlation | Same tag in multiple queries | Repeated searches for same value |

## Security Level Assessment

The SE scheme provides **IND-CPA Level 1 (Deterministic SSE)** security:

| Property | Status |
|---|---|
| Value confidentiality | YES — HMAC output hides actual value |
| Equality pattern hiding | NO — same value → same tag (deterministic) |
| Frequency hiding | NO — tag counts reveal value distribution |
| Query pattern hiding | NO — replicator sees every query |
| Access pattern hiding | NO — replicator sees which docs match |
| Schema hiding | NO — field names in plaintext |
| Document existence hiding | NO — doc IDs in plaintext |

This is the standard security level for searchable symmetric encryption (SSE) schemes used in practice (e.g., Song-Wagner-Perrig, Curtmola et al.). More advanced schemes (ORAM-based, frequency-hiding) exist but are significantly more expensive.

## Comparison with Go

Go DefraDB has identical leakage characteristics. This is a shared design decision with the same trust model and same information exposed to replicators.

## Risk Context

The SE scheme is designed for scenarios where:
1. The replicator is semi-trusted (won't actively attack, may be curious)
2. The data owner accepts that structural metadata is visible
3. The primary goal is preventing plaintext value exposure, not pattern hiding
4. Performance and simplicity are valued over maximum privacy

For scenarios requiring stronger privacy (hiding equality patterns, access patterns), consider:
- Client-side filtering (download all encrypted data, filter locally)
- ORAM-based schemes (log(n) overhead per query)
- Trusted execution environments (SGX/TDX on the replicator)

## Affected Code

- Entire `crates/db/src/se/` module — by design
- `crates/crypto/src/se/` — deterministic tag generation
- `crates/p2p/src/message/se.rs` — plaintext metadata in messages

## Cross-References

- Finding 01-17: Deterministic tags enable frequency analysis (INFORMATIONAL)
- Finding 01-18: SE artifact metadata leakage to replicators (MEDIUM)
- Finding 33: Storage key reveals document-tag associations
