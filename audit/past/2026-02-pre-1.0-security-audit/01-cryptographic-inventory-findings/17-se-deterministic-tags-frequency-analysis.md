# Finding: SE Deterministic Tags Enable Frequency Analysis (By Design)

**Stream**: 01 - Cryptographic Inventory
**Session**: 5 - Searchable Encryption & Merkle Proof
**Severity**: INFORMATIONAL (inherent to the scheme; documented in code)
**Category**: Searchable Encryption / Privacy Properties
**Status**: ACKNOWLEDGED

## Summary

The SE scheme uses deterministic HMAC-SHA256 tags — the same (key, identity, collection, field, value) inputs always produce the same 16-byte tag. This is required for equality search functionality but enables frequency analysis by any party that stores the artifacts (i.e., replicator nodes).

## Evidence

### Deterministic by Design

`crates/crypto/src/se/tag.rs:75-101`: The `generate_equality_tag` function is a pure function — no randomness is involved. Given the same inputs, it always returns the same 16-byte tag.

### Code Acknowledges This

`crates/crypto/src/se/tag.rs:26-31`:

```rust
/// This function generates the SAME tag for the same value across ALL documents
/// in the collection for the same identity. This enables efficient equality search
/// but reveals when multiple documents share the same field value (frequency analysis).
/// For fields with low cardinality (e.g., boolean, status codes), consider the
/// privacy implications.
```

### Tag Truncation Analysis

Tags are truncated to 16 bytes (128 bits) from the 32-byte HMAC-SHA256 output. Birthday bound analysis:

- **Collision probability reaches 50% at ~2^64 tags** (birthday bound for 128-bit space)
- For a single (collection, field) pair, this means 2^64 distinct values before collisions become likely
- This is more than sufficient — real-world cardinality is orders of magnitude lower

The 128-bit truncation is cryptographically sound for the tag collision resistance requirement.

### What a Replicator Learns

Given the artifact structure and storage format, a replicator storing SE artifacts can observe:

| Observable | Leaked Information |
|---|---|
| Same tag across documents | These documents share the same field value |
| Tag frequency distribution | Value distribution for the field (e.g., 70% "active", 30% "inactive") |
| Tag appearance over time | When new values are added, when values change |
| Tag count per document | Number of encrypted-indexed fields |
| Cross-query correlation | Which queries match which documents |

### Low-Cardinality Fields Are Most Vulnerable

| Field Type | Cardinality | Frequency Analysis Risk |
|---|---|---|
| Boolean (active/inactive) | 2 | **CRITICAL** — trivially reveals value |
| Enum/Status (pending/approved/denied) | 3-10 | **HIGH** — statistical inference easy |
| Country code | ~200 | **MEDIUM** — distribution reveals locale |
| Email address | High | **LOW** — mostly unique tags |
| Free text | Very high | **LOW** — tags are mostly unique |

### No Mitigation Mechanisms Present

The codebase contains:
- No runtime warnings for low-cardinality encrypted fields
- No schema-level guidance on which fields are safe to encrypt with SE
- No differential privacy noise injection
- No tag padding or bucketing to obscure frequency

## Impact

### Privacy Implications for Replicators

A replicator (untrusted remote node) that stores SE artifacts can build a complete frequency profile for every encrypted-indexed field. For boolean fields, the replicator can determine the exact value of every document's field with no cryptographic key material.

### This is a Known Trade-Off

Deterministic searchable encryption (SSE) is a well-studied class of schemes. The trade-off between search functionality and frequency leakage is inherent. The code correctly documents this. More advanced schemes (e.g., ORAM-based, frequency-hiding) exist but are significantly more complex and expensive.

## Affected Code

- `crates/crypto/src/se/tag.rs:75-101` — deterministic tag generation
- `crates/db/src/se/artifact_gen.rs` — artifact generation pipeline
- `crates/db/src/se/storage.rs` — artifact storage and query

## Remediation

No code change needed — this is a design decision. Consider:

1. **Documentation**: Add user-facing documentation warning about frequency analysis for low-cardinality fields
2. **Schema validation**: Optionally warn at schema creation time if a boolean or small-enum field has an encrypted index
3. **Long-term**: Evaluate frequency-hiding SE schemes for future versions
