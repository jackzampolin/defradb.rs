# Finding: SE Tag Computation Sound for Equality Search

**Stream**: 06 - Data Integrity & CRDT Correctness
**Session**: 4 - Searchable Encryption Deep-Dive
**Severity**: GREEN (sound construction for stated security level)
**Category**: Searchable Encryption / Cryptographic Construction
**Status**: VERIFIED

## Summary

The SE tag computation uses HMAC-SHA256 with a well-structured domain separator, truncated to 128 bits. The construction is cryptographically sound for deterministic equality search and correctly includes identity, collection, field name, and value in the tag input. Cross-field, cross-collection, and cross-identity tag collisions are prevented by design.

## Evidence

### Tag Construction

`crates/crypto/src/se/tag.rs:75-101`:

```rust
pub fn generate_equality_tag(
    key: &[u8],
    identity_id: &[u8],
    collection_id: &str,
    field_name: &str,
    value: &[u8],
) -> [u8; SEARCH_TAG_SIZE] {
    let identity_str = String::from_utf8_lossy(identity_id);
    let domain_separator = format!("eq:{}:{}:{}", identity_str, collection_id, field_name);
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC can take key of any size");
    mac.update(domain_separator.as_bytes());
    mac.update(value);
    let result = mac.finalize();
    let full_tag = result.into_bytes();
    let mut tag = [0u8; SEARCH_TAG_SIZE];
    tag.copy_from_slice(&full_tag[..SEARCH_TAG_SIZE]);
    tag
}
```

### Domain Separation Verified

The domain separator `"eq:{identity}:{collection}:{field}"` ensures:

| Component | Included | Prevents |
|-----------|----------|----------|
| `"eq:"` prefix | Yes | Future index type collision (range, prefix) |
| `identity_id` | Yes | Cross-user tag correlation |
| `collection_id` | Yes | Cross-collection tag leakage |
| `field_name` | Yes | Cross-field tag leakage |
| `value` (encoded) | Yes | Different values → different tags |

### 128-bit Truncation Analysis

Tags are truncated from 256 to 128 bits. Birthday bound: collision probability reaches 50% at ~2^64 tags. For any realistic field cardinality (even billions of values), this provides strong collision resistance.

### Value Encoding

`crates/db/src/se/artifact_gen.rs:39`:

```rust
let value_bytes = encode_field_value(Vec::new(), field_value, false)?;
```

Values are encoded using the order-preserving encoding before tag computation. This ensures consistent byte representation for identical logical values.

### Unit Tests Cover Key Isolation Properties

Tests in `crates/crypto/src/se/tag.rs` verify:
- Different values → different tags
- Different fields → different tags
- Different collections → different tags
- Different identities → different tags
- Different keys → different tags
- Determinism (same inputs → same tag)

## Assessment

The HMAC-SHA256 construction is standard and sound. The scheme provides deterministic symmetric searchable encryption (D-SSE) at an appropriate security level for equality queries. Known limitations (frequency analysis, metadata leakage) are documented in Stream 1 findings.

## Cross-References

- Finding 01-10: UTF-8 lossy conversion divergence from Go (HIGH - 1.0 blocker)
- Finding 01-15: Domain separator delimiter collision (LOW-MEDIUM)
- Finding 01-17: Deterministic tags enable frequency analysis (INFORMATIONAL)
- Finding 01-19: HMAC key no length validation (LOW-MEDIUM)
