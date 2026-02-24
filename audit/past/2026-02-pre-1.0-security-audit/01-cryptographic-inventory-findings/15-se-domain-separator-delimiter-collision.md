# Finding: SE Domain Separator Delimiter Collision Vulnerability

**Stream**: 01 - Cryptographic Inventory
**Session**: 5 - Searchable Encryption & Merkle Proof
**Severity**: LOW-MEDIUM (theoretical; exploitability constrained by public key structure)
**Category**: Searchable Encryption / Domain Separation
**Status**: NEW

## Summary

The SE tag domain separator `"eq:{identity}:{collection}:{field}"` uses `:` as a delimiter without escaping or length-prefixing the components. If any component contains the `:` character, different (identity, collection, field) tuples can produce identical domain separators, breaking tag isolation.

## Evidence

### Domain Separator Construction

`crates/crypto/src/se/tag.rs:84-85`:

```rust
let identity_str = String::from_utf8_lossy(identity_id);
let domain_separator = format!("eq:{}:{}:{}", identity_str, collection_id, field_name);
```

### Concrete Collision

Consider these inputs:

| Case | identity | collection | field | Domain separator |
|------|----------|------------|-------|------------------|
| A | `"a:b"` | `"c"` | `"d"` | `"eq:a:b:c:d"` |
| B | `"a"` | `"b:c"` | `"d"` | `"eq:a:b:c:d"` |
| C | `"a"` | `"b"` | `"c:d"` | `"eq:a:b:c:d"` |

All three produce identical domain separators and therefore identical HMAC tags for the same value and key.

### Why Identity Bytes Can Contain `:`

The identity is raw public key bytes. The byte `0x3A` (ASCII `:`) can appear naturally in any public key. After `from_utf8_lossy` conversion (or after Finding 10 is fixed and raw bytes are fed directly), the colon byte passes through to the domain separator.

For Ed25519 (32-byte keys), any byte position has approximately a 1/256 chance of being `0x3A`. The probability of at least one colon in 32 bytes is `1 - (255/256)^32 ≈ 11.8%` — not rare.

### Practical Exploitability is Limited

An attacker would need:
1. A valid public key on the curve whose raw bytes produce a colon at the right position
2. Knowledge of the shared `enc_key` (which is a separate trust boundary)
3. To target a specific victim identity

Since public keys are derived from private keys, the attacker cannot freely choose key bytes — they must brute-force until they find a key with the desired raw representation. This is feasible (seconds to minutes depending on key type) but the HMAC key requirement limits practical impact.

### Go Has the Same Vulnerability

Go's `fmt.Sprintf("eq:%s:%s:%s", identityStr, collectionID, fieldName)` in `internal/se/core/tag.go:41` has the identical delimiter collision issue. This is a shared design weakness, not a Rust-specific bug.

## Impact

### Tag Isolation Broken for Specific Key/Collection/Field Combinations

If two users have public keys where one key's suffix matches another key's prefix with a colon boundary, their tags for different (collection, field) pairs could collide. This would allow a replicator to correlate documents across users that should be isolated.

### Severity Mitigation

- Collection IDs are CID-like hashes (extremely unlikely to contain `:`)
- Field names are schema identifiers (extremely unlikely to contain `:`)
- The primary risk vector is the identity bytes, constrained by key generation

## Affected Code

- `crates/crypto/src/se/tag.rs:84-85` — domain separator construction
- Go `internal/se/core/tag.go:41` — same vulnerability

## Remediation

Use length-prefixed components instead of delimiter-separated:

```rust
// Instead of format!("eq:{}:{}:{}", identity, collection, field)
// Use length-prefixed: "eq" || len(identity) || identity || len(collection) || collection || len(field) || field
mac.update(b"eq");
mac.update(&(identity_id.len() as u32).to_be_bytes());
mac.update(identity_id);
mac.update(&(collection_id.len() as u32).to_be_bytes());
mac.update(collection_id.as_bytes());
mac.update(&(field_name.len() as u32).to_be_bytes());
mac.update(field_name.as_bytes());
```

**Note**: This would require a coordinated change with Go to maintain compatibility. Given the low practical exploitability, this may be acceptable as a known limitation for 1.0.
