# Finding 14: Clock Skew and Time Validation Correct

**Severity**: GREEN
**Category**: Authentication / Token Validation
**Status**: Verified sound

## Summary

The clock skew implementation in `verify_auth_token_with_skew()` correctly handles `exp`, `nbf`, and `aud` validation with 60-second tolerance. The use of `saturating_add` prevents integer overflow. All required claims are enforced by serde deserialization.

## Affected Files

- `crates/identity/src/token/mod.rs:142-185` — `verify_auth_token_with_skew()`
- `crates/identity/src/token/claims.rs:8-35` — `IdentityClaims` struct

## Details

### Time validation logic

```rust
// nbf check: reject if nbf > now + 60
if identity.claims.nbf > now.saturating_add(clock_skew_seconds) { ... }

// exp check: reject if exp + 60 < now (i.e., now > exp + 60)
if identity.claims.exp.saturating_add(clock_skew_seconds) < now { ... }
```

| Scenario | `nbf` | `exp` | `now` | Skew | Result |
|----------|-------|-------|-------|------|--------|
| Normal valid | 1000 | 4600 | 2000 | 60 | PASS |
| Just expired | 1000 | 1999 | 2000 | 60 | PASS (within skew) |
| Expired beyond skew | 1000 | 1930 | 2000 | 60 | FAIL (1930+60=1990 < 2000) |
| Future nbf within skew | 2050 | 5650 | 2000 | 60 | PASS (2050 ≤ 2000+60) |
| Future nbf beyond skew | 2100 | 5700 | 2000 | 60 | FAIL (2100 > 2060) |

### Overflow safety

`saturating_add` prevents `u64` overflow. If `exp = u64::MAX`, then `exp.saturating_add(60) = u64::MAX`, and `u64::MAX < now` is always false — the token never expires. This is only reachable if the legitimate signer set `exp = u64::MAX`, which represents an intentionally non-expiring token. Not an attack vector.

### Missing audience rejected

```rust
if let Some(ref audiences) = identity.claims.aud {
    if !audiences.contains(&expected_audience.to_string()) { Err }
} else {
    Err(Error::AudienceMismatch { ... })  // None → rejected
}
```

Tokens without `aud` are rejected. The `aud` field is `Option<Vec<String>>` with `skip_serializing_if = "Option::is_none"`, and serde deserializes missing JSON fields as `None`. This correctly prevents audience bypass via omission.

### Required claims enforcement

`IdentityClaims` requires `sub`, `iss`, `exp`, `nbf`, `iat`, `key_type` as non-optional fields. Missing any of these causes serde deserialization to fail in `decode_claims()`. Only `aud` and `authorized_account` are optional.

## Remediation

None required. The implementation is correct and handles all edge cases safely.
