# Finding 19: HTTP Identity Extraction and Audience Verification Correct

**Severity**: GREEN
**Category**: Authentication / HTTP Integration
**Status**: Verified sound

## Summary

The HTTP identity extractor (`ExtractIdentity` and `ExtractTokenIdentity`) correctly implements the JWT verification flow: signature verification → time validation → audience check using the lowercased `Host` header. Missing or invalid Host headers are rejected for authenticated requests. Empty tokens are treated as anonymous. The implementation matches Go DefraDB behavior.

## Affected Files

- `crates/http/src/identity_extractor.rs:1-408`

## Details

### Verification chain

```
Request → Extract Authorization header
  ├─ No header → Anonymous (Ok)
  ├─ "Bearer " + empty → Anonymous (Ok)
  ├─ "Bearer <token>" → Parse & verify:
  │   1. from_token(token)           ← Signature verified
  │   2. verify_auth_token(identity, host)  ← exp, nbf, aud checked
  │   3. Return DID                  ← Authenticated
  └─ Non-Bearer → Error 403
```

### Host header handling

```rust
fn extract_host_header(parts: &Parts) -> HostHeaderResult {
    match parts.headers.get(HOST) {
        Some(value) => match value.to_str() {
            Ok(s) if !s.is_empty() => HostHeaderResult::Valid(s.to_lowercase()),
            Ok(_) => HostHeaderResult::Missing,     // Empty → treated as missing
            Err(_) => HostHeaderResult::Invalid,     // Non-ASCII → invalid
        },
        None => HostHeaderResult::Missing,
    }
}
```

For **authenticated requests** (has Bearer token):
- Missing Host → Error 403 ("Host header required")
- Invalid Host → Error 403 ("valid Host header required")
- Valid Host → Used as expected audience (lowercased)

For **anonymous requests**:
- Host header not required (no audience to check)

### Bypass prevention

1. **Missing Host bypass**: An attacker cannot bypass audience checks by omitting the Host header — authenticated requests require it.
2. **Case normalization**: `s.to_lowercase()` ensures `Host: EXAMPLE.COM` matches audience `example.com`.
3. **Bearer prefix**: Only `"Bearer "` and `"bearer "` prefixes are accepted. Other schemes (Basic, Digest) return 403.

### Error responses

All authentication failures return **403 Forbidden** (not 401 Unauthorized), matching Go DefraDB behavior. This prevents information leakage about whether the token format was valid vs. expired vs. wrong audience.

## Remediation

None required. The implementation is correct and matches Go DefraDB behavior.
