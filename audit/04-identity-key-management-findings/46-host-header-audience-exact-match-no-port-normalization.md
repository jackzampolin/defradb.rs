# Host Header Audience Check: Exact String Match, No Port Normalization

- **Severity**: Low
- **Category**: Token Validation
- **Status**: Confirmed — Green (Matches Go)

## Summary

The audience check compares the Host header (lowercased) against the JWT `aud` claim as an exact string match. There is no port normalization (e.g., `localhost` vs `localhost:80` for HTTP), no subdomain matching, and no wildcard support. The lowercasing prevents case-sensitivity issues.

## Affected Files

- `crates/http/src/identity_extractor.rs:87` (lowercasing)
- `crates/http/src/identity_extractor.rs:172` (audience passed to verify_auth_token)
- `crates/cli/src/commands/client/mod.rs:206` (client-side audience generation)

## Details

```rust
// Server side: extract and lowercase
Ok(s) if !s.is_empty() => HostHeaderResult::Valid(s.to_lowercase()),

// Client side: strip scheme, use as audience
let audience_host = strip_url_scheme(audience);
// "http://localhost:9181" → "localhost:9181"
```

**Analysis**:
1. **Case sensitivity**: Handled correctly — both sides lowercase
2. **Port normalization**: Not done — `localhost` and `localhost:80` are different audiences. This is correct behavior since DefraDB always uses a non-standard port (9181)
3. **Subdomains**: Not supported — no wildcards in audience matching. This is correct for a database server
4. **IPv6**: Not specifically handled but should work via exact match (e.g., `[::1]:9181`)

The `verify_auth_token` function in the identity crate checks if the expected audience is contained in the token's `aud` array:
```rust
// identity crate: aud must contain the expected audience
```

## Remediation

No action needed. The exact-match behavior is correct for DefraDB's use case and matches Go.

## Test Gap

Test exists: `test_wrong_host_header_with_valid_token_returns_error` verifies audience mismatch rejection. Missing: IPv6 host header test, port-only mismatch test.
