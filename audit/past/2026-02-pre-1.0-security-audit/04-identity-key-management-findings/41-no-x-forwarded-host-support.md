# No X-Forwarded-Host Support for Audience Validation

- **Severity**: Medium
- **Category**: Deployment Security
- **Status**: Confirmed

## Summary

The audience validation uses only the `Host` HTTP header for JWT audience checking. When DefraDB runs behind a reverse proxy (nginx, Caddy, cloud load balancer), the `Host` header seen by DefraDB may differ from the host the client used to connect. There is no support for `X-Forwarded-Host`, `X-Original-Host`, or the RFC 7239 `Forwarded` header.

## Affected Files

- `crates/http/src/identity_extractor.rs:84-101` (extract_host_header)
- `crates/cli/src/commands/client/mod.rs:206-207` (client audience generation)

## Details

```rust
// identity_extractor.rs:84-101
fn extract_host_header(parts: &Parts) -> HostHeaderResult {
    match parts.headers.get(HOST) {
        Some(value) => match value.to_str() {
            Ok(s) if !s.is_empty() => HostHeaderResult::Valid(s.to_lowercase()),
            // ...
        },
        None => HostHeaderResult::Missing,
    }
}
```

**Scenario**: Client connects to `https://api.example.com` (reverse proxy) → proxy forwards to `http://localhost:9181` (DefraDB). The client generates a JWT with `aud: "api.example.com"`. DefraDB sees `Host: localhost:9181` and rejects the token with "audience mismatch".

The CLI client generates the audience from the URL it connects to:
```rust
// client/mod.rs:206
let audience_host = strip_url_scheme(audience);
```

So the client would generate `aud: "localhost:9181"` if connecting directly, or `aud: "api.example.com"` if connecting through a proxy, creating a mismatch.

**Mitigating factor**: Go DefraDB has the same limitation — it uses `req.Host` directly.

## Remediation

Add configurable proxy header support:

```rust
fn extract_host_header(parts: &Parts) -> HostHeaderResult {
    // Check X-Forwarded-Host first (reverse proxy)
    if let Some(forwarded_host) = parts.headers.get("x-forwarded-host") {
        if let Ok(s) = forwarded_host.to_str() {
            if !s.is_empty() {
                return HostHeaderResult::Valid(s.to_lowercase());
            }
        }
    }
    // Fall back to Host header
    match parts.headers.get(HOST) { ... }
}
```

**Important**: This should be opt-in via server configuration (e.g., `--trust-proxy-headers`) to prevent `X-Forwarded-Host` spoofing in non-proxy deployments.

## Test Gap

No test for proxy header scenarios. Need tests for:
- `X-Forwarded-Host` present and valid
- `X-Forwarded-Host` spoofing prevention when not behind proxy
- Multiple `X-Forwarded-Host` values (comma-separated)
