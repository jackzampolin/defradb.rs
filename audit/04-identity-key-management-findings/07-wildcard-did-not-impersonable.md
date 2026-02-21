# Finding 07: Wildcard DID Cannot Be Impersonated — Verified Safe

**Severity**: GREEN
**Category**: Access Control / Identity Spoofing
**Status**: Verified safe

## Summary

The wildcard DID `"*"` is used in ACP and NAC permission checks to represent "all actors." An attacker cannot impersonate the wildcard identity because all identity construction paths require valid cryptographic key material that would never derive to `"*"`.

## Affected Files

- `crates/identity/src/did.rs:64-66` — `Did::wildcard()`
- `crates/http/src/nac_guard.rs:49-58` — anonymous fallback to wildcard
- `crates/query/src/runner/executor.rs:50` — NAC check with wildcard for no identity
- `crates/acp/src/local.rs:55-166` — wildcard permission checks

## Details

### Why wildcard impersonation is impossible

1. **JWT tokens**: DID is derived from the public key in `claims.sub`, verified against `claims.iss`. No valid key pair produces `"*"` as a DID.
2. **Serde deserialization**: `Did::new("*")` fails because `"*"` doesn't start with `"did:key:"`. Crafted JSON payloads are rejected.
3. **HTTP identity extraction**: Requires a valid JWT bearer token → goes through `from_token()` → always produces a `did:key:z...` DID.
4. **CLI identity**: Requires a valid private key hex string → goes through `RawIdentity::from_bytes()` → DID derived from public key.

### Wildcard usage patterns (correct)

| Location | Usage | Safe? |
|----------|-------|-------|
| `nac_guard.rs:52` | Anonymous request → check if wildcard has permission | Yes — wildcard is the fallback, not the identity |
| `executor.rs:50` | No identity → use wildcard for NAC check | Yes — same pattern |
| `local.rs:69` | Check if `"*"` relationship exists for a document | Yes — wildcard grants are set by document owners |
| `nac_tests.rs:248` | Test: add wildcard as admin | Yes — test verifies wildcard admin grants work |

### The attacker scenario that doesn't work

An attacker who can submit queries or HTTP requests without authentication:
1. Their identity is `None` (no JWT token)
2. The system substitutes `Did::wildcard()` for NAC permission checks
3. If the node admin has granted wildcard permissions, the anonymous request succeeds — **this is intentional behavior**
4. The attacker cannot **become** the wildcard identity to gain access beyond what was explicitly granted to `"*"`

## Remediation

None required. The wildcard DID is safe from impersonation. Wildcard permission grants are an intentional feature for public access patterns.
