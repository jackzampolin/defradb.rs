# Finding 11: No Token Replay Protection

**Severity**: INFO
**Category**: Authentication / Token Security
**Status**: Confirmed (by design)

## Summary

JWT tokens lack a `jti` (JWT ID) claim or any nonce mechanism. A valid token can be replayed to the same host within its validity window (`exp - nbf`, typically 3600 seconds). This is consistent with Go DefraDB's behavior and is a known property of stateless JWT-based authentication.

## Affected Files

- `crates/identity/src/token/claims.rs:8-35` — `IdentityClaims` struct (no `jti` field)
- `crates/identity/src/token/mod.rs:142-185` — `verify_auth_token_with_skew()` (no replay check)

## Details

### Token reuse window

A valid token with `exp - nbf = 3600` (1 hour) plus the 60-second clock skew tolerance gives an effective replay window of **3660 seconds**. During this window, any party who obtains the token (e.g., through server-side logging, a compromised proxy, or a stolen TLS session) can reuse it.

### Mitigations already in place

| Mitigation | Effect |
|------------|--------|
| `aud` (audience) claim | Token only valid for specific host |
| `exp` (expiration) | Limits replay window |
| TLS transport | Prevents passive interception |
| Self-authenticating design | Attacker can only act as the legitimate identity, not impersonate others |

### Why this is INFO, not a finding

- Go DefraDB has the same behavior — this is a design choice, not a bug
- Stateless JWT verification cannot prevent replay without server-side state (e.g., a token blacklist)
- The audience binding significantly limits replay scope
- Short token lifetimes further reduce the window

## Remediation

None required for 1.0 parity. If replay protection is needed in the future:

1. Add `jti: String` field to `IdentityClaims` (UUID per token)
2. Maintain a server-side seen-jti set with TTL matching token lifetime
3. Reject tokens with previously-seen jti values

## Test Gap

No tests verify that tokens cannot be used after explicit revocation (because revocation doesn't exist in the current design).
