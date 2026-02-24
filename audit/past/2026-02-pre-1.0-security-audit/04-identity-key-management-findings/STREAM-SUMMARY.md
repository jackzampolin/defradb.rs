# Identity & Key Management Security Audit — Stream Summary

## Overview

**Stream**: Identity & Key Management (Audit Stream 4)
**Sessions**: 5 of 5 (complete)
**Total Findings**: 64 (numbered 00-63)
**Date**: February 2026

This stream audited the full identity and key management stack in defradb.rs, from low-level cryptographic primitives through JWT token handling, keyring backends, HTTP authentication middleware, CLI credential handling, and integration test coverage.

## Session Inventory

| Session | Focus | Findings |
|---------|-------|----------|
| 1 | Identity crate core: DID validation, RawIdentity, IdentityContext | 00-09 |
| 2 | Custom JWT implementation: DER conversion, algorithm dispatch, claim validation | 10-20 |
| 3 | Keyring security: FileKeyring JWE, SystemKeyring, SystemdCreds | 21-34 |
| 4 | HTTP auth middleware and CLI credential flow | 35-52 |
| 5 | Integration tests, cross-cutting security properties | 53-63 |

## Severity Distribution

| Severity | Count | Findings |
|----------|-------|----------|
| **HIGH** | 2 | 21, 37 |
| **MEDIUM** | 14 | 00, 10, 22, 23, 25, 26, 36, 40, 41, 42, 45, 51, 53, 58 |
| **LOW** | 14 | 01, 02, 03, 24, 27, 28, 29, 33, 35, 38, 44, 54, 55, 61 |
| **INFO** | 10 | 04, 11, 39, 43, 46, 47, 48, 50, 62, 12 |
| **GREEN** | 24 | 05, 06, 07, 08, 13, 14, 15, 16, 17, 18, 19, 30, 31, 32, 34, 49, 56, 57, 59, 60, 63, 09(summary), 20(summary), 52(summary) |

## High Severity Findings

### Finding 21: PBKDF2 Iteration Count Weak
**File**: `crates/keyring/src/file.rs`
**Issue**: FileKeyring uses PBKDF2 with 100,000 iterations. Modern recommendations (OWASP 2023) suggest 600,000 for SHA-256. GPU attacks can brute-force weak passphrases at this iteration count.
**Status**: Confirmed

### Finding 37: Debug Dump Endpoint Has No Identity or NAC Check
**File**: `crates/http/src/handlers/utility.rs`
**Issue**: The `/debug/dump` endpoint returns all database content without any identity or NAC permission check. Any client with network access can dump the entire database including ACP-protected documents.
**Status**: Confirmed

## Medium Severity Findings

| # | Finding | Category |
|---|---------|----------|
| 00 | Wildcard DID `key` portion causes panic on `did()` call | DID Validation |
| 10 | DER parser accepts lax/non-canonical encodings | JWT / DER |
| 22 | File keyring delete lacks fsync before unlink | Keyring |
| 23 | Keyring secret loaded from env, not zeroized | Keyring |
| 25 | SystemdCreds path lookup has no timeout | Keyring |
| 26 | SystemdCreds has no secure delete | Keyring |
| 36 | Empty Bearer token treated as anonymous | HTTP Auth |
| 40 | CORS allows wildcard origin with auth header | HTTP Auth |
| 41 | No X-Forwarded-Host support for audience validation | HTTP Auth |
| 42 | Private key passed as CLI argument visible in process table | CLI |
| 45 | Identity extraction is per-handler, not global middleware | HTTP Auth |
| 51 | Key type ambiguity for 32-byte keys | Identity |
| 53 | No expired token integration test through HTTP stack | Test Coverage |
| 58 | No identity confusion/substitution integration test | Test Coverage |

## Architecture Assessment

### Strengths

1. **Self-authenticating JWT design**: Tokens contain the public key in the `sub` claim, verified against the signature. No server-side state needed. Key-to-DID binding is verified on every token parse.

2. **Constant-time cryptography**: All three signature types (Ed25519, secp256k1, secp256r1) delegate to well-audited Rust crypto libraries with constant-time operations. Key comparison uses `subtle::ConstantTimeEq`.

3. **Correct verification ordering**: Signature is verified before claims are trusted. This prevents timing oracles that could distinguish "bad signature" from "bad claims + bad signature".

4. **Clean error paths**: All JWT failures result in `None` identity (anonymous) or 403 rejection. No partial identity states exist. Error messages don't leak sensitive data.

5. **Identity propagation**: Identity flows cleanly from HTTP extraction through the query pipeline to ACP checks via function parameters. No thread-local or global state that could cause identity confusion.

6. **P2P peer authentication**: Message signing verifies PeerId ↔ public key binding cryptographically, preventing peer impersonation.

7. **Test helper realism**: Integration tests use the real CLI binary and real signing path, ensuring test tokens go through the full production code.

8. **Three key types supported**: Ed25519, secp256k1, and secp256r1 all have complete signing, verification, DID derivation, and JWT support.

### Weaknesses

1. **No global auth middleware**: Each HTTP handler must opt-in to identity extraction. Forgetting it creates an unauthenticated endpoint (demonstrated by the dump endpoint).

2. **Token replay window**: Stateless JWT design means tokens can be replayed within their validity window (typically 1 hour + 60s clock skew). No `jti` or nonce mechanism.

3. **CLI credential exposure**: The `-i` flag passes private keys as command-line arguments visible in the process table. The `--identity-name` flag (keyring-based) is available but not the default.

4. **Keyring secret handling**: Passphrase loaded from environment variable is not zeroized. PBKDF2 iteration count is below modern recommendations.

5. **Integration test gaps**: Happy-path identity flows are well-tested, but negative cases (expired tokens, malformed tokens, identity substitution) are only covered at the unit level, not through the full HTTP stack.

6. **PeerId ≠ DID gap**: The P2P layer uses libp2p PeerIds while the ACP layer uses DIDs. The mapping between these two identity systems is not directly verified — they use different key types and encoding schemes.

### Test Coverage Summary

| Layer | Positive Tests | Negative Tests | Assessment |
|-------|---------------|----------------|------------|
| Crypto key operations | ✅ All 3 types | ✅ Invalid keys | Strong |
| JWT token creation | ✅ All 3 types | N/A | Strong |
| JWT token verification | ✅ Roundtrip, tamper | ✅ 10+ negative tests | Strong |
| JWT claim validation | ✅ Audience, exp, nbf | ✅ Wrong/missing | Strong |
| HTTP identity extractor | ✅ Valid token | ✅ 14 unit tests | Good |
| Identity lifecycle (CLI) | ✅ CRUD, reimport | ✅ Malformed input | Good |
| Keyring lifecycle | ✅ CRUD, Go interop | ✅ Invalid hex, conflicts | Good |
| ACP with identity | ✅ Multi-identity | ⚠️ No token abuse tests | Adequate |
| Integration (full stack) | ✅ Happy path | ❌ No expired/malformed tokens | Weak |
| Node identity | ⚠️ Existence only | ❌ No persistence/binding | Weak |
| P2P identity | ✅ Signing/verify | N/A (unit level) | Adequate |

## Recommended Priorities

### Must-fix for 1.0
1. **Finding 37**: Add identity/NAC check to debug dump endpoint
2. **Finding 21**: Increase PBKDF2 iterations to 600,000

### Should-fix for 1.0
3. **Finding 45**: Consider global auth middleware to prevent future unauthenticated endpoints
4. **Finding 53**: Add expired token integration test
5. **Finding 58**: Add identity substitution integration test

### Nice-to-have
6. **Finding 00**: Handle wildcard DID `did()` call without panic
7. **Finding 42**: Document `--identity-name` as the recommended flag (over `-i`)
8. **Finding 41**: Add X-Forwarded-Host support for reverse proxy deployments
9. **Finding 54**: Add anonymous access test to `acp_basic.rs`

## Conclusion

The identity and key management system in defradb.rs is **architecturally sound**. The self-authenticating JWT design, constant-time cryptography, correct verification ordering, and clean error handling provide a solid security foundation. The main risks are operational (keyring strength, CLI credential exposure) and test coverage (integration tests lack negative cases). The two high-severity findings (weak PBKDF2 iterations and unauthenticated dump endpoint) should be addressed before 1.0.
