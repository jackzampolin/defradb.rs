# Audit Stream 4: Identity & Key Management

## Scope

Identity creation, authentication, and key storage. Audit covers:
- JWT handling (creation, validation, algorithm enforcement)
- Keyring storage security (at-rest protection, access controls)
- Identity lifecycle (creation, attestation, revocation)
- Credential handling in CLI and HTTP API
- Private key serialization and deserialization
- DID (Decentralized Identifier) handling
- Token expiry and refresh logic

## Key Questions

- Is JWT algorithm confusion possible (e.g., none, HS256 vs RS256)?
- Are JWTs validated before every use, not just at session start?
- Are private keys stored encrypted at rest?
- Can keyring contents be extracted by other processes?
- Is there proper credential cleanup on process exit?
- Are there hardcoded keys, seeds, or test credentials in production paths?
- How are identities bound to network peers?

## Crates of Interest

- `identity/`
- `keyring/`
- `crypto/`
- `http/` (auth middleware)
- `cli/` (credential input handling)
- `p2p/` (peer identity binding)

## Recon Findings

### Surface Area
- **Identity crate**: 903 LOC (src), 7 test files - traits, RawIdentity, JWT tokens, DID, context
- **Keyring crate**: 905 LOC (src), 5 test files - file/system/systemd backends
- **HTTP auth**: 409 LOC (identity_extractor.rs)
- **Total auditable**: ~2,200 LOC (excludes crypto crate, covered in Stream 1)

### JWT Implementation
- **Custom** (not jsonwebtoken crate) - avoids PKCS#8 DER format mismatches
- **Algorithms**: EdDSA (Ed25519), ES256K (secp256k1), ES256 (secp256r1)
- **Validation**: Signature mandatory, audience check (Host header), clock skew 60s
- **No algorithm confusion possible** - key type determines algorithm

### Keyring Security
- **FileKeyring**: JWE PBES2-HS512-A256KW, PBKDF2 10k iterations, 32-byte salt
- **Password zeroization**: `Zeroizing<Vec<u8>>` wrapper
- **File permissions**: 0o700 dirs, 0o600 key files
- **Secure deletion**: File zeroed before unlinking
- **SystemKeyring**: OS-native (macOS Keychain, Linux Secret Service)

### DID Handling
- Format: `did:key:z<multibase>` - validated at construction time
- `Did::new()` enforces format, `new_unchecked()` for internal use only
- Wildcard `*` support for relationship operations

### HTTP Auth Flow
- Bearer token extraction -> JWT parse -> signature verify -> audience check
- Anonymous allowed (no header = Ok(None))
- Invalid token = 403 Forbidden (not 401)
- Missing Host header with auth = 403

### Red Flags: NONE
- No hardcoded secrets
- No algorithm confusion vectors
- Proper audience validation
- Zeroization on keyring passwords
- Private keys not cached (fetched on demand from keyring)

### Areas for Deep Dive
- DER signature conversion correctness (ES256K, ES256)
- Replay attack prevention (nonce handling, timestamp validation)
- Timing attack resistance in signature verification
- SystemKeyring base64 encoding safety

## Estimated Scope

**MEDIUM: 3-5 sessions**

### Session 1: Identity Crate Core (HIGH)

| File | Lines | Focus |
|------|-------|-------|
| `crates/identity/src/did.rs` | 32-84, 168-183 | DID validation, `new_unchecked()` usage, serde roundtrip |
| `crates/identity/src/key_type.rs` | 27-55 | Key type conversions, BLS12-381 rejected |
| `crates/identity/src/raw.rs` | 38-110, 211-217 | RawIdentity constructors, PrivateKey trait |
| `crates/identity/src/context.rs` | 46-109 | IdentityContext, anonymous, has_full_identity() |

**Checklist**: DID format enforcement, wildcard safety, key type consistency, public key derivation timing

**Status**: COMPLETE — 9 findings (1 MEDIUM, 3 LOW, 1 INFO, 4 GREEN). See `04-identity-key-management-findings/09-session1-core-identity-summary.md`.

### Session 2: JWT Token Implementation (CRITICAL)

| File | Lines | Focus |
|------|-------|-------|
| `crates/identity/src/token/encoding.rs` | 1-73 | EdDSA/ES256K/ES256 encoding, base64 URL_SAFE_NO_PAD |
| `crates/identity/src/token/decoding.rs` | 1-135 | 3-part split, pubkey from `sub`, algorithm parsing |
| `crates/identity/src/token/der.rs` | 1-256 | **DER<->raw conversion** - R/S padding, length encoding |
| `crates/identity/src/token/claims.rs` | 1-36 | sub, iss, exp, nbf, iat, aud claims |
| `crates/identity/src/token/mod.rs` | 43, 93-97, 120-182, 225-264 | Algorithm dispatch, clock skew, audience validation |

**Checklist**: No algorithm confusion, DER off-by-one, expiry/nbf/audience enforcement, DID-issuer match

### Session 3: Keyring Security (HIGH)

| File | Lines | Focus |
|------|-------|-------|
| `crates/keyring/src/file.rs` | 1-170 | PBKDF2 10k iter, JWE PBES2-HS512-A256KW, 0o600 perms, zero-before-delete |
| `crates/keyring/src/keyring.rs` | 1-31 | Trait: set/get/delete/list |
| `crates/keyring/src/system.rs` | 1-82 | OS-native keyring, base64 STANDARD encoding |
| `crates/keyring/src/systemd_creds.rs` | 1-207 | systemd 250+, stdin/stdout pipes, `.cred` extension |

**Checklist**: Password Zeroizing wrapper, Windows ACL equiv, base64 safety, secure deletion

**Status**: COMPLETE — 14 findings (3 MEDIUM, 7 LOW, 1 INFO, 3 GREEN). See `04-identity-key-management-findings/34-session3-keyring-backends-summary.md`.

### Session 4: HTTP Auth & CLI Credential Flow (MEDIUM)

| File | Lines | Focus |
|------|-------|-------|
| `crates/http/src/identity_extractor.rs` | 1-409 | Bearer token parse, Host header, 403 on invalid, anonymous path |
| `crates/cli/src/commands/keyring_cmd.rs` | 36-150 | Generate (Go-compat mode), key type dispatch |
| `crates/cli/src/commands/identity.rs` | all | Private key not logged/printed, secure prompting |

**Checklist**: Case-insensitive Bearer, empty token=anonymous, Host header for audience, non-ASCII rejection

**Status**: COMPLETE — 18 findings (1 HIGH, 6 MEDIUM, 5 LOW, 3 INFO, 3 GREEN). See `04-identity-key-management-findings/52-session4-http-auth-cli-credentials-summary.md`.

### Session 5: Integration Tests & Cross-Cutting (MEDIUM)

| File | Focus |
|------|-------|
| `tools/integration-test/tests/identity_lifecycle.rs` | Key CRUD round-trips (all 3 key types) |
| `tools/integration-test/tests/identity_types.rs` | Cross-key-type ACP |
| `tools/integration-test/tests/node_identity.rs` | Node identity endpoint |
| `tools/integration-test/tests/keyring_lifecycle.rs` | Keyring CRUD, Go interop |
| `tools/integration-test/tests/acp_basic.rs` | Basic ACP with identity |
| `tools/integration-test/tests/acp_multi_identity.rs` | Multi-identity ACP |
| `crates/crypto/src/keys/ed25519.rs` | Ed25519 verify (constant-time) |
| `crates/crypto/src/keys/secp256k1.rs` | secp256k1 verify (constant-time) |
| `crates/crypto/src/keys/secp256r1.rs` | secp256r1 verify (constant-time) |
| `crates/p2p/src/signing.rs` | P2P message signing/verification |

**Checklist**: Timing attack resistance, replay prevention, expired token rejection, identity confusion, cross-component flow

**Status**: COMPLETE — 11 findings (0 HIGH, 2 MEDIUM, 3 LOW, 1 INFO, 5 GREEN). See `04-identity-key-management-findings/64-session5-integration-cross-cutting-summary.md`.

## Stream Complete

All 5 sessions complete. Total: 64 findings (2 HIGH, 14 MEDIUM, 14 LOW, 10 INFO, 24 GREEN).
See `04-identity-key-management-findings/STREAM-SUMMARY.md` for the full summary.
