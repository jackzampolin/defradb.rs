# Stream 04: Identity & Key Management -- Verification Re-Audit

**Date**: 2026-02-23
**Auditor**: Claude Opus 4.6 (verification pass)
**Scope**: Cross-reference remediation findings with current codebase state

---

## 1. HIGH Findings

### 04-37: Debug Dump Endpoint No Identity or NAC Check

**Original Finding**: `GET /api/v0/debug/dump` had no identity extraction and no NAC permission check. Any unauthenticated client could dump all database contents.

**Verification**: REMEDIATED

The dump handler in `crates/http/src/handlers/utility.rs:52-67` now includes both fixes:

```rust
pub async fn dump(
    State(state): State<AppState>,
    identity: ExtractIdentity,          // <-- identity extraction added
) -> Result<Json<Vec<String>>, HttpError> {
    require_permission(&state, &identity, NodePermission::DocumentRead).await?; // <-- NAC check added

    if !state.dev_mode {                // <-- dev_mode gate added
        return Err(HttpError::BadRequest(
            "dump is only available in development mode".into(),
        ));
    }
    // ...
}
```

Three layers of protection:
1. Identity extraction via `ExtractIdentity` extractor
2. NAC permission check requiring `DocumentRead`
3. Dev-mode gate -- dump is only available when `--development` flag is set

**Test Coverage**: An integration test exists at `tools/integration-test/tests/acp/negative.rs:105-138` (`dump_requires_auth_test`) that verifies anonymous dump is denied when ACP is active. The test uses `.with_acp_local()` but not `.with_development()`, so the dump will fail for two independent reasons (NAC denial AND non-dev-mode). This makes the test robust against either protection being disabled individually.

The dump functional test at `tools/integration-test/tests/backup/dump.rs` correctly uses `.with_development()` to enable the dump endpoint for positive testing.

**Verdict**: PASS -- fix is correct, defense-in-depth (three layers), and tested.

---

### 04-45: Identity Extraction Per-Handler, Not Middleware

**Original Finding**: No global deny-by-default auth middleware. Each handler must explicitly include `ExtractIdentity` in its signature, and a developer omitting it creates an unauthenticated endpoint.

**Verification**: NOT REMEDIATED (no deny-by-default middleware)

The remediation roadmap (Session 1.2) called for adding a deny-by-default auth middleware layer (e.g., `RequireIdentityLayer` with an allowlist of public routes). This has NOT been implemented. The codebase still uses the Axum `FromRequestParts` extractor pattern where each handler must explicitly include `identity: ExtractIdentity`.

**What exists instead**: Each handler individually calls `require_permission()` from `crates/http/src/nac_guard.rs`. The router at `crates/http/src/router/routes.rs` has no middleware layer for authentication. The server builder at `crates/http/src/server.rs:355-460` applies `DefaultBodyLimit`, `ConcurrencyLimitLayer`, `TimeoutLayer`, `TraceLayer`, and `CorsLayer` -- but no auth middleware.

**Mitigating factors**:
- The dump endpoint (the specific example of a missing extractor) has been fixed (see 04-37 above)
- Intentionally public endpoints (`/health-check`, `/api/v0/version`) are correctly unauthenticated
- The `GET /api/v0/graphql/ws` endpoint returns 501 (not implemented), so its lack of auth is safe
- The `GET /api/v0/schema` endpoint is intentionally public (schema is public information)

**Remaining risk**: A future developer adding a new endpoint could omit the extractor and create an unauthenticated endpoint. There is no compile-time or test-time check that all endpoints have identity extraction.

**Verdict**: PARTIAL -- the specific vulnerable endpoint (dump) was fixed, but the structural vulnerability (no deny-by-default) remains open. This matches the remediation roadmap's Phase 1.2 session which bundles this with HTTP rate limiting work.

---

## 2. Should Fix (Phase 5.4) Findings

### 04-23: Keyring Secret from Env Not Zeroized

**Original Finding**: `load_secret_from_env()` returned plain `Vec<u8>`, leaving password bytes in memory without zeroization.

**Verification**: REMEDIATED

The function at `crates/keyring/src/lib.rs:46-53` now returns `Zeroizing<Vec<u8>>`:

```rust
pub fn load_secret_from_env() -> Result<Zeroizing<Vec<u8>>> {
    let secret = Zeroizing::new(
        std::env::var(KEYRING_SECRET_ENV)
            .map(|s| s.into_bytes())
            .map_err(|_| Error::SecretNotSet)?,
    );
    Ok(secret)
}
```

The `Zeroizing` wrapper is applied immediately after the `env::var()` call's `into_bytes()` conversion. The `open_file_keyring()` function at line 59-62 passes `&secret[..]` (a slice reference) to `FileKeyring::open()`, which then creates its own `Zeroizing<Vec<u8>>` copy internally. When `secret` goes out of scope, the `Zeroizing` drop handler zeroes the memory.

**Note**: The `FileKeyring::open()` signature still accepts `impl Into<Vec<u8>>` (line 44 of `file.rs`), not `Zeroizing<Vec<u8>>` directly. The remediation suggestion to change the signature was not implemented, but the practical effect is the same because callers now use `Zeroizing` at the call site.

**Residual risk**: The `env::var()` call internally copies from libc `getenv()`, so one copy of the secret may linger in the C runtime's environment buffer. This is unavoidable in Rust's standard library and was acknowledged in the original finding.

**Test Coverage**: Unit test at `crates/keyring/src/lib.rs:69-77` (`test_load_secret_from_env`) verifies the function returns the correct value. No test verifies the zeroization behavior itself (this would require memory inspection).

**Verdict**: PASS -- the primary fix is implemented. The residual libc risk is accepted and documented.

---

### 04-24: Keyring get() Returns Plain Vec

**Original Finding**: `Keyring::get()` trait method returned `Vec<u8>`, meaning decrypted key material lived in heap memory without zeroization.

**Verification**: REMEDIATED

The trait at `crates/keyring/src/keyring.rs:21` now returns `Zeroizing<Vec<u8>>`:

```rust
fn get(&self, name: &str) -> Result<Zeroizing<Vec<u8>>>;
```

All three backends implement this:
- **FileKeyring** (`file.rs:123`): `fn get(&self, name: &str) -> Result<Zeroizing<Vec<u8>>>` -- wraps decrypted data in `Zeroizing::new(decrypted)`
- **SystemKeyring** (`system.rs:52`): `fn get(&self, name: &str) -> Result<Zeroizing<Vec<u8>>>` -- wraps base64-decoded data in `Zeroizing::new(decoded)`
- **SystemdCredsKeyring** (`systemd_creds.rs:140`): `fn get(&self, name: &str) -> Result<Zeroizing<Vec<u8>>>` -- returns `Zeroizing`-wrapped data

The `KeyHandle::get_key_bytes()` at `crates/keyring/src/signer.rs:136` also returns `Zeroizing<Vec<u8>>`:

```rust
pub fn get_key_bytes(&self) -> Result<Zeroizing<Vec<u8>>> {
    self.keyring.get(self.key_name.as_str())
}
```

This propagates zeroization guarantees through the entire key access chain.

**Verdict**: PASS -- trait signature changed, all backends updated, `KeyHandle` propagates `Zeroizing`.

---

## 3. Test Coverage (Phase 6.2) Findings

### 04-53: Expired Token Integration Test

**Original Finding**: No integration test sent an expired JWT to the HTTP API and verified 403 rejection.

**Verification**: REMEDIATED

A comprehensive test exists at `tools/integration-test/tests/identity/negative.rs:117-136`:

```rust
async fn expired_token_rejected_test(cluster: TestCluster) {
    let api_url = cluster.api_url(0);
    let audience = api_url.strip_prefix("https://")
        .or_else(|| api_url.strip_prefix("http://"))
        .unwrap_or(api_url);

    let expired_jwt = build_expired_secp256k1_jwt(audience);
    let status = graphql_with_raw_auth(api_url, &format!("Bearer {}", expired_jwt)).await;

    assert_eq!(status, 403, "expired JWT must be rejected with 403, got {}", status);
}
```

The `build_expired_secp256k1_jwt()` function (lines 14-54) constructs a cryptographically valid JWT with:
- `exp` set to 120 seconds in the past (beyond the 60-second clock skew tolerance)
- Real secp256k1 key generation and signing
- Correct DID derivation and audience claim
- Proper DER-to-raw signature conversion

This is a genuine end-to-end test: the token is cryptographically signed with valid claims except for the expired `exp`, so the server must actually check expiration rather than just rejecting an invalid signature.

The test runs via `for_each_runtime!` macro, generating both `rust_expired_token_rejected` and `go_expired_token_rejected` variants.

Additional negative tests in the same file:
- `malformed_token_rejected_test` (lines 142-162): Tests garbage tokens, two-part tokens, garbage base64, non-Bearer scheme
- `unauthenticated_acp_query_filtered_test` (lines 170-212): Tests anonymous ACP filtering
- `identity_isolation_test` (lines 218-287): Tests identity isolation (see 04-58 below)

**Verdict**: PASS -- comprehensive expired token test with correct clock skew boundary handling.

---

### 04-58: Identity Confusion Test

**Original Finding**: No test verified that Alice's token cannot be used to gain Bob's permissions.

**Verification**: REMEDIATED

The `identity_isolation_test` at `tools/integration-test/tests/identity/negative.rs:218-287` explicitly tests identity confusion:

1. Creates two distinct identities (Alice and Bob)
2. Sets up ACP-protected schema with policy owned by Alice
3. Alice creates a document
4. **Bob queries** -- verifies Bob sees 0 documents (identity isolation, not permission escalation)
5. **Bob attempts to update Alice's document** -- verifies 0 affected rows
6. **Alice re-queries** -- verifies her document is unchanged (no mutation from Bob)

```rust
// Bob queries -- must see nothing (identity isolation via ACP)
let bob_users = bob_read["User"].as_array().expect("Bob User array");
assert_eq!(bob_users.len(), 0, "Bob must see 0 of Alice's ACP-protected documents");

// Bob attempts to update Alice's document -- must see no affected rows
assert_eq!(updated, 0, "Bob must not be able to update Alice's document");

// Alice still owns her document unchanged
assert_eq!(alice_users[0]["name"], "AliceDoc", "Alice's document name must be unchanged");
```

This test verifies the core identity isolation property: using Bob's token yields Bob's (empty) permissions, not Alice's. The self-authenticating JWT design means the token determines the identity, and this test confirms the server honors that binding end-to-end.

The test runs both Rust and Go runtimes via `for_each_runtime!(identity_isolation, identity_isolation_test, .with_acp_local())`.

**Note**: The test does not cover concurrent multi-identity requests or identity carry-over between sequential requests on the same connection. These remain as potential future test additions but are lower risk given Axum's per-request extractor model.

**Verdict**: PASS -- identity isolation test covers both read and write paths.

---

## 4. GREEN Finding Spot-Checks

### 04-05: JWT Algorithm Dispatch -- Still Only EdDSA/ES256K/ES256?

**Verification**: STILL GREEN

The `from_token()` function at `crates/identity/src/token/mod.rs:211-221`:

```rust
let claims: IdentityClaims = match header_alg.as_str() {
    "EdDSA" => decode_ed25519(token_str)?,
    "ES256K" => decode_secp256k1(token_str)?,
    "ES256" => decode_secp256r1(token_str)?,
    alg => {
        return Err(Error::TokenDecoding(format!(
            "unsupported algorithm: {}", alg
        )))
    }
};
```

Exactly three algorithms accepted. The `"none"` algorithm, `"HS256"`, `"RS256"`, and all other algorithms fall through to the error case. No regression.

Additionally, the `new_token()` function at line 93-98 dispatches on `KeyType` with `Bls12381` explicitly returning `UnsupportedKeyType`. The encoding path also only supports the same three algorithms.

**Verdict**: STILL GREEN -- no algorithm confusion possible.

---

### 04-17: Signature Verified Before Claims Trusted?

**Verification**: STILL GREEN

All three decode functions in `crates/identity/src/token/decoding.rs` follow the same pattern:

```
1. parse_jwt(token)                    -- split into 3 parts
2. decode_claims(jwt.payload)          -- decode claims (UNTRUSTED)
3. decode_public_key_from_claims(...)  -- extract key from untrusted sub
4. decode_signature(jwt.signature)     -- decode signature bytes
5. verify_signature(key, input, sig)   -- CRYPTOGRAPHIC VERIFICATION
6. Ok(claims)                          -- return ONLY if step 5 passes
```

Verified for:
- `decode_ed25519` (lines 78-88)
- `decode_secp256k1` (lines 90-101)
- `decode_secp256r1` (lines 103-115)

In all three, `verify_signature()` is called before `Ok(claims)` is returned. The `verify_signature()` function (lines 61-76) returns `Err` on verification failure, preventing claims from being returned.

Post-signature checks in `from_token()` (lines 223-264) add:
- Algorithm-key_type cross-check
- DID-issuer binding verification

**Verdict**: STILL GREEN -- verification ordering correct, no regression.

---

### 04-18: Signature Verification Constant-Time?

**Verification**: STILL GREEN

All three verification paths delegate to the same constant-time crypto libraries:

- **Ed25519** (`crates/crypto/src/keys/ed25519.rs:228-243`): Uses `ed25519_dalek::VerifyingKey::verify()` which uses `curve25519-dalek` constant-time field arithmetic. Length check `signature.len() != 64` is on public data (not secret).

- **secp256k1** (`crates/crypto/src/keys/secp256k1.rs:190-213`): Uses `k256::ecdsa::VerifyingKey::verify_digest()` which uses RustCrypto constant-time field operations. `normalize_s()` operates on signature value (public).

- **secp256r1** (`crates/crypto/src/keys/secp256r1.rs:183-206`): Uses `p256::ecdsa::VerifyingKey::verify_digest()` with same RustCrypto constant-time guarantees.

Key comparison in `Key::equal()` implementations uses `ct_eq()` (constant-time equality from the `subtle` crate via `ConstantTimeEq` trait), verified at:
- `ed25519.rs:215`: `self_raw.ct_eq(&other_raw).into()`
- `secp256r1.rs:171`: `self_raw.ct_eq(&other_raw).into()`

**Verdict**: STILL GREEN -- constant-time crypto, no custom comparisons, no regression.

---

### 04-32: Key Name Validation Prevents Path Traversal

**Verification**: STILL GREEN

`KeyName::validate()` at `crates/keyring/src/key_name.rs:47-65`:

```rust
pub fn validate(name: &str) -> Result<()> {
    if name.is_empty() { return Err(...); }
    if name.contains('/') || name.contains('\\') || name.contains('\0') { return Err(...); }
    if name == "." || name == ".." { return Err(...); }
    Ok(())
}
```

Rejects: empty strings, forward slash, backslash, null bytes, `.`, `..`. Comprehensive unit tests exist at lines 91-147 covering valid names, empty names, path separators, and dot names.

The `KeyName` type is used in `KeyHandle` (via `KeyName::new()` at `signer.rs:77`) and in `FileKeyring::key_path()` (via `KeyName::validate()` at `file.rs:62`), ensuring all key access paths go through validation.

**Verdict**: STILL GREEN -- path traversal prevention intact, no regression.

---

## 5. Summary

| Finding | Status | Verdict |
|---------|--------|---------|
| **04-37** (HIGH) Debug dump no auth | REMEDIATED | PASS -- identity extraction + NAC + dev_mode gate |
| **04-45** (HIGH) No deny-by-default middleware | PARTIALLY REMEDIATED | PARTIAL -- specific endpoint fixed, structural gap remains |
| **04-23** (Should Fix) Env secret not zeroized | REMEDIATED | PASS -- `Zeroizing<Vec<u8>>` wrapper applied |
| **04-24** (Should Fix) Keyring get() plain Vec | REMEDIATED | PASS -- trait + all backends + KeyHandle updated |
| **04-53** (Test) Expired token test | REMEDIATED | PASS -- end-to-end test with correct clock skew handling |
| **04-58** (Test) Identity confusion test | REMEDIATED | PASS -- Bob cannot read/write Alice's documents |
| **04-05** (GREEN) JWT algorithm dispatch | No regression | STILL GREEN |
| **04-17** (GREEN) Signature before claims | No regression | STILL GREEN |
| **04-18** (GREEN) Constant-time verify | No regression | STILL GREEN |
| **04-32** (GREEN) Key name validation | No regression | STILL GREEN |

### Open Items

1. **04-45 structural fix**: A deny-by-default auth middleware layer is still needed to prevent future endpoints from being accidentally unauthenticated. This is tracked in the remediation roadmap under Session 1.2 alongside HTTP rate limiting. The specific endpoint that was vulnerable (dump) has been fixed, reducing the urgency, but the systemic issue remains.

2. **04-23 residual**: The `FileKeyring::open()` signature still accepts `impl Into<Vec<u8>>` rather than requiring `Zeroizing<Vec<u8>>`. This is a minor hardening opportunity -- callers currently do the right thing, but the type system does not enforce it.

3. **04-53 boundary**: The expired token test uses 120 seconds past (well beyond the 60-second tolerance). A boundary test at exactly 61 seconds and 59 seconds (just outside/inside the skew window) would provide stronger coverage. The existing test is sufficient for regression detection but does not validate the exact skew boundary.
