# PBKDF2 Iteration Count Below Modern Recommendations

- **Severity**: Medium
- **Category**: Key Derivation
- **Status**: Open

## Summary

FileKeyring uses PBKDF2-SHA512 with 10,000 iterations for password-based key wrapping (PBES2-HS512-A256KW). While this matches Go DefraDB's default (Go jwx v2 default) and satisfies the OWASP 2017 minimum, it is well below modern recommendations. OWASP 2023 recommends 210,000 iterations for PBKDF2-SHA512. The iteration count is a hardcoded constant with no configuration mechanism or migration path.

## Affected Files

- `crates/keyring/src/file.rs:21` — `const PBKDF2_ITER_COUNT: usize = 10000`

## Details

```rust
// file.rs:20-21
/// PBKDF2 iteration count — matches Go jwx default (10000).
const PBKDF2_ITER_COUNT: usize = 10000;
```

The JWE header embeds `p2c: 10000`, which means existing encrypted files are self-describing — the decrypter reads the iteration count from the header. However, new files are always created with 10,000 iterations because the constant is not configurable.

**Risk assessment**: An attacker who obtains a `.key` file from the keyring directory can attempt offline brute-force attacks against the password. At 10k iterations, modern GPUs can test roughly 1M–10M passwords/second against PBKDF2-HMAC-SHA512, making dictionary and targeted brute-force attacks feasible for weak passwords.

**Mitigating factors**:
1. Go compatibility requires matching the iteration count — changing it unilaterally would break cross-node keyring portability.
2. Key files are protected by 0o600 permissions, so the attacker needs filesystem access.
3. The PBES2-HS512-A256KW scheme uses SHA-512 (2x slower than SHA-256 per iteration).

**JWE self-describing format advantage**: Because `p2c` is embedded in each JWE token, it is possible to increase iterations for *new* keys while still decrypting *old* keys at their original iteration count. The decrypt path already reads `p2c` from the header automatically via josekit.

## Remediation

1. **Coordinate with Go DefraDB** to agree on a higher iteration count (e.g., 600,000 for PBKDF2-SHA512 per OWASP 2023) for new keys.
2. Since JWE tokens are self-describing (`p2c` in header), decryption is already forward-compatible — old tokens decrypt at 10k, new tokens encrypt at the higher count.
3. Consider adding a `--pbkdf2-iterations` config option or auto-upgrading: re-encrypt keys at higher iterations when accessed.
4. Alternatively, evaluate Argon2id as a future KDF (would require a new JWE algorithm or custom wrapping).

## Test Gap

- No test verifies the iteration count is at least a configurable minimum.
- No test exercises key migration (re-encryption at higher iterations).
- The `test_jwe_format_go_compatible` test validates `p2c: 10000` but does not flag it as a concern.
