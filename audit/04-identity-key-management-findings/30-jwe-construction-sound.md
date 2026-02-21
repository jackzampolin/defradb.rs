# JWE Construction: josekit Library, PBES2-HS512-A256KW — Sound

- **Severity**: Informational (green)
- **Category**: Cryptographic Construction
- **Status**: Verified clean

## Summary

The JWE encryption implementation uses the `josekit` crate (v0.8), a well-maintained Rust JOSE library, for PBES2-HS512-A256KW key wrapping with A256GCM content encryption. The construction is correct, the salt is generated per-encryption by josekit (32-byte CSPRNG), and the JWE compact serialization format matches Go DefraDB's format.

## Verified Properties

1. **Library**: `josekit` v0.8 — not hand-rolled crypto. josekit uses ring/openssl under the hood for primitives.

2. **Algorithm chain**: PBES2-HS512-A256KW (key wrapping) + A256GCM (content encryption). This is a standard JOSE combination.

3. **Salt generation**: josekit generates a fresh random salt per encryption using the configured salt length (32 bytes). Verified by test `test_jwe_format_go_compatible`:
   ```rust
   let salt_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
       .decode(p2s).unwrap();
   assert_eq!(salt_bytes.len(), 32, "salt should be 32 bytes");
   ```

4. **Salt storage**: Salt is embedded in the JWE header (`p2s` field), which is correct — the decrypter reads it from the header.

5. **Iteration count**: Embedded as `p2c` in the JWE header, making each token self-describing for decryption.

6. **JWE compact serialization**: 5-part dot-separated format (header.encrypted_key.iv.ciphertext.tag). Verified by test.

7. **Go compatibility**: The format matches Go's `lestrrat-go/jwx/v2/jwe` output. The test explicitly verifies `alg: PBES2-HS512+A256KW`, `enc: A256GCM`, `p2c: 10000`, and 32-byte salt.

## Checked and Clean

- No hand-rolled crypto in the encryption/decryption path
- Salt is unique per encryption (CSPRNG)
- JWE format is standards-compliant
- Error messages do not leak key material or salt
- The decrypt path correctly rejects tampered ciphertext (verified by `test_file_keyring_corrupted_file_detection`)
