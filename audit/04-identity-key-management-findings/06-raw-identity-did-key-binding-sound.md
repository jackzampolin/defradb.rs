# Finding 06: RawIdentity DID-PublicKey Binding — Verified Sound

**Severity**: GREEN
**Category**: Type Safety / Cryptographic Binding
**Status**: Verified safe

## Summary

`RawIdentity` derives its DID from the stored public key at call time via `self.pub_key().did()`. There is no way to construct a `RawIdentity` with a mismatched DID and public key because the DID is never stored separately — it's always derived from the cryptographic key material.

## Affected Files

- `crates/identity/src/raw.rs:192-208` — `Identity` trait implementation
- `crates/identity/src/raw.rs:38-110` — constructors

## Details

```rust
impl Identity for RawIdentity {
    fn did(&self) -> Result<Did> {
        let did_string = self
            .pub_key()
            .did()
            .map_err(|e| Error::InvalidDid(format!("failed to derive DID: {}", e)))?;
        Ok(Did::new_unchecked(did_string))
    }
}
```

### Key properties verified

1. **No stored DID**: The DID is computed on every call, not cached. This prevents stale or tampered DIDs.
2. **new_unchecked() is safe here**: The DID string comes from `PublicKey::did()` in the crypto crate, which produces valid `did:key:z...` strings. The `pub(crate)` visibility prevents external callers from misusing it.
3. **Constructor validation**: All constructors (`from_ed25519`, `from_secp256k1`, `from_secp256r1`, `from_private_key`, `from_bytes`) derive the public key from the private key and validate the key bytes, ensuring the private-public key pair is consistent.
4. **No key type confusion**: The `IdentityInner` enum stores concrete key types (not `dyn` trait objects), so an Ed25519 key cannot be confused with a secp256k1 key.

### TokenIdentity stores DID

Unlike `RawIdentity`, `TokenIdentity` stores the DID directly (`pub(crate) did: Did`) rather than deriving it. However, the DID is set in `from_token()` after verifying that it matches the DID derived from the public key (line 253-264 of `token/mod.rs`), so it's also safe.

## Remediation

None required. The design is cryptographically sound.
