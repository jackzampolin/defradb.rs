# Finding: Batch Signing Excludes secp256r1 Key Type

**Stream**: 01 - Cryptographic Inventory
**Severity**: LOW
**Category**: Signing / Feature Completeness
**Status**: NEW

## Summary

The batch signing functions `sign_batch()` and `verify_batch_signature()` only support Ed25519 and secp256k1 key types. Nodes using secp256r1 (P-256) identities — the key type used by browser-based identities via Web Crypto API — cannot create or verify batch signatures. This is a functional gap that may represent a Go compatibility issue.

## Affected Code

### `sign_batch()` — `crates/crypto/src/batch.rs:36-54`

```rust
let (sig_type, sig_bytes) = match config.key_type.as_str() {
    "ed25519" => {
        let key = Ed25519PrivateKey::from_bytes(&config.private_key_bytes)?;
        let sig = key.sign(&root)?;
        ("EdDSA".to_string(), sig)
    }
    "secp256k1" => {
        let key = Secp256k1PrivateKey::from_bytes(&config.private_key_bytes)?;
        let sig = key.sign(&root)?;
        ("ES256K".to_string(), sig)
    }
    other => return Err(format!("unsupported key type: {}", other)),  // secp256r1 falls here
};
```

### `verify_batch_signature()` — `crates/crypto/src/batch.rs:76-92`

```rust
match sig.sig_type.as_str() {
    "EdDSA" => { /* Ed25519 verify */ }
    "ES256K" => { /* secp256k1 verify */ }
    other => Err(format!("unsupported sig type: {}", other)),  // "ES256" falls here
}
```

## Impact

### Direct

A node configured with a secp256r1 identity that attempts batch signing will receive an error: `"unsupported key type: secp256r1"`. Batch signing is used during block creation when multiple documents are committed in a single transaction.

### Go Compatibility

If Go DefraDB supports batch signing with P-256 keys, Rust nodes cannot produce or verify those batch signatures. This would cause replication failures when Go nodes using P-256 identities batch-sign blocks that Rust nodes need to verify.

### Practical Scope

secp256r1 is used for browser-based identities (Web Crypto API). If browser clients don't perform batch signing (only authentication via JWT), this gap has no practical effect. The gap matters if server-side nodes can be configured with P-256 identities.

## Remediation

Add secp256r1 support to both functions:

```rust
// In sign_batch:
"secp256r1" => {
    let key = Secp256r1PrivateKey::from_bytes(&config.private_key_bytes)?;
    let sig = key.sign(&root)?;
    ("ES256".to_string(), sig)
}

// In verify_batch_signature:
"ES256" => {
    let pubkey = Secp256r1PublicKey::from_bytes(&pub_bytes)?;
    pubkey.verify(&root, &sig.value)
}
```

The signing and verification implementations already exist in the secp256r1 key module — this is purely a dispatch gap.
