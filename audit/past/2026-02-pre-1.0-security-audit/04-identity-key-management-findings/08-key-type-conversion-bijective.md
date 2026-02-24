# Finding 08: Key Type Conversions Are Bijective and BLS12-381 Correctly Rejected

**Severity**: GREEN
**Category**: Type Safety / Key Type Confusion
**Status**: Verified safe

## Summary

The conversions between `IdentityKeyType` (identity crate) and `KeyType` (crypto crate) are bijective for the three supported types (Ed25519, secp256k1, secp256r1). BLS12-381 is correctly rejected at the identity boundary. There is no path for key type confusion.

## Affected Files

- `crates/identity/src/key_type.rs:27-55` — conversions

## Details

```rust
// IdentityKeyType → KeyType (total function, infallible)
impl From<IdentityKeyType> for KeyType {
    fn from(ikt: IdentityKeyType) -> Self {
        match ikt {
            IdentityKeyType::Ed25519 => KeyType::Ed25519,
            IdentityKeyType::Secp256k1 => KeyType::Secp256k1,
            IdentityKeyType::Secp256r1 => KeyType::Secp256r1,
        }
    }
}

// KeyType → IdentityKeyType (partial function, rejects BLS12-381)
impl TryFrom<KeyType> for IdentityKeyType {
    fn try_from(key_type: KeyType) -> Result<Self, Self::Error> {
        match key_type {
            KeyType::Ed25519 => Ok(IdentityKeyType::Ed25519),
            KeyType::Secp256k1 => Ok(IdentityKeyType::Secp256k1),
            KeyType::Secp256r1 => Ok(IdentityKeyType::Secp256r1),
            KeyType::Bls12381 => Err(Error::UnsupportedKeyType(key_type)),
        }
    }
}
```

### Properties verified

1. **Bijective**: `to_crypto_key_type()` and `TryFrom<KeyType>` are inverses for the supported set.
2. **BLS12-381 rejected**: Cannot enter the identity system. Also rejected in `RawIdentity::from_private_key()` (line 108) and `RawIdentity::from_bytes()` (line 130) and `new_token()` (line 97).
3. **Exhaustive match**: The Rust compiler ensures all `KeyType` variants are handled in `TryFrom`, so adding a new `KeyType` variant to the crypto crate will cause a compile error until handled.
4. **No type confusion in RawIdentity**: `IdentityInner` stores concrete types (`Ed25519PrivateKey`, `Secp256k1PrivateKey`, etc.), not trait objects, so a key's algorithm cannot change after construction.

## Remediation

None required. The type system enforces correctness.
