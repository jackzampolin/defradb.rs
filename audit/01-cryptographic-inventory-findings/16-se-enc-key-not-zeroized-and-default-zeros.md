# Finding: SE Encryption Key Not Zeroized and Default All-Zeros

**Stream**: 01 - Cryptographic Inventory
**Session**: 5 - Searchable Encryption & Merkle Proof
**Severity**: MEDIUM (key material lingers in memory; insecure default could reach production)
**Category**: Key Lifecycle / Searchable Encryption
**Status**: NEW

## Summary

The SE encryption key (`enc_key: Vec<u8>`) stored in `SECoordinatorConfig` is never zeroized when the coordinator is dropped. This extends the pattern identified in Finding 00 (private keys not zeroized) to searchable encryption key material. Additionally, the default config initializes with an all-zeros key, which if used in production would make all search tags globally predictable.

## Evidence

### No Zeroize on Drop

`crates/db/src/se/coordinator.rs:55-63`:

```rust
#[derive(Debug, Clone)]
pub struct SECoordinatorConfig {
    /// SE encryption key (32 bytes).
    pub enc_key: Vec<u8>,
    /// Identity's public key for tag isolation.
    pub identity_pubkey: Option<Vec<u8>>,
    /// Maximum number of retry attempts.
    pub max_retries: usize,
}
```

Neither `SECoordinatorConfig` nor `SECoordinator` implement `Drop` or derive `Zeroize`. The `enc_key` is a plain `Vec<u8>` that is deallocated without zeroing when the coordinator goes out of scope.

Confirmed via grep: no `Zeroize`, `Drop for SE`, or `Drop for.*Coordinator` matches in `crates/db/src/se/`.

### Insecure Default

`crates/db/src/se/coordinator.rs:65-72`:

```rust
impl Default for SECoordinatorConfig {
    fn default() -> Self {
        Self {
            enc_key: vec![0u8; 32],  // ALL-ZEROS KEY
            identity_pubkey: None,
            max_retries: 5,
        }
    }
}
```

An all-zeros 32-byte key is the default. While `Default` impls are typically for convenience, the `with_key` constructor uses `..Default::default()`:

```rust
pub fn with_key(enc_key: Vec<u8>) -> Self {
    Self::new(SECoordinatorConfig {
        enc_key,
        ..Default::default()
    })
}
```

This is fine when a real key is provided. However, if `SECoordinatorConfig::default()` is ever used directly (e.g., in tests that accidentally reach production paths), the all-zeros key makes every tag predictable.

### Production Usage Without Identity

`crates/db/src/push_docs.rs:212`:

```rust
let coordinator = crate::se::SECoordinator::with_key(se_key.to_vec());
```

The `push_docs` code creates a coordinator with only the encryption key and no identity pubkey. This means all artifacts generated during document push have empty identity bytes in the domain separator, removing identity-based tag isolation.

### enc_key Exposed via Public Accessor

`crates/db/src/se/coordinator.rs:100-102`:

```rust
pub fn enc_key(&self) -> &[u8] {
    &self.config.enc_key
}
```

The encryption key is accessible via a public method, making it easy for any code with a coordinator reference to read the key material.

## Impact

### Memory Residue

After an `SECoordinator` is dropped, the 32-byte encryption key remains in heap memory until the allocator reuses the page. In long-running processes, this key material could persist for extended periods and be recoverable via memory dumps.

### All-Zeros Key Risk

If the default config is used without providing a real key, all tags across all collections, fields, and identities become predictable — anyone with knowledge of the scheme can compute what any tag will be.

## Affected Code

- `crates/db/src/se/coordinator.rs:55-63` — `SECoordinatorConfig` struct (no Zeroize)
- `crates/db/src/se/coordinator.rs:65-72` — `Default` impl with all-zeros key
- `crates/db/src/se/coordinator.rs:100-102` — public `enc_key()` accessor
- `crates/db/src/push_docs.rs:212` — coordinator created without identity

## Remediation

1. **Zeroize**: Derive or implement `Zeroize` + `ZeroizeOnDrop` for `SECoordinatorConfig`:

```rust
use zeroize::{Zeroize, ZeroizeOnDrop};

#[derive(Debug, Clone, Zeroize, ZeroizeOnDrop)]
pub struct SECoordinatorConfig {
    pub enc_key: Vec<u8>,
    #[zeroize(skip)]
    pub identity_pubkey: Option<Vec<u8>>,
    #[zeroize(skip)]
    pub max_retries: usize,
}
```

2. **Remove Default impl** or make it return an error/panic to prevent accidental use of an insecure key. Use a constructor that requires a non-empty key.

3. **Validate key length** in the constructor — reject keys that aren't exactly 32 bytes.
