# Finding: SE enc_key Stored as Plain Vec<u8> — No Zeroization on Drop

**Stream**: 06 - Data Integrity & CRDT Correctness
**Session**: 4 - Searchable Encryption Deep-Dive
**Severity**: MEDIUM (key material persists in heap memory after coordinator is dropped)
**Category**: Searchable Encryption / Key Lifecycle
**Status**: CONFIRMED (cross-reference with Stream 1 Finding 16)

## Summary

The SE encryption key (`enc_key: Vec<u8>`) in `SECoordinatorConfig` is never zeroized when the coordinator is dropped. The key persists in heap memory until the allocator reuses the page. In long-running node processes, this key material could remain accessible for extended periods. Additionally, the `enc_key` is cloned in the `push_docs` path, creating additional unzeroized copies.

## Evidence

### No Zeroize Derive or Drop Implementation

`crates/db/src/se/coordinator.rs:55-63`:

```rust
#[derive(Debug, Clone)]  // Clone creates additional unzeroized copies
pub struct SECoordinatorConfig {
    pub enc_key: Vec<u8>,           // Plain Vec<u8>, no Zeroize
    pub identity_pubkey: Option<Vec<u8>>,
    pub max_retries: usize,
}
```

### Zeroize Only Used in Keyring Crate

From the grep results, only `crates/keyring/src/file.rs` uses `zeroize::Zeroizing`. The SE key transitions from the keyring (where it's protected) to a plain `Vec<u8>` when passed to the coordinator.

### Key Cloned in Push Path

`crates/db/src/push_docs.rs:212`:

```rust
let coordinator = crate::se::SECoordinator::with_key(se_key.to_vec());
```

`se_key.to_vec()` creates a new heap allocation of the key. When `coordinator` is dropped at the end of the function, this allocation is freed but not zeroed.

### FFI Path Also Clones

`crates/ffi/src/p2p/push.rs:72`:

```rust
let push_se_key = state.se_encryption_key.clone();
```

Another clone of the SE key for the push task, which is not zeroized when the task completes.

### Key Accessible via Public Accessor

`crates/db/src/se/coordinator.rs:100-102`:

```rust
pub fn enc_key(&self) -> &[u8] {
    &self.config.enc_key
}
```

Any code with a coordinator reference can read the key material.

## Impact

In a memory dump or core dump of a running DefraDB process, the SE encryption key could be recovered from freed heap memory. With the SE key, an attacker can:
- Compute any search tag for any value
- Reverse the tag isolation (determine what value produced a given tag via brute force)
- Generate fake artifacts that would pass validation

## Affected Code

- `crates/db/src/se/coordinator.rs:55-63` — `SECoordinatorConfig` struct
- `crates/db/src/push_docs.rs:212` — key cloned into coordinator
- `crates/ffi/src/p2p/push.rs:72` — key cloned for push task
- `crates/ffi/src/state/mod.rs:110` — key stored as `Option<Vec<u8>>` in FFI state

## Remediation

See Stream 1 Finding 16 for the detailed remediation plan. Summary:

1. Use `zeroize::Zeroizing<Vec<u8>>` for `enc_key` in `SECoordinatorConfig`
2. Derive `ZeroizeOnDrop` for `SECoordinatorConfig`
3. Use `Zeroizing<Vec<u8>>` in the FFI state for `se_encryption_key`

## Cross-References

- Finding 01-16: SE enc_key not zeroized and default zeros (MEDIUM)
