# Finding: SE Push Docs Creates Coordinator Without Identity — Tags Not Isolated

**Stream**: 06 - Data Integrity & CRDT Correctness
**Session**: 4 - Searchable Encryption Deep-Dive
**Severity**: MEDIUM (identity-based tag isolation disabled for all replicator pushes)
**Category**: Searchable Encryption / Identity Isolation
**Status**: NEW

## Summary

The `push_existing_docs` function creates an `SECoordinator` with `with_key()`, which sets `identity_pubkey` to `None`. This means all SE artifacts pushed to replicators have an empty identity in their domain separator, removing identity-based tag isolation. All users sharing the same SE key produce identical tags for identical values, enabling cross-user tag correlation by replicators.

## Evidence

### Production Code Path

`crates/db/src/push_docs.rs:211-212`:

```rust
if let Some(se_key) = se_encryption_key {
    let coordinator = crate::se::SECoordinator::with_key(se_key.to_vec());
```

### with_key Constructor Omits Identity

`crates/db/src/se/coordinator.rs:92-97`:

```rust
pub fn with_key(enc_key: Vec<u8>) -> Self {
    Self::new(SECoordinatorConfig {
        enc_key,
        ..Default::default()  // identity_pubkey: None
    })
}
```

### Default Identity is None → Empty Bytes

`crates/db/src/se/artifact_gen.rs:42`:

```rust
let identity_bytes = identity_pubkey.unwrap_or(&[]);
```

When `identity_pubkey` is `None`, the identity bytes are empty. The domain separator becomes `"eq::collection:field"` — identical for all users.

### No Callers Pass Identity

Grep for `SECoordinator::new` shows only the `with_key` path is used in production. No production code sets `identity_pubkey`:

- `push_docs.rs:212` — `with_key()` only
- No other instantiation of `SECoordinator` outside tests

## Impact

### Cross-User Tag Correlation on Replicators

When Alice and Bob both create documents with the same field value (e.g., `age = 30`), the replicator receives identical search tags for both documents. The replicator can determine that both documents share the same value without knowing the actual value.

With proper identity isolation, Alice's and Bob's tags for the same value would be different, preventing this correlation.

### Threat Model Context

This matters when:
1. Multiple identities share a replicator node (multi-tenant)
2. The replicator is untrusted (by design — replicators store encrypted data)
3. Users expect their data patterns to be isolated from other users' patterns

## Affected Code

- `crates/db/src/push_docs.rs:211-212` — coordinator creation without identity
- `crates/db/src/se/coordinator.rs:92-97` — `with_key` constructor

## Remediation

Pass the current identity's public key when creating the coordinator:

```rust
let coordinator = crate::se::SECoordinator::new(SECoordinatorConfig {
    enc_key: se_key.to_vec(),
    identity_pubkey: Some(current_identity_pubkey.to_vec()),
    max_retries: 5,
});
```

This requires threading the identity context through to the `push_existing_docs` call site.
