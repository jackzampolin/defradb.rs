# Keyring get() Returns Plain Vec<u8> — Key Material Not Zeroized

- **Severity**: Medium
- **Category**: Key Material Handling
- **Status**: Open

## Summary

The `Keyring::get()` trait method returns `Vec<u8>`, meaning decrypted private key material lives in heap memory without automatic zeroization on drop. All callers receive raw key bytes that persist until the allocator reuses the memory. This includes the critical peer key path at node startup, identity export, and every `KeyHandle::get_key_bytes()` call.

## Affected Files

- `crates/keyring/src/keyring.rs:18` — trait returns `Result<Vec<u8>>`
- `crates/keyring/src/signer.rs:133-134` — `get_key_bytes()` returns `Result<Vec<u8>>`
- `crates/cli/src/commands/start/p2p.rs:22` — peer key loaded as plain `Vec<u8>`
- `crates/cli/src/commands/keyring_cmd.rs:179-184` — export prints key then drops
- `crates/cli/src/commands/identity.rs:136,162,189` — key material in plain `Vec<u8>`

## Details

```rust
// keyring.rs:18
fn get(&self, name: &str) -> Result<Vec<u8>>;
```

All three backends (FileKeyring, SystemKeyring, SystemdCredsKeyring) return decrypted key material as `Vec<u8>`. The `KeyHandle` documentation states keys are "fetched on-demand (not cached)" to minimize memory exposure, but the returned `Vec<u8>` has no zeroization guarantee:

```rust
// signer.rs:133-134
pub fn get_key_bytes(&self) -> Result<Vec<u8>> {
    self.keyring.get(self.key_name.as_str())
}
```

**Critical paths where key material lingers**:

1. **Node startup** (`p2p.rs:22-25`): Peer key bytes live on the stack/heap through `derive_and_log_identity_did()` and `keypair_from_ed25519_bytes()`. After the keypair is constructed, the original `Vec<u8>` is dropped without zeroizing.

2. **Export command** (`keyring_cmd.rs:179-184`): Key bytes are hex-encoded and printed to stdout, then the `Vec<u8>` is dropped without zeroizing.

3. **Identity new** (`identity.rs:162`): `private_key_hex = hex::encode(&raw_bytes)` — the hex string is also not zeroized.

**Contrast with password handling**: The `FileKeyring` password is correctly wrapped in `Zeroizing<Vec<u8>>`, demonstrating that the codebase is aware of zeroization. The asymmetry between password zeroization and key material non-zeroization is an oversight.

## Remediation

1. Change the `Keyring::get()` return type to `Result<Zeroizing<Vec<u8>>>`. This is the most impactful change — all backends and consumers would automatically benefit.
2. Alternatively, change `KeyHandle::get_key_bytes()` to return `Zeroizing<Vec<u8>>` as a wrapper layer.
3. For CLI export paths where key material is printed, zeroize the buffer after printing.
4. Note: Changing the trait signature is a breaking change but the keyring crate is internal.

## Test Gap

- No test verifies key material zeroization after use.
- No test for `KeyHandle::get_key_bytes()` return value lifecycle.
