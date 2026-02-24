# Finding: Key::raw() Trait Returns Unprotected Vec&lt;u8&gt;

**Stream**: 01 - Cryptographic Inventory
**Severity**: LOW-MEDIUM
**Category**: API Design / Key Lifecycle
**Status**: NEW

## Summary

The `Key` trait defines `fn raw(&self) -> Vec<u8>`, returning private key bytes in a plain `Vec<u8>` that is never zeroed. This is an architectural issue — every caller of `raw()` on a private key receives unprotected key material with no way to ensure cleanup. There are 10+ call sites across 5 crates.

## Trait Definition

`crates/crypto/src/keys/mod.rs:19-20`:

```rust
pub trait Key: defra_core::thread_bounds::MaybeSendSync {
    fn raw(&self) -> Vec<u8>;  // Returns unprotected key bytes
    // ...
}
```

The `to_hex_string()` default method (line 23-25) compounds this by calling `raw()` internally:

```rust
fn to_hex_string(&self) -> String {
    hex::encode(self.raw())  // Temporary Vec<u8> created and dropped unzeroed
}
```

## Call Site Inventory

Every call site receives unprotected private key bytes. None use `Zeroizing` wrappers.

### Identity Crate

| File | Line(s) | Usage | Exposure |
|------|---------|-------|----------|
| `crates/identity/src/raw.rs` | 91, 97, 103 | `private_key.raw()` → `from_bytes()` | Short-lived, immediate conversion |
| `crates/identity/src/raw.rs` | 176-178 | `private_key_bytes()` returns `private_key.raw()` | Propagated to callers |

### CLI Crate

| File | Line(s) | Usage | Exposure |
|------|---------|-------|----------|
| `crates/cli/src/commands/identity.rs` | 136 | `identity.private_key_bytes()` | Stored in local var, hex-encoded to stdout, stored in keyring |
| `crates/cli/src/commands/identity.rs` | 162 | `hex::encode(&raw_bytes)` | Plaintext hex string on stack |
| `crates/cli/src/commands/keyring_cmd.rs` | 102 | `private_key.raw()` → `keyring.set()` | Immediate storage |
| `crates/cli/src/commands/keyring_cmd.rs` | 140, 145, 150 | `private_key.raw().to_vec()` | Additional heap copy, returned to caller |
| `crates/cli/src/commands/start/p2p.rs` | 33 | `private_key.raw()` | Stored in keyring, converted to libp2p keypair |

### WASM Crate

| File | Line(s) | Usage | Exposure |
|------|---------|-------|----------|
| `crates/wasm/src/verification.rs` | 167, 189 | `private_key.to_hex_string()` | Hex-encoded in JSON response to WASM caller |

### Crypto Crate (Tests)

| File | Line(s) | Usage | Exposure |
|------|---------|-------|----------|
| `crates/crypto/src/batch.rs` | 131, 143 | `private_key.raw()` | Test-only, stored in signing config |

## Specific Concerns

### 1. Heap Fragmentation

`Vec<u8>` may be reallocated by the allocator during construction (e.g., `extend_from_slice` on an undersized buffer). Old copies in freed heap blocks are never zeroed.

### 2. Double Copy in `keyring_cmd.rs`

```rust
Ok(private_key.raw().to_vec())  // raw() creates Vec, .to_vec() clones it
```

This creates two heap-allocated copies of key material, neither zeroed.

### 3. Hex String Materialization

`to_hex_string()` creates a temporary `Vec<u8>` via `raw()` that is dropped unzeroed, plus the hex-encoded `String` itself contains the key in a different encoding (also unzeroed).

### 4. Keyring `get()` Return Type

`crates/keyring/src/file.rs:123` — `fn get(&self, name: &str) -> Result<Vec<u8>>` returns decrypted key material as plain `Vec<u8>`. The password is correctly wrapped in `Zeroizing<Vec<u8>>` (line 36), but the decrypted key output is not.

## Why LOW-MEDIUM

- The `raw()` bytes are typically short-lived (created, used for serialization/storage, dropped)
- Most callers immediately pass bytes to a constructor or encrypted storage
- The attack requires memory access to the running process
- However, the systemic nature means EVERY key operation leaks, creating a broad surface

## Remediation

### Option A: Change Trait Return Type (Breaking)

```rust
pub trait Key {
    fn raw(&self) -> Zeroizing<Vec<u8>>;
    // ...
}
```

This forces all callers to handle `Zeroizing`-wrapped bytes. Breaking change to the trait and all implementors.

### Option B: Add Parallel Method (Non-Breaking)

```rust
pub trait Key {
    fn raw(&self) -> Vec<u8>;
    fn raw_zeroizing(&self) -> Zeroizing<Vec<u8>> {
        Zeroizing::new(self.raw())
    }
}
```

Allows gradual migration. Callers of `raw()` can switch to `raw_zeroizing()` incrementally.

### Option C: Wrapper at Call Sites (Minimal)

Wrap `raw()` output at each call site:

```rust
let raw_bytes = Zeroizing::new(private_key.raw());
```

Lowest effort but relies on discipline at every call site.
