# Finding 00: Wildcard DID key_portion() Panics on Out-of-Bounds Slice

**Severity**: MEDIUM
**Category**: Type Safety / Denial of Service
**Status**: Confirmed (latent — not currently triggered in production paths)

## Summary

`Did::key_portion()` performs an unconditional slice at offset `DID_KEY_PREFIX.len()` (8 bytes), but wildcard DIDs contain only `"*"` (1 byte). Calling `key_portion()` on a wildcard DID will panic with an index-out-of-bounds error. The same issue exists in `zanzibar::Did::key_portion()`.

## Affected Files

- `crates/identity/src/did.rs:81-83`
- `crates/zanzibar/src/did.rs:53-55`

## Details

```rust
// crates/identity/src/did.rs:81
pub fn key_portion(&self) -> &str {
    &self.0[DID_KEY_PREFIX.len()..] // DID_KEY_PREFIX.len() == 8
}
```

`Did` has two construction paths that bypass the `did:key:` prefix requirement:

1. `Did::wildcard()` — creates `Did("*")`
2. `Did::new_unchecked()` — no runtime validation in release builds

Both produce `Did` instances where `key_portion()` will panic because the inner string is shorter than 8 bytes.

### Current call sites

`key_portion()` is only called in unit tests (`did.rs:147-152`), so this is currently a latent bug. However, the method is `pub` and available to any consumer of the identity crate.

### Panic scenario

```rust
let wildcard = Did::wildcard(); // Did("*")
let _ = wildcard.key_portion(); // PANIC: byte index 8 is out of bounds of `*`
```

## Remediation

Option A — Guard at the call site:
```rust
pub fn key_portion(&self) -> Option<&str> {
    if self.0.len() > DID_KEY_PREFIX.len() && self.0.starts_with(DID_KEY_PREFIX) {
        Some(&self.0[DID_KEY_PREFIX.len()..])
    } else {
        None
    }
}
```

Option B — Panic with a clear message (if wildcard should never be passed):
```rust
pub fn key_portion(&self) -> &str {
    assert!(!self.is_wildcard(), "key_portion() called on wildcard DID");
    &self.0[DID_KEY_PREFIX.len()..]
}
```

Apply the same fix to `zanzibar::Did::key_portion()`.

## Test Gap

- No test for `Did::wildcard().key_portion()` behavior
- No test for `Did::new_unchecked("short").key_portion()` behavior
