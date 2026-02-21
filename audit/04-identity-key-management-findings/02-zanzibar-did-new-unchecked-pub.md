# Finding 02: zanzibar::Did::new_unchecked() Is pub Instead of pub(crate)

**Severity**: LOW
**Category**: API Surface / Defense in Depth
**Status**: Confirmed

## Summary

`identity::Did::new_unchecked()` is correctly scoped as `pub(crate)`, restricting unchecked DID construction to within the identity crate. However, `zanzibar::Did::new_unchecked()` is `pub`, meaning any crate in the workspace can construct a `zanzibar::Did` without validation. Combined with `from_zdid()` which converts `zanzibar::Did` to `identity::Did` using `.expect()`, this creates a path from unvalidated input to a panic.

## Affected Files

- `crates/zanzibar/src/did.rs:36-38` — `pub fn new_unchecked()`
- `crates/identity/src/did.rs:55` — `pub(crate) fn new_unchecked()` (correct)
- `crates/acp/src/zanzibar/acp/mod.rs:216-218` — `from_zdid()` with `.expect()`

## Details

```rust
// crates/zanzibar/src/did.rs:36 — pub, not pub(crate)
pub fn new_unchecked(s: String) -> Self {
    debug_assert!(s.starts_with(DID_KEY_PREFIX) || s == "*");
    Self(s)
}
```

```rust
// crates/acp/src/zanzibar/acp/mod.rs:216 — panics on invalid zanzibar DID
pub(crate) fn from_zdid(did: &Did) -> identity::Did {
    identity::Did::new(did.as_str()).expect("zanzibar::Did should always be valid identity::Did")
}
```

### Panic chain

```rust
// Any crate can do this:
let bad_did = zanzibar::Did::new_unchecked("not-a-did".to_string());

// If this DID flows into from_zdid():
let identity_did = from_zdid(&bad_did); // PANIC: "zanzibar::Did should always be valid identity::Did"
```

The `debug_assert!` in `new_unchecked()` only fires in debug builds — release builds skip it entirely.

### Current call sites of zanzibar::Did::new_unchecked()

- `crates/acp/src/zanzibar/acp/document_acp.rs:17` — `to_zdid()`, converting from validated `identity::Did`
- `crates/acp/src/zanzibar/acp/mod.rs:212` — `to_zdid()`, same pattern

Both current call sites are safe because they convert from already-validated `identity::Did` instances. The risk is from future callers who might use the `pub` API directly.

## Remediation

Restrict visibility to match the identity crate's pattern:

```rust
pub(crate) fn new_unchecked(s: String) -> Self {
    debug_assert!(s.starts_with(DID_KEY_PREFIX) || s == "*");
    Self(s)
}
```

Or replace the `.expect()` in `from_zdid()` with proper error handling:

```rust
pub(crate) fn from_zdid(did: &Did) -> Result<identity::Did> {
    identity::Did::new(did.as_str())
        .map_err(|e| Error::InvalidDid(e.to_string()))
}
```

## Test Gap

- No test verifying that `zanzibar::Did::new_unchecked()` with invalid input causes issues
- No test for `from_zdid()` with a non-`did:key:` zanzibar DID
