# Finding 04: IdentityContext Has No Public-Key-Only State

**Severity**: INFO
**Category**: API Design / Dead Code
**Status**: Confirmed (not exploitable)

## Summary

`IdentityContext` documents three possible states: full identity (with signing), public identity (read-only), and empty. However, the internal `IdentityHolder` enum only has a `Full` variant — there is no `PublicOnly` variant. This means `has_identity()` and `has_full_identity()` are always equivalent, and any code that distinguishes between them is checking a condition that can never differ.

## Affected Files

- `crates/identity/src/context.rs:39-44` — `IdentityHolder` enum
- `crates/identity/src/context.rs:67-74` — `has_identity()` vs `has_full_identity()`

## Details

```rust
// crates/identity/src/context.rs:40
enum IdentityHolder {
    Full(Arc<RawIdentity>), // Only variant
    // No PublicOnly(Arc<dyn Identity>) variant
}
```

```rust
// These two methods always return the same value:
pub fn has_identity(&self) -> bool {
    self.inner.is_some() // true iff Full
}

pub fn has_full_identity(&self) -> bool {
    matches!(&self.inner, Some(IdentityHolder::Full(_))) // true iff Full
}
```

### Security implications

None. This is a design observation, not a vulnerability:

1. **No privilege escalation**: Since there's no public-only state, there's no way to accidentally treat a public-only identity as a full identity.
2. **TokenIdentity not in context**: `TokenIdentity` (from JWT parsing) implements `Identity` but not `FullIdentity`, yet it's never wrapped in `IdentityContext`. HTTP handlers extract identity directly from JWT tokens.
3. **Safe default**: The empty state is `None`, so unauthenticated requests correctly report no identity.

### Why this matters for future development

If a `PublicOnly` variant is ever added (e.g., for read-only identity derived from JWT tokens without private key access), the existing `has_identity()` / `has_full_identity()` distinction would become meaningful. Until then, consumers should be aware that checking one is equivalent to checking the other.

## Remediation

No action required. If the `PublicOnly` state is intentionally deferred, consider simplifying the API to avoid confusion:

```rust
// Remove has_identity() and only expose has_full_identity()
// OR add a comment explaining the intentional simplification
```

## Test Gap

- Tests verify `has_identity()` and `has_full_identity()` return the same values but don't test a public-only state (because it doesn't exist)
