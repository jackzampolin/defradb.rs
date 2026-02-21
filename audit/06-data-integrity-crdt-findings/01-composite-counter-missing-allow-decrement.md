# Composite Counter Missing allow_decrement Check

**Severity:** Medium
**Category:** Data Integrity / Access Control Bypass
**Status:** Open
**CRDT Type:** Composite (Counter path)

## Summary

The Composite CRDT's inline counter merge does not enforce the `allow_decrement` policy. A negative increment arriving via a CompositeDelta is always applied, regardless of whether the schema designates the field as a positive-only counter (PCounter). This allows a malicious or buggy peer to decrement a counter that should only allow increments.

## Affected Files

- `crates/crdt/src/composite.rs` lines 263-327 (counter merge path)
- `crates/crdt/src/counter.rs` lines 415-416 (standalone check)

## Details

The standalone Counter enforces the `allow_decrement` flag:

```rust
// counter.rs:415-416
if !self.allow_decrement && increment < 0 {
    return Err(Error::MergeError("decrement not allowed".into()));
}
```

The Composite's counter path has no such check:

```rust
// composite.rs:309-318 - No allow_decrement check anywhere
let increment = i64::from_be_bytes(data[..8].try_into().unwrap());
let new_value = current.wrapping_add(increment);
rw.set(&value_key, &new_value.to_be_bytes())
    .await
    .map_err(|e| Error::Storage(e.to_string()))?;
```

Furthermore, the Composite's `FieldCrdtType` enum has no way to store the `allow_decrement` flag:

```rust
enum FieldCrdtType {
    Lww,
    Counter,  // No allow_decrement, no NumericKind
}
```

**Impact:** If the Composite merge path is used during P2P replication, a remote peer can send a negative counter increment to a field configured as a positive-only counter (e.g., a "views" or "likes" counter). The decrement will be applied, breaking the schema's semantic guarantees.

## Remediation

Extend `FieldCrdtType::Counter` to carry `allow_decrement` and `NumericKind`:

```rust
enum FieldCrdtType {
    Lww,
    Counter { allow_decrement: bool, kind: NumericKind },
}
```

Add the check in `apply_field_delta`:

```rust
if !allow_decrement && increment < 0 {
    return Err(Error::MergeError("decrement not allowed".into()));
}
```

## Test Gap

No test sends a negative increment to a Composite with a positive-only counter field.
