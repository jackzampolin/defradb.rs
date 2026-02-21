# Composite Counter Writes Value Before Nonce (Crash Double-Count)

**Severity:** Medium
**Category:** Data Integrity / CRDT Correctness
**Status:** Open
**CRDT Type:** Composite (Counter path)

## Summary

The Composite CRDT's inline counter merge writes the updated counter value to storage BEFORE marking the nonce as applied. This is the opposite of the standalone Counter, which intentionally marks the nonce first. On crash recovery, this ordering can cause a delta to be applied twice (double-counting), violating CRDT idempotency.

## Affected Files

- `crates/crdt/src/composite.rs` lines 319-324

## Details

The standalone Counter documents and implements safe nonce ordering (`counter.rs:378-389`):

```rust
// counter.rs: Safe ordering - nonce FIRST
self.mark_nonce(rw, delta.nonce).await?;  // Step 1: Mark nonce
match new_value {
    NewValue::Int64(v) => self.set_int64(rw, v).await?,  // Step 2: Update value
    NewValue::Float64(v) => self.set_float64(rw, v).await?,
}
```

The Composite does the opposite:

```rust
// composite.rs lines 319-324: Unsafe ordering - value FIRST
rw.set(&value_key, &new_value.to_be_bytes())  // Step 1: Update value
    .await
    .map_err(|e| Error::Storage(e.to_string()))?;
rw.set(&nonce_key, &[1])                      // Step 2: Mark nonce
    .await
    .map_err(|e| Error::Storage(e.to_string()))?;
```

**Crash scenario:** If the process crashes after the value write (Step 1) but before the nonce write (Step 2), the delta's increment is persisted but the nonce is not marked. When the delta is replayed during recovery, the nonce check passes (nonce not found), and the increment is applied again.

The standalone Counter's comment explains this explicitly:
> "Nonce is marked FIRST, then value is updated. If crash occurs after nonce but before value update: delta is lost (under-count). If crash occurred with old ordering (value then nonce): would double-count. Under-counting on crash is safer than over-counting because over-counting violates CRDT idempotency guarantees."

## Remediation

Swap the write ordering in `composite.rs` to match the standalone Counter:

```rust
// Mark nonce FIRST
rw.set(&nonce_key, &[1])
    .await
    .map_err(|e| Error::Storage(e.to_string()))?;
// THEN update value
rw.set(&value_key, &new_value.to_be_bytes())
    .await
    .map_err(|e| Error::Storage(e.to_string()))?;
```

## Test Gap

No crash-recovery tests exist for either the Composite or standalone Counter merge paths.
