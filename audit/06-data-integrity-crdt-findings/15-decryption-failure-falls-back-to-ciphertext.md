# Decryption Failure Falls Back to Raw Ciphertext

**Severity:** Low
**Category:** Data Integrity / Encryption
**Status:** Confirmed

## Summary

When block decryption fails (wrong key, corrupted ciphertext, missing encryption block), the merge handler silently falls back to using the raw encrypted bytes as if they were plaintext CRDT data. For LWW fields, this means ciphertext bytes are passed to `ciborium::from_reader` for CBOR decoding, which will either produce garbage values (if the ciphertext happens to be valid CBOR) or fail with a decode error. For the composite merge path, a decode error aborts the entire composite merge, effectively denying the merge.

## Affected Files

- `crates/db/src/merge_handler/mod.rs:376` (LWW fallback to `&block`)
- `crates/db/src/merge_handler/mod.rs:398` (Counter fallback to `&block`)
- `crates/db/src/merge_handler/composite.rs:300-302` (linked LWW decrypt fallback)
- `crates/db/src/merge_handler/composite.rs:313-314` (linked Counter decrypt fallback)
- `crates/db/src/merge_handler/batch.rs:178` (batch LWW decrypt fallback)
- `crates/db/src/merge_handler/batch.rs:199` (batch Counter decrypt fallback)

## Details

### Fallback Chain

```rust
// mod.rs:357-403 — top-level block processing
let effective_block = if block.encryption.is_some() {
    match &block.delta {
        CrdtDelta::Lww(payload) => {
            match self.decrypt_block_data(&payload.data, block.encryption.as_ref()).await {
                Ok(decrypted_data) => { /* use decrypted */ }
                Err(_) => &block,  // <-- FALLBACK: use encrypted data as-is
            }
        }
        // ... same for Counter ...
    }
};
```

```rust
// composite.rs:290-302 — linked block processing
CrdtDelta::Lww(p) if linked_block.encryption.is_some() => {
    match self.decrypt_block_data(&p.data, linked_block.encryption.as_ref()).await {
        Ok(decrypted) => { /* use decrypted */ }
        Err(_) => linked_block.delta.clone(),  // <-- FALLBACK: ciphertext as delta
    }
}
```

### Impact Analysis

**Case 1: ACP-protected collection, unauthorized node**
The `should_skip_encrypted_merge` function returns `true`, and field merges are skipped entirely at line 280. The fallback code at line 300-302 is NOT reached. This is the **correct** path.

**Case 2: Authorized node, decryption fails (key mismatch/corruption)**
- `should_skip_encrypted_merge` returns `false` (node is authorized)
- Decryption fails → fallback to ciphertext
- Ciphertext bytes passed to LWW merge as `payload.data`
- `ciborium::from_reader` tries to decode ciphertext as CBOR
- Almost certainly fails with `BlockDecode` error → `process_error` set, composite merge aborted
- Transaction discarded, block NOT merged
- Result: denial of merge for this block

**Case 3: Attacker crafts ciphertext that IS valid CBOR**
- Statistically improbable but theoretically possible
- Garbage value stored as field data
- Document state corrupted with attacker-controlled value

### Go Comparison

Go's merge handler checks `canRead` (from KMS) and skips the entire field merge if false. There is no fallback path that passes ciphertext to the CRDT merge. The Rust implementation's fallback is an artifact of the different encryption architecture (no KMS).

## Remediation

Make decryption failure explicit rather than falling back:

```rust
CrdtDelta::Lww(payload) => {
    match self.decrypt_block_data(&payload.data, block.encryption.as_ref()).await {
        Ok(decrypted_data) => { /* use decrypted */ }
        Err(e) => {
            tracing::warn!(error = %e, "Decryption failed for encrypted block");
            // Return early with skip, don't try to merge ciphertext
            return Ok(MergeOutcome::skipped("decryption failed"));
        }
    }
}
```

For the linked block path in composite.rs, set `process_error` or skip the field entirely rather than using `linked_block.delta.clone()`.

## Test Gap

No test covers the decryption failure fallback path:
- Unit test: encrypted block with wrong key → verify merge is skipped, not corrupted
- Unit test: encrypted block with missing encryption block → verify clean error
- Integration test: P2P node without encryption key receives encrypted document
