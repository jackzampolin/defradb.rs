# LWW Deletion and Resurrection — Fully Deterministic

**Severity:** Informational (Verified Clean)
**Category:** Data Integrity / CRDT Correctness
**Status:** Verified
**Session:** 6 of 6

## Summary

LWW deletion and resurrection semantics are correct and deterministic:

1. Delete (empty data) at higher priority always wins over current value
2. Delete at same priority loses to non-empty value (lexicographic: empty < anything)
3. Resurrection requires higher priority than the delete
4. After resurrection, the field has the resurrected value; other fields are NOT affected at the CRDT layer

## Affected Files

- `crates/crdt/src/lww.rs` lines 188-247 (LWW merge with delete handling)
- `crates/crdt/tests/lww_tests.rs` lines 232-310 (`test_lww_deletion_resurrection_with_priority`)

## Details

### LWW Delete Mechanics

Deletion is represented as an LwwDelta with empty data:

```rust
pub fn delete(doc_id: Vec<u8>, field_name: String, priority: u64,
              schema_version_id: String) -> Result<Self> {
    Self::new(doc_id, field_name, priority, schema_version_id, Vec::new())
}
```

When applied:
```rust
if data.is_empty() {
    rw.delete(&self.value_key).await?;  // Remove from storage
} else {
    rw.set(&self.value_key, data).await?;
}
// Priority always updated regardless of empty/non-empty
rw.set(&self.priority_key, &priority_bytes).await?;
```

### Tie-Breaking Favors Non-Deletion

At equal priority, the existing non-empty value wins:

```rust
Ordering::Equal => {
    if data <= &current_value[..] {
        return Ok(MergeResult::RejectedTieBreak);
    }
}
```

Since empty `data` (`[]`) is lexicographically less than any non-empty value, deletion always loses tie-breaks against existing values. This is the correct CRDT behavior: in case of ambiguity, prefer existence over deletion.

### Document-Level Resurrection

At the document level, resurrection works through the composite block status field:
- `status: 2` (deleted) — sets deletion marker
- A later `status: 1` block with higher priority — stores document content

The merge handler determines visibility based on the highest-priority composite's status.

### Per-Field Independence

Each LWW field is independent. Deleting one field does not affect others. Document-level deletion (composite with status=2) is separate from field-level deletion (LWW with empty data). This means:

- A document can have some fields deleted and others active
- A concurrent write to any field with sufficient priority resurrects that field
- Other fields that weren't written remain in their last state

### Test Coverage

`test_lww_deletion_resurrection_with_priority` exercises:
- Delete at lower priority (rejected)
- Delete at same priority (loses tie-break)
- Delete at higher priority (succeeds)
- Resurrection at lower priority than delete (rejected)
- Resurrection at higher priority than delete (succeeds)

## Security Assessment

The deletion/resurrection semantics are sound and well-tested. No edge case was found that could cause inconsistent state.

## Test Gap

None for field-level LWW. Missing: integration test for document-level delete + resurrect via P2P merge.
