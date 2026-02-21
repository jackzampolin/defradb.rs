# Finding: Weak Mutation Denial Assertions Use Silent-Skip Pattern

**Stream**: 02 - Access Control Policy
**Severity**: LOW
**Category**: Test Quality
**Status**: CONFIRMED
**Session**: S4 - Integration Test Validation

## Summary

Multiple ACP integration tests assert that unauthorized mutations are denied, but use an `if let Ok(result)` pattern that silently skips the assertion if the mutation returns an error. This means the test passes regardless of whether the mutation was correctly denied (empty result), incorrectly allowed, or failed for an unrelated reason. The assertion only fires when the mutation returns `Ok` — if it returns `Err`, the denial is assumed but never verified.

## Affected Tests

### acp_multi_role.rs — Reader update denial (lines 103-117)

```rust
// Dave tries to update doc1 (reader can't update) — expect failure
let dave_update = node.query_with_identity(/* ... */);
// Reader update should either fail or return empty result
if let Ok(result) = dave_update {  // ← if Err, assertion is SKIPPED
    let updated = result["update_Document"]
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0);
    assert_eq!(updated, 0, "Dave (reader) should not update doc1");
}
```

### acp_multi_role.rs — Writer delete denial (lines 135-148)

```rust
let carol_delete = node.query_with_identity(/* ... */);
if let Ok(result) = carol_delete {  // ← if Err, assertion is SKIPPED
    let deleted = result["delete_Document"]
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0);
    assert_eq!(deleted, 0, "Carol (writer) should not delete doc1");
}
```

### cross_compartment_isolation.rs — Four instances (lines 197-294)

```rust
// xbot write to trail (cross-compartment)
if let Ok(result) = xbot_write_trail {  // ← if Err, skipped
    assert_eq!(count, 0, "xbot should NOT write to Trail");
}

// hiking_svc write to tweet (cross-compartment)
if let Ok(result) = hiking_write_tweet {  // ← if Err, skipped
    assert_eq!(count, 0, "hiking_svc should NOT write to Tweet");
}

// xbot update trail (reader, not writer)
if let Ok(result) = xbot_update_trail {  // ← if Err, skipped
    assert_eq!(count, 0, "xbot should NOT update Trail");
}

// xbot delete tweet (writer, not admin)
if let Ok(result) = xbot_delete_tweet {  // ← if Err, skipped
    assert_eq!(count, 0, "xbot should NOT delete Tweet");
}
```

### encrypted_acp.rs — Rogue update denial (lines 128-141)

```rust
let rogue_update = node.query_with_identity(/* ... */);
if let Ok(result) = rogue_update {  // ← if Err, skipped
    let updated = result["update_User"]
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0);
    assert_eq!(updated, 0, "rogue should NOT update encrypted doc");
}
```

## Impact

The pattern has two problems:

1. **False pass on unrelated errors**: If the mutation fails due to a network error, serialization bug, or server crash, the test reports success. The denial behavior is never actually verified.

2. **Ambiguous expected behavior**: The comment "should either fail or return empty result" reveals uncertainty about what the correct denial behavior *is*. DefraDB's ACP can deny mutations in two ways:
   - Return Ok with empty result (0 documents affected)
   - Return an error message

   The test should pin down which behavior is correct and assert it explicitly.

## Correct Pattern

```rust
// Option A: Assert Ok with empty result (if DefraDB returns success with 0 affected)
let dave_update = node.query_with_identity(/* ... */)
    .expect("mutation should return Ok even when denied");
let updated = result["update_Document"]
    .as_array()
    .map(|a| a.len())
    .unwrap_or(0);
assert_eq!(updated, 0, "Dave (reader) should not update doc1");

// Option B: Assert Err (if DefraDB returns an error on denied mutations)
let dave_update = node.query_with_identity(/* ... */);
assert!(dave_update.is_err(), "reader mutation should be denied");
```

## Severity Rationale

LOW because:
- The secondary assertions (verifying data unchanged after denial attempt) do provide catch coverage
- The vulnerability would only manifest if the mutation silently succeeded AND returned Ok
- This is a test quality issue, not a missing test — the intent is correct, the assertion is weak
