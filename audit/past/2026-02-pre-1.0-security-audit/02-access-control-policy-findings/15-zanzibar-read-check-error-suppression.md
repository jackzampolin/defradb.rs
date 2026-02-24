# Finding: Zanzibar Document ACP Read Check Suppresses Errors

**Stream**: 02 - Access Control Policy
**Severity**: MEDIUM
**Category**: Fail-Open on Error
**Status**: CONFIRMED
**Session**: S2 - NAC and Zanzibar Evaluation

## Summary

The `ZanzibarDocumentACP::check_doc_access()` implementation for `Read` permission swallows errors from the permission engine. When checking read access, the code iterates through `["read", "update", "delete"]` permissions and treats `Err(_)` the same as `Ok(false)` — silently continuing to the next check. If all three checks fail with errors (e.g., corrupted store, deserialization failure), access is denied. But if the engine errors on `read` and `update` but succeeds on `delete`, a user with only delete permission gets read access through the error-suppression path.

## Affected Files

| File | Line | Issue |
|------|------|-------|
| `crates/acp/src/zanzibar/acp/document_acp.rs` | 118-134 | Read permission check catches and ignores errors |

## Details

### The Error Suppression

```rust
// crates/acp/src/zanzibar/acp/document_acp.rs:118-134
let granted = if permission == DocumentPermission::Read {
    let engine = self.engine.read().await;
    let mut result = false;
    for perm_name in &["read", "update", "delete"] {
        match engine.check(policy_id, resource_name, doc_id, perm_name, &zdid).await {
            Ok(true) => {
                result = true;
                break;
            }
            Ok(false) => continue,
            Err(_) => continue,  // ERROR SILENTLY IGNORED
        }
    }
    result
} else {
    let relation = Self::permission_to_relation(permission);
    let engine = self.engine.read().await;
    engine.check(policy_id, resource_name, doc_id, relation, &zdid).await?
    //                                                                  ^^ errors propagated for non-read
};
```

### Contrast: Update and Delete Are Fail-Closed

For `Update` and `Delete` permissions, errors propagate via `?`:
```rust
engine.check(policy_id, resource_name, doc_id, relation, &zdid).await?
```

This means store errors during update/delete checks cause the operation to fail (fail-closed). But store errors during read checks are silently eaten (fail-open for that specific sub-check).

### Why This Matters

The read check iterates multiple permissions because "if you can update or delete, you can read." This is correct logic. But the error handling creates an asymmetry:

1. **Store corruption in `read` relation** + **valid `update` relation**: User gets read access (correct, but via error path)
2. **Store corruption in all three relations**: User is denied (correct)
3. **Store corruption in `read`** + **no `update` or `delete`**: User is denied (correct)

The issue is that scenario 1 masks a store corruption problem. An operator won't know that the `read` relation check is failing because errors are swallowed. This could hide data integrity issues.

### The Pattern in LocalDocumentACP

The `LocalDocumentACP` likely has the same pattern (it predates Zanzibar). This error suppression may be intentional for Go compatibility but violates the fail-closed principle documented in the codebase.

### Severity Rationale

MEDIUM because:
1. In practice, most errors would affect all three checks equally (e.g., store unavailable), resulting in correct denial
2. But partial store corruption could produce silent incorrect results
3. No diagnostic logging when errors are suppressed — makes debugging difficult
4. Contradicts the fail-closed error handling used for update/delete

## Remediation

Log suppressed errors and consider fail-closed behavior:

```rust
for perm_name in &["read", "update", "delete"] {
    match engine.check(policy_id, resource_name, doc_id, perm_name, &zdid).await {
        Ok(true) => {
            result = true;
            break;
        }
        Ok(false) => continue,
        Err(e) => {
            tracing::warn!(
                target: "acp::audit",
                event = "read_check_error",
                permission = %perm_name,
                error = %e,
                "Error during read permission sub-check"
            );
            continue;  // Or: return Err(e) for fail-closed
        }
    }
}
```
