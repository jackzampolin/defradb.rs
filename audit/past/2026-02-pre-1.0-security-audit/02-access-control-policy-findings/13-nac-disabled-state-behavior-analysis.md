# Finding: NAC DisabledTemporarily State Allows All Operations Except Relationship Writes

**Stream**: 02 - Access Control Policy
**Severity**: INFO
**Category**: Security Architecture
**Status**: VERIFIED CORRECT
**Session**: S2 - NAC and Zanzibar Evaluation

## Summary

The NAC three-state machine (`NotConfigured`, `Enabled`, `DisabledTemporarily`) is well-designed and correctly prevents privilege escalation during the disabled state. When NAC is temporarily disabled, all permission checks return `true` (allow), but relationship modification operations (add/remove admin, grant/revoke permission) are explicitly blocked. This prevents an attacker from granting themselves permissions while enforcement is suspended.

## Analyzed Files

| File | Line | Behavior |
|------|------|----------|
| `crates/acp/src/nac/node_acp/operations.rs` | 23-28 | `check_permission()` returns `Ok(true)` when not Enabled |
| `crates/acp/src/nac/node_acp/operations.rs` | 113-120 | `add_admin()` blocks when DisabledTemporarily |
| `crates/acp/src/nac/node_acp/operations.rs` | 182-189 | `remove_admin()` blocks when DisabledTemporarily |
| `crates/acp/src/nac/node_acp/operations.rs` | 246-253 | `add_permission_grant()` blocks when DisabledTemporarily |
| `crates/acp/src/nac/node_acp/operations.rs` | 316-322 | `remove_permission_grant()` blocks when DisabledTemporarily |
| `crates/acp/src/nac/node_acp/lifecycle.rs` | 59-109 | `disable()` — persists disabled flag via sentinel relation |
| `crates/acp/src/nac/node_acp/lifecycle.rs` | 111-156 | `re_enable()` — removes disabled flag |

## Details

### State Machine Correctness

| State | Permission Checks | Relationship Writes | Intended Behavior |
|-------|-------------------|--------------------|--------------------|
| `NotConfigured` | All allowed | N/A (no NAC) | Permissive default |
| `Enabled` | Enforced (owner + admin) | Allowed (with auth) | Normal operation |
| `DisabledTemporarily` | All allowed | **Blocked** | Maintenance mode |

### Privilege Escalation Prevention

The critical security property: when an admin temporarily disables NAC for maintenance, an attacker cannot:

```rust
// crates/acp/src/nac/node_acp/operations.rs:113-120
pub async fn add_admin(&self, requestor: &Did, target: &Did) -> Result<bool> {
    let status = *self.status.read().await;
    if status == NacStatus::DisabledTemporarily {
        return Err(Error::InvalidPolicy(
            "cannot modify relationships while NAC is disabled - re-enable NAC first".into(),
        ));
    }
    // ...
}
```

This pattern is repeated for all four write operations.

### Re-enable Uses Persisted Admin Check

```rust
// crates/acp/src/nac/node_acp/mod.rs (NodeAcpOperations trait impl)
// The re_enable handler in nac.rs calls nac.re_enable(&requestor)
// which internally uses is_admin_persisted() — checks stored relationships
// even when NAC status is DisabledTemporarily
```

This is correct: re-enabling requires admin status verified against stored relationships, not the runtime status check (which would always return `true` when disabled).

### Disabled State Persistence

The disabled state survives restarts via a sentinel relation:

```rust
const DISABLED_RELATION: &str = "_disabled";
```

Stored as a Zanzibar relationship `(node, singleton, _disabled, owner_did)`. On load, if this relationship exists, status is set to `DisabledTemporarily`. This prevents a restart from silently re-enabling NAC.

### Security Considerations

1. **During disabled window**: All operations succeed without auth. This is intentional for maintenance but means the window should be minimized.
2. **Re-enable auth**: Uses `is_admin_persisted()`, not `is_admin()`, correctly checking stored relationships even when status says "allow all."
3. **The `_disabled` sentinel cannot be injected via API**: It's not a `NodePermission` variant, so `add_permission_grant()` can't write it. Only `disable()` / `re_enable()` manage this relation.

## Conclusion

The NAC state machine is well-designed. The decision to block writes during disabled state is the correct security tradeoff. No vulnerabilities found in this subsystem.
