# Priority Ceiling: u64::MAX Makes Field Permanently Immutable

**Severity:** Informational
**Category:** Design / Adversarial Resilience
**Status:** By Design (document)
**CRDT Type:** LWW

## Summary

A delta with priority `u64::MAX` permanently wins all future conflicts for that field. Any subsequent write with any lower priority (including `u64::MAX - 1`) will be rejected. At equal priority (`u64::MAX`), only a lexicographically greater value can win, which may or may not be possible depending on the current value. This is an inherent property of priority-based LWW CRDTs, not a bug.

## Affected Files

- `crates/crdt/src/lww.rs` lines 200-226
- `crates/crdt/src/composite.rs` lines 225-246

## Details

The LWW merge logic:

```rust
match incoming_priority.cmp(&current_priority) {
    Ordering::Less => return Ok(MergeResult::RejectedLowerPriority { ... }),
    Ordering::Equal => {
        if data <= &current_value[..] {
            return Ok(MergeResult::RejectedTieBreak);
        }
    }
    Ordering::Greater => { /* update */ }
}
```

If an attacker (or a bug) sets priority to `u64::MAX`:
- All writes with priority < `u64::MAX` are rejected forever
- Writes with priority == `u64::MAX` can only win if their byte value is lexicographically greater than the current value
- If the current value is `[0xFF, 0xFF, ..., 0xFF]` (all max bytes), no value can ever win

**Attack scenario:** A compromised node publishes a delta with `priority: u64::MAX` and `data: [0xFF; 1024]`. The field becomes permanently frozen at that value on all nodes in the network.

**Mitigation in practice:** Priority values are typically derived from a Merkle-DAG height counter, not user-controlled. The defense is at the priority generation layer, not the CRDT layer.

## Remediation

No code change needed. This is inherent to LWW CRDTs. Document as a known property:

1. Ensure priority generation is not user-controllable
2. Consider adding priority bounds validation at the delta ingestion layer (before CRDT merge)
3. Consider a "priority reset" mechanism for administrative recovery (out of CRDT scope)

## Test Gap

The unit test `test_lww_priority_max` correctly tests this behavior. No gap.
