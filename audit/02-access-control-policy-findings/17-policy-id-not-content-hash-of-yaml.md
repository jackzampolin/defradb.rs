# Finding: Policy ID Is Not a Simple Content Hash of YAML

**Stream**: 02 - Access Control Policy
**Severity**: INFO
**Category**: Architecture Observation
**Status**: VERIFIED CORRECT
**Session**: S2 - NAC and Zanzibar Evaluation

## Summary

The policy ID generation is a two-stage SHA256 hash of parsed policy fields (not raw YAML), combined with a monotonic counter. This is Go-compatible by design but has implications for policy identity: two different YAML documents with the same semantic content produce the same inner hash but different policy IDs if the counter differs. Conversely, the same YAML submitted at different counter values produces different policy IDs.

## Analyzed Files

| File | Line | Behavior |
|------|------|----------|
| `crates/acp/src/policy_yaml/mod.rs` | 18-27 | `generate_policy_id()` — double-hash with counter |
| `crates/acp/src/policy_yaml/mod.rs` | 29-54 | `hash_policy_fields()` — inner hash of sorted fields |

## Details

### The ID Generation Algorithm

```rust
// crates/acp/src/policy_yaml/mod.rs:18-27
pub fn generate_policy_id(parsed: &ParsedPolicy, counter: u64) -> String {
    let inner_hash = hash_policy_fields(parsed);

    let mut outer_hasher = Sha256::new();
    outer_hasher.update(&inner_hash);
    outer_hasher.update(format!("{}", counter).as_bytes());

    let hash = outer_hasher.finalize();
    hash.iter().map(|b| format!("{:02x}", b)).collect()
}
```

### What's Hashed

The inner hash includes:
1. Policy name
2. Sorted resource names
3. Sorted relation names (per resource)
4. Sorted permission names and expressions (per resource)

### What's NOT Hashed

1. Policy description
2. Relation `manages` lists
3. Relation type restrictions
4. YAML formatting/whitespace
5. Comment structure

### Implications

1. **Counter dependency**: The same policy YAML submitted as the 1st vs 2nd policy on a node produces different IDs. This is intentional for Go compatibility but means policy IDs are not portable across nodes.

2. **Manages not in hash**: Two policies differing only in `manages` lists (which control who can grant/revoke relations) will have the same policy ID. This means changing the management hierarchy doesn't change the policy ID.

3. **Deterministic within a node**: Given the same counter value and same semantic policy, the ID is deterministic. This is the desired behavior for idempotency.

### Verification

The implementation correctly matches Go DefraDB's `acp_core` `IdTransformer.Transform` + `hashPol` pattern:
- Inner hash: `SHA256(name + sorted_resources + sorted_relations + sorted_permissions)`
- Outer hash: `SHA256(inner_hash_bytes + counter_as_string)`

## Conclusion

The policy ID generation is correct for Go compatibility. The counter-based approach prevents collisions between semantically different policies submitted in sequence, while the inner hash ensures content-based deduplication within a counter value. No security vulnerability, but operators should be aware that policy IDs are node-specific (counter-dependent).
