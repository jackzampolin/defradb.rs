# Finding: Policy YAML Parsing Has No Size Limits

**Stream**: 02 - Access Control Policy
**Severity**: LOW
**Category**: Resource Exhaustion
**Status**: CONFIRMED
**Session**: S2 - NAC and Zanzibar Evaluation

## Summary

The policy YAML parser (`parse_policy_yaml()`) and associated processing functions accept arbitrarily large input with no size bounds. A malicious user with policy-add permission could submit a very large YAML document to exhaust memory or CPU during parsing, duplicate-key scanning, expression validation, or policy ID hashing.

## Affected Files

| File | Line | Issue |
|------|------|-------|
| `crates/acp/src/policy_yaml/parse.rs` | 3-5 | `parse_policy_yaml()` — no size check |
| `crates/acp/src/policy_yaml/parse.rs` | 48-102 | `scan_raw_yaml_for_duplicates()` — O(n) line-by-line with stack |
| `crates/acp/src/policy_yaml/mod.rs` | 18-27 | `generate_policy_id()` — hashes entire parsed structure |
| `crates/acp/src/policy_yaml/mod.rs` | 121-168 | `build_policy()` — creates N relations from N resources × N permissions |
| `crates/acp/src/policy_yaml/validate.rs` | 9-58 | `validate_policy_expressions()` — iterates all resources × permissions |

## Details

### No Input Validation

```rust
// crates/acp/src/policy_yaml/parse.rs:3-5
pub fn parse_policy_yaml(yaml: &str) -> Result<ParsedPolicy, String> {
    serde_yaml::from_str(yaml).map_err(|e| format!("invalid policy YAML: {}", e))
}
```

No size check, no resource limit, no nesting limit.

### Amplification Vectors

1. **Many resources**: A policy with 10,000 resources creates 10,000 `Resource` objects, each potentially with many relations.

2. **Many permissions per resource**: Each permission is parsed, validated, and converted to a `Relation` with computed expression.

3. **Duplicate-key scanning**: `scan_raw_yaml_for_duplicates()` maintains a stack of seen keys per indent level. A deeply nested YAML with many keys at each level forces stack growth.

4. **Policy ID hashing**: `hash_policy_fields()` iterates all resources × relations × permissions, sorting each list first. For N items, this is O(N log N) per sort plus O(N) for hashing.

5. **Expression parsing**: Each permission expression is parsed into an AST via `RelationExpression::parse()`. Deeply nested parenthesized expressions cause recursive parsing.

### Attack Scenario

```bash
# Generate a 10MB policy YAML with 100,000 relations
python3 -c "
print('name: huge')
print('resources:')
for i in range(10000):
    print(f'  - name: resource_{i}')
    print('    relations:')
    for j in range(10):
        print(f'      - name: rel_{j}')
    print('    permissions:')
    for j in range(10):
        print(f'      - name: perm_{j}')
        print(f'        expr: rel_0 + rel_1')
" > huge_policy.yaml

curl -X POST http://node:9181/api/v0/acp/policy \
  -H 'Content-Type: application/yaml' \
  --data-binary @huge_policy.yaml
```

This creates:
- 10,000 resources × 10 relations × 10 permissions = 200,000 relations in the Zanzibar policy
- 200,000 relation objects stored in memory
- 100,000 expression parse operations
- Multiple O(N log N) sorts during policy ID generation

### HTTP Layer

The HTTP handler at `crates/http/src/handlers/acp.rs` passes the request body directly to the parser without size validation:

```rust
// crates/http/src/handlers/acp.rs:44
require_permission(&state, &identity, NodePermission::DacPolicyAdd).await?;
// ... body goes to parser without size check
```

Note: `DacPolicyAdd` NAC permission is required, limiting this to authorized users. But a compromised admin identity or a node without NAC enabled is vulnerable.

### Severity Rationale

LOW because:
1. Requires `DacPolicyAdd` permission (admin-level operation)
2. Axum has configurable body size limits (default varies)
3. The attack causes temporary resource exhaustion, not persistent compromise
4. Standard web server best practices (request size limits, timeouts) mitigate this

## Remediation

Add a size check before parsing:

```rust
const MAX_POLICY_YAML_SIZE: usize = 64 * 1024; // 64KB

pub fn parse_policy_yaml(yaml: &str) -> Result<ParsedPolicy, String> {
    if yaml.len() > MAX_POLICY_YAML_SIZE {
        return Err(format!("policy YAML too large: {} bytes (max {})", yaml.len(), MAX_POLICY_YAML_SIZE));
    }
    serde_yaml::from_str(yaml).map_err(|e| format!("invalid policy YAML: {}", e))
}
```

Additionally, limit the number of resources and permissions per policy after parsing.
