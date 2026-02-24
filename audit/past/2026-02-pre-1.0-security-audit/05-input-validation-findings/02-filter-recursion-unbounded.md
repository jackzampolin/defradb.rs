# Finding: Filter Logical Operators Allow Unbounded Recursion

**Stream**: 05 - Input Validation
**Severity**: MEDIUM
**Category**: Denial of Service
**Status**: CONFIRMED

## Summary

The `_and`, `_or`, and `_not` filter operators can nest to arbitrary depth with no recursion limit. This applies in both the parser (where filters are converted to JSON) and the evaluator (where filters are matched against documents). An attacker can craft a filter with thousands of nesting levels to cause stack overflow or excessive CPU usage during query evaluation.

## Affected Files

| File | Function | Issue |
|------|----------|-------|
| `crates/query/src/mapper/filter/filter_impl.rs:63-100` | `eval_conditions()` | Recursive evaluation of `_and`/`_or`/`_not`, no depth counter |
| `crates/query/src/mapper/filter/json_match.rs:19-50` | `matches_scalar_value()` | Recursive for `_and`/`_or`, no depth counter |
| `crates/query/src/mapper/filter/eval_relation.rs:28-61` | Relation filter eval | Recursive `_and`/`_or`/`_not` evaluation |
| `crates/query/src/query_parse/parser.rs:529-553` | `parse_field_to_select()` filter parsing | No validation on filter nesting depth |

## Details

### Parser: Filter Accepted As-Is

Filters are parsed by `parse_filter_value()` in `filters.rs`, which calls `graphql_value_to_json()` to convert the entire filter tree to JSON. There is no depth check during this conversion. The parser validates that `_and`/`_or` arrays don't contain null elements (lines 536-548 in parser.rs) but does not check nesting depth.

### Evaluator: Unbounded Recursion

The filter evaluator in `filter_impl.rs` recursively processes logical operators:

```rust
// filter_impl.rs:73-89
FilterOp::And => {
    let arr = value.as_array()
        .ok_or_else(|| QueryError::invalid_filter("_and requires array"))?;
    for item in arr {
        let sub_conditions: HashMap<String, JsonValue> =
            serde_json::from_value(item.clone())
                .map_err(|e| QueryError::invalid_filter(e.to_string()))?;
        if !self.eval_conditions(&sub_conditions, fields, mapping)? {
            //     ^^^^^^^^^^^^^^^^ recursive call, no depth tracking
            return Ok(false);
        }
    }
}
```

Each `_and`/`_or`/`_not` can contain another `_and`/`_or`/`_not`, leading to unbounded recursion. The same pattern appears in `matches_scalar_value()` (json_match.rs) and relation filter evaluation (eval_relation.rs).

### Width Amplification

Each `_and`/`_or` accepts an **array** of sub-conditions. A filter like:

```graphql
filter: {_and: [{_or: [{_and: [...10 items...]}, ..., {_and: [...10 items...]}]}, ...]}
```

With branching factor 10 and depth 10, this creates 10^10 (10 billion) leaf conditions to evaluate. Each leaf involves a HashMap deserialization (`serde_json::from_value`), making this a CPU exhaustion vector.

### Attack Payload

```graphql
query {
  User(filter: {
    _and: [{
      _or: [{
        _and: [{
          _or: [{
            # ... nest to 500+ levels
            name: {_eq: "x"}
          }]
        }]
      }]
    }]
  }) { name }
}
```

At 500 nesting levels, each level pushes a stack frame for `eval_conditions()` plus a `HashMap` allocation from `serde_json::from_value()`. This is likely to cause a stack overflow before the default 8MB stack is exhausted.

### Difference from Selection Depth

The planner's `MAX_NESTING_DEPTH = 10` check in `joins/mod.rs` only limits **selection set nesting** (joins). Filters are evaluated separately and are not subject to this limit. A flat query (`{ User(filter: ...) { name } }`) with depth-1 selection but depth-500 filter nesting will pass the planner depth check and hit the evaluator's unbounded recursion.

## Impact

- **Stack overflow**: ~500+ levels of filter nesting causes process crash
- **CPU exhaustion**: Width * depth amplification in filter evaluation (10^N leaf evaluations)
- **Per-document cost**: Filters are evaluated per document, so even a small result set pays the full evaluation cost on every scanned document
- **Bypasses planner depth check**: Filter recursion is orthogonal to selection nesting

## Remediation

### Add Depth Limit to Filter Evaluation

Add a `depth` parameter to `eval_conditions()` and `matches_scalar_value()`:

```rust
const MAX_FILTER_DEPTH: usize = 20;

fn eval_conditions(
    &self,
    conditions: &HashMap<String, JsonValue>,
    fields: &[Option<JsonValue>],
    mapping: &DocumentMapping,
    depth: usize,
) -> Result<bool> {
    if depth > MAX_FILTER_DEPTH {
        return Err(QueryError::invalid_filter("filter nesting depth exceeds maximum"));
    }
    // ... existing code, passing depth + 1 to recursive calls
}
```

### Add Width Limit to Filter Parsing

Limit the number of conditions in `_and`/`_or` arrays:

```rust
const MAX_FILTER_ARRAY_SIZE: usize = 50;

if arr.len() > MAX_FILTER_ARRAY_SIZE {
    return Err(QueryError::invalid_filter("_and/_or array too large"));
}
```

## Test Gap

No tests for:
- Deeply nested `_and`/`_or`/`_not` filters (even 5 levels)
- Width amplification in filters (large arrays in `_and`/`_or`)
- Combined depth + width filter stress tests
- Stack overflow recovery from malicious filters
