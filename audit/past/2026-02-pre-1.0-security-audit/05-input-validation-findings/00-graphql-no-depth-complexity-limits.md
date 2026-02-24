# Finding: GraphQL Parser Has No Depth or Complexity Limits

**Stream**: 05 - Input Validation
**Severity**: HIGH
**Category**: Denial of Service
**Status**: CONFIRMED (deepened in Session 1)

## Summary

The GraphQL parser accepts queries of arbitrary depth and complexity. While the query planner has a `MAX_NESTING_DEPTH = 10` check for join recursion, the parser itself has no limits. An attacker can craft queries with thousands of fields, deeply nested filters, or fragment-amplified selections that cause excessive CPU and memory consumption before the planner is ever reached.

## Affected Files

| File | Function | Issue |
|------|----------|-------|
| `crates/query/src/query_parse/parser.rs:127-194` | `parse_selection_to_selects()` | Recursive descent, no depth counter |
| `crates/query/src/query_parse/parser.rs:689-858` | `parse_selection_set()` | Recursive via nested fields, no depth counter |
| `crates/query/src/query_parse/filters.rs:17-46` | `parse_filter_value()` | Delegates to `graphql_value_to_json()` which recurses on nested objects |
| `crates/query/src/query_parse/ordering.rs:120-195` | `parse_order_condition()` | Recursive for nested relation ordering, no depth limit |
| `crates/query/src/query_parse/mutations.rs:303-313` | `parse_document_input()` | Delegates to `graphql_value_to_json()` for nested objects |
| `crates/http/src/handlers/graphql/query.rs:96-111` | `graphql()` | No query size validation before parse |
| `crates/http/src/handlers/graphql/query.rs:128-162` | `graphql_get()` | GET query param also unbounded |
| `crates/query/src/query_parse/parser.rs:311-318` | `parse_request_with_variables()` | Entry point calls `graphql_parser::parse_query()` with no size guard |

## Details

### Parser Layer: No Depth Limit

The parser uses `graphql_parser` crate (v0.4) which parses into an AST without depth limits. Our `parse_selection_set()` then recursively processes nested selections without tracking depth:

```rust
// parser.rs:737-739 — recursive nesting with no depth counter
} else if !field.selection_set.items.is_empty() {
    let nested = parse_field_to_select(field, variables, fragments, visiting)?;
    // ...
```

A query like `{ User { friends { friends { friends { ... 1000 levels ... } } } } }` will be fully parsed into an AST, fully converted into nested `Select` structures, and only then hit the planner's depth check. The parser allocates `O(depth)` stack frames and `O(depth)` heap-allocated `Select` structures before any limit is checked.

### Planner Layer: MAX_NESTING_DEPTH = 10 (Partial Mitigation)

The planner's `apply_joins()` (in `crates/query/src/planner/joins/mod.rs:73`) checks:

```rust
if depth > MAX_NESTING_DEPTH {
    return Err(QueryError::execution(format!(
        "Query nesting depth {} exceeds maximum allowed depth of {}...",
        depth, MAX_NESTING_DEPTH
    )));
}
```

This prevents unbounded join execution but does NOT prevent:
- Parser-level OOM from deeply nested AST allocation
- Width bombs (thousands of fields at the same level)
- Filter recursion (see finding 02)
- Fragment amplification (see finding 04)

### Width Bomb Vector

No limit on the number of fields per selection level. A query requesting 10,000 fields:

```graphql
{ User { f1 f2 f3 ... f10000 } }
```

Each field generates a `Requestable::Field`, a `DocumentMapping` entry, and mapping indices. With 10,000 fields across 10 levels, this is 100,000 allocations from a single request.

### Ordering Recursion

`parse_order_condition()` in `ordering.rs:163-191` recurses for nested relation ordering:

```rust
Value::Object(nested_obj) => {
    // Recursively parse the nested object
    let nested_condition = parse_order_condition(nested_field.clone(), nested_direction, variables)?;
    // ...
}
```

No depth limit. An attacker can craft `order: {a: {b: {c: {d: ...}}}}` to arbitrary depth.

### graphql_parser Crate (v0.4) Limits

The `graphql_parser` crate has no built-in depth, width, or size limits. It will parse any syntactically valid GraphQL document regardless of complexity. The crate's `parse_query()` function uses Rust's default stack size (~8MB on most platforms), so extremely deep nesting (~10,000+ levels) would cause a stack overflow panic rather than a graceful error.

### Attack Vectors

1. **Depth bomb**: Deeply nested selection sets causing recursive allocation and eventual planner rejection (wasted CPU/memory up to that point)
2. **Width bomb**: Query with thousands of fields/aliases at the same level (bypasses depth check entirely)
3. **Ordering depth bomb**: Nested `order` argument recursion
4. **Combined**: Width * depth amplification (100 fields at each of 10 nesting levels = 10^20 theoretical combinations)

## Impact

A single malicious HTTP request could:
- Exhaust server memory (OOM kill) via width bombs that bypass the planner depth check
- Pin CPU for extended duration during parsing/planning
- Crash the process via stack overflow from graphql_parser on ~10,000+ nesting levels
- Block other queries from executing during planning

This is exploitable by anyone with network access to the HTTP API (no authentication required for queries on public collections).

## Remediation

### Immediate (query parser)

Add depth and width tracking to the parser entry point:

```rust
pub struct QueryLimits {
    pub max_depth: usize,           // e.g., 20
    pub max_fields_per_level: usize, // e.g., 200
    pub max_total_fields: usize,     // e.g., 1000
    pub max_aliases: usize,          // e.g., 100
    pub max_query_bytes: usize,      // e.g., 256KB
}
```

Add `depth` parameter to `parse_selection_set()` and `parse_selection_to_selects()`, incrementing on each recursive call.

### Immediate (HTTP layer)

Add explicit body size limit and query string length check before calling `graphql_parser::parse_query()`:

```rust
if query.len() > MAX_QUERY_SIZE {
    return Err(QueryError::parse("query exceeds maximum size"));
}
```

### Reference

Go DefraDB should be checked for equivalent limits to maintain parity. The planner's `MAX_NESTING_DEPTH = 10` check in the Rust implementation is a good partial mitigation that Go may lack.

## Test Gap

No tests for:
- Queries exceeding planner depth limit (verify the error path works)
- Width bomb queries (thousands of fields)
- Stack overflow from graphql_parser on extreme depth
- Combined depth + width queries
