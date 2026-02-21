# Finding: GraphQL Parser Has No Depth or Complexity Limits

**Stream**: 05 - Input Validation
**Severity**: HIGH
**Category**: Denial of Service
**Status**: CONFIRMED

## Summary

The GraphQL parser accepts queries of arbitrary depth and complexity. An attacker can craft a deeply nested query or a query with thousands of fields that causes excessive CPU and memory consumption, potentially taking down the node.

## Affected Files

| File | Function | Issue |
|------|----------|-------|
| `crates/query/src/query_parse/parser.rs` | `parse_selection_set()` (recursive) | No recursion depth limit |
| `crates/http/src/handlers/graphql/query.rs:96-111` | `graphql()` | No query size validation before parse |
| `crates/http/src/handlers/graphql/query.rs:128-162` | `graphql_get()` | GET query param also unbounded |

## Details

### No Depth Limit

The parser uses `graphql_parser` crate which parses into an AST without depth limits. Our `parse_selection_set()` then recursively processes nested selections. A query like:

```graphql
{ User { friends { friends { friends { friends { ... 100 levels deep ... } } } } } }
```

Will be fully parsed and then planned into a deeply nested query execution plan.

Search for `max_depth`, `query_depth`, `depth_limit`, `complexity_limit`, or `max_complexity` across the entire query crate returns **zero results**.

### No Complexity Limit

There is no check on:
- Number of fields per selection set
- Total number of fields across the query
- Number of aliases (each alias executes independently)
- Number of fragments (circular detection exists, but flat explosion doesn't)

### No Query Size Limit

The HTTP handler accepts the query via `Json<QueryRequest>` (Axum's JSON extractor). Axum's default body limit is 2MB, but this is not explicitly configured and could change with framework updates. A 2MB GraphQL query string is enormous and could contain millions of nested fields.

### Attack Vectors

1. **Depth bomb**: Deeply nested selection sets causing recursive execution
2. **Width bomb**: Query with thousands of fields/aliases at the same level
3. **Fragment explosion**: Multiple fragments that expand to large selection sets
4. **Introspection abuse**: `__schema` queries that enumerate all types (mitigated: introspection handled separately)

## Impact

A single malicious HTTP request could:
- Exhaust server memory (OOM kill)
- Pin CPU at 100% for extended duration
- Block other queries from executing (if single-threaded query planning)
- Crash the node process

This is exploitable by anyone with network access to the HTTP API (no authentication required for queries on public collections).

## Remediation

### Immediate (query crate)

Add configurable limits to the query parser:

```rust
pub struct QueryLimits {
    pub max_depth: usize,        // e.g., 20
    pub max_fields: usize,       // e.g., 1000
    pub max_aliases: usize,      // e.g., 100
}
```

Track depth during recursive `parse_selection_set()` and reject queries exceeding limits.

### Immediate (HTTP layer)

Add explicit body size limit to the Axum router:

```rust
use axum::extract::DefaultBodyLimit;

router.layer(DefaultBodyLimit::max(1_048_576)) // 1MB
```

### Reference

Go DefraDB should be checked for equivalent limits to maintain parity. If Go also lacks limits, this should be flagged as a shared vulnerability.
