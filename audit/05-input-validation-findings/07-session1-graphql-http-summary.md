# Session 1 Summary: GraphQL Parser & HTTP Body Handling

**Stream**: 05 - Input Validation
**Session**: 1 of 4
**Date**: 2026-02-21

## Scope

Deep-dive into the GraphQL query parser and HTTP request body handling, looking for depth bombs, width bombs, fragment explosion, unbounded recursion, and missing body size limits.

## Files Audited

| File | Lines | Focus |
|------|-------|-------|
| `crates/query/src/query_parse/parser.rs` | 1167 | Full read — recursive descent, fragment handling |
| `crates/query/src/query_parse/filters.rs` | 62 | Full read — filter parsing |
| `crates/query/src/query_parse/mutations.rs` | 421 | Full read — mutation input parsing |
| `crates/query/src/query_parse/aggregates.rs` | 389 | Full read — aggregate parsing |
| `crates/query/src/query_parse/ordering.rs` | 196 | Full read — order by recursion |
| `crates/query/src/query_parse/mod.rs` | 23 | Full read — module exports |
| `crates/query/src/sdl_parse/parser.rs` | 289 | Full read — SDL parsing |
| `crates/http/src/server.rs` | 508 | Full read — router/middleware stack |
| `crates/http/src/handlers/graphql/query.rs` | 354 | Full read — GraphQL handlers + SSE |
| `crates/http/src/handlers/backup.rs` | 341 | Full read — backup import limit |
| `crates/http/src/handlers/schema.rs` | 48 | Full read — schema handler |
| `crates/http/src/handlers/collections.rs` | 498 | Full read — collection handlers |
| `crates/http/src/handlers/documents.rs` | 216 | Full read — document CRUD |
| `crates/http/src/router/routes.rs` | 259 | Full read — all route definitions |
| `crates/query/src/planner/builder/mod.rs` | 543 | Full read — planner, MAX_NESTING_DEPTH |
| `crates/query/src/planner/joins/mod.rs` | 120 | Partial — depth check |
| `crates/query/src/mapper/filter/filter_impl.rs` | 100 | Partial — eval_conditions recursion |
| `crates/query/src/mapper/filter/json_match.rs` | 50 | Partial — scalar filter recursion |

## Findings

| # | Title | Severity | Status |
|---|-------|----------|--------|
| 00 | GraphQL Parser Has No Depth or Complexity Limits | HIGH | CONFIRMED (updated) |
| 01 | No Explicit HTTP Request Body Size Limit | HIGH | CONFIRMED (updated) |
| 02 | Filter Logical Operators Allow Unbounded Recursion | MEDIUM | NEW |
| 03 | SDL Schema Endpoint Accepts Unbounded Input | MEDIUM | NEW |
| 04 | Fragment Width Amplification (Non-Cyclic) | LOW | NEW |
| 05 | No Query Timeout or Cost Budget | MEDIUM | NEW |
| 06 | SSE Subscription Has No Connection or Resource Limits | MEDIUM | NEW |

## Key Observations

### Positive Findings (Things That Work)

1. **Fragment cycle detection is correct**: The `visiting` HashSet in `parse_selection_to_selects()` and `parse_selection_set()` properly detects circular fragment references (A -> B -> A). The remove-after-processing pattern correctly allows the same fragment in sibling positions.

2. **Planner nesting depth check exists**: `MAX_NESTING_DEPTH = 10` in the planner's join logic prevents unbounded join recursion. This is a good defense-in-depth measure.

3. **Backup import has explicit size check**: The 100MB `MAX_IMPORT_SIZE` constant is the only explicit body size limit, demonstrating the pattern that should be applied elsewhere.

4. **WebSocket handler returns 501**: The unimplemented WebSocket handler avoids accidentally exposing an unbounded WebSocket endpoint.

5. **Aggregates do not nest**: Aggregate fields (`_count`, `_sum`, etc.) parse their arguments but do not recurse into sub-aggregates. The aggregate parser is flat.

6. **Grouping is flat**: The group-by parser (`parse_group_by_value()`) accepts a simple list of field names. No recursion.

7. **Ordering has bounded practical depth**: While `parse_order_condition()` recurses for nested relation ordering, it requires exactly one field per nested object, limiting the recursion to the depth of the relation chain.

### Defense-in-Depth Gaps

The findings form a layered attack surface:

```
Layer 1: HTTP body      → No size limit (finding 01)
Layer 2: Parser         → No depth/width limit (finding 00)
Layer 3: Filter eval    → No recursion limit (finding 02)
Layer 4: Planner        → MAX_NESTING_DEPTH = 10 (partial)
Layer 5: Execution      → No timeout or cost budget (finding 05)
Layer 6: Connection     → No concurrent limit (finding 06)
```

A robust defense needs limits at every layer.

### Priority Remediation Order

1. **Add DefaultBodyLimit to router** (finding 01) — lowest effort, highest impact
2. **Add query timeout** (finding 05) — prevents any query from running forever
3. **Add depth counter to parser** (finding 00) — catches depth bombs before planning
4. **Add filter recursion limit** (finding 02) — prevents filter-specific DoS
5. **Add SSE connection limits** (finding 06) — prevents subscription amplification
6. **Add SDL size/type limits** (finding 03) — prevents schema endpoint abuse

## Remaining Work for Sessions 2-4

- **Session 2**: GraphQL mutation input validation, document field validation, type coercion edge cases
- **Session 3**: Variable injection, operator injection, CID/docID format validation
- **Session 4**: Cross-cutting concerns — error message information leakage, timing side channels, Unicode normalization
