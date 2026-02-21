# Finding: No Query Timeout or Cost Budget

**Stream**: 05 - Input Validation
**Severity**: MEDIUM
**Category**: Denial of Service
**Status**: CONFIRMED

## Summary

There is no per-query timeout, cost estimation, or resource budget. Once a query passes parsing and planning, it executes until completion with no upper bound on time or resources consumed. The planner's `MAX_NESTING_DEPTH = 10` prevents unbounded join recursion, but a query within that depth limit can still trigger expensive operations (full collection scans, cartesian joins, etc.) with no abort mechanism. There is also no concurrent query limit or per-IP rate limiting.

## Affected Files

| File | Function | Issue |
|------|----------|-------|
| `crates/query/src/planner/builder/mod.rs:178-536` | `plan_with_index_info()` | No cost estimation or budget |
| `crates/query/src/planner/joins/mod.rs:59-79` | `apply_joins()` | Depth check only, no cost check |
| `crates/http/src/handlers/graphql/query.rs:96-111` | `graphql()` | No query timeout |
| `crates/http/src/handlers/graphql/query.rs:199-262` | `graphql_transactional()` | No query timeout |
| `crates/http/src/server.rs:471-501` | `Server::run()` | No connection limits |

## Details

### No Query Timeout

The GraphQL handler calls the executor and awaits the result with no timeout:

```rust
// query.rs:106
let response = execute_with_context(&state, &identity, request).await;
// ^^^ No tokio::time::timeout wrapper — runs until completion
```

A query that triggers a full table scan of a large collection with complex filter evaluation will run for as long as it takes, blocking the tokio worker thread for that duration.

### No Cost Estimation in Planner

The planner builds execution plans without any cost model. It does not estimate:
- Number of documents to scan
- Number of join iterations (nested loop joins are O(n*m))
- Filter evaluation cost per document
- Aggregate computation cost

### Planner Depth Check: Necessary but Insufficient

The `MAX_NESTING_DEPTH = 10` check prevents unbounded recursion in join planning:

```rust
// joins/mod.rs:73-79
if depth > MAX_NESTING_DEPTH {
    return Err(QueryError::execution(format!(
        "Query nesting depth {} exceeds maximum allowed depth of {}...",
        depth, MAX_NESTING_DEPTH
    )));
}
```

However, within 10 levels of nesting, a query can be extremely expensive:
- Each level is a nested loop join (no hash join optimization)
- 10 levels with 1,000 documents each = 1,000^10 potential join iterations
- Filter evaluation happens at each level

### No Concurrent Query Limits

There are no limits on:
- Total concurrent queries across the server
- Per-IP concurrent queries
- Per-identity concurrent queries
- Request rate limiting

An attacker can open many connections and send moderately expensive queries to all of them simultaneously, exhausting the tokio thread pool.

### No Per-IP Rate Limiting

The HTTP server (Axum with tokio) has no rate limiting middleware:

```rust
// server.rs:403 — only TraceLayer + CorsLayer
Ok(router.layer(TraceLayer::new_for_http()).layer(cors))
```

No `tower::limit::RateLimit`, `tower::limit::ConcurrencyLimit`, or equivalent.

### Amplification via Aggregates

Aggregate queries can be expensive: `_count`, `_sum`, `_avg` over large result sets require scanning all matching documents. Combined with relation aggregates (e.g., `_count(books: {})`), a single query can trigger multiple full collection scans.

## Impact

- **Query-level DoS**: A single expensive query can block a tokio worker for minutes
- **Concurrent query flood**: Many simultaneous moderately-expensive queries can exhaust the thread pool
- **No automatic recovery**: Long-running queries cannot be cancelled or timed out
- **Resource starvation**: Other legitimate queries starved of CPU/memory while expensive query runs

## Remediation

### Immediate: Add Query Timeout

Wrap query execution with a timeout:

```rust
use tokio::time::{timeout, Duration};

let result = timeout(Duration::from_secs(30), execute_with_context(&state, &identity, request)).await;
match result {
    Ok(response) => Ok(Json(response)),
    Err(_) => Err(HttpError::GatewayTimeout("query execution timed out".into())),
}
```

### Add Concurrency Limit

Use tower's ConcurrencyLimit layer:

```rust
use tower::limit::ConcurrencyLimitLayer;

router.layer(ConcurrencyLimitLayer::new(100))  // Max 100 concurrent requests
```

### Future: Cost-Based Query Budget

Add cost estimation to the planner that considers:
- Estimated scan size (based on collection stats)
- Join fan-out estimation
- Filter complexity
- Aggregate computation

Reject queries exceeding a configurable cost threshold.

## Test Gap

No tests for:
- Query timeout behavior
- Concurrent query limits
- Long-running query cancellation
- Resource exhaustion from parallel queries
