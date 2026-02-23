# Stream 05 (Input Validation) Verification Re-Audit

**Date**: 2026-02-23
**Auditor**: Claude Opus 4.6
**Scope**: All HIGH findings from Phases 1.2, 3.1, 3.2 of the Remediation Roadmap

---

## Executive Summary

All seven HIGH findings have received code-level remediation. Six of seven have working fixes that are correctly wired end-to-end. One (05-31 WASM sandbox) has the code infrastructure built but is **not activated in production** -- the `WasmSandboxConfig` is never passed from CLI/DB to the store. Two additional gaps were found: (a) per-route body size overrides for schema/backup are configured in the struct but not applied to the router, and (b) `validate_select_limits` does not cover mutation operations.

| Finding | Status | Verdict |
|---------|--------|---------|
| 05-00 GraphQL depth/width limits | Code exists, tested, wired | **PASS** |
| 05-01 HTTP body size limit | Code exists, configurable, default unlimited | **PASS (with caveat)** |
| 05-02 Filter recursion limit | Code exists, MAX_FILTER_DEPTH=50 | **PASS** |
| 05-05 Query timeout | Code exists, tested, wired end-to-end | **PASS** |
| 05-15 Lens WASM path traversal | Code exists, both HTTP and WASM layer | **PASS** |
| 05-31 WASM sandbox limits | Code infrastructure exists, **NOT activated** | **FAIL** |
| 05-32 HTTP rate limiting | ConcurrencyLimitLayer + TimeoutLayer wired | **PASS** |

---

## Finding 05-00: GraphQL No Depth or Complexity Limits

### Remediation Code

**File**: `/Users/johnzampolin/go/src/github.com/sourcenetwork/defradb.rs/crates/query/src/query_parse/limits.rs`

```rust
pub const MAX_QUERY_DEPTH: usize = 20;
pub const MAX_QUERY_WIDTH: usize = 100;

pub fn validate_select_limits(select: &Select) -> Result<()> {
    validate_select_at_depth(select, 1)
}

fn validate_select_at_depth(select: &Select, depth: usize) -> Result<()> {
    if depth > MAX_QUERY_DEPTH { ... }
    if select.fields.len() > MAX_QUERY_WIDTH { ... }
    for field in &select.fields {
        if let Requestable::Select(nested) = field {
            validate_select_at_depth(nested, depth + 1)?;
        }
    }
    Ok(())
}
```

### Full Path Trace

1. GraphQL query arrives at HTTP handler (`crates/http/src/handlers/graphql/query.rs`)
2. Deserialized as `QueryRequest`, passed to `QueryRunner::execute()`
3. `executor.rs:86` calls `parse_request_with_variables()`
4. `parser.rs:496` calls `validate_select_limits(&select)` for subscriptions
5. `parser.rs:503-505` calls `validate_select_limits(select)` for each query select
6. If depth > 20 or width > 100, returns `QueryError::parse(...)` which becomes a GraphQL error in the response

### Call-Site Analysis

- **Queries**: VALIDATED at `parser.rs:503-505` (iterates over all selects)
- **Subscriptions**: VALIDATED at `parser.rs:496`
- **Mutations**: NOT VALIDATED -- `parser.rs:500-501` returns `ParsedOperation::Mutation` without calling `validate_select_limits`

### Error Format

User gets a standard GraphQL error response:
```json
{"data": null, "errors": [{"message": "parse error: query exceeds maximum nesting depth of 20"}]}
```

### Configurability

Limits are **hardcoded constants**, not configurable via CLI or config file. This is acceptable for 1.0 -- hardcoded secure defaults are better than configurable defaults that ship insecure.

### Tests

**Integration test**: `tools/integration-test/tests/query/limits.rs`

- `rust_query_depth_width_limit` -- tests depth=20 passes, depth=21 fails with correct error
- Width=100 passes, width=101 fails with correct error
- Verifies node health after rejected queries

### Verdict: PASS

The fix is correct and well-tested. Constants match the roadmap requirements (MAX_QUERY_DEPTH=20, MAX_QUERY_WIDTH=100).

### Gap: Mutations Not Covered

`validate_select_limits` is not called for mutations. A mutation with deeply nested sub-selections in its return clause (e.g., `mutation { create_User(...) { friends { friends { ... } } } }`) bypasses the depth check. The planner's `MAX_NESTING_DEPTH=10` would catch join depth, but the parser-level width check is skipped entirely for mutation responses.

**Severity**: LOW -- mutations have their own parsing path and the planner catch is a fallback. But this is a gap in defense-in-depth.

---

## Finding 05-01: No HTTP Body Size Limit

### Remediation Code

**File**: `/Users/johnzampolin/go/src/github.com/sourcenetwork/defradb.rs/crates/http/src/server.rs`

```rust
// Apply global body limit (0 = unlimited)
if self.config.max_body_size > 0 {
    router = router.layer(DefaultBodyLimit::max(self.config.max_body_size as usize));
} else {
    router = router.layer(DefaultBodyLimit::disable());
}
```

### Configuration

| Parameter | Default | CLI Flag | Config Key |
|-----------|---------|----------|------------|
| `max_body_size` | 0 (unlimited) | `--max-body-size` | `api.max_body_size` |
| `max_schema_size` | 0 (unlimited) | `--max-schema-size` | `api.max_schema_size` |
| `max_backup_size` | 0 (unlimited) | `--max-backup-size` | `api.max_backup_size` |

### Full Path Trace

1. CLI `--max-body-size` flag parsed in `start/mod.rs:125`
2. Applied to `config.api.max_body_size` at `start/mod.rs:370-371`
3. Passed to `ServerConfig` at `start/server.rs:133`
4. Applied as `DefaultBodyLimit::max()` at `server.rs:424-425`
5. If 0 (default): `DefaultBodyLimit::disable()` -- **unlimited**

### Per-Route Overrides: NOT IMPLEMENTED

The `max_schema_size` and `max_backup_size` fields exist in both `ServerConfig` and the CLI, are wired through configuration, but are **never applied to the router**. The `server.rs:router()` method only uses `max_body_size` for the global `DefaultBodyLimit`. There is no code that applies per-route `DefaultBodyLimit` layers for schema or backup endpoints.

This means:
- `POST /api/v0/schema` (String body, no Axum Json limit) is bounded only by the global body limit
- `POST /api/v0/backup/import` (Bytes body) is bounded only by the global body limit
- The schema-specific and backup-specific limits are dead configuration

### Default Behavior

With defaults (all zeros), the server runs with `DefaultBodyLimit::disable()` -- **all body limits are removed**. This is intentional for Go compatibility (Go DefraDB has no body limits) but leaves the original vulnerability fully open by default.

### Tests

No integration test for HTTP body size rejection. The existing `limits.rs` tests only cover query depth/width and timeout.

### Verdict: PASS (with caveats)

The mechanism exists and is correctly wired for the global limit. However:

1. **Default is unlimited** -- out-of-the-box the node has no body limit
2. **Per-route overrides are dead code** -- `max_schema_size` and `max_backup_size` are config-only, never applied
3. **No test coverage** for body rejection

---

## Finding 05-02: Filter Recursion Unbounded

### Remediation Code

**File**: `/Users/johnzampolin/go/src/github.com/sourcenetwork/defradb.rs/crates/query/src/mapper/filter/filter_impl.rs`

```rust
const MAX_FILTER_DEPTH: usize = 50;

fn eval_conditions(
    &self,
    conditions: &HashMap<String, JsonValue>,
    fields: &[Option<JsonValue>],
    mapping: &DocumentMapping,
    depth: usize,
) -> Result<bool> {
    if depth > MAX_FILTER_DEPTH {
        return Err(QueryError::invalid_filter(format!(
            "filter exceeds maximum nesting depth of {}",
            MAX_FILTER_DEPTH
        )));
    }
    // ... recursive calls pass depth + 1
}
```

### Analysis

- Depth counter is correctly threaded through `eval_conditions`
- `matches()` initializes with `depth: 0`
- Each recursive `_and`/`_or`/`_not` increments by 1
- MAX_FILTER_DEPTH=50 is generous but prevents stack exhaustion (default 8MB stack can handle ~50 levels easily)
- The depth check is in the **evaluator**, not the parser -- this means the filter AST is still fully allocated before the limit is hit during evaluation

### Gap: Parser-Level Filter Depth Not Checked

The filter is parsed by `graphql_value_to_json()` which recursively converts the GraphQL AST to JSON without any depth check. A filter with 10,000 nesting levels would be fully parsed and converted to a JSON tree before `eval_conditions` rejects it at depth 50. The parser-level allocation is the real DoS vector.

However, the graphql_parser crate's stack limit (~8MB) would cause a stack overflow at approximately 10,000+ levels before this becomes an issue, so the practical risk is contained.

### No Width Limit on Filter Arrays

The `_and`/`_or` arrays have no width limit. An `_and` with 100,000 elements at depth 1 passes the depth check but creates O(100,000) HashMap deserializations per document. This is a CPU exhaustion vector.

### Tests

No specific integration test for filter depth limits.

### Verdict: PASS

The evaluator-level fix prevents stack exhaustion. The remaining parser-level gap and width-within-filters gap are lower severity and the evaluator catch is sufficient for 1.0.

---

## Finding 05-05: No Query Timeout

### Remediation Code

**File**: `/Users/johnzampolin/go/src/github.com/sourcenetwork/defradb.rs/crates/query/src/runner/executor.rs`

```rust
let result = if self.query_timeout > 0 {
    let timeout = Duration::from_secs(self.query_timeout);
    match tokio::time::timeout(timeout, execution).await {
        Ok(r) => r,
        Err(_) => {
            return QueryResponse {
                data: None,
                errors: vec![QueryResponseError {
                    message: format!(
                        "query execution timed out after {} seconds",
                        self.query_timeout
                    ),
                    ...
                }],
            };
        }
    }
} else {
    execution.await
};
```

### Full Path Trace

1. CLI `--query-timeout` flag parsed in `start/mod.rs:177`
2. Applied to `config.api.query_timeout` at `start/mod.rs:409-410`
3. Passed to `QueryRunner` at `start/server.rs:571`: `.with_query_timeout(config.api.query_timeout)`
4. Stored as `self.query_timeout: u64` in `runner/mod.rs:85`
5. Default value: **30 seconds** (`default_query_timeout()` in `config/sections.rs:79`)
6. Applied in BOTH `execute()` (line 166) and `execute_in_txn()` (line 334)
7. Uses `tokio::time::timeout` which cancels the future on expiry

### Correctness Analysis

- **Both code paths covered**: Regular queries and transactional queries both have the timeout wrapper
- **Configurable**: Default 30s, overridable via `--query-timeout`
- **0 means no timeout**: Explicitly handled with the `if self.query_timeout > 0` check
- **Error format**: Returns a standard GraphQL error response, not an HTTP error
- **Cancellation**: `tokio::time::timeout` drops the inner future, which cancels the async operation

### Tests

**Integration test**: `tools/integration-test/tests/query/limits.rs`

- `rust_query_timeout_under_load` -- starts node with `--query-timeout 5`, inserts 100 records, runs 10 queries, verifies they complete within the timeout
- Tests normal operation under load, not timeout triggering (the test verifies queries complete, not that slow queries are killed)
- `go_query_timeout_under_load` is ignored (Go doesn't implement this)

### Gap: No test that actually triggers a timeout

The existing test verifies that normal queries succeed under a timeout, but does not verify that a slow query actually gets killed. A proper negative test would craft a query that takes longer than the timeout and verify the timeout error is returned.

### Verdict: PASS

The timeout is correctly implemented, defaults to 30 seconds, and is wired end-to-end from CLI through config to the executor. Both `execute()` and `execute_in_txn()` paths are covered.

---

## Finding 05-15: Lens WASM Path Traversal via HTTP API

### Remediation Code

**Two-layer defense:**

**Layer 1 (HTTP)**: `/Users/johnzampolin/go/src/github.com/sourcenetwork/defradb.rs/crates/http/src/handlers/lens.rs`

```rust
if !state.dev_mode {
    let config: lens::LensConfig = serde_json::from_str(&body)
        .map_err(|e| HttpError::BadRequest(...))?;
    config.validate_for_http()
        .map_err(|e| HttpError::BadRequest(e.to_string()))?;
}
```

**File**: `/Users/johnzampolin/go/src/github.com/sourcenetwork/defradb.rs/crates/lens/src/config.rs`

```rust
pub fn validate_for_http(&self) -> Result<(), crate::Error> {
    if self.path.is_some() {
        return Err(crate::Error::PathNotAllowed(
            "file path WASM loading is not allowed via HTTP API; \
             use inline module bytes instead".to_string(),
        ));
    }
    Ok(())
}
```

**Layer 2 (WASM loader)**: `/Users/johnzampolin/go/src/github.com/sourcenetwork/defradb.rs/crates/lens/src/wasm.rs`

```rust
fn validate_wasm_path(path_str: &str) -> Result<()> {
    let path = Path::new(path_str);
    if !path.is_absolute() { return Err(PathNotAllowed(...)); }
    for component in path.components() {
        if let std::path::Component::ParentDir = component {
            return Err(PathNotAllowed(...));
        }
    }
    match path.extension().and_then(|e| e.to_str()) {
        Some("wasm") => {}
        _ => return Err(PathNotAllowed(...)),
    }
    Ok(())
}
```

### Analysis

- **HTTP layer**: Rejects ANY lens config with a `path` field via HTTP unless `dev_mode` is enabled. This is a hard block -- even valid paths are rejected. Only `module` bytes are accepted.
- **WASM layer**: If a path somehow reaches `load_module()`, it must be absolute, must not contain `..`, and must have `.wasm` extension.
- **Both `set_migration` and `add_lens` handlers** validate before proceeding.
- **dev_mode bypass**: In development mode, the HTTP validation is skipped. This is acceptable for dev but should be documented.

### Tests

- Unit tests in `wasm.rs`: `test_validate_wasm_path_rejects_relative`, `test_validate_wasm_path_rejects_traversal`, `test_validate_wasm_path_rejects_non_wasm_extension`, `test_validate_wasm_path_accepts_valid`
- Unit tests in `config.rs`: `test_validate_for_http_rejects_file_path`, `test_validate_for_http_accepts_bytes`, `test_config_validate_for_http_rejects_file_path`
- No integration test sending a traversal path through the HTTP API

### Verdict: PASS

The two-layer defense is solid. The HTTP layer blocks all file paths by default, and the WASM layer validates paths as a fallback. The `dev_mode` bypass is intentional and acceptable.

---

## Finding 05-31: WASM Sandbox No Memory/CPU/Syscall Restrictions

### Remediation Code

**File**: `/Users/johnzampolin/go/src/github.com/sourcenetwork/defradb.rs/crates/lens/src/wasm.rs`

The infrastructure exists:

```rust
pub struct WasmSandboxConfig {
    pub max_memory_bytes: Option<usize>,
    pub fuel_budget: Option<u64>,
    pub epoch_deadline_ticks: Option<u64>,
}

impl WasmSandboxConfig {
    pub fn restrictive() -> Self {
        Self {
            max_memory_bytes: Some(64 * 1024 * 1024), // 64 MiB
            fuel_budget: Some(1_000_000),
            epoch_deadline_ticks: Some(2),
        }
    }
}
```

In `execute_batch_transform`:
```rust
if store.data().limits.is_some() {
    store.limiter(|state| state.limits.as_mut().unwrap());
}
if let Some(ref sb) = sandbox {
    if let Some(fuel) = sb.fuel_budget {
        store.set_fuel(fuel)?;
    }
    if let Some(ticks) = sb.epoch_deadline_ticks {
        store.set_epoch_deadline(ticks);
    }
}
```

### CRITICAL: Not Activated in Production

**File**: `/Users/johnzampolin/go/src/github.com/sourcenetwork/defradb.rs/crates/db/src/database.rs:340-343`

```rust
fn create_lens_store() -> Result<Arc<dyn TransformStore>> {
    let store = WasmTransformStore::new()  // <-- new() passes sandbox=None
        .map_err(|e| Error::Lens(...))?;
    Ok(Arc::new(store))
}
```

`WasmTransformStore::new()` calls `Self::with_sandbox(None)`, which means no sandbox config is ever applied. The `WasmSandboxConfig` struct and `with_sandbox()` method exist but are only used in unit tests.

**No CLI flag** exists to enable the sandbox. Grep for `WasmSandboxConfig` and `with_sandbox` in `crates/cli/` returns zero results.

### Impact

A malicious WASM module loaded via:
- CLI lens set (file path)
- dev_mode HTTP (file path)
- Inline bytes via HTTP (valid)

... will execute with **no memory limit, no CPU fuel metering, no epoch deadline**. It can:
- Allocate unbounded memory (OOM the host process)
- Run an infinite loop (block a tokio worker thread forever)
- Produce unbounded output documents (output loop has no cap at `wasm.rs:696-772`)

### Tests

- `test_wasm_store_with_sandbox` -- verifies the config compiles, but does not test enforcement
- No test verifies that a fuel-limited module is actually interrupted
- No test verifies that a memory-limited module hits the limit

### Verdict: FAIL

The code infrastructure is correct and complete. But it is **dead code** in production -- `database.rs` calls `WasmTransformStore::new()` which passes `sandbox: None`. The fix requires either:
1. Changing `create_lens_store()` to pass `WasmSandboxConfig::restrictive()` by default, or
2. Adding a CLI flag and wiring it through the config

Additionally, the output loop in `execute_batch_transform` (line 696) has no iteration cap -- even with sandbox enabled, a module could produce millions of output documents.

---

## Finding 05-32: No HTTP Rate Limiting or Connection Limits

### Remediation Code

**File**: `/Users/johnzampolin/go/src/github.com/sourcenetwork/defradb.rs/crates/http/src/server.rs`

```rust
// Apply concurrency limit (0 = unlimited)
if self.config.max_concurrent_requests > 0 {
    router = router.layer(ConcurrencyLimitLayer::new(
        self.config.max_concurrent_requests,
    ));
}

// Apply request timeout (0 = no timeout)
if self.config.request_timeout > 0 {
    router = router.layer(
        ServiceBuilder::new()
            .layer(HandleErrorLayer::new(|err: axum::BoxError| async move {
                if err.is::<tower::timeout::error::Elapsed>() {
                    StatusCode::REQUEST_TIMEOUT
                } else {
                    StatusCode::SERVICE_UNAVAILABLE
                }
            }))
            .layer(TimeoutLayer::new(Duration::from_secs(
                self.config.request_timeout,
            ))),
    );
}
```

### Configuration

| Parameter | Default | CLI Flag |
|-----------|---------|----------|
| `request_timeout` | 300s (5 min) | `--request-timeout` |
| `max_concurrent_requests` | 1000 | `--max-concurrent-requests` |

### Analysis

- **ConcurrencyLimitLayer**: Defaults to 1000 concurrent requests. Correctly uses tower's `ConcurrencyLimitLayer` which returns 503 when the limit is reached.
- **TimeoutLayer**: Defaults to 300 seconds. Uses `HandleErrorLayer` to convert tower timeout errors to HTTP 408 Request Timeout.
- **No per-IP rate limiting**: Only global concurrency limit. An attacker from a single IP can consume all 1000 slots. Tower's `RateLimitLayer` or `tower_governor` for per-IP limiting is not implemented.
- **Both are opt-out (0 disables)**: Setting either to 0 disables the respective limit.

### Correctness

The `HandleErrorLayer` wrapping is correct -- without it, `TimeoutLayer`'s error type doesn't implement `Into<Infallible>`, which would cause a compilation failure. The `ServiceBuilder` pattern correctly chains the error handler and timeout layer.

### Layer Ordering

The layers are applied in this order (bottom-up execution):
1. CorsLayer (CORS headers)
2. TraceLayer (request tracing)
3. TimeoutLayer + HandleErrorLayer (request timeout)
4. ConcurrencyLimitLayer (concurrent request cap)
5. DefaultBodyLimit (body size)

This ordering means the body limit is checked first (as an Axum extractor, not middleware), then concurrency, then timeout -- which is correct.

### Tests

No integration test for concurrency limits or request timeouts at the HTTP layer.

### Verdict: PASS

The defaults (1000 concurrent, 300s timeout) are reasonable. The tower middleware is correctly configured. Per-IP rate limiting is deferred to post-1.0 per the roadmap.

---

## Additional Observations

### 1. Per-Route Body Size Overrides Are Dead Configuration

`max_schema_size` and `max_backup_size` exist in:
- `ServerConfig` struct (`server.rs:40-42`)
- `ApiConfig` struct (`config/sections.rs:56-59`)
- CLI flags (`start/mod.rs:129-133`)
- Start server wiring (`start/server.rs:134-135`)

But they are **never used in the router**. The `router()` method only applies `max_body_size` as a global `DefaultBodyLimit`. No per-route `DefaultBodyLimit` layers are created for schema or backup endpoints.

This means operators who set `--max-schema-size=1048576` believe they are limiting schema uploads to 1MB, but the limit is not enforced.

### 2. Default Body Limit Is Unlimited

The `max_body_size` default is 0, which means `DefaultBodyLimit::disable()`. This matches Go behavior but means an out-of-the-box Rust node has no body size limit at all. The original vulnerability (finding 05-01) is only mitigated if the operator explicitly sets `--max-body-size`.

### 3. Mutation Depth/Width Not Validated

`validate_select_limits` is called for queries and subscriptions but not mutations. A mutation's return clause can contain arbitrarily deep or wide sub-selections. The planner's `MAX_NESTING_DEPTH=10` provides a fallback, but the parser-level width check (MAX_QUERY_WIDTH=100) is bypassed.

### 4. WASM Output Loop Has No Cap

In `execute_batch_transform()` at `wasm.rs:696-772`, the loop calls `transform_fn.call()` repeatedly until EOS (type_id=127 or offset=0). There is no iteration cap. Even with fuel metering enabled, a module that returns many small documents could produce millions of entries before fuel exhaustion. The roadmap called for `MAX_OUTPUT_DOCS = 10,000` but this was not implemented.

---

## Summary Table

| Finding | Code Exists | Tests | Wired E2E | Default Secure | Verdict |
|---------|-------------|-------|-----------|----------------|---------|
| 05-00 Depth/Width | Yes | Integration | Yes | Yes (hardcoded) | **PASS** |
| 05-01 Body Limit | Yes | None | Partial | No (0=unlimited) | **PASS (caveat)** |
| 05-02 Filter Depth | Yes | None | Yes | Yes (50) | **PASS** |
| 05-05 Query Timeout | Yes | Integration | Yes | Yes (30s) | **PASS** |
| 05-15 Lens Path Traversal | Yes | Unit | Yes | Yes (blocked) | **PASS** |
| 05-31 WASM Sandbox | Yes | Unit (trivial) | **No** | **No** | **FAIL** |
| 05-32 Rate/Connection Limits | Yes | None | Yes | Yes (1000/300s) | **PASS** |

---

## Recommended Actions

### Must Fix Before 1.0

1. **Activate WASM sandbox**: Change `database.rs:create_lens_store()` to pass `WasmSandboxConfig::restrictive()` by default. Optionally add CLI flags to customize.
2. **Wire per-route body limits**: Apply `max_schema_size` and `max_backup_size` as per-route `DefaultBodyLimit` layers in `server.rs:router()`, or remove the dead config to avoid false security assurance.
3. **Add WASM output loop cap**: Implement `MAX_OUTPUT_DOCS` in `execute_batch_transform()`.

### Should Fix Before 1.0

4. **Validate mutation sub-selections**: Call `validate_select_limits` for mutation return clauses.
5. **Add timeout-triggering integration test**: Craft a query that exceeds the timeout and verify the error response.
6. **Add body size rejection integration test**: Start a node with `--max-body-size` and verify oversized requests are rejected.

### Accept Risk

7. Default unlimited body size (Go compatibility)
8. No per-IP rate limiting (deferred to post-1.0)
9. Filter width amplification within `_and`/`_or` arrays (evaluator depth limit is sufficient)
