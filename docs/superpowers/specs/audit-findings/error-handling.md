# Error Handling Audit Findings

## Summary
- Total findings: 23
- Critical: 4 | High: 6 | Medium: 9 | Low: 4

## Findings

### Finding 1
- **severity:** medium
- **category:** anti-pattern
- **crate:** p2p
- **file:** crates/p2p/src/bitswap/store.rs
- **line:** 15
- **pattern:** anyhow-in-library
- **description:** The `p2p` library crate uses `anyhow::Result` as the return type for `Store` trait implementations. While the `iroh_bitswap::Store` trait requires `anyhow::Result`, the `anyhow` dependency leaks into a library crate. The `map_err(|e| anyhow!(...))` calls also erase the original typed error.
- **training_ref:** rust-patterns-book ch10 "thiserror vs anyhow -- Library vs Application"
- **suggested_fix:** This is partially forced by the upstream `iroh_bitswap::Store` trait contract. Add a code comment documenting this constraint. If the trait is local, consider switching it to a typed error.

### Finding 2
- **severity:** medium
- **category:** anti-pattern
- **crate:** embedded
- **file:** crates/embedded/src/node.rs
- **line:** 17
- **pattern:** anyhow-in-library
- **description:** The `embedded` library crate uses `anyhow::{anyhow, Context, Result}` throughout its public API. This means callers cannot match on specific error variants and must use downcasting.
- **training_ref:** rust-patterns-book ch10 "thiserror vs anyhow -- Library vs Application"
- **suggested_fix:** Define an `EmbeddedError` enum with `thiserror` in the `embedded` crate and convert to it. Reserve `anyhow` for the binary crates (`cli`, `defra-node`) that consume `embedded`.

### Finding 3
- **severity:** high
- **category:** anti-pattern
- **crate:** embedded
- **file:** crates/embedded/src/lib.rs
- **line:** 27-58
- **pattern:** string-errors
- **description:** The `P2POperations` trait uses `Result<T, String>` for all 18 of its methods. This is the most pervasive `String`-as-error-type pattern in the codebase. The `TransportDocPusher` trait (transport_doc_pusher.rs:15-41) similarly uses `Result<T, String>` for 10+ methods. This cascades: every implementor must `.map_err(|e| e.to_string())`, destroying error type information.
- **training_ref:** rust-patterns-book ch10 "thiserror vs anyhow -- Library vs Application"
- **suggested_fix:** Define a `P2PError` enum with `thiserror` and use it as the error type for these traits. This enables callers to match on specific failure modes (network unreachable, peer not found, etc.) instead of parsing error strings.

### Finding 4
- **severity:** critical
- **category:** bug
- **crate:** http
- **file:** crates/http/src/query_context.rs
- **line:** 44, 75, 99
- **pattern:** bare-expect
- **description:** Three `.expect("query execution task panicked")` calls on `JoinHandle` results in production HTTP handler code. If a `spawn_blocking` task panics (e.g., from any `unwrap()` deeper in the stack), this `.expect()` will panic the HTTP handler, which can crash the tokio runtime or propagate to the connection handler. This is user-facing code that processes arbitrary GraphQL queries.
- **training_ref:** async-book ch13 "Error Handling in Async Code" -- "The error boundary problem"
- **suggested_fix:** Replace `.expect(...)` with proper error handling: `match handle.await { Ok(response) => response, Err(join_err) => QueryResponse::error(format!("internal error: {}", join_err)) }`. This follows the double-`?` pattern from the training material.

### Finding 5
- **severity:** critical
- **category:** bug
- **crate:** query
- **file:** crates/query/src/subscription.rs
- **line:** 64, 138
- **pattern:** bare-unwrap
- **description:** `query[after_field..].find('(').unwrap()` on user-provided GraphQL subscription queries. The `find('(')` can return `None` for malformed queries, causing a panic on user input. Line 64 handles `_commits` subscriptions and line 138 handles CID injection -- both process external user queries.
- **training_ref:** rust-patterns-book ch10 "Panics, catch_unwind, and When to Abort"
- **suggested_fix:** Return a `QueryError::parse("expected '(' in subscription query")` instead of panicking. The function should return `Result<String, QueryError>`.

### Finding 6
- **severity:** critical
- **category:** bug
- **crate:** document
- **file:** crates/document/src/encoding.rs
- **line:** 557, 593, 619, 645
- **pattern:** bare-unwrap
- **description:** Four `opt.unwrap()` calls on `Option` values inside array decoding (`bools.into_iter().map(|opt| opt.unwrap()).collect()`). The code checks `has_null` first and only reaches these lines when all values are `Some`, but the unwrap is still fragile -- any future refactor that changes the null-tracking logic could introduce a panic on user-provided CBOR data. This processes external document data.
- **training_ref:** rust-patterns-book ch10 "Panics, catch_unwind, and When to Abort"
- **suggested_fix:** Use `.map(|opt| opt.expect("null check guarantees Some"))` to document the invariant, or better, use `opt.ok_or(Error::CborDecode("unexpected null in array".into()))?` with `collect::<Result<Vec<_>, _>>()`.

### Finding 7
- **severity:** high
- **category:** bug
- **crate:** crdt
- **file:** crates/crdt/src/composite.rs
- **line:** 317, 336, 344, 369
- **pattern:** bare-unwrap
- **description:** `data[..8].try_into().unwrap()` on data received from the storage layer during CRDT merge operations. If stored counter data is corrupted or truncated (fewer than 8 bytes), this panics. The code at line 330-334 properly validates length and returns an error for the current-value read, but the *incoming delta* (line 317) and the *accumulator reads* (336, 369) do not validate before unwrapping.
- **training_ref:** rust-patterns-book ch10 "Panics, catch_unwind, and When to Abort"
- **suggested_fix:** Add length validation before each `try_into().unwrap()` and return `Error::MergeError(...)` for invalid data, matching the existing pattern at lines 330-334.

### Finding 8
- **severity:** high
- **category:** bug
- **crate:** query
- **file:** crates/query/src/plan/type_join/type_join_one.rs
- **line:** 381-383, 455-457
- **pattern:** bare-unwrap
- **description:** Six `as_ref().unwrap()` calls on `Option` fields (`parent_collection`, `parent_scan_mapping`, `fetcher`) in the type-join query plan. These fields are `Option` because they are set during initialization, but nothing prevents `next()` from being called before `init()`. A logic error in the planner would cause a panic during query execution on user queries.
- **training_ref:** rust-patterns-book ch10 "Panics, catch_unwind, and When to Abort"
- **suggested_fix:** Return `QueryError::internal("type join not initialized")` when these fields are `None`. Alternatively, restructure to use a builder pattern where the fully-initialized state is a different type that doesn't need `Option`.

### Finding 9
- **severity:** critical
- **category:** bug
- **crate:** pg-compat
- **file:** crates/pg-compat/src/handler/mod.rs
- **line:** 642, 646
- **pattern:** bare-unwrap
- **description:** `Regex::new(...).unwrap()` called on every invocation of `extract_filter_from_graphql()`. While the regex patterns are compile-time constants and will always compile, constructing them per-call is wasteful. More importantly, this function processes user-provided SQL translated to GraphQL -- any future regex changes that introduce a syntax error would panic on the hot path.
- **training_ref:** rust-patterns-book ch10 "Panics, catch_unwind, and When to Abort"
- **suggested_fix:** Use `OnceLock` or `lazy_static!` to compile regexes once, and use `.expect("valid regex literal")` to document the safety invariant.

### Finding 10
- **severity:** medium
- **category:** anti-pattern
- **crate:** db
- **file:** crates/db/src/merge_handler/composite.rs
- **line:** 94, 694, 847, 853, 1303, 1322
- **pattern:** bare-unwrap
- **description:** `Mutex::lock().unwrap()` calls throughout the merge handler on `merged_composites`, `batch_merged`, and `pending_events` mutexes. If any thread panics while holding these locks, the Mutex becomes poisoned and all subsequent lock attempts panic, cascading the failure across the entire merge pipeline.
- **training_ref:** rust-patterns-book ch10 "Panics, catch_unwind, and When to Abort"
- **suggested_fix:** Use `.lock().unwrap_or_else(|e| e.into_inner())` to recover from poisoned locks (accepting potentially inconsistent state with logging), or propagate as `Error::LockPoisoned`. The `parking_lot::Mutex` crate doesn't have poisoning, which is another option.

### Finding 11
- **severity:** high
- **category:** anti-pattern
- **crate:** acp
- **file:** crates/acp/src/persistent.rs
- **line:** 155-434 (throughout)
- **pattern:** map-err-to-string
- **description:** Over 35 instances of `.map_err(|e| Error::Storage(e.to_string()))` in the persistent ACP store. The original error types (from `storage::corekv` and `serde_json`) are converted to `String`, losing their type information. Callers cannot distinguish between a serialization error and a storage I/O error.
- **training_ref:** rust-patterns-book ch10 "Error Conversion Chains (#[from])"
- **suggested_fix:** Add `#[from]` variants to `acp::Error` for `storage::corekv::Error` and use the existing structured variants (`StorageRead`, `StorageWrite`, etc.) consistently instead of the catch-all `Storage(String)`.

### Finding 12
- **severity:** medium
- **category:** anti-pattern
- **crate:** db
- **file:** crates/db/src/error.rs
- **line:** 78, 81
- **pattern:** string-errors
- **description:** `Error::Acp(String)` and `Error::Lens(String)` variants store error messages as strings instead of wrapping the actual error types. Since both `acp::Error` and `lens::Error` exist as proper `thiserror` enums, these should use `#[from]`.
- **training_ref:** rust-patterns-book ch10 "Error Conversion Chains (#[from])"
- **suggested_fix:** Change to `Acp(#[from] acp::Error)` and `Lens(#[from] lens::Error)`.

### Finding 13
- **severity:** medium
- **category:** anti-pattern
- **crate:** pg-compat
- **file:** crates/pg-compat/src/lib.rs
- **line:** 49
- **pattern:** box-dyn-error
- **description:** `PgServer::run()` returns `Result<(), Box<dyn std::error::Error>>`. This is the main entry point for the Postgres wire protocol server. Using `Box<dyn Error>` prevents callers from matching on specific error types.
- **training_ref:** rust-patterns-book ch10 "thiserror vs anyhow -- Library vs Application"
- **suggested_fix:** Define a `PgServerError` enum or use `anyhow::Error` (since this is effectively a top-level runner). If the crate is consumed as a library, prefer a typed error.

### Finding 14
- **severity:** medium
- **category:** anti-pattern
- **crate:** db
- **file:** crates/db/src/embedding.rs
- **line:** 9-11
- **pattern:** box-dyn-error
- **description:** `type EmbeddingError = Box<dyn std::error::Error + Send + Sync>` used as the error type for the embedding subsystem. This type-erases all embedding errors, making it impossible to distinguish between network errors, JSON parse errors, and API errors without string matching.
- **training_ref:** rust-patterns-book ch10 "thiserror vs anyhow -- Library vs Application"
- **suggested_fix:** Define an `EmbeddingError` enum with variants for network, parsing, and API errors.

### Finding 15
- **severity:** low
- **category:** improvement
- **crate:** ffi
- **file:** crates/ffi/src/mobile.rs
- **line:** 597-623
- **pattern:** missing-catch-unwind
- **description:** `defra_mobile_peer_info`, `defra_mobile_connect`, and `defra_mobile_notify_network_change` are `extern "C"` functions that do NOT use the `ffi_entry!` macro for `catch_unwind` protection. They delegate to other FFI functions that do use it, so in practice panics are caught one level deeper. However, if `default_identity_cstring()` panics, the unwind would cross the FFI boundary.
- **training_ref:** rust-patterns-book ch10 "Panics, catch_unwind, and When to Abort"
- **suggested_fix:** Wrap these functions in `ffi_entry!` for defense-in-depth. The `defra_version()` and `defra_init()` functions (lib.rs:191, 203) should also be wrapped.

### Finding 16
- **severity:** high
- **category:** bug
- **crate:** query
- **file:** crates/query/src/sdl_parse/builder.rs
- **line:** 121, 197, 222, 291
- **pattern:** bare-unwrap
- **description:** `self.type_defs.get(type_name).unwrap()` in the SDL schema builder. The `type_name` comes from user-provided SDL schema definitions. If the type names in the dependency graph and the `type_defs` map get out of sync (e.g., due to a bug in type resolution or external type handling), this panics during schema parsing of user input.
- **training_ref:** rust-patterns-book ch10 "Panics, catch_unwind, and When to Abort"
- **suggested_fix:** Use `.ok_or_else(|| QueryError::parse(format!("unknown type: {}", type_name)))?`.

### Finding 17
- **severity:** medium
- **category:** anti-pattern
- **crate:** lens
- **file:** crates/lens/src/wasm.rs
- **line:** 190
- **pattern:** bare-expect
- **description:** `Self::new().expect("failed to create WASM engine")` in the `Default` impl for `WasmTransformStore`. WASM engine creation can fail for system-level reasons (memory allocation, platform support). Using `expect` in a `Default` impl means any caller using `default()` gets a panic instead of an error.
- **training_ref:** rust-patterns-book ch10 "Panics, catch_unwind, and When to Abort"
- **suggested_fix:** Remove the `Default` impl or have it return a no-op store. Callers should use `WasmTransformStore::new()` which returns `Result`.

### Finding 18
- **severity:** high
- **category:** bug
- **crate:** db
- **file:** crates/db/src/block_builder/write.rs
- **line:** 54, 77, 272
- **pattern:** bare-unwrap
- **description:** `snapshot.as_ref().unwrap()` on a value that is `None` when `is_create` is true. The code at line 51-55 shows: `if is_create { 1 } else { snapshot.as_ref().unwrap().max_priority() + 1 }`. The else branch only runs when `is_create` is false, but the safety depends entirely on the `if is_create` check. A refactor that changes the control flow could silently introduce a panic. Lines 77 and 272 have the same pattern.
- **training_ref:** rust-patterns-book ch10 "Panics, catch_unwind, and When to Abort"
- **suggested_fix:** Use `snapshot.as_ref().ok_or_else(|| Error::Internal("snapshot required for updates"))?` to make the invariant explicit and fail gracefully.

### Finding 19
- **severity:** low
- **category:** improvement
- **crate:** query
- **file:** crates/query/src/sdl_parse/helpers.rs
- **line:** 32, 35, 62
- **pattern:** bare-unwrap
- **description:** `Regex::new(...).unwrap()` for compile-time constant regex patterns in SDL parsing helpers. These will never fail, but they are compiled on every call rather than cached.
- **training_ref:** rust-patterns-book ch10 "Panics, catch_unwind, and When to Abort"
- **suggested_fix:** Use `OnceLock<Regex>` or `LazyLock<Regex>` to compile once. Use `.expect("valid regex literal")` to document the safety invariant.

### Finding 20
- **severity:** low
- **category:** improvement
- **crate:** query
- **file:** crates/query/src/plan/groupby/rendering.rs
- **line:** 459, 474
- **pattern:** bare-unwrap
- **description:** `write!(buf, ...).unwrap()` when writing to a `String` buffer. Writing to `String` via `fmt::Write` is infallible (it can only fail on OOM, which aborts), so the unwrap is technically safe. However, it's unconventional and obscures intent.
- **training_ref:** rust-patterns-book ch10 "Panics, catch_unwind, and When to Abort"
- **suggested_fix:** Use `let _ = write!(buf, ...)` or the `write!` macro pattern that ignores the result, since `String::write_fmt` never fails in practice.

### Finding 21
- **severity:** medium
- **category:** anti-pattern
- **crate:** p2p
- **file:** crates/p2p/src/two_stream/runner.rs
- **line:** 103, 142, 176, 222, 252, 272
- **pattern:** bare-expect
- **description:** Six `.acquire().await.expect("semaphore closed")` calls in the P2P stream runner. Semaphore `acquire` only fails when the semaphore is closed, which indicates a shutdown race condition. Panicking on shutdown is user-hostile -- the node should gracefully exit instead.
- **training_ref:** rust-patterns-book ch10 "Panics, catch_unwind, and When to Abort"
- **suggested_fix:** Handle `Err` by breaking out of the loop (returning early), which is the correct behavior during shutdown. This matches the graceful shutdown pattern from the async training material (ch13).

### Finding 22
- **severity:** medium
- **category:** anti-pattern
- **crate:** db
- **file:** crates/db/src/auto_commit_mutator/create_many.rs
- **line:** 18
- **pattern:** bare-unwrap
- **description:** `docs.into_iter().next().unwrap()` on the single-doc fast path. The `docs.len() == 1` check on line 17 guarantees this is safe, but the unwrap is redundant with the length check and would panic if the length check were removed.
- **training_ref:** rust-patterns-book ch10 "Panics, catch_unwind, and When to Abort"
- **suggested_fix:** Use `docs.into_iter().next().expect("length checked above")` to document the invariant, or destructure with `if let [doc] = docs.as_slice()`.

### Finding 23
- **severity:** low
- **category:** improvement
- **crate:** defra-core, storage
- **file:** crates/defra-core/Cargo.toml, crates/storage/Cargo.toml
- **line:** 13, 38 (respectively)
- **pattern:** anyhow-in-library
- **description:** Both `defra-core` and `storage` list `anyhow` as a dependency in their Cargo.toml, but neither actually uses it in source code (grep confirms zero `anyhow::` or `use anyhow` in their `src/` directories). This is dead dependency weight.
- **training_ref:** rust-patterns-book ch10 "thiserror vs anyhow -- Library vs Application"
- **suggested_fix:** Remove `anyhow.workspace = true` from both Cargo.toml files.
