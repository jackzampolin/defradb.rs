# Consolidated Audit Findings

## Summary
- Total findings: 127 (after dedup from 129 raw findings across 7 audits; 2 merges)
- By severity: Critical: 6 | High: 27 | Medium: 48 | Low: 46
- By category: Bug: 10 | Unsound: 2 | Anti-pattern: 26 | Improvement: 50 | Structure: 39

## Crate Priority Ranking

Rank crates by total finding weight (critical=4, high=3, medium=2, low=1).
This determines which crates to fix first in Phase 2.

| Rank | Crate | Score | Critical | High | Medium | Low |
|------|-------|-------|----------|------|--------|-----|
| 1 | query | 49 | 1 | 4 | 8 | 12 |
| 2 | db | 38 | 0 | 3 | 7 | 7 |
| 3 | ffi | 33 | 0 | 3 | 5 | 5 |
| 4 | p2p | 32 | 0 | 3 | 6 | 4 |
| 5 | defra-core | 25 | 0 | 1 | 6 | 5 |
| 6 | blockstore | 19 | 0 | 2 | 3 | 2 |
| 7 | embedded | 18 | 0 | 2 | 3 | 1 |
| 8 | zanzibar | 14 | 0 | 3 | 1 | 1 |
| 9 | cli | 13 | 0 | 2 | 1 | 2 |
| 10 | crdt | 12 | 0 | 1 | 2 | 3 |
| 11 | storage | 12 | 1 | 1 | 0 | 2 |
| 12 | defra-node | 11 | 0 | 2 | 1 | 1 |
| 13 | events | 8 | 0 | 0 | 3 | 1 |
| 14 | sourcehub | 7 | 0 | 0 | 3 | 1 |
| 15 | document | 6 | 1 | 0 | 1 | 0 |
| 16 | pg-compat | 6 | 1 | 0 | 1 | 0 |
| 17 | acp | 5 | 0 | 1 | 2 | 0 |
| 18 | http | 4 | 1 | 0 | 0 | 0 |
| 19 | lens | 2 | 0 | 0 | 1 | 0 |
| 20 | schema | 3 | 0 | 0 | 1 | 1 |
| 21 | identity | 1 | 0 | 0 | 0 | 1 |
| 22 | crypto | 3 | 0 | 0 | 1 | 1 |

## Findings by Crate

---

### query (20 findings)

#### query-1: Bare unwrap on user-provided GraphQL subscription queries
- **severity:** critical
- **category:** bug
- **file:** `crates/query/src/subscription.rs`
- **line:** 64, 138
- **patterns:** bare-unwrap
- **description:** `query[after_field..].find('(').unwrap()` on user-provided GraphQL subscription queries. The `find('(')` can return `None` for malformed queries, causing a panic on user input. Line 64 handles `_commits` subscriptions and line 138 handles CID injection -- both process external user queries.
- **training_refs:** rust-patterns-book ch10 "Panics, catch_unwind, and When to Abort"
- **suggested_fix:** Return a `QueryError::parse("expected '(' in subscription query")` instead of panicking. The function should return `Result<String, QueryError>`.

#### query-2: Transmute of fat pointer layout in FetcherWrapper (merged: unsound + unsafe Send/Sync)
- **severity:** critical
- **category:** unsound
- **file:** `crates/query/src/runner/fetcher.rs`
- **line:** 28-73
- **patterns:** transmute-fat-pointer-layout, unsafe-send-sync-impl
- **description:** `FetcherWrapper` uses `std::mem::transmute` to decompose and reconstruct trait object fat pointers (`*const dyn DocFetcher` to/from `(*const (), *const ())`). The layout of fat pointers is **not guaranteed by the Rust reference or any RFC**. While it works on all current targets, this is technically relying on an implementation detail. The comment on line 28-30 acknowledges this: "relies on the standard fat pointer layout... which is stable in practice but not formally guaranteed." This makes the code fragile to compiler changes. Additionally, the `get_fetcher()` method on line 55-65 dereferences a raw pointer with a lifetime that cannot be statically verified -- if the original reference is dropped before the wrapper, this is use-after-free. Furthermore, `unsafe impl Send for FetcherWrapper {}` and `unsafe impl Sync for FetcherWrapper {}` (lines 72-73) are implemented with a safety comment that argues correctness based on `DocFetcher: Send + Sync`. However, the wrapper holds raw pointers (`*const ()`), which are neither `Send` nor `Sync`. The safety argument is sound IF the lifetime invariant holds, but this invariant is not enforced by the type system.
- **training_refs:** rust-patterns-book ch12 "Common UB Pitfalls" -- "Invalid enum value / transmute... Almost always wrong"; rust-patterns-book ch12 "Writing Sound Abstractions" -- "Encapsulate -- the unsafe is inside a safe API; users can't trigger UB"
- **suggested_fix:** Replace the transmute-based fat pointer decomposition with `std::ptr::metadata` and `std::ptr::from_raw_parts` once the `ptr_metadata` feature stabilizes. In the interim, restructure to pass `Arc<dyn DocFetcher>` directly (the planner already requires `Arc<dyn DocFetcher>`). If the reference-based approach must remain, add a `PhantomData<&'a dyn DocFetcher>` with a proper lifetime parameter to make the lifetime constraint compile-time enforced. At minimum, add a module-level `#[deny(unsafe_op_in_unsafe_fn)]` and restrict `FetcherWrapper::new()` visibility.

#### query-3: Bare unwrap in SDL schema builder on user-provided type names
- **severity:** high
- **category:** bug
- **file:** `crates/query/src/sdl_parse/builder.rs`
- **line:** 121, 197, 222, 291
- **patterns:** bare-unwrap
- **description:** `self.type_defs.get(type_name).unwrap()` in the SDL schema builder. The `type_name` comes from user-provided SDL schema definitions. If the type names in the dependency graph and the `type_defs` map get out of sync (e.g., due to a bug in type resolution or external type handling), this panics during schema parsing of user input.
- **training_refs:** rust-patterns-book ch10 "Panics, catch_unwind, and When to Abort"
- **suggested_fix:** Use `.ok_or_else(|| QueryError::parse(format!("unknown type: {}", type_name)))?`.

#### query-4: Bare unwrap on Option fields in type-join query plan
- **severity:** high
- **category:** bug
- **file:** `crates/query/src/plan/type_join/type_join_one.rs`
- **line:** 381-383, 455-457
- **patterns:** bare-unwrap
- **description:** Six `as_ref().unwrap()` calls on `Option` fields (`parent_collection`, `parent_scan_mapping`, `fetcher`) in the type-join query plan. These fields are `Option` because they are set during initialization, but nothing prevents `next()` from being called before `init()`. A logic error in the planner would cause a panic during query execution on user queries.
- **training_refs:** rust-patterns-book ch10 "Panics, catch_unwind, and When to Abort"
- **suggested_fix:** Return `QueryError::internal("type join not initialized")` when these fields are `None`. Alternatively, restructure to use a builder pattern where the fully-initialized state is a different type that doesn't need `Option`.

#### query-5: Select starvation in P2P host event loop
- **severity:** medium
- **category:** anti-pattern
- **file:** `crates/p2p/src/host/p2p_host/mod.rs`
- **line:** 326-354
- **patterns:** select-starvation
- **description:** The P2P host event loop uses `biased` select with swarm events as the highest priority. The comment at line 327 explains this is intentional for ordering guarantees. However, under heavy peer activity (many connections, frequent gossip), the swarm branch will always be ready, starving the command channel. This means `HostCommand::Shutdown` cannot be delivered, potentially causing the node to hang during shutdown. The two-stream events channel has the same starvation risk as commands.
- **training_refs:** async-book ch12 "select! Fairness and Starvation"
- **suggested_fix:** Process swarm events in a batch (drain up to N events per iteration) then always poll the command channel once per iteration. Alternatively, add a `CancellationToken` that is checked at the top of the loop, independent of `select!`: `if self.cancel_token.is_cancelled() { break; }`.

**NOTE:** This finding is filed under `query` because it was reported in the async audit but actually belongs to the `p2p` crate. It is cross-listed under p2p as well. See p2p-2.

#### query-6: Oversized file: runner/query/nested.rs (1821 lines)
- **severity:** high
- **category:** structure
- **file:** `crates/query/src/runner/query/nested.rs`
- **line:** 1-1821
- **patterns:** oversized-file
- **description:** File contains 4 distinct concerns: (1) profiling structs and the main `execute_nested_select_with_planner` method (~lines 1-400), (2) scoped full-text search scoring and sort logic (~lines 400-700), (3) post-processing helpers: clean_filter_only_relation_fields, apply_deferred_relation_limits, sort_relation_items, strip_ordering_only_fields (~lines 700-1050), (4) unit tests for scoped fulltext, precompute scores, and relation path scoring (~lines 1050-1821).
- **training_refs:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Split into:
  - `crates/query/src/runner/query/nested/mod.rs` -- re-exports + execute_nested_select_with_planner (est. ~400 lines)
  - `crates/query/src/runner/query/nested/scoped_fulltext.rs` -- apply_scoped_relation_fulltext, compute_scoped_fulltext_scores, scoped profiling (est. ~300 lines)
  - `crates/query/src/runner/query/nested/post_process.rs` -- clean_filter_only_relation_fields, apply_deferred_relation_limits, sort_relation_items, strip_ordering_only_fields (est. ~350 lines)
  - `crates/query/src/runner/query/nested/tests.rs` -- all #[cfg(test)] code (est. ~770 lines)

#### query-7: Oversized file: runner/commits.rs (1545 lines)
- **severity:** medium
- **category:** structure
- **file:** `crates/query/src/runner/commits.rs`
- **line:** 1-1545
- **patterns:** oversized-file
- **description:** File contains 3 distinct concerns: (1) inline unit tests for height range extraction (~lines 26-200), (2) helper types and functions for commit numeric values, height extraction, aggregation (~lines 200-600), (3) the main `execute_commits_query` method and rendering/filtering/grouping logic (~lines 600-1545).
- **training_refs:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Split into:
  - `crates/query/src/runner/commits/mod.rs` -- re-exports + execute_commits_query (est. ~600 lines)
  - `crates/query/src/runner/commits/height_range.rs` -- CommitsHeightRange, HeightRangeExtraction, extract_commits_height_range (est. ~200 lines)
  - `crates/query/src/runner/commits/render.rs` -- render_commit, render_document_fields, commit_to_fields, build_commits_mapping (est. ~400 lines)
  - `crates/query/src/runner/commits/filter.rs` -- json_item_matches_filter, check_filter_op, aggregation helpers (est. ~200 lines)
  - `crates/query/src/runner/commits/tests.rs` -- all unit tests (est. ~200 lines)

#### query-8: Oversized file: planner/joins/mod.rs (1395 lines)
- **severity:** medium
- **category:** structure
- **file:** `crates/query/src/planner/joins/mod.rs`
- **line:** 1-1395
- **patterns:** oversized-file
- **description:** Despite having 6 sub-modules (aggregate_joins, filter_only, filter_relation, mapping, multi_level, secondary_id), mod.rs still contains the core `apply_joins` method which is 1395 lines. It mixes one-to-one join building, one-to-many join building, index selection, filter extraction, FK resolution, and ordering inversion logic.
- **training_refs:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Split into:
  - `crates/query/src/planner/joins/mod.rs` -- JoinResult type, SelectionJoinInfo, re-exports (est. ~80 lines)
  - `crates/query/src/planner/joins/apply.rs` -- apply_joins main loop and dispatch (est. ~300 lines)
  - `crates/query/src/planner/joins/one_to_one.rs` -- TypeJoinOne construction, ordering inversion (est. ~350 lines)
  - `crates/query/src/planner/joins/one_to_many.rs` -- TypeJoinMany construction, groupBy, indexed child cache (est. ~350 lines)
  - `crates/query/src/planner/joins/helpers.rs` -- FK resolution, filter extraction, index selection for joins (est. ~300 lines)

#### query-9: Oversized file: sdl_parse/builder.rs (1040 lines)
- **severity:** medium
- **category:** structure
- **file:** `crates/query/src/sdl_parse/builder.rs`
- **line:** 1-1040
- **patterns:** oversized-file
- **description:** File contains 3 concerns: (1) build_collections and validation (~lines 1-200), (2) build_collection for individual type definitions including FK field generation (~lines 200-700), (3) resolve_field_kind and Tarjan SCC algorithm for cycle detection (~lines 700-1040).
- **training_refs:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Split into:
  - `crates/query/src/sdl_parse/builder.rs` -- build_collections, collect_primary_directives (est. ~200 lines)
  - `crates/query/src/sdl_parse/build_collection.rs` -- build_collection, FK field generation (est. ~500 lines)
  - `crates/query/src/sdl_parse/resolve_kind.rs` -- resolve_field_kind, find_sccs (Tarjan), detect_collection_set (est. ~340 lines)

#### query-10: Oversized file: query_parse/parser.rs (1270 lines)
- **severity:** medium
- **category:** structure
- **file:** `crates/query/src/query_parse/parser.rs`
- **line:** 1-1270
- **patterns:** oversized-file
- **description:** File contains 4 concerns: (1) types (ExplainType, ParsedOperation ~lines 1-80), (2) top-level parse functions parse_request, parse_document (~lines 80-300), (3) field parsing (parse_field_to_select, parse_selection_set ~lines 300-800), (4) argument parsing helpers (parse_doc_ids_value, parse_cid_value, resolve_bool_value ~lines 800-1270).
- **training_refs:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Split into:
  - `crates/query/src/query_parse/parser.rs` -- types + parse_request + parse_document (est. ~300 lines)
  - `crates/query/src/query_parse/field_parser.rs` -- parse_field_to_select, parse_selection_set (est. ~500 lines)
  - `crates/query/src/query_parse/args.rs` -- parse_doc_ids_value, parse_cid_value, resolve_bool_value, parse_optional_int_value (est. ~400 lines)

#### query-11: Oversized file: plan/mutation/create.rs (969 lines)
- **severity:** medium
- **category:** structure
- **file:** `crates/query/src/plan/mutation/create.rs`
- **line:** 1-969
- **patterns:** oversized-file
- **description:** File contains 3 concerns: (1) CreateInput type and its to_document/to_document_with_schema methods (~lines 1-200), (2) json_to_normal_value and coerce_json_to_scalar_array -- exhaustive type conversion covering every scalar and array variant (~lines 200-565), (3) CreateNode plan node implementation (~lines 565-969).
- **training_refs:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Split into:
  - `crates/query/src/plan/mutation/create.rs` -- CreateInput, CreateNode (est. ~400 lines)
  - `crates/query/src/plan/mutation/type_coercion.rs` -- json_to_normal_value, coerce_json_to_scalar_array, coerce_json_to_scalar (est. ~500 lines). This type coercion logic is also reused by update/upsert and should be shared.

#### query-12: Internal modules exposed publicly
- **severity:** medium
- **category:** improvement
- **file:** `crates/query/src/lib.rs`
- **line:** 21-38
- **patterns:** pub-should-be-pub-crate
- **description:** Internal modules `json_convert`, `test_utils`, `select_convert` are exposed publicly. `json_convert` is purely internal. `test_utils` should be behind `#[cfg(test)]` or a test feature flag. `select_convert` has a single public function that is already re-exported.
- **training_refs:** rust-patterns-book ch15 "Visibility modifiers"
- **suggested_fix:** Change `json_convert` to `pub(crate) mod`. Gate `test_utils` with `#[cfg(any(test, feature = "test-utils"))]`. Keep `select_convert` as-is since it is re-exported.

#### query-13: Oversized file: rest.rs (863 lines)
- **severity:** medium
- **category:** structure
- **file:** `crates/query/src/rest.rs`
- **line:** 1-863
- **patterns:** oversized-file
- **description:** File contains 3 concerns: (1) RestError type and RestOperations trait (~lines 1-200), (2) RestOperationsImpl with CRUD method implementations (~lines 200-600), (3) helper methods for document get/list/truncate (~lines 600-863).
- **training_refs:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Split into:
  - `crates/query/src/rest/mod.rs` -- RestError, RestOperations trait, re-exports (est. ~200 lines)
  - `crates/query/src/rest/operations.rs` -- RestOperationsImpl CRUD methods (est. ~400 lines)
  - `crates/query/src/rest/helpers.rs` -- document_get_by_id, list, truncate helpers (est. ~260 lines)

#### query-14: Regex compiled per-call in SDL parsing helpers
- **severity:** low
- **category:** improvement
- **file:** `crates/query/src/sdl_parse/helpers.rs`
- **line:** 32, 35, 62
- **patterns:** bare-unwrap
- **description:** `Regex::new(...).unwrap()` for compile-time constant regex patterns in SDL parsing helpers. These will never fail, but they are compiled on every call rather than cached.
- **training_refs:** rust-patterns-book ch10 "Panics, catch_unwind, and When to Abort"
- **suggested_fix:** Use `OnceLock<Regex>` or `LazyLock<Regex>` to compile once. Use `.expect("valid regex literal")` to document the safety invariant.

#### query-15: write!() unwrap on infallible String buffer
- **severity:** low
- **category:** improvement
- **file:** `crates/query/src/plan/groupby/rendering.rs`
- **line:** 459, 474
- **patterns:** bare-unwrap
- **description:** `write!(buf, ...).unwrap()` when writing to a `String` buffer. Writing to `String` via `fmt::Write` is infallible (it can only fail on OOM, which aborts), so the unwrap is technically safe. However, it's unconventional and obscures intent.
- **training_refs:** rust-patterns-book ch10 "Panics, catch_unwind, and When to Abort"
- **suggested_fix:** Use `let _ = write!(buf, ...)` or the `write!` macro pattern that ignores the result, since `String::write_fmt` never fails in practice.

#### query-16: Oversized file: sdl_parse/parser_tests.rs (1813 lines)
- **severity:** low
- **category:** structure
- **file:** `crates/query/src/sdl_parse/parser_tests.rs`
- **line:** 1-1813
- **patterns:** oversized-file
- **description:** Pure test file with 50+ independent test functions covering simple types, arrays, CRDTs, relations, directives, views, indexes, FTS, self-refs, etc. While test files naturally grow large, at 1813 lines navigation is difficult.
- **training_refs:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Split into:
  - `crates/query/src/sdl_parse/tests/basic_types.rs` -- simple type parsing, arrays, scalars (est. ~300 lines)
  - `crates/query/src/sdl_parse/tests/directives.rs` -- @crdt, @primary, @index, @relation, @default, @size tests (est. ~400 lines)
  - `crates/query/src/sdl_parse/tests/relations.rs` -- relation resolution, self-refs, collection sets, named kinds (est. ~400 lines)
  - `crates/query/src/sdl_parse/tests/views.rs` -- view definitions, lens, downsample, embedding tests (est. ~350 lines)
  - `crates/query/src/sdl_parse/tests/errors.rs` -- error cases, unknown types, invalid combinations (est. ~360 lines)

#### query-17: Oversized file: mapper/filter/filter_tests.rs (1126 lines)
- **severity:** low
- **category:** structure
- **file:** `crates/query/src/mapper/filter/filter_tests.rs`
- **line:** 1-1126
- **patterns:** oversized-file
- **description:** Large test file for filter mapping logic. Tests are independent and could be grouped by filter type (comparison, logical, nested, edge cases).
- **training_refs:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Split tests into submodules within `filter_tests.rs` using `mod comparison_tests`, `mod logical_tests`, `mod nested_tests`, `mod edge_case_tests` with natural groupings. Alternatively split into multiple test files.

#### query-18: Oversized file: plan/type_join/type_join_one.rs (938 lines)
- **severity:** low
- **category:** structure
- **file:** `crates/query/src/plan/type_join/type_join_one.rs`
- **line:** 1-938
- **patterns:** oversized-file
- **description:** TypeJoinOne plan node with inverted join logic. Contains both normal and inverted join iteration, plus FK resolution.
- **training_refs:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Extract inverted join logic into `type_join_one_inverted.rs` (est. ~400 lines), keeping normal join in `type_join_one.rs` (est. ~540 lines).

#### query-19: Oversized file: plan/type_join/type_join_many/plan_node.rs (932 lines)
- **severity:** low
- **category:** structure
- **file:** `crates/query/src/plan/type_join/type_join_many/plan_node.rs`
- **line:** 1-932
- **patterns:** oversized-file
- **description:** TypeJoinMany plan node with per-parent filtering, ordering, grouping, and indexed child fetch. Four distinct iteration strategies in one file.
- **training_refs:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Extract indexed child fetch logic into `indexed_child.rs` (est. ~300 lines) and grouping logic into `grouping.rs` (est. ~200 lines).

#### query-20: Oversized file: runner/mutation.rs (882 lines)
- **severity:** low
- **category:** structure
- **file:** `crates/query/src/runner/mutation.rs`
- **line:** 1-882
- **patterns:** oversized-file
- **description:** Mutation execution with create/update/delete/upsert handling, ACP permission checks, and batch processing all in one file.
- **training_refs:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Split ACP permission checking into `mutation_acp.rs` (est. ~200 lines) and keep mutation execution in `mutation.rs` (est. ~680 lines).

---

### db (17 findings)

#### db-1: Bare unwrap on snapshot in block builder
- **severity:** high
- **category:** bug
- **file:** `crates/db/src/block_builder/write.rs`
- **line:** 54, 77, 272
- **patterns:** bare-unwrap
- **description:** `snapshot.as_ref().unwrap()` on a value that is `None` when `is_create` is true. The code at line 51-55 shows: `if is_create { 1 } else { snapshot.as_ref().unwrap().max_priority() + 1 }`. The else branch only runs when `is_create` is false, but the safety depends entirely on the `if is_create` check. A refactor that changes the control flow could silently introduce a panic. Lines 77 and 272 have the same pattern.
- **training_refs:** rust-patterns-book ch10 "Panics, catch_unwind, and When to Abort"
- **suggested_fix:** Use `snapshot.as_ref().ok_or_else(|| Error::Internal("snapshot required for updates"))?` to make the invariant explicit and fail gracefully.

#### db-2: Oversized file: downsample.rs (2036 lines)
- **severity:** high
- **category:** structure
- **file:** `crates/db/src/downsample.rs`
- **line:** 1-2036
- **patterns:** oversized-file
- **description:** File contains 5 distinct concerns: (1) types/enums for downsample planning (AggregateField, NumericValue, SourceSample, WindowAggregate, PendingWindowAggregate, DownsamplePlan ~lines 1-160), (2) pure utility functions for duration parsing, time conversion, value conversion (~lines 160-560), (3) plan building and validation on `DB<S>` (~lines 560-912), (4) execution logic - processing source docs, persisting windows, aggregating samples (~lines 912-1904), (5) background task loop and event handling (~lines 1904-2036).
- **training_refs:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Split into:
  - `crates/db/src/downsample/mod.rs` -- public API re-exports + GcDownsampleHistoriesOptions (est. ~40 lines)
  - `crates/db/src/downsample/types.rs` -- AggregateField, NumericValue, SourceSample, WindowAggregate, PendingWindowAggregate, DownsamplePlan, SourceKind, ParsedSourceQuery (est. ~160 lines)
  - `crates/db/src/downsample/parse.rs` -- duration parsing, source query parsing, value conversion utilities (est. ~400 lines)
  - `crates/db/src/downsample/plan.rs` -- build_downsample_plan, validate_downsample_collection, downsample_plans, downsample_depth, validate_downsample_cycle (est. ~350 lines)
  - `crates/db/src/downsample/execute.rs` -- process_source_doc_for_plan, persist_window_update, aggregate_samples_into_windows, build_source_samples (est. ~600 lines)
  - `crates/db/src/downsample/gc.rs` -- gc_downsample_histories, gc_source_doc_for_plans, prune_source_doc_history (est. ~250 lines)
  - `crates/db/src/downsample/task.rs` -- start_downsample_task, bootstrap_downsamples, process_downsample_update (est. ~130 lines)

#### db-3: Internal modules exposed publicly
- **severity:** high
- **category:** improvement
- **file:** `crates/db/src/lib.rs`
- **line:** 46-98
- **patterns:** pub-should-be-pub-crate
- **description:** Almost every module in the `db` crate is `pub mod`, including implementation details like `collection_loader`, `collection_cache`, `collection_snapshot`, `commit_priority_index`, `lensed_fetcher`, `lensed_auto_commit_fetcher`, `schema_loader`, `json_patch`, `lens_utils`, `txn_context`, `versioned_fetcher`, `se`, `embedding`. Many of these expose internal types that should not be part of the public API. The crate already re-exports key types at the bottom, so the modules themselves do not need to be public.
- **training_refs:** rust-patterns-book ch15 "Visibility modifiers"
- **suggested_fix:** Change internal modules to `pub(crate) mod` and only keep `pub mod` for modules whose types are part of the intended public API. Use re-exports in lib.rs for specific types that need to be public.

#### db-4: Blocking fs::read in async fn add_lens()
- **severity:** medium
- **category:** anti-pattern
- **file:** `crates/db/src/migration/mod.rs`
- **line:** 50
- **patterns:** blocking-in-async
- **description:** `std::fs::read(path)` is called inside `async fn add_lens()` to load WASM bytes from disk. WASM modules can be large (multiple MB), making this a potentially significant block of the executor thread.
- **training_refs:** async-book ch12 "Blocking the Executor"
- **suggested_fix:** Use `tokio::fs::read(path).await` or `tokio::task::spawn_blocking(move || std::fs::read(path)).await?`.

#### db-5: Mutex::lock().unwrap() throughout merge handler
- **severity:** medium
- **category:** anti-pattern
- **file:** `crates/db/src/merge_handler/composite.rs`
- **line:** 94, 694, 847, 853, 1303, 1322
- **patterns:** bare-unwrap
- **description:** `Mutex::lock().unwrap()` calls throughout the merge handler on `merged_composites`, `batch_merged`, and `pending_events` mutexes. If any thread panics while holding these locks, the Mutex becomes poisoned and all subsequent lock attempts panic, cascading the failure across the entire merge pipeline.
- **training_refs:** rust-patterns-book ch10 "Panics, catch_unwind, and When to Abort"
- **suggested_fix:** Use `.lock().unwrap_or_else(|e| e.into_inner())` to recover from poisoned locks (accepting potentially inconsistent state with logging), or propagate as `Error::LockPoisoned`. The `parking_lot::Mutex` crate doesn't have poisoning, which is another option.

#### db-6: Error::Acp(String) and Error::Lens(String) should wrap typed errors
- **severity:** medium
- **category:** anti-pattern
- **file:** `crates/db/src/error.rs`
- **line:** 78, 81
- **patterns:** string-errors
- **description:** `Error::Acp(String)` and `Error::Lens(String)` variants store error messages as strings instead of wrapping the actual error types. Since both `acp::Error` and `lens::Error` exist as proper `thiserror` enums, these should use `#[from]`.
- **training_refs:** rust-patterns-book ch10 "Error Conversion Chains (#[from])"
- **suggested_fix:** Change to `Acp(#[from] acp::Error)` and `Lens(#[from] lens::Error)`.

#### db-7: Box<dyn Error> for embedding subsystem
- **severity:** medium
- **category:** anti-pattern
- **file:** `crates/db/src/embedding.rs`
- **line:** 9-11
- **patterns:** box-dyn-error
- **description:** `type EmbeddingError = Box<dyn std::error::Error + Send + Sync>` used as the error type for the embedding subsystem. This type-erases all embedding errors, making it impossible to distinguish between network errors, JSON parse errors, and API errors without string matching.
- **training_refs:** rust-patterns-book ch10 "thiserror vs anyhow -- Library vs Application"
- **suggested_fix:** Define an `EmbeddingError` enum with variants for network, parsing, and API errors.

#### db-8: Oversized file: merge_handler/composite.rs (1372 lines)
- **severity:** medium
- **category:** structure
- **file:** `crates/db/src/merge_handler/composite.rs`
- **line:** 1-1372
- **patterns:** oversized-file
- **description:** Single `process_composite_delta` method with nested transaction handling, field iteration, headstore writes, event emission, and post-commit hooks. Contains 3 distinct phases: (1) head processing and field delta iteration (~lines 1-400), (2) document storage writes within transaction (~lines 400-800), (3) headstore updates and event emission (~lines 800-1372).
- **training_refs:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Split into:
  - `crates/db/src/merge_handler/composite.rs` -- process_composite_delta orchestrator (est. ~300 lines)
  - `crates/db/src/merge_handler/composite_fields.rs` -- field delta processing loop, value merging (est. ~400 lines)
  - `crates/db/src/merge_handler/composite_headstore.rs` -- headstore key writes, priority encoding (est. ~350 lines)
  - `crates/db/src/merge_handler/composite_events.rs` -- event emission, post-commit hooks (est. ~200 lines)

#### db-9: Oversized file: merge_handler/mod.rs (1100 lines)
- **severity:** medium
- **category:** structure
- **file:** `crates/db/src/merge_handler/mod.rs`
- **line:** 1-1100
- **patterns:** oversized-file
- **description:** Despite having 8 sub-modules (batch, collection, composite, counter, definition, hook, lww, se_merge), mod.rs contains: (1) MergeError enum (~60 lines), (2) DbMergeHandler struct and MergeHandler trait impl (~200 lines), (3) block decryption logic (~100 lines), (4) signature verification (~200 lines), (5) inline unit tests for signature verification (~400 lines). The tests alone are 400+ lines.
- **training_refs:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Split into:
  - `crates/db/src/merge_handler/mod.rs` -- MergeError, DbMergeHandler struct, MergeHandler impl dispatch (est. ~300 lines)
  - `crates/db/src/merge_handler/decrypt.rs` -- block decryption logic (est. ~100 lines)
  - `crates/db/src/merge_handler/signature.rs` -- verify_block_signature (est. ~200 lines)
  - `crates/db/src/merge_handler/signature_tests.rs` -- all signature verification tests (est. ~400 lines)

#### db-10: Bare unwrap on single-doc fast path
- **severity:** medium
- **category:** anti-pattern
- **file:** `crates/db/src/auto_commit_mutator/create_many.rs`
- **line:** 18
- **patterns:** bare-unwrap
- **description:** `docs.into_iter().next().unwrap()` on the single-doc fast path. The `docs.len() == 1` check on line 17 guarantees this is safe, but the unwrap is redundant with the length check and would panic if the length check were removed.
- **training_refs:** rust-patterns-book ch10 "Panics, catch_unwind, and When to Abort"
- **suggested_fix:** Use `docs.into_iter().next().expect("length checked above")` to document the invariant, or destructure with `if let [doc] = docs.as_slice()`.

#### db-11: Oversized file: tests/index_manager_tests.rs (1541 lines)
- **severity:** low
- **category:** structure
- **file:** `crates/db/tests/index_manager_tests.rs`
- **line:** 1-1541
- **patterns:** oversized-file
- **description:** Test file with 30+ test functions covering index CRUD, composite indexes, unique constraints, bulk operations, iteration, edge cases. All tests share a common `test_schema()` helper.
- **training_refs:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Split into:
  - `crates/db/tests/index_manager/mod.rs` -- test_schema() helper + basic CRUD tests (est. ~400 lines)
  - `crates/db/tests/index_manager/composite.rs` -- composite index tests (est. ~350 lines)
  - `crates/db/tests/index_manager/unique.rs` -- unique constraint tests (est. ~300 lines)
  - `crates/db/tests/index_manager/iteration.rs` -- range/prefix iteration tests (est. ~300 lines)
  - `crates/db/tests/index_manager/edge_cases.rs` -- edge cases, bulk ops, error handling (est. ~200 lines)

#### db-12: Vec param should accept slice in GcDownsampleHistoriesOptions
- **severity:** low
- **category:** improvement
- **file:** `crates/db/src/downsample.rs`
- **line:** 69
- **patterns:** vec-param-should-be-slice
- **description:** `GcDownsampleHistoriesOptions::with_names(names: Vec<String>)` takes owned Vec. Since this is a builder-style constructor that stores the value, this is borderline acceptable, but `impl Into<Vec<String>>` or accepting `&[impl Into<String>]` would be more flexible.
- **training_refs:** rust-patterns-book ch15 "Ergonomic Parameter Patterns"
- **suggested_fix:** Consider `pub fn with_names(names: impl Into<Vec<String>>)` to accept both `Vec<String>` and conversions from iterators.

#### db-13: Oversized file: runner/plan.rs (873 lines)
- **severity:** low
- **category:** structure
- **file:** `crates/query/src/runner/plan.rs`
- **line:** 1-873
- **patterns:** oversized-file
- **description:** Plan execution with explain rendering (simple, execute, debug) and result collection. Explain logic is half the file.
- **training_refs:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** The `runner/explain/` directory already exists with mutation.rs and execute.rs. Move remaining explain logic from plan.rs into `runner/explain/` if not already there, reducing plan.rs to ~400 lines.

**NOTE:** This finding was reported in the file-structure audit under query crate but the file path is in `query`. Cross-listed here as it was noted in the db section of the audit but belongs to query. Moved to query crate in numbering.

#### db-14 (renumbered): Oversized file: runner/plan.rs
Removed -- see query crate. Renumbering below.

---

**Correction:** db-13 above is actually a query finding. Removing and renumbering. The db crate has 16 findings (after removing db-13 which belongs to query).

Let me re-state: db has findings db-1 through db-12 as listed above, plus the following:

#### db-13: String error type for batch signing
- **severity:** medium
- **category:** improvement
- **file:** `crates/crypto/src/batch.rs`
- **line:** 33
- **patterns:** string-error-should-be-typed
- **description:** `pub fn sign_batch(cids: &[Cid], config: &SigningConfig) -> Result<BatchSignature, String>` returns `String` as the error type. The training material explicitly recommends structured error types over opaque strings.
- **training_refs:** rust-patterns-book ch15 "Case Study: Designing a Public Crate API"
- **suggested_fix:** Define a `BatchSignError` enum with variants for the different failure modes (missing key, invalid key type, signing failed).

**NOTE:** This is actually a crypto crate finding. See crypto-2.

---

**Final db crate count: 12 findings (db-1 through db-12).**

---

### ffi (13 findings)

#### ffi-1: Missing catch_unwind on extern "C" functions (merged: error-handling + unsafe audit)
- **severity:** high
- **category:** anti-pattern
- **file:** `crates/ffi/src/mobile.rs`
- **line:** 597-624
- **patterns:** missing-catch-unwind, missing-catch-unwind-on-extern-c
- **description:** Three `extern "C"` functions -- `defra_mobile_peer_info` (line 597), `defra_mobile_connect` (line 608), and `defra_mobile_notify_network_change` (line 618) -- execute code before delegating to inner FFI functions that have `ffi_entry!`. The code between the outer `extern "C"` boundary and the inner call (specifically `default_identity_cstring()` on lines 598, 609, 619) is NOT wrapped in `catch_unwind`. If `default_identity_cstring` panics, the panic will unwind across the FFI boundary, which is undefined behavior. These functions also lack the `ffi_entry!` macro for `catch_unwind` protection that the rest of the FFI functions use.
- **training_refs:** rust-patterns-book ch10 "Panics, catch_unwind, and When to Abort"; rust-patterns-book ch12 "FFI Patterns" -- "Calling Rust from C"; engineering-book ch5 "Miri, Valgrind, and Sanitizers" -- the decision tree mandates `catch_unwind` at every FFI boundary
- **suggested_fix:** Wrap each function body in `ffi_entry! { ... }`. For example: `pub extern "C" fn defra_mobile_peer_info(node_ptr: usize) -> FfiResult { ffi_entry! { let identity = ... } }`.

#### ffi-2: Missing catch_unwind on defra_init() and defra_version()
- **severity:** high
- **category:** anti-pattern
- **file:** `crates/ffi/src/lib.rs`
- **line:** 191-209
- **patterns:** missing-catch-unwind-on-extern-c
- **description:** `defra_init()` (line 191) and `defra_version()` (line 203) are `extern "C"` functions without `ffi_entry!`. While `defra_init` is extremely unlikely to panic (it calls `init_runtime()` which handles errors internally and stores to an atomic), `defra_version` calls `CString::new(...).unwrap_or_else(...)` which theoretically cannot panic but still lacks the safety net. The `defra_init` function is more concerning because `init_runtime()` calls `tokio::runtime::Builder::new_multi_thread().enable_all().build()` which could theoretically panic in exotic conditions.
- **training_refs:** rust-patterns-book ch12 "FFI Patterns" -- every exported `extern "C"` function should use `catch_unwind`
- **suggested_fix:** Wrap both in `ffi_entry!`. For `defra_init()` which returns `void`, add a return type (e.g., `FfiResult`) or use a specialized panic-catching wrapper that ignores the return value.

#### ffi-3: Missing catch_unwind on defra_mobile_close_node
- **severity:** high
- **category:** anti-pattern
- **file:** `crates/ffi/src/mobile.rs`
- **line:** 458-459
- **patterns:** missing-catch-unwind-on-extern-c
- **description:** `defra_mobile_close_node` is `extern "C"` and directly delegates to `node_close(node_ptr)` without `ffi_entry!`. While `node_close` itself uses `ffi_entry!`, this is a coincidence of implementation -- the function signature at the `extern "C"` boundary is the contract that matters. If `node_close`'s implementation were ever changed to not use `ffi_entry!`, this would silently become unsound.
- **training_refs:** rust-patterns-book ch12 "FFI Patterns"
- **suggested_fix:** Wrap in `ffi_entry!` for defense-in-depth: `pub extern "C" fn defra_mobile_close_node(node_ptr: usize) -> FfiResult { ffi_entry! { node_close(node_ptr) } }`.

#### ffi-4: Missing SAFETY comment and length bound on from_raw_parts (identity)
- **severity:** medium
- **category:** improvement
- **file:** `crates/ffi/src/acp/identity.rs`
- **line:** 306
- **patterns:** missing-safety-comment-from-raw-parts
- **description:** `std::slice::from_raw_parts(public_key_ptr, public_key_len)` is called without a `// SAFETY:` comment. The null check on line 300 validates the pointer is non-null and length is non-zero, but there is no upper bound check on `public_key_len`. A malicious or buggy caller could pass `usize::MAX` as the length, causing `from_raw_parts` to create a slice spanning invalid memory.
- **training_refs:** rust-patterns-book ch12 "The three rules of sound unsafe code" -- "Document invariants -- every SAFETY comment explains why the operation is valid"
- **suggested_fix:** Add a maximum length check (e.g., `if public_key_len > 256 { return Err(...) }`) and add a `// SAFETY:` comment: `// SAFETY: public_key_ptr is non-null (checked above), and public_key_len is bounded by the max check. The caller (Go FFI) guarantees the pointer is valid for public_key_len bytes.`

#### ffi-5: Missing SAFETY comment on from_raw_parts (signing key)
- **severity:** medium
- **category:** improvement
- **file:** `crates/ffi/src/node.rs`
- **line:** 136-142
- **patterns:** missing-safety-comment-from-raw-parts
- **description:** `std::slice::from_raw_parts(options.signing_private_key, options.signing_private_key_len)` has a null check (line 129) and a max length check (`MAX_PRIVATE_KEY_LEN` = 128, line 130-134), which is good. However, there is no `// SAFETY:` comment documenting why the operation is sound.
- **training_refs:** rust-patterns-book ch12 "The three rules of sound unsafe code" -- "Document invariants"
- **suggested_fix:** Add `// SAFETY: signing_private_key is non-null (checked on line 129), and signing_private_key_len <= MAX_PRIVATE_KEY_LEN (checked on line 130). The caller guarantees the pointer is valid for this many bytes.`

#### ffi-6: Missing SAFETY comment on from_raw_parts (sourcehub signer key)
- **severity:** medium
- **category:** improvement
- **file:** `crates/ffi/src/node.rs`
- **line:** 181-186
- **patterns:** missing-safety-comment-from-raw-parts
- **description:** Same pattern as ffi-5 for `sourcehub_signer_key`. The null/length checks exist (lines 169-178) but the `from_raw_parts` call lacks a `// SAFETY:` comment.
- **training_refs:** rust-patterns-book ch12 "The three rules of sound unsafe code"
- **suggested_fix:** Add `// SAFETY:` comment documenting the precondition checks.

#### ffi-7: Missing SAFETY comment on from_raw_parts (SE key)
- **severity:** medium
- **category:** improvement
- **file:** `crates/ffi/src/se_key.rs`
- **line:** 38
- **patterns:** missing-safety-comment-from-raw-parts
- **description:** `std::slice::from_raw_parts(key_ptr, key_len)` has excellent validation (null check line 27, exact length check line 31) but no `// SAFETY:` comment.
- **training_refs:** rust-patterns-book ch12 "The three rules of sound unsafe code"
- **suggested_fix:** Add `// SAFETY: key_ptr is non-null (checked above) and key_len == 32 (checked above). The caller guarantees the pointer is valid for 32 bytes.`

#### ffi-8: Missing SAFETY comment on c_str_to_string (query)
- **severity:** low
- **category:** improvement
- **file:** `crates/ffi/src/query/mod.rs`
- **line:** 61
- **patterns:** missing-safety-comment
- **description:** `unsafe { c_str_to_string(identity_did) }` in `check_and_set_dac_bypass` lacks a `// SAFETY:` comment. The function is `pub(crate)` and receives a raw pointer from FFI context, but doesn't document the safety invariant.
- **training_refs:** rust-patterns-book ch12 "The three rules of sound unsafe code"
- **suggested_fix:** Add `// SAFETY: identity_did is either null or a valid C string from the FFI caller.`

#### ffi-9: Missing SAFETY comment on c_str_to_string (NAC check)
- **severity:** low
- **category:** improvement
- **file:** `crates/ffi/src/nac_check.rs`
- **line:** 41
- **patterns:** missing-safety-comment
- **description:** `unsafe { c_str_to_string(identity_did) }` in `check_nac_permission` lacks a `// SAFETY:` comment. The function's parameter is a raw pointer but the function itself is safe (not `unsafe fn`), so callers don't get a compiler warning about the contract.
- **training_refs:** rust-patterns-book ch12 "The three rules of sound unsafe code"
- **suggested_fix:** Either make `check_nac_permission` an `unsafe fn` (since it requires a valid C string pointer), or add a `// SAFETY:` comment and document the requirement in the function's doc comment.

#### ffi-10: SeqCst for ID counters in state registry
- **severity:** medium
- **category:** improvement
- **file:** `crates/ffi/src/state/registry.rs`
- **line:** 30, 108
- **patterns:** seqcst-for-id-counter
- **description:** `SeqCst` is used for handle generation counters in the FFI state registry. These counters only need uniqueness (no two callers get the same ID). `SeqCst` establishes a total ordering across all atomic operations on all variables, which is far stronger than needed for a simple counter.
- **training_refs:** rust-patterns-book ch6 "Shared State: Arc, Mutex, RwLock, Atomics" -- "Atomics: Lock-free for simple values"
- **suggested_fix:** Use `Ordering::Relaxed` for monotonic ID/handle counters where the only invariant is uniqueness. `fetch_add` with `Relaxed` still guarantees atomicity (no two threads get the same value).

#### ffi-11: repr(C) struct padding in NodeInitOptions
- **severity:** low
- **category:** improvement
- **file:** `crates/ffi/src/types.rs`
- **line:** 165
- **patterns:** repr-c-alignment
- **description:** `NodeInitOptions` is `#[repr(C)]` with mixed field types (pointers, `c_int`, `u16`, `usize`, `u32`, `f64`). The `iroh_bind_port: u16` field at line 208 sits between pointer-sized fields, which will cause 6 bytes of padding on 64-bit platforms. While correct, the struct could be reordered to minimize padding. However, since this is an FFI struct matching Go's layout, reordering requires coordinating with the Go side.
- **training_refs:** rust-patterns-book ch11 "Binary Data and repr(C)"
- **suggested_fix:** No action needed for correctness. If performance matters (it doesn't -- this is init-only), reorder fields to minimize padding. Document the intentional layout choice.

#### ffi-12: Missing catch_unwind coverage in mobile.rs (additional functions)
- **severity:** low
- **category:** improvement
- **file:** `crates/ffi/src/mobile.rs`
- **line:** 597-623
- **patterns:** missing-catch-unwind
- **description:** `defra_mobile_peer_info`, `defra_mobile_connect`, and `defra_mobile_notify_network_change` delegate to other FFI functions that do use `ffi_entry!`, so in practice panics are caught one level deeper. However, if `default_identity_cstring()` panics, the unwind would cross the FFI boundary. (This overlaps with ffi-1 but was separately reported in the error-handling audit with lower severity as an improvement rather than an anti-pattern.)
- **training_refs:** rust-patterns-book ch10 "Panics, catch_unwind, and When to Abort"
- **suggested_fix:** See ffi-1 suggested fix.

**NOTE:** This finding is subsumed by ffi-1. Retained for traceability to the error-handling audit but no separate action needed.

#### ffi-13: Missing catch_unwind on defra_version and defra_init (error-handling audit)
- **severity:** low
- **category:** improvement
- **file:** `crates/ffi/src/lib.rs`
- **line:** 191, 203
- **patterns:** missing-catch-unwind
- **description:** `defra_version()` and `defra_init()` should also be wrapped. (Overlaps with ffi-2 but reported from error-handling audit at lower severity.)
- **training_refs:** rust-patterns-book ch10 "Panics, catch_unwind, and When to Abort"
- **suggested_fix:** See ffi-2 suggested fix.

**NOTE:** This finding is subsumed by ffi-2. Retained for traceability.

---

### p2p (13 findings)

#### p2p-1: Lock held across await in two-stream messaging handlers
- **severity:** high
- **category:** anti-pattern
- **file:** `crates/p2p/src/host/command_handler/messaging.rs`
- **line:** 119-143
- **patterns:** lock-held-across-await
- **description:** Multiple handler functions (`handle_send_doc_sync_request`, `handle_send_doc_sync_response`, `handle_send_branchable_sync_request`, `handle_send_branchable_sync_response`, `handle_send_se_artifacts`, `handle_send_car_request`, `handle_send_car_response`) acquire the `tokio::sync::Mutex` on `two_stream_handler` and then call `.await` on network I/O while still holding the lock guard. For example, at line 137: `let mut h = handler.lock().await;` followed by `h.send_doc_sync_request_fire_and_forget(peer_id, request).await;`. This serializes all outbound two-stream operations through a single mutex, creating a bottleneck. If a network write stalls, all other two-stream operations are blocked. Contrast with `handle_send_two_stream_request` (line 64-111) which correctly releases the lock between sending and waiting for the response.
- **training_refs:** async-book ch8 "Tokio Sync Primitives" -- "don't use std::sync::Mutex across .await points"
- **suggested_fix:** Follow the pattern already used in `handle_send_two_stream_request`: acquire the lock, perform the minimal stream-opening operation, release the lock, then do any I/O outside the lock scope. For fire-and-forget sends, the handler methods could return a `Future` or stream handle that completes the write without the lock.

#### p2p-2: Select starvation in P2P host event loop
- **severity:** medium
- **category:** anti-pattern
- **file:** `crates/p2p/src/host/p2p_host/mod.rs`
- **line:** 326-354
- **patterns:** select-starvation
- **description:** The P2P host event loop uses `biased` select with swarm events as the highest priority. The comment at line 327 explains this is intentional for ordering guarantees. However, under heavy peer activity (many connections, frequent gossip), the swarm branch will always be ready, starving the command channel. This means `HostCommand::Shutdown` cannot be delivered, potentially causing the node to hang during shutdown. The two-stream events channel has the same starvation risk as commands.
- **training_refs:** async-book ch12 "select! Fairness and Starvation"
- **suggested_fix:** Process swarm events in a batch (drain up to N events per iteration) then always poll the command channel once per iteration. Alternatively, add a `CancellationToken` that is checked at the top of the loop, independent of `select!`: `if self.cancel_token.is_cancelled() { break; }`.

#### p2p-3: Hot-path clone of PushLogBroadcast in broadcaster
- **severity:** high
- **category:** anti-pattern
- **file:** `crates/p2p/src/sync/broadcaster.rs`
- **line:** 79-83
- **patterns:** hot-path-clone
- **description:** `broadcast_update()` clones the entire `PushLogBroadcast` twice -- once for the document topic publish and once for the collection topic. `PushLogBroadcast` contains `block: Vec<u8>` (the full IPLD block bytes) and `cid: Vec<u8>`. For a typical 4 KB block, this is ~8 KB of deep copies per broadcast. Every local document write triggers this.
- **training_refs:** rust-patterns-book ch11 "bytes::Bytes -- Reference-Counted Buffers"
- **suggested_fix:** Change `PushLogBroadcast.block` and `.cid` from `Vec<u8>` to `bytes::Bytes`. Clone becomes O(1). Alternatively, pass the broadcast by `Arc` to the transport layer so both publishes share the same allocation.

#### p2p-4: N*M deep copies in push_dag_to_replicators
- **severity:** high
- **category:** anti-pattern
- **file:** `crates/p2p/src/sync/coordinator/broadcast.rs`
- **line:** 108-113
- **patterns:** hot-path-clone
- **description:** In `push_dag_to_replicators`, for each replicator peer, each DAG block's data is cloned into a new `PushLogRequest` (`block_data.clone()` on line 113). If there are N replicators and M blocks in a DAG, this produces N*M copies of each block's bytes. For a 10-block DAG with 3 replicators, that is 30 deep copies of block data.
- **training_refs:** rust-patterns-book ch11 "bytes::Bytes -- Reference-Counted Buffers"
- **suggested_fix:** Store DAG block data as `Arc<Vec<u8>>` or `bytes::Bytes` in the `dag_blocks` vector so cloning into each `PushLogRequest` is O(1). The `PushLogRequest.block` field should also be `Bytes`.

#### p2p-5: anyhow::Result in library crate (bitswap store)
- **severity:** medium
- **category:** anti-pattern
- **file:** `crates/p2p/src/bitswap/store.rs`
- **line:** 15
- **patterns:** anyhow-in-library
- **description:** The `p2p` library crate uses `anyhow::Result` as the return type for `Store` trait implementations. While the `iroh_bitswap::Store` trait requires `anyhow::Result`, the `anyhow` dependency leaks into a library crate. The `map_err(|e| anyhow!(...))` calls also erase the original typed error.
- **training_refs:** rust-patterns-book ch10 "thiserror vs anyhow -- Library vs Application"
- **suggested_fix:** This is partially forced by the upstream `iroh_bitswap::Store` trait contract. Add a code comment documenting this constraint. If the trait is local, consider switching it to a typed error.

#### p2p-6: Semaphore expect("semaphore closed") panics during shutdown
- **severity:** medium
- **category:** anti-pattern
- **file:** `crates/p2p/src/two_stream/runner.rs`
- **line:** 103, 142, 176, 222, 252, 272
- **patterns:** bare-expect
- **description:** Six `.acquire().await.expect("semaphore closed")` calls in the P2P stream runner. Semaphore `acquire` only fails when the semaphore is closed, which indicates a shutdown race condition. Panicking on shutdown is user-hostile -- the node should gracefully exit instead.
- **training_refs:** rust-patterns-book ch10 "Panics, catch_unwind, and When to Abort"
- **suggested_fix:** Handle `Err` by breaking out of the loop (returning early), which is the correct behavior during shutdown. This matches the graceful shutdown pattern from the async training material (ch13).

#### p2p-7: PushLogBroadcast from_request/to_request deep clones
- **severity:** medium
- **category:** improvement
- **file:** `crates/p2p/src/message/pushlog.rs`
- **line:** 283-303
- **patterns:** hot-path-clone
- **description:** `PushLogBroadcast::from_request()` deep-clones every field including `block: Vec<u8>` and `cid: Vec<u8>`. Similarly, `to_request()` deep-clones everything back. These conversions happen in the P2P hot path (every incoming/outgoing message). With `Bytes` fields, these conversions would be O(1).
- **training_refs:** rust-patterns-book ch11 "bytes::Bytes -- Reference-Counted Buffers"
- **suggested_fix:** Use `Bytes` for `block` and `cid` fields in both `PushLogRequest` and `PushLogBroadcast`. The `from_request`/`to_request` conversions become trivially cheap.

#### p2p-8: serde(flatten) on PushLogRequest causes performance and compatibility issues
- **severity:** medium
- **category:** anti-pattern
- **file:** `crates/p2p/src/message/pushlog.rs`
- **line:** 16
- **patterns:** serde-flatten-cbor
- **description:** `PushLogRequest` uses `#[serde(flatten)]` on the `metadata` field. The code comments on `PushLogReply` (line 119-122) explicitly note that `#[serde(flatten)]` is NOT used because "serde_cbor produces indefinite-length maps when flatten is used... causing signature verification to fail." Yet `PushLogRequest` still uses flatten. This inconsistency may cause subtle signature issues. Additionally, `#[serde(flatten)]` has a known performance cost -- serde buffers the entire struct into an intermediate map representation, adding allocation overhead per message.
- **training_refs:** rust-patterns-book ch11 "Common serde Attributes"
- **suggested_fix:** Remove `#[serde(flatten)]` from `PushLogRequest` (and `DocSyncRequest`, `BranchableSyncRequest`, `SEKeyRequest`) and duplicate the metadata fields directly, matching the pattern used by `PushLogReply`. This fixes both the performance overhead and potential wire compatibility issues.

#### p2p-9: Oversized file: iroh/endpoint.rs (1420 lines)
- **severity:** high
- **category:** structure
- **file:** `crates/p2p/src/iroh/endpoint.rs`
- **line:** 1-1420
- **patterns:** oversized-file
- **description:** File contains 4 distinct concerns: (1) types and config (IrohEndpointConfig, TopicSubscription, ActiveSync ~lines 1-77), (2) endpoint spawning and main event loop (~lines 77-500), (3) command dispatch - a massive match on IrohCommand variants (~lines 500-900), (4) free functions for fire-and-forget, request-response, block sync, relay/discovery/bind config (~lines 900-1420).
- **training_refs:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Split into:
  - `crates/p2p/src/iroh/endpoint/mod.rs` -- IrohEndpointConfig, spawn_endpoint, main loop (est. ~350 lines)
  - `crates/p2p/src/iroh/endpoint/commands.rs` -- handle_command dispatch + per-command handlers (est. ~400 lines)
  - `crates/p2p/src/iroh/endpoint/transport.rs` -- fire_and_forget, request_response, block_sync (est. ~350 lines)
  - `crates/p2p/src/iroh/endpoint/config.rs` -- relay_mode_from_config, apply_discovery_config, apply_bind_config (est. ~120 lines)

#### p2p-10: Unnecessary full block read for get_size in bitswap
- **severity:** low
- **category:** improvement
- **file:** `crates/p2p/src/bitswap/store.rs`
- **line:** 62-66
- **patterns:** unnecessary-read
- **description:** `BitswapStoreAdapter::get_size()` fetches the full block data (`self.blockstore.get(cid)`) just to call `.len()` on it. The blockstore has a dedicated `get_size()` method that avoids reading the entire block.
- **training_refs:** rust-patterns-book ch11 "bytes::Bytes -- Reference-Counted Buffers"
- **suggested_fix:** Use `self.blockstore.get_size(cid)` instead of `self.blockstore.get(cid).map(|data| data.len())`. This avoids reading and potentially allocating the full block data just to measure its size.

#### p2p-11: Untracked spawn for per-connection handler tasks (Iroh)
- **severity:** low
- **category:** improvement
- **file:** `crates/p2p/src/iroh/endpoint.rs`
- **line:** 246, 267, 873
- **patterns:** untracked-spawn
- **description:** Per-connection and per-stream handler tasks are spawned via `tokio::spawn` without collecting JoinHandles. These are fire-and-forget tasks that naturally terminate when connections close. During shutdown (lines 173-179), subscription reader tasks and active sync tasks are properly aborted, but in-flight connection handler tasks are not. In practice, these tasks will exit when the endpoint drops and connections reset, but there is a brief window where they continue running after the event loop has exited.
- **training_refs:** async-book ch13 "Structured Concurrency: JoinSet and TaskTracker"
- **suggested_fix:** Use a `JoinSet` or `CancellationToken` shared with connection handler tasks so they can be signaled during shutdown. Given these tasks self-terminate on connection close, this is a robustness improvement rather than a bug fix.

#### p2p-12: Unnecessary alloc for MetaData version string
- **severity:** low
- **category:** improvement
- **file:** `crates/p2p/src/message/metadata.rs`
- **line:** 48-52
- **patterns:** unnecessary-alloc
- **description:** `MetaData::new()` and `set_version()` call `MESSAGE_VERSION.to_string()` which allocates a new String. `MESSAGE_VERSION` is a static `&str`. Since this is called once per P2P message construction, the allocation is minor but could be avoided with a `Cow<'static, str>` version field.
- **training_refs:** rust-patterns-book ch11 "Zero-Copy Deserialization"
- **suggested_fix:** This is low priority since message construction is not the bottleneck. If optimizing, change `version: String` to `Cow<'static, str>` and initialize with `Cow::Borrowed(MESSAGE_VERSION)`. However, the serde CBOR compatibility constraints may make this impractical.

#### p2p-13: Oversized file: sync/replication/mod.rs (883 lines)
- **severity:** medium
- **category:** structure
- **file:** `crates/p2p/src/sync/replication/mod.rs`
- **line:** 1-883
- **patterns:** oversized-file
- **description:** Contains ReplicationLoop, ReplicationConfig, ReplicationResult, parallel worker pool, batch processing, and retry logic in one file.
- **training_refs:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Split into `replication/mod.rs` (types + re-exports ~100 lines), `replication/loop.rs` (ReplicationLoop ~400 lines), `replication/worker.rs` (parallel worker pool ~380 lines).

#### p2p-14: Unnecessary SAFETY comment on safe code
- **severity:** low
- **category:** improvement
- **file:** `crates/p2p/src/sync/dag_sync/config.rs`
- **line:** 98-99
- **patterns:** unnecessary-safety-comment
- **description:** The comment `// SAFETY: 16 is non-zero` is on a call to `NonZeroUsize::new(16).unwrap()`. This is not actually unsafe code -- `NonZeroUsize::new` returns `Option`, and `.unwrap()` is safe Rust that will never panic because 16 is provably non-zero. The `SAFETY` comment is misleading because it implies there's an `unsafe` block.
- **training_refs:** rust-patterns-book ch12 "The three rules of sound unsafe code" -- SAFETY comments should only appear on `unsafe` blocks
- **suggested_fix:** Change comment to a regular comment: `// 16 is non-zero, so unwrap is safe.` or remove it entirely since the intent is obvious.

---

### defra-core (11 findings)

#### defra-core-1: Stringly-typed SigningConfig.key_type
- **severity:** high
- **category:** anti-pattern
- **file:** `crates/defra-core/src/signing.rs`
- **line:** 44
- **patterns:** raw-primitive-id
- **description:** `SigningConfig.key_type` is a raw `String` that is matched against string literals ("ed25519", "secp256k1", "secp256r1", "bls") in `signature_type()` at line 84. Any typo in a string literal silently falls through to the error branch at runtime. There is already a `SignatureType` enum in `block.rs:693` -- the key type should be an enum, not a stringly-typed field. This field crosses crate boundaries (identity, FFI, HTTP) making confusion likely.
- **training_refs:** rust-patterns-book ch3 "Newtype: Zero-Cost Type Safety"
- **suggested_fix:** Replace `pub key_type: String` with a `SigningKeyType` enum:
```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SigningKeyType {
    Ed25519,
    Secp256k1,
    Secp256r1,
    Bls,
}
```
Then `signature_type()` becomes an infallible `From` conversion instead of a `Result`.

#### defra-core-2: Raw u64 priority in CRDT delta payloads; status as magic u8
- **severity:** medium
- **category:** improvement
- **file:** `crates/defra-core/src/block.rs`
- **line:** 324-392
- **patterns:** raw-primitive-id
- **description:** The CRDT delta payload structs (`LwwDeltaPayload`, `CounterDeltaPayload`, `CompositeDeltaPayload`) all use `pub doc_id: Vec<u8>`, `pub schema_version_id: String`, and `pub priority: u64` as raw public fields. The `priority` field is a raw `u64` while `defra-core/src/types.rs:114` defines a `Priority(pub u64)` newtype that is never used in these structs. The `status: u8` field at line 391 is a magic number (1 = active, 2 = deleted) that should be an enum.
- **training_refs:** rust-patterns-book ch3 "Newtype: Zero-Cost Type Safety"
- **suggested_fix:** Use the existing `Priority` newtype for `priority` fields. Replace `status: u8` with:
```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum DocumentStatus {
    Active = 1,
    Deleted = 2,
}
```

#### defra-core-3: DocId::new() accepts any string without validation
- **severity:** medium
- **category:** improvement
- **file:** `crates/defra-core/src/types.rs`
- **line:** 10-16
- **patterns:** parse-dont-validate
- **description:** `DocId::new()` accepts any string without validation. The format is documented as `"bae-<base32-encoded-bytes>"` but `DocId::new("hello")` succeeds. Meanwhile, `crates/document/src/doc_id.rs` has a `DocID` type with full parsing/validation. The `defra-core::DocId` is used throughout the core API (`Document.id`, `DocumentUpdate.id`) as a pass-through wrapper providing no guarantees. Consumers cannot trust that a `DocId` is actually valid without re-parsing.
- **training_refs:** type-driven-correctness-book ch7 "Parse, Don't Validate"
- **suggested_fix:** Either (a) add validation to `DocId::new()` matching the `bae-` prefix format, or (b) consolidate on the `document::DocID` type which already validates. If backward compatibility requires accepting arbitrary strings, add `DocId::parse(s: &str) -> Result<Self>` and deprecate `new()`.

#### defra-core-4: Public error enums lack #[non_exhaustive]
- **severity:** medium
- **category:** improvement
- **file:** `crates/defra-core/src/error.rs`
- **line:** 10
- **patterns:** missing-non-exhaustive
- **description:** `defra_core::Error` is a public enum with 14 variants that any downstream crate can exhaustively match on. Adding a new error variant would be a breaking change for any external consumer that has a `match` without a wildcard arm. The same applies to `schema::SchemaError`, `acp::Error`, `zanzibar::Error`, `document::Error`, and `identity::Error`. Only `NodePermission` currently has `#[non_exhaustive]`.
- **training_refs:** rust-patterns-book ch3 "The Newtype and Type-State Patterns"
- **suggested_fix:** Add `#[non_exhaustive]` to all public error enums across crates: `defra_core::Error`, `schema::SchemaError`, `acp::Error`, `zanzibar::Error`, `document::Error`, `identity::Error`. This is especially important for `defra_core::Error` since it is re-exported as the foundational error type.

#### defra-core-5: CrdtDelta and SignatureType lack #[non_exhaustive]
- **severity:** medium
- **category:** improvement
- **file:** `crates/defra-core/src/block.rs`
- **line:** 220-248
- **patterns:** missing-non-exhaustive
- **description:** `CrdtDelta` is a public enum with 7 variants. New CRDT types (e.g., a Set CRDT or RGA for text) would require adding variants. Same applies to `SignatureType` at line 693 (4 variants, new algorithms will be added). Both are used across crate boundaries and should be `#[non_exhaustive]`.
- **training_refs:** rust-patterns-book ch3 "The Newtype and Type-State Patterns"
- **suggested_fix:** Add `#[non_exhaustive]` to `CrdtDelta` and `SignatureType`.

#### defra-core-6: Encryption key stored as raw Vec<u8> with no zeroization
- **severity:** medium
- **category:** improvement
- **file:** `crates/defra-core/src/encryption.rs`
- **line:** 12-16
- **patterns:** raw-primitive-id
- **description:** `EncryptionConfig.encryption_key` is a raw `Vec<u8>` with no type distinction from other `Vec<u8>` fields like `doc_id` or `data`. At the call site in `derive_key()` (line 29), the key bytes, doc_id bytes, and field name bytes are concatenated -- swapping arguments would produce a wrong derived key silently. More critically, `encryption_key` is `Clone`-able and stored in a `HashMap` at line 65, meaning key material is freely duplicated in memory with no zeroization on drop.
- **training_refs:** type-driven-correctness-book ch3 "Single-Use Types -- Cryptographic Guarantees via Ownership"
- **suggested_fix:** Wrap the encryption key in a newtype that implements `Drop` with zeroization:
```rust
pub struct EncryptionKey(Vec<u8>);
impl Drop for EncryptionKey {
    fn drop(&mut self) { self.0.iter_mut().for_each(|b| *b = 0); }
}
```
Consider using the `zeroize` crate for compiler-safe zeroization.

#### defra-core-7: Collection.version is raw u32 instead of SchemaVersion
- **severity:** low
- **category:** improvement
- **file:** `crates/defra-core/src/collection.rs`
- **line:** 16
- **patterns:** raw-primitive-id
- **description:** `Collection.version` is a raw `u32` while the crate defines `SchemaVersion(u32)` at `types.rs:48`. The `Collection` struct uses `CollectionId` for the `id` field but uses a raw `u32` for `version`, inconsistently. The `Collection::new()` constructor at line 20 takes `version: u32` where it should take `SchemaVersion`.
- **training_refs:** rust-patterns-book ch3 "Newtype: Zero-Cost Type Safety"
- **suggested_fix:** Change `pub version: u32` to `pub version: SchemaVersion` and update `Collection::new()` to accept `SchemaVersion`.

#### defra-core-8: Dead anyhow dependency
- **severity:** low
- **category:** anti-pattern
- **file:** `crates/defra-core/Cargo.toml`
- **line:** 13
- **patterns:** anyhow-in-library
- **description:** `defra-core` lists `anyhow` as a dependency in Cargo.toml, but neither actually uses it in source code (grep confirms zero `anyhow::` or `use anyhow` in their `src/` directories). This is dead dependency weight.
- **training_refs:** rust-patterns-book ch10 "thiserror vs anyhow -- Library vs Application"
- **suggested_fix:** Remove `anyhow.workspace = true` from `crates/defra-core/Cargo.toml`.

#### defra-core-9: Oversized test file: block_tests.rs (1080 lines)
- **severity:** low
- **category:** structure
- **file:** `crates/defra-core/tests/block_tests.rs`
- **line:** 1-1080
- **patterns:** oversized-file
- **description:** Tests for Block type covering DAG-CBOR encoding, CID generation, Go compatibility. Natural split by test category.
- **training_refs:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Split into test submodules: `encoding_tests`, `cid_tests`, `go_compat_tests`.

#### defra-core-10: Collection::new name param should use impl Into<String>
- **severity:** low
- **category:** improvement
- **file:** `crates/defra-core/src/collection.rs`
- **line:** 20
- **patterns:** string-param-should-be-impl-into
- **description:** `pub fn new(id: CollectionId, name: String, version: u32)` takes an owned String for name. Since the value is stored, `impl Into<String>` would be more ergonomic.
- **training_refs:** rust-patterns-book ch15 "Ergonomic Parameter Patterns"
- **suggested_fix:** Change to `pub fn new(id: CollectionId, name: impl Into<String>, version: u32)`.

#### defra-core-11: SigningConfig.set_request_bearer_token token param should use impl Into<String>
- **severity:** low
- **category:** improvement
- **file:** `crates/defra-core/src/signing.rs`
- **line:** 194
- **patterns:** string-param-should-be-impl-into
- **description:** `pub fn set_request_bearer_token(did: &str, token: String)` takes `token` as owned String. Since it is stored, `impl Into<String>` would allow passing `&str` without allocation at call sites that already have a `String`.
- **training_refs:** rust-patterns-book ch15 "Ergonomic Parameter Patterns"
- **suggested_fix:** Change to `pub fn set_request_bearer_token(did: &str, token: impl Into<String>)`.

---

### blockstore (7 findings)

#### blockstore-1: Hot-path clone of Vec<u8> on every cache hit
- **severity:** high
- **category:** anti-pattern
- **file:** `crates/blockstore/src/lib.rs`
- **line:** 190-191
- **patterns:** hot-path-clone
- **description:** `Blockstore::get()` clones the cached `Vec<u8>` on every cache hit (`data.clone()` on line 191). This is the primary block read path -- every document query, merge, and P2P sync operation hits this. The LRU cache stores `Vec<u8>` so each clone is an O(n) copy of the full block. For a 4 KB block, this is 4 KB of allocation+copy per read.
- **training_refs:** rust-patterns-book ch11 "bytes::Bytes -- Reference-Counted Buffers"
- **suggested_fix:** Change the cache type from `LruCache<Cid, Vec<u8>>` to `LruCache<Cid, bytes::Bytes>`. `Bytes::clone()` is O(1) refcount increment. The `get()` return type can stay `Vec<u8>` at the trait boundary (call `.to_vec()` only when the caller actually needs mutation), or better yet, change the `Blockstore` trait to return `Bytes`. The `put` path already has `data.to_vec()` which would become `Bytes::copy_from_slice(data)`.

#### blockstore-2: Hot-path to_vec() in put/put_many cache population
- **severity:** high
- **category:** anti-pattern
- **file:** `crates/blockstore/src/lib.rs`
- **line:** 244, 277
- **patterns:** hot-path-to-vec
- **description:** `Blockstore::put()` (line 244) and `put_many()` (line 277) call `data.to_vec()` to populate the write-through cache. In `put_many`, the block bytes are copied into `written: Vec<(Cid, Vec<u8>)>` and then moved into the cache, but the initial copy from the `&[u8]` parameter is unavoidable with `Vec<u8>`. With `Bytes`, if the caller already has `Bytes`, no copy is needed.
- **training_refs:** rust-patterns-book ch11 "bytes::Bytes -- Reference-Counted Buffers"
- **suggested_fix:** Same as blockstore-1: switch cache to `Bytes`. Accept `impl Into<Bytes>` in `put` so callers that already have `Bytes` (e.g., P2P bitswap) avoid the copy entirely.

#### blockstore-3: Relaxed ordering for hash-on-read config flag
- **severity:** medium
- **category:** improvement
- **file:** `crates/blockstore/src/lib.rs`
- **line:** 94, 123, 188, 203, 361
- **patterns:** relaxed-ordering-for-config-flag
- **description:** The `rehash` (`AtomicBool`) flag uses `Ordering::Relaxed` for both loads and stores. This flag controls whether hash verification is performed on block reads. When one thread calls `hash_on_read(true)`, other threads using `Ordering::Relaxed` may not see the update for an arbitrarily long time on weakly-ordered architectures (e.g., ARM). On x86, this happens to work due to strong TSO guarantees, but is not portable. In practice, a delay in enabling hash verification could allow unverified reads after the caller believes verification is active.
- **training_refs:** rust-patterns-book ch6 "Lock-Free Patterns" -- Acquire/Release semantics for publishing data
- **suggested_fix:** Use `Ordering::Release` for the `store` in `hash_on_read()` and `Ordering::Acquire` for the `load` in `get()`. This ensures that when hash verification is enabled, subsequent reads on other threads see the updated value promptly. The `Debug` impl and `rehash_enabled()` accessor can remain `Relaxed` since they are informational.

#### blockstore-4: Unnecessary Vec<u8> allocation for hash verification
- **severity:** medium
- **category:** improvement
- **file:** `crates/blockstore/src/verify.rs`
- **line:** 32
- **patterns:** unnecessary-alloc
- **description:** `verify_block_cid` allocates a `Vec<u8>` for the SHA-256 digest (`hasher.finalize().to_vec()`) solely to compare it with the CID's digest bytes. The `finalize()` output is a fixed-size `[u8; 32]` array (GenericArray) that can be compared directly against the slice without heap allocation.
- **training_refs:** rust-patterns-book ch11 "Binary Data and repr(C)"
- **suggested_fix:** Replace `let computed: Vec<u8> = hasher.finalize().to_vec()` with `let computed = hasher.finalize()` and compare with `mh.digest() != computed.as_slice()`. The same pattern exists in `lib.rs:153-158`.

#### blockstore-5: Unnecessary Vec<u8> allocation for hash verification (lib.rs)
- **severity:** medium
- **category:** improvement
- **file:** `crates/blockstore/src/lib.rs`
- **line:** 153-158
- **patterns:** unnecessary-alloc
- **description:** Same as blockstore-4. `verify_hash()` in the main blockstore file allocates a `Vec<u8>` for the SHA-256 digest just to compare it. This runs on every block read when `hash_on_read` is enabled.
- **training_refs:** rust-patterns-book ch11 "Binary Data and repr(C)"
- **suggested_fix:** Use `hasher.finalize()` directly and compare the `GenericArray` as a slice. Saves one heap allocation per verified block read.

#### blockstore-6: Oversized test file: blockstore_tests.rs (1450 lines)
- **severity:** low
- **category:** structure
- **file:** `crates/blockstore/tests/blockstore_tests.rs`
- **line:** 1-1450
- **patterns:** oversized-file
- **description:** Test file with section comment markers for Basic CRUD, Hash Verification, Merge Tracking, Go Compatibility, Concurrency, Edge Cases/Stress. Each section is a self-contained test group.
- **training_refs:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Split into:
  - `crates/blockstore/tests/blockstore/mod.rs` -- helpers + basic CRUD tests (est. ~200 lines)
  - `crates/blockstore/tests/blockstore/hash_verify.rs` -- hash_on_read tests (est. ~200 lines)
  - `crates/blockstore/tests/blockstore/merge_tracking.rs` -- P2P merge tracking tests (est. ~300 lines)
  - `crates/blockstore/tests/blockstore/go_compat.rs` -- Go compatibility tests (est. ~250 lines)
  - `crates/blockstore/tests/blockstore/concurrency.rs` -- concurrent access tests (est. ~200 lines)
  - `crates/blockstore/tests/blockstore/stress.rs` -- stress/edge case tests (est. ~300 lines)

#### blockstore-7: Duplicate hash verification allocation
- **severity:** low
- **category:** improvement
- **file:** `crates/blockstore/src/lib.rs`
- **line:** 153-158
- **patterns:** unnecessary-alloc
- **description:** See blockstore-5 -- same allocation pattern duplicated in the main lib.rs file.
- **training_refs:** rust-patterns-book ch11 "Binary Data and repr(C)"
- **suggested_fix:** See blockstore-5.

**NOTE:** blockstore-7 is subsumed by blockstore-5. Retained for traceability. No separate action needed.

---

### embedded (6 findings)

#### embedded-1: Blocking filesystem ops in async build()
- **severity:** high
- **category:** anti-pattern
- **file:** `crates/embedded/src/node.rs`
- **line:** 156
- **patterns:** blocking-in-async
- **description:** `std::fs::create_dir_all` is called inside `async fn build()`. This blocks the executor thread while the OS performs directory creation. While this only runs at startup, it sets a bad precedent and could be problematic if the filesystem is slow (network mount, encrypted disk with passphrase prompt).
- **training_refs:** async-book ch12 "Blocking the Executor"
- **suggested_fix:** Use `tokio::fs::create_dir_all(parent).await` or wrap in `tokio::task::spawn_blocking`.

#### embedded-2: Blocking filesystem ops in load_or_generate_iroh_secret_key
- **severity:** high
- **category:** anti-pattern
- **file:** `crates/embedded/src/node.rs`
- **line:** 1236-1267
- **patterns:** blocking-in-async
- **description:** `load_or_generate_iroh_secret_key` performs multiple blocking filesystem operations (`std::fs::read`, `std::fs::create_dir_all`, `std::fs::write`, `std::fs::set_permissions`) and is called from `async fn setup_iroh()` at line 719. All of these block the executor thread. The function is sync (`fn`, not `async fn`), so it blocks the calling async task's executor thread for the entire duration.
- **training_refs:** async-book ch12 "Blocking the Executor"
- **suggested_fix:** Either convert to `async fn` using `tokio::fs::*`, or wrap the call site: `let secret_key = tokio::task::spawn_blocking(move || load_or_generate_iroh_secret_key(path)).await??;`

#### embedded-3: Unbounded channel for PushFailure
- **severity:** medium
- **category:** anti-pattern
- **file:** `crates/embedded/src/node.rs`
- **line:** 633, 749
- **patterns:** unbounded-channel
- **description:** `tokio::sync::mpsc::unbounded_channel::<PushFailure>()` is used for the failure reporting channel in both the libp2p path (line 633) and iroh path (line 749). Under sustained push failure conditions (e.g., a peer is unreachable but keeps being targeted), failures could accumulate without bound, growing memory until the node OOMs.
- **training_refs:** async-book ch13 "Backpressure with Bounded Channels"
- **suggested_fix:** Replace with a bounded channel: `mpsc::channel::<PushFailure>(1024)`. The sender side should use `try_send` and log/drop excess failures rather than applying backpressure to the sync coordinator.

#### embedded-4: Untracked spawn of P2P host task
- **severity:** medium
- **category:** improvement
- **file:** `crates/embedded/src/node.rs`
- **line:** 594-596
- **patterns:** untracked-spawn
- **description:** The P2P host task is spawned with `tokio::spawn` but the `JoinHandle` is immediately dropped. While the host shuts down via the command channel (`HostCommand::Shutdown`), there is no way to await completion of the host task. During shutdown (lines 684-692), the abort list includes `host_event_task`, `replication_task`, `failure_recorder_task`, and `retry_loop_task`, but NOT the host task itself. If the host task is slow to exit (e.g., flushing a large gossip queue), shutdown will not wait for it.
- **training_refs:** async-book ch13 "Structured Concurrency: JoinSet and TaskTracker"
- **suggested_fix:** Capture the JoinHandle: `let host_task = tokio::spawn(async move { host.run().await; });` and include `host_task.abort_handle()` in the `ShutdownHandle::libp2p` abort list, or better yet, await it during shutdown with a timeout.

#### embedded-5: anyhow in library crate
- **severity:** medium
- **category:** anti-pattern
- **file:** `crates/embedded/src/node.rs`
- **line:** 17
- **patterns:** anyhow-in-library
- **description:** The `embedded` library crate uses `anyhow::{anyhow, Context, Result}` throughout its public API. This means callers cannot match on specific error variants and must use downcasting.
- **training_refs:** rust-patterns-book ch10 "thiserror vs anyhow -- Library vs Application"
- **suggested_fix:** Define an `EmbeddedError` enum with `thiserror` in the `embedded` crate and convert to it. Reserve `anyhow` for the binary crates (`cli`, `defra-node`) that consume `embedded`.

#### embedded-6: Oversized file: node.rs (1267 lines)
- **severity:** low
- **category:** structure
- **file:** `crates/embedded/src/node.rs`
- **line:** 1-1267
- **patterns:** oversized-file
- **description:** File contains 5 concerns: (1) EmbeddedNode struct and basic methods (~lines 1-80), (2) BackgroundTasks struct (~lines 80-130), (3) NodeBuilder and build() method (~lines 130-500), (4) free functions for spawning background tasks (~lines 500-1000), (5) restore functions for persisted replicators/documents (~lines 1000-1267).
- **training_refs:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Split into:
  - `crates/embedded/src/node.rs` -- EmbeddedNode struct, BackgroundTasks (est. ~130 lines)
  - `crates/embedded/src/node_builder.rs` -- NodeBuilder + build() method (est. ~400 lines)
  - `crates/embedded/src/background_tasks.rs` -- spawn_* functions for event handlers, replication, failure recording (est. ~400 lines)
  - `crates/embedded/src/restore.rs` -- restore_libp2p_replicators, restore_libp2p_documents, restore_iroh_replicators (est. ~250 lines)

---

### zanzibar (5 findings)

#### zanzibar-1: Five bare &str params in permission check (security risk)
- **severity:** high
- **category:** anti-pattern
- **file:** `crates/zanzibar/src/engine/mod.rs`
- **line:** 138-157
- **patterns:** raw-primitive-id
- **description:** `PermissionEngine::check()` takes five `&str` parameters: `policy_id`, `resource`, `object_id`, `relation`, `subject`. All five are bare `&str` at the call site. Swapping `resource` and `object_id`, or `relation` and `object_id`, compiles fine but produces wrong permission checks -- a silent security bug. The `PermissionCheckRequest` struct at line 17 has the same problem with four `&str` fields. This is the central permission check for the entire ACP system.
- **training_refs:** rust-patterns-book ch3 "Newtype: Zero-Cost Type Safety" (the `create_user(name, email, age, id)` example)
- **suggested_fix:** Introduce newtypes for the three distinct string-domain concepts:
```rust
pub struct PolicyId<'a>(&'a str);
pub struct ResourceName<'a>(&'a str);
pub struct RelationName<'a>(&'a str);
// object_id stays &str -- it is intentionally opaque
```
Then `check()` becomes `fn check(&self, policy: PolicyId, resource: ResourceName, object_id: &str, relation: RelationName, subject: &Did)`. Callers cannot accidentally swap resource and relation.

#### zanzibar-2: Unvalidated Relationship fields enable path traversal in storage keys
- **severity:** high
- **category:** anti-pattern
- **file:** `crates/zanzibar/src/types/relationship.rs`
- **line:** 7-13
- **patterns:** parse-dont-validate
- **description:** `Relationship` has four public `String` fields (`resource`, `object_id`, `relation`, `subject`) with no validation at construction time. The `new()` constructor at line 16 accepts any strings. Compare with the ACP crate's `RelationTuple` at `crates/acp/src/relation.rs:32` which validates path components in `try_new()` to prevent path traversal in storage keys. `Relationship` constructs storage keys at line 39 via `format!("/rel/{}/{}/{}/{}", ...)` with no validation, meaning path traversal is possible. The ACP crate added validation for its own `RelationTuple` but the underlying zanzibar `Relationship` remains unvalidated.
- **training_refs:** type-driven-correctness-book ch7 "Validated Boundaries -- Parse, Don't Validate"
- **suggested_fix:** Add `try_new()` validation to `Relationship` matching what `RelationTuple` does, and make the fields private with accessor methods. Alternatively, make `Relationship::new()` return `Result` and validate path components.

#### zanzibar-3: Did::new_unchecked is pub instead of pub(crate)
- **severity:** high
- **category:** anti-pattern
- **file:** `crates/zanzibar/src/did.rs`
- **line:** 36
- **patterns:** parse-dont-validate
- **description:** `Did::new_unchecked()` is `pub` (not `pub(crate)`) in the zanzibar crate. Any downstream crate can bypass DID validation by calling `Did::new_unchecked()` with an arbitrary string. Compare with `crates/identity/src/did.rs:55` where the same function is correctly scoped as `pub(crate)`. The zanzibar version breaks the "private constructor = unforgeable" principle from the capability token pattern.
- **training_refs:** type-driven-correctness-book ch4 "Zero-Sized Types as Proof Tokens" (private constructor principle)
- **suggested_fix:** Change `pub fn new_unchecked` to `pub(crate) fn new_unchecked` in `crates/zanzibar/src/did.rs:36` to match the identity crate's approach.

#### zanzibar-4: #[non_exhaustive] missing on zanzibar::Error
- **severity:** medium
- **category:** improvement
- **file:** `crates/zanzibar/src/error.rs`
- **line:** 6
- **patterns:** missing-non-exhaustive
- **description:** `zanzibar::Error` is a public enum without `#[non_exhaustive]`. Adding new error variants would be a breaking change for downstream match statements.
- **training_refs:** rust-patterns-book ch3 "The Newtype and Type-State Patterns"
- **suggested_fix:** Add `#[non_exhaustive]` to `zanzibar::Error`.

#### zanzibar-5: Duplicate Did newtype across identity and zanzibar
- **severity:** low
- **category:** improvement
- **file:** `crates/identity/src/did.rs` and `crates/zanzibar/src/did.rs`
- **line:** identity:30, zanzibar:19
- **patterns:** duplicate-newtype
- **description:** There are two separate `Did` newtype implementations with identical structure and nearly identical validation logic: `identity::Did` and `zanzibar::Did`. The zanzibar crate has its own `Did` presumably to avoid a dependency on the identity crate. The ACP crate bridges between them with `to_zdid()`/`from_zdid()` conversion functions. This duplication means validation rules could diverge (and already have -- see zanzibar-3 where `new_unchecked` visibility differs). Having two `Did` types also means every cross-crate call site needs conversion.
- **training_refs:** type-driven-correctness-book ch7 "Validated Boundaries" (single source of truth for validation)
- **suggested_fix:** Extract `Did` into a shared micro-crate (e.g., `defra-did`) that both `identity` and `zanzibar` depend on. This eliminates the conversion layer and ensures validation is consistent.

---

### cli (5 findings)

#### cli-1: Oversized file: commands/start/server.rs (1428 lines)
- **severity:** high
- **category:** structure
- **file:** `crates/cli/src/commands/start/server.rs`
- **line:** 1-1428
- **patterns:** oversized-file
- **description:** Single function `init_store_and_server` spans essentially the entire file. It handles: database initialization, ACP setup, query runner creation, P2P host/coordinator/replication wiring, HTTP server adapter creation (14 different adapters), PG wire protocol setup, and downsample task spawning. This is a God Function doing server assembly.
- **training_refs:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Split into:
  - `crates/cli/src/commands/start/server.rs` -- top-level init_store_and_server that calls subfunctions (est. ~200 lines)
  - `crates/cli/src/commands/start/database.rs` -- database creation, identity setup, embedding config (est. ~200 lines)
  - `crates/cli/src/commands/start/p2p_setup.rs` -- P2P host creation, coordinator, replication loop, event handling (est. ~500 lines)
  - `crates/cli/src/commands/start/http_wiring.rs` -- HTTP server adapter registration (all 14 with_*_arc calls) (est. ~300 lines)
  - `crates/cli/src/commands/start/acp_setup.rs` -- ACP/NAC/SourceHub initialization (est. ~200 lines)

#### cli-2: All adapter modules are pub instead of pub(crate)
- **severity:** high
- **category:** improvement
- **file:** `crates/cli/src/lib.rs`
- **line:** 1-31
- **patterns:** pub-should-be-pub-crate
- **description:** All 20+ adapter modules are declared `pub mod`, making every internal adapter type part of the crate's public API. These adapters (acp_adapter, backup_adapter, block_adapter, collection_mgmt_adapter, doc_acp_adapter, dump_adapter, encrypted_index_adapter, index_adapter, lens_adapter, nac_adapter, schema_adapter, sourcehub_acp_adapter, transport_doc_pusher, transport_version_syncer, txn_adapter, version_syncer, view_adapter) are implementation details used only by `commands/start/server.rs`. Only `cli`, `commands`, `config`, `error`, and `logging` should be `pub`.
- **training_refs:** rust-patterns-book ch15 "Public API Design Checklist" / "Visibility modifiers"
- **suggested_fix:** Change adapter modules to `pub(crate) mod` instead of `pub mod`. Only export modules that external consumers (the `defra` binary) actually need.

#### cli-3: Oversized file: p2p_adapter.rs (1214 lines)
- **severity:** medium
- **category:** structure
- **file:** `crates/cli/src/p2p_adapter.rs`
- **line:** 1-1214
- **patterns:** oversized-file
- **description:** File contains 3 concerns: (1) CollectionLookup, DocPusher trait definitions and DbDocPusher impl (~lines 1-350), (2) P2PAdapter struct with P2POperations impl for HTTP bridge (~lines 350-900), (3) replicator management logic (add_replicator, remove_replicator with complex capability validation ~lines 900-1214).
- **training_refs:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Split into:
  - `crates/cli/src/p2p_adapter/mod.rs` -- re-exports (est. ~20 lines)
  - `crates/cli/src/p2p_adapter/doc_pusher.rs` -- DocPusher trait, CollectionLookup, DbDocPusher (est. ~350 lines)
  - `crates/cli/src/p2p_adapter/operations.rs` -- P2PAdapter struct, basic P2P operations (est. ~400 lines)
  - `crates/cli/src/p2p_adapter/replicator.rs` -- add_replicator, remove_replicator, capability validation (est. ~350 lines)

#### cli-4: Blocking fs::write/read in async backup commands
- **severity:** low
- **category:** anti-pattern
- **file:** `crates/cli/src/commands/client/backup.rs`
- **line:** 81, 94
- **patterns:** blocking-in-async
- **description:** `std::fs::write` (line 81) and `std::fs::read_to_string` (line 94) are used in `async fn execute()` methods. Backup files can be large, making these significant blocking calls. However, the CLI context has minimal concurrent async work, so the practical impact is low.
- **training_refs:** async-book ch12 "Blocking the Executor"
- **suggested_fix:** Use `tokio::fs::write` and `tokio::fs::read_to_string`. For CLI commands this is low priority since the process is typically single-purpose.

#### cli-5: Blocking fs::read_to_string in async view commands
- **severity:** low
- **category:** anti-pattern
- **file:** `crates/cli/src/commands/client/view.rs`
- **line:** 79, 90
- **patterns:** blocking-in-async
- **description:** `std::fs::read_to_string` is called twice in `async fn execute()` to read query and SDL files. Files are typically small (a few KB), so the blocking duration is minimal. Same low-impact pattern as cli-4.
- **training_refs:** async-book ch12 "Blocking the Executor"
- **suggested_fix:** Use `tokio::fs::read_to_string`. Low priority.

---

### crdt (6 findings)

#### crdt-1: Bare unwrap on storage data during CRDT merge
- **severity:** high
- **category:** bug
- **file:** `crates/crdt/src/composite.rs`
- **line:** 317, 336, 344, 369
- **patterns:** bare-unwrap
- **description:** `data[..8].try_into().unwrap()` on data received from the storage layer during CRDT merge operations. If stored counter data is corrupted or truncated (fewer than 8 bytes), this panics. The code at line 330-334 properly validates length and returns an error for the current-value read, but the *incoming delta* (line 317) and the *accumulator reads* (336, 369) do not validate before unwrapping.
- **training_refs:** rust-patterns-book ch10 "Panics, catch_unwind, and When to Abort"
- **suggested_fix:** Add length validation before each `try_into().unwrap()` and return `Error::MergeError(...)` for invalid data, matching the existing pattern at lines 330-334.

#### crdt-2: Counter merge allocates Vec<u8> for 8-byte values
- **severity:** medium
- **category:** improvement
- **file:** `crates/crdt/src/composite.rs`
- **line:** 341, 387
- **patterns:** hot-path-to-vec
- **description:** Counter merge operations in `CompositeDAG::apply_field_delta` create `new_value_bytes: Vec<u8>` from fixed-size `to_be_bytes().to_vec()` (8-byte value). This allocates a heap `Vec` for exactly 8 bytes that is immediately written to storage. This runs per counter field per document merge.
- **training_refs:** rust-patterns-book ch11 "Binary Data and repr(C)"
- **suggested_fix:** Use a stack-allocated `[u8; 8]` array instead. `rw.set()` accepts `&[u8]` so `&value.to_be_bytes()` works directly without any allocation.

#### crdt-3: CounterDelta stores 8 bytes as heap Vec<u8>
- **severity:** medium
- **category:** improvement
- **file:** `crates/crdt/src/counter.rs`
- **line:** 74, 111
- **patterns:** unnecessary-alloc
- **description:** `CounterDelta::new_int64` and `new_float64` store increment values as `data: Vec<u8>` via `increment.to_be_bytes().to_vec()`. This always allocates 8 bytes on the heap. The data is later read back with `decode_int64`/`decode_float64`. A fixed-size `[u8; 8]` would avoid the allocation entirely.
- **training_refs:** rust-patterns-book ch11 "Binary Data and repr(C)"
- **suggested_fix:** Change `CounterDelta.data` to `[u8; 8]` instead of `Vec<u8>`. This eliminates heap allocation for every counter delta creation. The serde `with = "serde_bytes"` attribute works with fixed-size arrays too.

#### crdt-4: Priority encoding allocates Vec per merge operation
- **severity:** low
- **category:** improvement
- **file:** `crates/crdt/src/priority.rs`
- **line:** 15-19
- **patterns:** unnecessary-alloc
- **description:** `encode_priority()` allocates a new `Vec<u8>` for each priority encoding. Priority encoding is called per CRDT merge operation (every field write). The varint output is at most 10 bytes, so a stack-allocated `[u8; 10]` with a length marker would avoid the heap allocation.
- **training_refs:** rust-patterns-book ch11 "Binary Data and repr(C)"
- **suggested_fix:** Return a fixed-size array or use `SmallVec<[u8; 10]>` to keep the encoding on the stack. Alternatively, write directly into the storage write buffer if the API supports it.

#### crdt-5: String params should use impl Into<String>
- **severity:** low
- **category:** improvement
- **file:** `crates/crdt/src/lww.rs` and `crates/crdt/src/composite.rs`
- **line:** lww.rs:153, composite.rs:49, composite.rs:142-151
- **patterns:** string-param-should-be-impl-into
- **description:** Multiple constructors take owned `String` parameters where `impl Into<String>` would be more ergonomic: `LwwDelta::new(schema_version_id: String, ...)`, `CompositeDelta::add_field_delta(field_name: String, ...)`, `CompositeState::new(doc_id: DocId, schema_version_id: String)`, `register_lww_field(field_name: String)`.
- **training_refs:** rust-patterns-book ch15 "Ergonomic Parameter Patterns" -- "`impl Into<T>` -- Accept Anything Convertible"
- **suggested_fix:** Use `impl Into<String>` for parameters that are stored, allowing callers to pass `&str` without explicit `.to_string()`.

#### crdt-6: Oversized test file: property_tests.rs (1269 lines)
- **severity:** low
- **category:** structure
- **file:** `crates/crdt/tests/property_tests.rs`
- **line:** 1-1269
- **patterns:** oversized-file
- **description:** Property-based tests for LWW and Counter CRDTs. Tests naturally cluster by CRDT type and property.
- **training_refs:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Split into:
  - `crates/crdt/tests/property_tests/lww.rs` -- LWW property tests (est. ~500 lines)
  - `crates/crdt/tests/property_tests/counter.rs` -- Counter property tests (est. ~500 lines)
  - `crates/crdt/tests/property_tests/helpers.rs` -- shared test helpers, strategies (est. ~270 lines)

---

### storage (4 findings)

#### storage-1: Transmute for lifetime extension in RocksDB OwnedSnapshot
- **severity:** critical
- **category:** unsound
- **file:** `crates/storage/src/backends/rocksdb/transaction.rs`
- **line:** 32-42
- **patterns:** transmute-lifetime-extension
- **description:** `OwnedSnapshot::new()` uses `std::mem::transmute` to extend the lifetime of a `SnapshotWithThreadMode<'_, DB>` to `SnapshotWithThreadMode<'static, DB>`. The safety argument is that the `Arc<DB>` stored alongside it keeps the DB alive. However, if `OwnedSnapshot` fields are reordered (e.g., by a future refactor moving `snapshot` before `_db`), Rust's drop order (fields drop in declaration order) would drop the snapshot while the DB is still alive, which is correct -- but if `_db` were moved after `snapshot`, the snapshot's destructor could access freed DB memory. The current field order is safe, but the invariant is fragile and not enforced by the type system.
- **training_refs:** rust-patterns-book ch12 "Common UB Pitfalls" -- "Dangling pointer: Dereference after drop()"
- **suggested_fix:** Add a comment `// IMPORTANT: _db MUST be declared before snapshot to ensure correct drop order.` and consider using `ManuallyDrop<SnapshotWithThreadMode<'static, DB>>` with an explicit `Drop` impl that drops in the correct order. Alternatively, use a `Pin` or an ouroboros-style self-referential struct.

#### storage-2: RwLock<bool> for closed flag should be AtomicBool
- **severity:** high
- **category:** anti-pattern
- **file:** `crates/storage/src/backends/redb/store.rs`, `crates/storage/src/backends/rocksdb/store.rs`, `crates/storage/src/backends/fjall/store.rs`, `crates/storage/src/backends/memory/store.rs`
- **line:** redb:28, rocksdb:18, fjall:26, memory:19
- **patterns:** rwlock-for-bool-flag
- **description:** All four storage backends use `Arc<RwLock<bool>>` for the `closed` flag. This is a tokio async `RwLock` wrapping a single boolean, which means every `new_txn()` call must `.await` on a read lock acquisition just to check a flag. In redb/rocksdb/fjall, the read lock is held while incrementing `active_txn_count` to prevent a TOCTOU race with `close()`. However, this entire protocol can be implemented with `AtomicBool` + a state machine.
- **training_refs:** rust-patterns-book ch6 "Shared State: Arc, Mutex, RwLock, Atomics" -- "Atomics: Lock-free for simple values"
- **suggested_fix:** Replace `Arc<RwLock<bool>>` with `AtomicBool`. For the TOCTOU protection in redb/rocksdb/fjall, use a CAS loop: (1) load `closed`, (2) if false, `fetch_add(1)` on `active_txn_count`, (3) re-check `closed`, (4) if now true, `fetch_sub(1)` and return error. In `close()`, set `closed=true` then wait for `active_txn_count==0`. This is the standard "reference-counted close" pattern used in production databases.

#### storage-3: SeqCst for transaction counter should be AcqRel
- **severity:** low
- **category:** improvement
- **file:** `crates/storage/src/backends/redb/store.rs`, `crates/storage/src/backends/rocksdb/store.rs`, `crates/storage/src/backends/fjall/store.rs`
- **line:** redb:175,289,298,341,352,354; rocksdb:153,174,184,186; fjall:137,158,166,203,213,215
- **patterns:** seqcst-for-txn-counter
- **description:** The `active_txn_count` uses `Ordering::SeqCst` for all operations (`load`, `fetch_add`, `fetch_sub`). This counter tracks active transactions for graceful shutdown. Since it coordinates with only the `closed` flag (a single other variable), `Acquire`/`Release` semantics would be sufficient and cheaper on weakly-ordered architectures.
- **training_refs:** rust-patterns-book ch6 "Lock-Free Patterns" -- use Acquire/Release for paired atomics
- **suggested_fix:** Use `Ordering::AcqRel` for `fetch_add`/`fetch_sub` and `Ordering::Acquire` for `load`. This provides the necessary happens-before relationship without the overhead of sequential consistency. On x86 this makes no performance difference, but on ARM it avoids unnecessary memory barriers.

#### storage-4: Dead anyhow dependency
- **severity:** low
- **category:** anti-pattern
- **file:** `crates/storage/Cargo.toml`
- **line:** 38
- **patterns:** anyhow-in-library
- **description:** `storage` lists `anyhow` as a dependency in Cargo.toml but never uses it in source code.
- **training_refs:** rust-patterns-book ch10 "thiserror vs anyhow -- Library vs Application"
- **suggested_fix:** Remove `anyhow.workspace = true` from `crates/storage/Cargo.toml`.

#### storage-5: Misleading SAFETY comment on OwnedSnapshot Send/Sync
- **severity:** low
- **category:** improvement
- **file:** `crates/storage/src/backends/rocksdb/transaction.rs`
- **line:** 28-29
- **patterns:** missing-safety-comment
- **description:** `unsafe impl Send for OwnedSnapshot {}` and `unsafe impl Sync for OwnedSnapshot {}` have a safety comment (lines 26-27) but it says "the underlying SnapshotWithThreadMode is Send+Sync". This is misleading because if `SnapshotWithThreadMode` were already `Send + Sync`, the `unsafe impl` would not be needed. The real reason is the self-referential `'static` lifetime.
- **training_refs:** rust-patterns-book ch12 "Writing Sound Abstractions"
- **suggested_fix:** Update the safety comment to: `// SAFETY: OwnedSnapshot is safe to Send/Sync because: (1) the Arc<DB> ensures the DB outlives the snapshot, (2) rocksdb::SnapshotWithThreadMode is internally thread-safe (uses a C pointer to a snapshot handle), (3) no &mut access to the snapshot is possible through &OwnedSnapshot.`

---

### defra-node (4 findings)

#### defra-node-1: Blocking filesystem ops in async build() and load_or_generate_secret_key
- **severity:** high
- **category:** anti-pattern
- **file:** `crates/defra-node/src/lib.rs`
- **line:** 566, 891-915
- **patterns:** blocking-in-async
- **description:** `async fn build()` calls `std::fs::create_dir_all` at line 566. Additionally, `load_or_generate_secret_key` (lines 891-915) performs `std::fs::read`, `std::fs::create_dir_all`, `std::fs::write`, and `std::fs::set_permissions`, called from `async fn setup_p2p` at line 759. Same issue as embedded-2 in a different crate.
- **training_refs:** async-book ch12 "Blocking the Executor"
- **suggested_fix:** Same as embedded-2: use `tokio::fs` or `spawn_blocking`. Since both crates have the identical pattern, consider extracting a shared `async fn load_or_generate_secret_key` utility.

#### defra-node-2: Duplicate P2P operation traits across workspace
- **severity:** high
- **category:** improvement
- **file:** `crates/defra-node/src/lib.rs`
- **line:** 201
- **patterns:** duplicate-trait-definitions
- **description:** The `P2POps` trait in defra-node duplicates functionality already defined in `embedded::P2POperations` and `defra_http::P2POperations`. Three near-identical P2P operation traits exist across the workspace: `defra-node::P2POps`, `embedded::P2POperations`, and `defra_http::router::P2POperations`. Each defines `local_peer_id`, `connect_peer`, `connected_peers`, `set_replicator`, etc. with slight signature variations. This trait duplication forces adapter boilerplate like `HttpP2PAdapter` which just forwards calls.
- **training_refs:** rust-patterns-book ch15 "Workspace Organization" -- "Clean dependency boundaries between components"
- **suggested_fix:** Define a single canonical `P2POperations` trait in `defra-core` or a shared crate, and have all consumers depend on that single definition. Remove the duplicate trait definitions and adapter glue.

#### defra-node-3: Oversized file: lib.rs (939 lines)
- **severity:** medium
- **category:** structure
- **file:** `crates/defra-node/src/lib.rs`
- **line:** 1-939
- **patterns:** oversized-file
- **description:** File contains 4 concerns: (1) types (StorageBackend, NodeBuilder, HttpConfig, P2PConfig ~lines 1-520), (2) NodeBuilder::build() with storage backend dispatch (~lines 520-700), (3) EmbeddedNode struct and methods (~lines 700-800), (4) HttpP2PAdapter implementation (~lines 20-200). The HttpP2PAdapter is conditionally compiled and should be a separate file.
- **training_refs:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Split into:
  - `crates/defra-node/src/lib.rs` -- re-exports, StorageBackend, NodeBuilder (est. ~300 lines)
  - `crates/defra-node/src/node.rs` -- EmbeddedNode struct and methods (est. ~200 lines)
  - `crates/defra-node/src/builder.rs` -- NodeBuilder::build() implementation (est. ~250 lines)
  - `crates/defra-node/src/http_adapter.rs` -- HttpP2PAdapter (est. ~200 lines)

#### defra-node-4: Oversized file: benchmark_support.rs (985 lines)
- **severity:** low
- **category:** structure
- **file:** `crates/defra-node/src/benchmark_support.rs`
- **line:** 1-985
- **patterns:** oversized-file
- **description:** Benchmark fixture and execution code: (1) fixture SDL and config types (~lines 1-100), (2) fixture data generation (~lines 100-500), (3) query case definitions and execution (~lines 500-985).
- **training_refs:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Split into:
  - `crates/defra-node/src/benchmark_support/mod.rs` -- re-exports (est. ~20 lines)
  - `crates/defra-node/src/benchmark_support/fixture.rs` -- SDL, config, data generation (est. ~500 lines)
  - `crates/defra-node/src/benchmark_support/cases.rs` -- SearchQueryCase, query rendering (est. ~250 lines)
  - `crates/defra-node/src/benchmark_support/runner.rs` -- run_search_benchmark, summarize, formatting (est. ~200 lines)

---

### events (4 findings)

#### events-1: Vec<u8> block in Update events causes O(n) clone per subscriber
- **severity:** medium
- **category:** anti-pattern
- **file:** `crates/events/src/event.rs`
- **line:** 117
- **patterns:** vec-u8-to-bytes
- **description:** The `Update` event struct carries `block: Vec<u8>` which is deep-copied every time the event is broadcast to subscribers (`msg.clone()` in `channel_bus.rs:132`). The event bus fans out to multiple subscribers (HTTP SSE, P2P sync, merge processor). Each subscriber gets a full copy of the block bytes. This is the primary event delivery path for every document mutation.
- **training_refs:** rust-patterns-book ch11 "bytes::Bytes -- Reference-Counted Buffers"
- **suggested_fix:** Change `Update.block` from `Vec<u8>` to `bytes::Bytes`. All subscriber clones become O(1). The `Update::new()` constructor can accept `impl Into<Bytes>`.

#### events-2: SeqCst for subscription ID counter
- **severity:** medium
- **category:** improvement
- **file:** `crates/events/src/channel_bus.rs`
- **line:** 197
- **patterns:** seqcst-for-id-counter
- **description:** The `next_id` counter in `ChannelBus` uses `Ordering::SeqCst` for `fetch_add`. This counter generates unique subscription IDs and only needs monotonicity (no two callers get the same ID). `SeqCst` is far stronger than needed for a simple counter.
- **training_refs:** rust-patterns-book ch6 "Shared State: Arc, Mutex, RwLock, Atomics" -- "Atomics: Lock-free for simple values" uses `Ordering::Relaxed` for counters
- **suggested_fix:** Use `Ordering::Relaxed` for monotonic ID/handle counters where the only invariant is uniqueness.

#### events-3: Mixed orderings on dropped_count counter (Relaxed write, SeqCst read)
- **severity:** medium
- **category:** improvement
- **file:** `crates/events/src/channel_bus.rs`
- **line:** 136
- **patterns:** relaxed-ordering-for-observable-counter
- **description:** The `dropped_count` per-subscriber uses `Ordering::Relaxed` for `fetch_add` in `publish()`, but `Ordering::SeqCst` for `swap` and `load` in `Subscription::check_and_reset_dropped()` and `dropped_count()`. The mixed orderings are inconsistent. The `SeqCst` on the read side buys nothing when the write side is `Relaxed`.
- **training_refs:** rust-patterns-book ch6 "Lock-Free Patterns" -- consistent ordering pairs
- **suggested_fix:** Use `Ordering::Relaxed` on both sides since this is an advisory counter, or use `Release` on the write side and `Acquire` on the read side for prompt visibility. The current asymmetry pays the cost of `SeqCst` without the benefit.

#### events-4: Oversized file: runner/plan.rs (873 lines)
- **severity:** low
- **category:** structure
- **file:** `crates/query/src/runner/plan.rs`
- **line:** 1-873
- **patterns:** oversized-file
- **description:** Plan execution with explain rendering (simple, execute, debug) and result collection. Explain logic is half the file.
- **training_refs:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Move remaining explain logic from plan.rs into `runner/explain/` if not already there, reducing plan.rs to ~400 lines.

**NOTE:** This was originally reported under the query crate in the file-structure audit. It belongs to query, not events. See the cross-listing note.

---

### sourcehub (4 findings)

#### sourcehub-1: std::sync::Mutex<u64> for nonce in async context
- **severity:** medium
- **category:** improvement
- **file:** `crates/sourcehub/src/hub_rs/provider.rs`
- **line:** 26, 90-99
- **patterns:** std-mutex-in-async-context
- **description:** `HubRsProvider` uses `std::sync::Mutex<u64>` for the nonce counter. The `send_tx` method (line 90) acquires this lock inside an `async fn`. While the lock is released before the first `.await` point (line 101), this is fragile -- any future refactoring that moves the `await` inside the lock scope would silently create a blocking hold. More importantly, `std::sync::Mutex` will block the tokio worker thread if contended.
- **training_refs:** async-book ch8 "Tokio Sync Primitives" -- "don't use std::sync::Mutex across .await points"
- **suggested_fix:** Replace `Mutex<u64>` with `AtomicU64` and use `fetch_add(1, Ordering::Relaxed)` to get the next nonce. This is lock-free, cannot block the tokio runtime, and is semantically correct for a monotonic counter.

#### sourcehub-2: std::sync::Mutex in async observer loop
- **severity:** medium
- **category:** improvement
- **file:** `crates/sourcehub/src/hub_rs/provider.rs`
- **line:** 27, 214, 249
- **patterns:** std-mutex-in-async-observer
- **description:** `Arc<Mutex<HubRsLightClientObservability>>` uses `std::sync::Mutex` in the `run_light_client_observer` async function. At line 249, the lock is acquired inside a `loop` that calls `.await`. Although the lock is released before the next `await`, the `std::sync::Mutex` in a long-running async loop is a code smell.
- **training_refs:** async-book ch8 "Tokio Sync Primitives" -- sync primitives in async code
- **suggested_fix:** Since `HubRsLightClientObservability` is a tiny struct (single `Option<u64>`), consider using `AtomicU64` for `last_invalidation_height` (with 0 meaning "none"). This eliminates the mutex entirely. Alternatively, use `parking_lot::Mutex` which never poisons and has shorter critical sections.

#### sourcehub-3: Relaxed ordering for request ID counter (positive example)
- **severity:** low
- **category:** improvement
- **file:** `crates/sourcehub/src/hub_rs/client.rs`
- **line:** 34
- **patterns:** relaxed-ordering-for-id-counter
- **description:** `HubRsClient::next_id()` uses `Ordering::Relaxed` for a request ID counter. This is correct -- the counter only needs uniqueness, and `fetch_add` provides that guarantee even with `Relaxed`. This is noted as a positive example.
- **training_refs:** rust-patterns-book ch6 "Shared State: Arc, Mutex, RwLock, Atomics"
- **suggested_fix:** No change needed. This is the correct pattern for ID generation counters.

#### sourcehub-4: Oversized file: embedded/libp2p_adapter.rs (896 lines)
- **severity:** medium
- **category:** structure
- **file:** `crates/embedded/src/libp2p_adapter.rs`
- **line:** 1-896
- **patterns:** oversized-file
- **description:** Contains trait definitions (CollectionLookup, DocPusher, VersionSyncer), P2PAdapter implementation, and DbDocPusher implementation. Similar structure to cli/p2p_adapter.rs.
- **training_refs:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Split into `libp2p_adapter/traits.rs` (trait definitions ~150 lines), `libp2p_adapter/adapter.rs` (P2PAdapter ~400 lines), `libp2p_adapter/doc_pusher.rs` (DbDocPusher ~350 lines).

**NOTE:** This finding is actually in the embedded crate, not sourcehub. It was collected here during consolidation but belongs to embedded. Cross-listed as embedded-7.

---

### document (2 findings)

#### document-1: Bare unwrap in array decoding on user-provided CBOR data
- **severity:** critical
- **category:** bug
- **file:** `crates/document/src/encoding.rs`
- **line:** 557, 593, 619, 645
- **patterns:** bare-unwrap
- **description:** Four `opt.unwrap()` calls on `Option` values inside array decoding (`bools.into_iter().map(|opt| opt.unwrap()).collect()`). The code checks `has_null` first and only reaches these lines when all values are `Some`, but the unwrap is still fragile -- any future refactor that changes the null-tracking logic could introduce a panic on user-provided CBOR data. This processes external document data.
- **training_refs:** rust-patterns-book ch10 "Panics, catch_unwind, and When to Abort"
- **suggested_fix:** Use `.map(|opt| opt.expect("null check guarantees Some"))` to document the invariant, or better, use `opt.ok_or(Error::CborDecode("unexpected null in array".into()))?` with `collect::<Result<Vec<_>, _>>()`.

#### document-2: Extensive cloning in NormalValue encoding
- **severity:** medium
- **category:** improvement
- **file:** `crates/document/src/encoding.rs`
- **line:** 119, 125, 133, 153, 212, 213, 223, 257, 261, 339, 349, 401, 410, 474, 490, 629, 715, 728
- **patterns:** encoding-string-clone
- **description:** `normal_value_to_json` and `normal_value_to_cbor` clone String and Vec values extensively when converting `NormalValue` to JSON/CBOR representation. For a document with 10 string fields, that is 10 string allocations.
- **training_refs:** rust-patterns-book ch11 "Zero-Copy Deserialization"
- **suggested_fix:** Consider taking `NormalValue` by value (`fn normal_value_to_cbor(value: NormalValue)`) instead of by reference in the encoding paths where the source value is not needed afterward. This enables moving strings/vecs into the target representation without cloning.

---

### pg-compat (2 findings)

#### pg-compat-1: Regex compiled per-call with unwrap in hot path
- **severity:** critical
- **category:** bug
- **file:** `crates/pg-compat/src/handler/mod.rs`
- **line:** 642, 646
- **patterns:** bare-unwrap
- **description:** `Regex::new(...).unwrap()` called on every invocation of `extract_filter_from_graphql()`. While the regex patterns are compile-time constants and will always compile, constructing them per-call is wasteful. More importantly, this function processes user-provided SQL translated to GraphQL -- any future regex changes that introduce a syntax error would panic on the hot path.
- **training_refs:** rust-patterns-book ch10 "Panics, catch_unwind, and When to Abort"
- **suggested_fix:** Use `OnceLock` or `lazy_static!` to compile regexes once, and use `.expect("valid regex literal")` to document the safety invariant.

#### pg-compat-2: Box<dyn Error> return type on PgServer::run()
- **severity:** medium
- **category:** anti-pattern
- **file:** `crates/pg-compat/src/lib.rs`
- **line:** 49
- **patterns:** box-dyn-error
- **description:** `PgServer::run()` returns `Result<(), Box<dyn std::error::Error>>`. This is the main entry point for the Postgres wire protocol server. Using `Box<dyn Error>` prevents callers from matching on specific error types.
- **training_refs:** rust-patterns-book ch10 "thiserror vs anyhow -- Library vs Application"
- **suggested_fix:** Define a `PgServerError` enum or use `anyhow::Error` (since this is effectively a top-level runner). If the crate is consumed as a library, prefer a typed error.

---

### acp (3 findings)

#### acp-1: 35+ instances of map_err to String in persistent ACP store
- **severity:** high
- **category:** anti-pattern
- **file:** `crates/acp/src/persistent.rs`
- **line:** 155-434 (throughout)
- **patterns:** map-err-to-string
- **description:** Over 35 instances of `.map_err(|e| Error::Storage(e.to_string()))` in the persistent ACP store. The original error types (from `storage::corekv` and `serde_json`) are converted to `String`, losing their type information. Callers cannot distinguish between a serialization error and a storage I/O error.
- **training_refs:** rust-patterns-book ch10 "Error Conversion Chains (#[from])"
- **suggested_fix:** Add `#[from]` variants to `acp::Error` for `storage::corekv::Error` and use the existing structured variants (`StorageRead`, `StorageWrite`, etc.) consistently instead of the catch-all `Storage(String)`.

#### acp-2: NAC lifecycle uses runtime state checks instead of type-state
- **severity:** medium
- **category:** improvement
- **file:** `crates/acp/src/nac/node_acp/lifecycle.rs`
- **line:** 18-199
- **patterns:** runtime-state-check
- **description:** The NAC lifecycle (`enable`, `disable`, `re_enable`, `purge`) uses runtime `match` checks on `NacStatus` at the top of each method to reject invalid transitions. The valid state machine is: `NotConfigured -> Enabled -> DisabledTemporarily -> Enabled` and `* -> NotConfigured` (via purge). This is a textbook case for type-state encoding, but because the status is stored in `RwLock<NacStatus>` for async access, a full type-state encoding would require significant refactoring.
- **training_refs:** type-driven-correctness-book ch5 "Protocol State Machines -- Type-State for Real Hardware"
- **suggested_fix:** This is a pragmatic trade-off. Consider adding `#[doc = "State machine: NotConfigured -> Enabled <-> DisabledTemporarily, * -> NotConfigured (purge)"]` to `NacStatus` and keep runtime checks. If the async constraint is ever relaxed, convert to type-state.

#### acp-3: normalize_auth_error takes owned String instead of &str
- **severity:** medium
- **category:** improvement
- **file:** `crates/acp/src/auth_error.rs`
- **line:** 7
- **patterns:** string-param-should-be-str
- **description:** `pub fn normalize_auth_error(err: String, permission: &str) -> String` takes `err` by owned `String`. Since the function only reads and reformats the error, it should accept `&str` to avoid forcing callers to allocate.
- **training_refs:** rust-patterns-book ch15 "Ergonomic Parameter Patterns"
- **suggested_fix:** Change signature to `pub fn normalize_auth_error(err: &str, permission: &str) -> String`.

---

### http (1 finding)

#### http-1: expect() on JoinHandle in production HTTP handler
- **severity:** critical
- **category:** bug
- **file:** `crates/http/src/query_context.rs`
- **line:** 44, 75, 99
- **patterns:** bare-expect
- **description:** Three `.expect("query execution task panicked")` calls on `JoinHandle` results in production HTTP handler code. If a `spawn_blocking` task panics (e.g., from any `unwrap()` deeper in the stack), this `.expect()` will panic the HTTP handler, which can crash the tokio runtime or propagate to the connection handler. This is user-facing code that processes arbitrary GraphQL queries.
- **training_refs:** async-book ch13 "Error Handling in Async Code" -- "The error boundary problem"
- **suggested_fix:** Replace `.expect(...)` with proper error handling: `match handle.await { Ok(response) => response, Err(join_err) => QueryResponse::error(format!("internal error: {}", join_err)) }`. This follows the double-`?` pattern from the training material.

---

### lens (1 finding)

#### lens-1: expect() in Default impl for WasmTransformStore
- **severity:** medium
- **category:** anti-pattern
- **file:** `crates/lens/src/wasm.rs`
- **line:** 190
- **patterns:** bare-expect
- **description:** `Self::new().expect("failed to create WASM engine")` in the `Default` impl for `WasmTransformStore`. WASM engine creation can fail for system-level reasons (memory allocation, platform support). Using `expect` in a `Default` impl means any caller using `default()` gets a panic instead of an error.
- **training_refs:** rust-patterns-book ch10 "Panics, catch_unwind, and When to Abort"
- **suggested_fix:** Remove the `Default` impl or have it return a no-op store. Callers should use `WasmTransformStore::new()` which returns `Result`.

---

### schema (2 findings)

#### schema-1: CollectionBuilder lacks #[must_use]
- **severity:** medium
- **category:** improvement
- **file:** `crates/schema/src/collection.rs`
- **line:** 436-488
- **patterns:** missing-must-use
- **description:** `CollectionBuilder` is a builder type where calling `.field()` or `.scalar()` chains mutations, but forgetting to call `.build()` silently drops the builder and all accumulated state. The builder is not `#[must_use]`, so `CollectionBuilder::new("users", "1").scalar("1", "name", FieldKind::string());` compiles without warning.
- **training_refs:** type-driven-correctness-book ch3 "Single-Use Types" (builder pattern)
- **suggested_fix:** Add `#[must_use = "CollectionBuilder does nothing until .build() is called"]` to the `CollectionBuilder` struct.

#### schema-2: CollectionVersion uses raw String IDs
- **severity:** low
- **category:** improvement
- **file:** `crates/schema/src/collection.rs`
- **line:** 38-42
- **patterns:** raw-primitive-id
- **description:** `CollectionVersion.version_id` and `CollectionVersion.collection_id` are raw `String` fields that serve as critical identifiers. These are distinct domain concepts (content-addressed version hash vs stable collection identity) but are both `String` at the type level, making them interchangeable at any call site.
- **training_refs:** rust-patterns-book ch3 "Newtype: Zero-Cost Type Safety"
- **suggested_fix:** Introduce `VersionId(String)` and `CollectionIdStr(String)` newtypes. This is low severity because these fields are mostly read from deserialized JSON and passed through.

---

### identity (1 finding)

#### identity-1: Duplicate Did newtype (see zanzibar-5)
- **severity:** low
- **category:** improvement
- **file:** `crates/identity/src/did.rs`
- **line:** 30
- **patterns:** duplicate-newtype
- **description:** See zanzibar-5 for the full description. The identity crate's `Did` and zanzibar crate's `Did` are duplicates that should be unified.
- **training_refs:** type-driven-correctness-book ch7 "Validated Boundaries"
- **suggested_fix:** See zanzibar-5.

---

### crypto (2 findings)

#### crypto-1: Relaxed ordering for deterministic nonce flag
- **severity:** low
- **category:** improvement
- **file:** `crates/crypto/src/encryption/nonce.rs`
- **line:** 26, 63-64
- **patterns:** relaxed-ordering-for-config-flag
- **description:** `USE_DETERMINISTIC_NONCE` uses `Ordering::Relaxed` for both the store (in `ffi/src/lib.rs:196`) and the load (line 26). This is a test-only flag that switches between secure random nonces and deterministic nonces. If the flag is set by one thread and read by another, `Relaxed` could theoretically cause the reader to miss the update. In practice, this flag is set once during initialization, so the risk is academic.
- **training_refs:** rust-patterns-book ch6 "Lock-Free Patterns" -- Acquire/Release for flag publishing
- **suggested_fix:** Use `Ordering::Release` for the store and `Ordering::Acquire` for the load. This has zero cost on x86 and ensures the flag is visible promptly on ARM. Given this is test-only infrastructure, the severity is low.

#### crypto-2: String error type for batch signing
- **severity:** medium
- **category:** improvement
- **file:** `crates/crypto/src/batch.rs`
- **line:** 33
- **patterns:** string-error-should-be-typed
- **description:** `pub fn sign_batch(cids: &[Cid], config: &SigningConfig) -> Result<BatchSignature, String>` returns `String` as the error type.
- **training_refs:** rust-patterns-book ch15 "Case Study: Designing a Public Crate API"
- **suggested_fix:** Define a `BatchSignError` enum with variants for the different failure modes (missing key, invalid key type, signing failed).

---

### embedded (additional finding)

#### embedded-7: Oversized file: libp2p_adapter.rs (896 lines)
- **severity:** low
- **category:** structure
- **file:** `crates/embedded/src/libp2p_adapter.rs`
- **line:** 1-896
- **patterns:** oversized-file
- **description:** Contains trait definitions (CollectionLookup, DocPusher, VersionSyncer), P2PAdapter implementation, and DbDocPusher implementation. Similar structure to cli/p2p_adapter.rs.
- **training_refs:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Split into `libp2p_adapter/traits.rs` (trait definitions ~150 lines), `libp2p_adapter/adapter.rs` (P2PAdapter ~400 lines), `libp2p_adapter/doc_pusher.rs` (DbDocPusher ~350 lines).

---

## Cross-Cutting Findings

Findings that affect multiple crates or require coordinated changes across crate boundaries. These are flagged for the cross-cutting follow-up pass (Task 31).

### CC-1: Vec<u8> to bytes::Bytes migration
- **Crates affected:** blockstore, events, p2p, crdt, document
- **Findings:** blockstore-1, blockstore-2, p2p-3, p2p-4, p2p-7, events-1
- **Description:** The hot-path data flow is: document write -> CRDT merge -> block builder -> blockstore put -> event bus -> P2P broadcaster. Block data (`Vec<u8>`) is deep-copied at each stage. Migrating to `bytes::Bytes` across this entire pipeline would eliminate O(n) copies and replace them with O(1) refcount bumps. This requires coordinated changes to the `Blockstore` trait, `Update` event struct, `PushLogBroadcast`/`PushLogRequest` message types, and `CompositeDAG` merge output.

### CC-2: #[non_exhaustive] on all public enums
- **Crates affected:** defra-core, schema, acp, zanzibar, document, identity, db, query, storage, blockstore, crypto, p2p
- **Findings:** defra-core-4, defra-core-5, zanzibar-4 (plus file-structure Finding 31)
- **Description:** Only 3 enums in the entire codebase use `#[non_exhaustive]`. All public error enums and extensible domain enums should be annotated to prevent adding variants from being a breaking change.

### CC-3: Sealed trait pattern for core extension traits
- **Crates affected:** blockstore, db, query, events, identity, crypto, zanzibar, acp, p2p
- **Findings:** file-structure Finding 30
- **Description:** 20+ public traits lack the sealed pattern. Priority candidates: `Blockstore`, `DocFetcher`, `QueryExecutor`, `TransactionRegistry`, `PlanNode`, `Bus`, `DocMutator`.

### CC-4: Unified P2POperations trait
- **Crates affected:** defra-core (or new shared crate), embedded, cli, defra-node, http
- **Findings:** defra-node-2, embedded-3 (P2POperations uses Result<T, String>)
- **Description:** Three near-identical P2P operation traits exist. Unify into a single canonical trait with proper typed errors (not `Result<T, String>`).

### CC-5: Shared load_or_generate_secret_key utility
- **Crates affected:** embedded, defra-node
- **Findings:** embedded-2, defra-node-1
- **Description:** Both crates have identical `load_or_generate_secret_key` functions with blocking filesystem ops in async context. Extract to a shared async utility.

### CC-6: Duplicate Did newtype unification
- **Crates affected:** identity, zanzibar, acp
- **Findings:** zanzibar-5, identity-1, zanzibar-3
- **Description:** Two `Did` types with diverging validation. Extract to a shared micro-crate.

### CC-7: P2POperations Result<T, String> to typed errors
- **Crates affected:** embedded, cli, http, p2p
- **Findings:** error-handling Finding 3
- **Description:** `P2POperations` trait and `TransportDocPusher` trait use `Result<T, String>` for 28+ methods total. Define a `P2PError` enum with `thiserror`.

## Verification Candidates

### Miri Candidates
- `query::runner::fetcher.rs:47` -- The `transmute` of fat pointer layout should be tested with Miri under both Stacked Borrows (default) and Tree Borrows models to detect any aliasing violations when the wrapper is used across async boundaries.
- `storage::backends::rocksdb::transaction.rs:35` -- The lifetime-extended snapshot transmute cannot be directly tested by Miri (RocksDB is a C library, FFI is opaque to Miri), but unit tests that exercise `OwnedSnapshot` creation and use patterns could be run under Miri if the rocksdb dependency is mocked.

### Valgrind Candidates
- `ffi::acp::identity.rs:28` -- The `FfiRemoteSigner::sign_sync` method calls a C function pointer callback. This entire callback path should be tested with Valgrind to verify the C-side memory handling is correct (buffer sizes, write bounds, etc.).
- `ffi::node.rs:136` and `ffi::node.rs:181` -- The `from_raw_parts` calls that read C-provided byte slices should be tested with Valgrind under the Go FFI harness to verify memory is valid.
- All `extern "C"` functions in the `ffi` crate -- Run the Go FFI test suite under Valgrind memcheck to detect any memory leaks from `CString::into_raw()` that are never freed by `defra_free_string()`.

### loom Candidates
- No loom candidates identified. The codebase uses standard synchronization primitives (`RwLock`, `Mutex`, `AtomicUsize`, `OnceLock`) throughout and does not implement any custom lock-free data structures. The `FetcherWrapper` in `query` uses raw pointers but is not a concurrent data structure -- it's a lifetime-erasing wrapper used within a single query execution.
