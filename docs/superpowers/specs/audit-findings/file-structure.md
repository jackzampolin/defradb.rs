# File Structure & API Design Audit Findings

Audit based on Microsoft Rust Training: `rust-patterns-book/src/ch15-crate-architecture-and-api-design.md`

## Summary
- Total findings: 42
- Critical: 0 | High: 9 | Medium: 16 | Low: 17

---

## Findings

---

### Finding 1
- **severity:** high
- **category:** structure
- **crate:** db
- **file:** `crates/db/src/downsample.rs` (2036 lines)
- **pattern:** oversized-file
- **description:** File contains 5 distinct concerns: (1) types/enums for downsample planning (AggregateField, NumericValue, SourceSample, WindowAggregate, PendingWindowAggregate, DownsamplePlan ~lines 1-160), (2) pure utility functions for duration parsing, time conversion, value conversion (~lines 160-560), (3) plan building and validation on `DB<S>` (~lines 560-912), (4) execution logic - processing source docs, persisting windows, aggregating samples (~lines 912-1904), (5) background task loop and event handling (~lines 1904-2036).
- **training_ref:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Split into:
  - `crates/db/src/downsample/mod.rs` -- public API re-exports + GcDownsampleHistoriesOptions (est. ~40 lines)
  - `crates/db/src/downsample/types.rs` -- AggregateField, NumericValue, SourceSample, WindowAggregate, PendingWindowAggregate, DownsamplePlan, SourceKind, ParsedSourceQuery (est. ~160 lines)
  - `crates/db/src/downsample/parse.rs` -- duration parsing, source query parsing, value conversion utilities (est. ~400 lines)
  - `crates/db/src/downsample/plan.rs` -- build_downsample_plan, validate_downsample_collection, downsample_plans, downsample_depth, validate_downsample_cycle (est. ~350 lines)
  - `crates/db/src/downsample/execute.rs` -- process_source_doc_for_plan, persist_window_update, aggregate_samples_into_windows, build_source_samples (est. ~600 lines)
  - `crates/db/src/downsample/gc.rs` -- gc_downsample_histories, gc_source_doc_for_plans, prune_source_doc_history (est. ~250 lines)
  - `crates/db/src/downsample/task.rs` -- start_downsample_task, bootstrap_downsamples, process_downsample_update (est. ~130 lines)

---

### Finding 2
- **severity:** high
- **category:** structure
- **crate:** query
- **file:** `crates/query/src/runner/query/nested.rs` (1821 lines)
- **pattern:** oversized-file
- **description:** File contains 4 distinct concerns: (1) profiling structs and the main `execute_nested_select_with_planner` method (~lines 1-400), (2) scoped full-text search scoring and sort logic (~lines 400-700), (3) post-processing helpers: clean_filter_only_relation_fields, apply_deferred_relation_limits, sort_relation_items, strip ordering-only fields (~lines 700-1050), (4) unit tests for scoped fulltext, precompute scores, and relation path scoring (~lines 1050-1821).
- **training_ref:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Split into:
  - `crates/query/src/runner/query/nested/mod.rs` -- re-exports + execute_nested_select_with_planner (est. ~400 lines)
  - `crates/query/src/runner/query/nested/scoped_fulltext.rs` -- apply_scoped_relation_fulltext, compute_scoped_fulltext_scores, scoped profiling (est. ~300 lines)
  - `crates/query/src/runner/query/nested/post_process.rs` -- clean_filter_only_relation_fields, apply_deferred_relation_limits, sort_relation_items, strip_ordering_only_fields (est. ~350 lines)
  - `crates/query/src/runner/query/nested/tests.rs` -- all #[cfg(test)] code (est. ~770 lines)

---

### Finding 3
- **severity:** low
- **category:** structure
- **crate:** query
- **file:** `crates/query/src/sdl_parse/parser_tests.rs` (1813 lines)
- **pattern:** oversized-file
- **description:** Pure test file with 50+ independent test functions covering simple types, arrays, CRDTs, relations, directives, views, indexes, FTS, self-refs, etc. While test files naturally grow large, at 1813 lines navigation is difficult.
- **training_ref:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Split into:
  - `crates/query/src/sdl_parse/tests/basic_types.rs` -- simple type parsing, arrays, scalars (est. ~300 lines)
  - `crates/query/src/sdl_parse/tests/directives.rs` -- @crdt, @primary, @index, @relation, @default, @size tests (est. ~400 lines)
  - `crates/query/src/sdl_parse/tests/relations.rs` -- relation resolution, self-refs, collection sets, named kinds (est. ~400 lines)
  - `crates/query/src/sdl_parse/tests/views.rs` -- view definitions, lens, downsample, embedding tests (est. ~350 lines)
  - `crates/query/src/sdl_parse/tests/errors.rs` -- error cases, unknown types, invalid combinations (est. ~360 lines)

---

### Finding 4
- **severity:** medium
- **category:** structure
- **crate:** query
- **file:** `crates/query/src/runner/commits.rs` (1545 lines)
- **pattern:** oversized-file
- **description:** File contains 3 distinct concerns: (1) inline unit tests for height range extraction (~lines 26-200), (2) helper types and functions for commit numeric values, height extraction, aggregation (~lines 200-600), (3) the main `execute_commits_query` method and rendering/filtering/grouping logic (~lines 600-1545).
- **training_ref:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Split into:
  - `crates/query/src/runner/commits/mod.rs` -- re-exports + execute_commits_query (est. ~600 lines)
  - `crates/query/src/runner/commits/height_range.rs` -- CommitsHeightRange, HeightRangeExtraction, extract_commits_height_range (est. ~200 lines)
  - `crates/query/src/runner/commits/render.rs` -- render_commit, render_document_fields, commit_to_fields, build_commits_mapping (est. ~400 lines)
  - `crates/query/src/runner/commits/filter.rs` -- json_item_matches_filter, check_filter_op, aggregation helpers (est. ~200 lines)
  - `crates/query/src/runner/commits/tests.rs` -- all unit tests (est. ~200 lines)

---

### Finding 5
- **severity:** low
- **category:** structure
- **crate:** db
- **file:** `crates/db/tests/index_manager_tests.rs` (1541 lines)
- **pattern:** oversized-file
- **description:** Test file with 30+ test functions covering index CRUD, composite indexes, unique constraints, bulk operations, iteration, edge cases. All tests share a common `test_schema()` helper.
- **training_ref:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Split into:
  - `crates/db/tests/index_manager/mod.rs` -- test_schema() helper + basic CRUD tests (est. ~400 lines)
  - `crates/db/tests/index_manager/composite.rs` -- composite index tests (est. ~350 lines)
  - `crates/db/tests/index_manager/unique.rs` -- unique constraint tests (est. ~300 lines)
  - `crates/db/tests/index_manager/iteration.rs` -- range/prefix iteration tests (est. ~300 lines)
  - `crates/db/tests/index_manager/edge_cases.rs` -- edge cases, bulk ops, error handling (est. ~200 lines)

---

### Finding 6
- **severity:** low
- **category:** structure
- **crate:** blockstore
- **file:** `crates/blockstore/tests/blockstore_tests.rs` (1450 lines)
- **pattern:** oversized-file
- **description:** Test file with section comment markers for Basic CRUD, Hash Verification, Merge Tracking, Go Compatibility, Concurrency, Edge Cases/Stress. Each section is a self-contained test group.
- **training_ref:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Split into:
  - `crates/blockstore/tests/blockstore/mod.rs` -- helpers + basic CRUD tests (est. ~200 lines)
  - `crates/blockstore/tests/blockstore/hash_verify.rs` -- hash_on_read tests (est. ~200 lines)
  - `crates/blockstore/tests/blockstore/merge_tracking.rs` -- P2P merge tracking tests (est. ~300 lines)
  - `crates/blockstore/tests/blockstore/go_compat.rs` -- Go compatibility tests (est. ~250 lines)
  - `crates/blockstore/tests/blockstore/concurrency.rs` -- concurrent access tests (est. ~200 lines)
  - `crates/blockstore/tests/blockstore/stress.rs` -- stress/edge case tests (est. ~300 lines)

---

### Finding 7
- **severity:** high
- **category:** structure
- **crate:** cli
- **file:** `crates/cli/src/commands/start/server.rs` (1428 lines)
- **pattern:** oversized-file
- **description:** Single function `init_store_and_server` spans essentially the entire file. It handles: database initialization, ACP setup, query runner creation, P2P host/coordinator/replication wiring, HTTP server adapter creation (14 different adapters), PG wire protocol setup, and downsample task spawning. This is a God Function doing server assembly.
- **training_ref:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Split into:
  - `crates/cli/src/commands/start/server.rs` -- top-level init_store_and_server that calls subfunctions (est. ~200 lines)
  - `crates/cli/src/commands/start/database.rs` -- database creation, identity setup, embedding config (est. ~200 lines)
  - `crates/cli/src/commands/start/p2p_setup.rs` -- P2P host creation, coordinator, replication loop, event handling (est. ~500 lines)
  - `crates/cli/src/commands/start/http_wiring.rs` -- HTTP server adapter registration (all 14 with_*_arc calls) (est. ~300 lines)
  - `crates/cli/src/commands/start/acp_setup.rs` -- ACP/NAC/SourceHub initialization (est. ~200 lines)

---

### Finding 8
- **severity:** high
- **category:** structure
- **crate:** p2p
- **file:** `crates/p2p/src/iroh/endpoint.rs` (1420 lines)
- **pattern:** oversized-file
- **description:** File contains 4 distinct concerns: (1) types and config (IrohEndpointConfig, TopicSubscription, ActiveSync ~lines 1-77), (2) endpoint spawning and main event loop (~lines 77-500), (3) command dispatch - a massive match on IrohCommand variants (~lines 500-900), (4) free functions for fire-and-forget, request-response, block sync, relay/discovery/bind config (~lines 900-1420).
- **training_ref:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Split into:
  - `crates/p2p/src/iroh/endpoint/mod.rs` -- IrohEndpointConfig, spawn_endpoint, main loop (est. ~350 lines)
  - `crates/p2p/src/iroh/endpoint/commands.rs` -- handle_command dispatch + per-command handlers (est. ~400 lines)
  - `crates/p2p/src/iroh/endpoint/transport.rs` -- fire_and_forget, request_response, block_sync (est. ~350 lines)
  - `crates/p2p/src/iroh/endpoint/config.rs` -- relay_mode_from_config, apply_discovery_config, apply_bind_config (est. ~120 lines)

---

### Finding 9
- **severity:** medium
- **category:** structure
- **crate:** query
- **file:** `crates/query/src/planner/joins/mod.rs` (1395 lines)
- **pattern:** oversized-file
- **description:** Despite having 6 sub-modules (aggregate_joins, filter_only, filter_relation, mapping, multi_level, secondary_id), mod.rs still contains the core `apply_joins` method which is 1395 lines. It mixes one-to-one join building, one-to-many join building, index selection, filter extraction, FK resolution, and ordering inversion logic.
- **training_ref:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Split into:
  - `crates/query/src/planner/joins/mod.rs` -- JoinResult type, SelectionJoinInfo, re-exports (est. ~80 lines)
  - `crates/query/src/planner/joins/apply.rs` -- apply_joins main loop and dispatch (est. ~300 lines)
  - `crates/query/src/planner/joins/one_to_one.rs` -- TypeJoinOne construction, ordering inversion (est. ~350 lines)
  - `crates/query/src/planner/joins/one_to_many.rs` -- TypeJoinMany construction, groupBy, indexed child cache (est. ~350 lines)
  - `crates/query/src/planner/joins/helpers.rs` -- FK resolution, filter extraction, index selection for joins (est. ~300 lines)

---

### Finding 10
- **severity:** medium
- **category:** structure
- **crate:** db
- **file:** `crates/db/src/merge_handler/composite.rs` (1372 lines)
- **pattern:** oversized-file
- **description:** Single `process_composite_delta` method with nested transaction handling, field iteration, headstore writes, event emission, and post-commit hooks. Contains 3 distinct phases: (1) head processing and field delta iteration (~lines 1-400), (2) document storage writes within transaction (~lines 400-800), (3) headstore updates and event emission (~lines 800-1372).
- **training_ref:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Split into:
  - `crates/db/src/merge_handler/composite.rs` -- process_composite_delta orchestrator (est. ~300 lines)
  - `crates/db/src/merge_handler/composite_fields.rs` -- field delta processing loop, value merging (est. ~400 lines)
  - `crates/db/src/merge_handler/composite_headstore.rs` -- headstore key writes, priority encoding (est. ~350 lines)
  - `crates/db/src/merge_handler/composite_events.rs` -- event emission, post-commit hooks (est. ~200 lines)

---

### Finding 11
- **severity:** medium
- **category:** structure
- **crate:** query
- **file:** `crates/query/src/query_parse/parser.rs` (1270 lines)
- **pattern:** oversized-file
- **description:** File contains 4 concerns: (1) types (ExplainType, ParsedOperation ~lines 1-80), (2) top-level parse functions parse_request, parse_document (~lines 80-300), (3) field parsing (parse_field_to_select, parse_selection_set ~lines 300-800), (4) argument parsing helpers (parse_doc_ids_value, parse_cid_value, resolve_bool_value ~lines 800-1270).
- **training_ref:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Split into:
  - `crates/query/src/query_parse/parser.rs` -- types + parse_request + parse_document (est. ~300 lines)
  - `crates/query/src/query_parse/field_parser.rs` -- parse_field_to_select, parse_selection_set (est. ~500 lines)
  - `crates/query/src/query_parse/args.rs` -- parse_doc_ids_value, parse_cid_value, resolve_bool_value, parse_optional_int_value (est. ~400 lines)

---

### Finding 12
- **severity:** low
- **category:** structure
- **crate:** crdt
- **file:** `crates/crdt/tests/property_tests.rs` (1269 lines)
- **pattern:** oversized-file
- **description:** Property-based tests for LWW and Counter CRDTs. Tests naturally cluster by CRDT type and property.
- **training_ref:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Split into:
  - `crates/crdt/tests/property_tests/lww.rs` -- LWW property tests (est. ~500 lines)
  - `crates/crdt/tests/property_tests/counter.rs` -- Counter property tests (est. ~500 lines)
  - `crates/crdt/tests/property_tests/helpers.rs` -- shared test helpers, strategies (est. ~270 lines)

---

### Finding 13
- **severity:** high
- **category:** structure
- **crate:** embedded
- **file:** `crates/embedded/src/node.rs` (1267 lines)
- **pattern:** oversized-file
- **description:** File contains 5 concerns: (1) EmbeddedNode struct and basic methods (~lines 1-80), (2) BackgroundTasks struct (~lines 80-130), (3) NodeBuilder and build() method (~lines 130-500), (4) free functions for spawning background tasks (spawn_libp2p_event_handler, spawn_replication_loop, spawn_failure_recorder, spawn retry loops ~lines 500-1000), (5) restore functions for persisted replicators/documents (~lines 1000-1267).
- **training_ref:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Split into:
  - `crates/embedded/src/node.rs` -- EmbeddedNode struct, BackgroundTasks (est. ~130 lines)
  - `crates/embedded/src/node_builder.rs` -- NodeBuilder + build() method (est. ~400 lines)
  - `crates/embedded/src/background_tasks.rs` -- spawn_* functions for event handlers, replication, failure recording (est. ~400 lines)
  - `crates/embedded/src/restore.rs` -- restore_libp2p_replicators, restore_libp2p_documents, restore_iroh_replicators (est. ~250 lines)

---

### Finding 14
- **severity:** medium
- **category:** structure
- **crate:** cli
- **file:** `crates/cli/src/p2p_adapter.rs` (1214 lines)
- **pattern:** oversized-file
- **description:** File contains 3 concerns: (1) CollectionLookup, DocPusher trait definitions and DbDocPusher impl (~lines 1-350), (2) P2PAdapter struct with P2POperations impl for HTTP bridge (~lines 350-900), (3) replicator management logic (add_replicator, remove_replicator with complex capability validation ~lines 900-1214).
- **training_ref:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Split into:
  - `crates/cli/src/p2p_adapter/mod.rs` -- re-exports (est. ~20 lines)
  - `crates/cli/src/p2p_adapter/doc_pusher.rs` -- DocPusher trait, CollectionLookup, DbDocPusher (est. ~350 lines)
  - `crates/cli/src/p2p_adapter/operations.rs` -- P2PAdapter struct, basic P2P operations (est. ~400 lines)
  - `crates/cli/src/p2p_adapter/replicator.rs` -- add_replicator, remove_replicator, capability validation (est. ~350 lines)

---

### Finding 15
- **severity:** low
- **category:** structure
- **crate:** query
- **file:** `crates/query/src/mapper/filter/filter_tests.rs` (1126 lines)
- **pattern:** oversized-file
- **description:** Large test file for filter mapping logic. Tests are independent and could be grouped by filter type (comparison, logical, nested, edge cases).
- **training_ref:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Split tests into submodules within `filter_tests.rs` using `mod comparison_tests`, `mod logical_tests`, `mod nested_tests`, `mod edge_case_tests` with natural groupings. Alternatively split into multiple test files.

---

### Finding 16
- **severity:** medium
- **category:** structure
- **crate:** db
- **file:** `crates/db/src/merge_handler/mod.rs` (1100 lines)
- **pattern:** oversized-file
- **description:** Despite having 8 sub-modules (batch, collection, composite, counter, definition, hook, lww, se_merge), mod.rs contains: (1) MergeError enum (~60 lines), (2) DbMergeHandler struct and MergeHandler trait impl (~200 lines), (3) block decryption logic (~100 lines), (4) signature verification (~200 lines), (5) inline unit tests for signature verification (~400 lines). The tests alone are 400+ lines.
- **training_ref:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Split into:
  - `crates/db/src/merge_handler/mod.rs` -- MergeError, DbMergeHandler struct, MergeHandler impl dispatch (est. ~300 lines)
  - `crates/db/src/merge_handler/decrypt.rs` -- block decryption logic (est. ~100 lines)
  - `crates/db/src/merge_handler/signature.rs` -- verify_block_signature (est. ~200 lines)
  - `crates/db/src/merge_handler/signature_tests.rs` -- all signature verification tests (est. ~400 lines)

---

### Finding 17
- **severity:** low
- **category:** structure
- **crate:** defra-core
- **file:** `crates/defra-core/tests/block_tests.rs` (1080 lines)
- **pattern:** oversized-file
- **description:** Tests for Block type covering DAG-CBOR encoding, CID generation, Go compatibility. Natural split by test category.
- **training_ref:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Split into test submodules: `encoding_tests`, `cid_tests`, `go_compat_tests`.

---

### Finding 18
- **severity:** medium
- **category:** structure
- **crate:** query
- **file:** `crates/query/src/sdl_parse/builder.rs` (1040 lines)
- **pattern:** oversized-file
- **description:** File contains 3 concerns: (1) build_collections and validation (~lines 1-200), (2) build_collection for individual type definitions including FK field generation (~lines 200-700), (3) resolve_field_kind and Tarjan SCC algorithm for cycle detection (~lines 700-1040).
- **training_ref:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Split into:
  - `crates/query/src/sdl_parse/builder.rs` -- build_collections, collect_primary_directives (est. ~200 lines)
  - `crates/query/src/sdl_parse/build_collection.rs` -- build_collection, FK field generation (est. ~500 lines)
  - `crates/query/src/sdl_parse/resolve_kind.rs` -- resolve_field_kind, find_sccs (Tarjan), detect_collection_set (est. ~340 lines)

---

### Finding 19
- **severity:** medium
- **category:** structure
- **crate:** defra-node
- **file:** `crates/defra-node/src/benchmark_support.rs` (985 lines)
- **pattern:** oversized-file
- **description:** Benchmark fixture and execution code: (1) fixture SDL and config types (~lines 1-100), (2) fixture data generation (create_session, create_messages, create_actions ~lines 100-500), (3) query case definitions and execution (SearchQueryCase, run_search_benchmark ~lines 500-985).
- **training_ref:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Split into:
  - `crates/defra-node/src/benchmark_support/mod.rs` -- re-exports (est. ~20 lines)
  - `crates/defra-node/src/benchmark_support/fixture.rs` -- SDL, config, data generation (est. ~500 lines)
  - `crates/defra-node/src/benchmark_support/cases.rs` -- SearchQueryCase, query rendering (est. ~250 lines)
  - `crates/defra-node/src/benchmark_support/runner.rs` -- run_search_benchmark, summarize, formatting (est. ~200 lines)

---

### Finding 20
- **severity:** medium
- **category:** structure
- **crate:** query
- **file:** `crates/query/src/plan/mutation/create.rs` (969 lines)
- **pattern:** oversized-file
- **description:** File contains 3 concerns: (1) CreateInput type and its to_document/to_document_with_schema methods (~lines 1-200), (2) json_to_normal_value and coerce_json_to_scalar_array -- exhaustive type conversion covering every scalar and array variant (~lines 200-565), (3) CreateNode plan node implementation (~lines 565-969).
- **training_ref:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Split into:
  - `crates/query/src/plan/mutation/create.rs` -- CreateInput, CreateNode (est. ~400 lines)
  - `crates/query/src/plan/mutation/type_coercion.rs` -- json_to_normal_value, coerce_json_to_scalar_array, coerce_json_to_scalar (est. ~500 lines). This type coercion logic is also reused by update/upsert and should be shared.

---

### Finding 21
- **severity:** medium
- **category:** structure
- **crate:** defra-node
- **file:** `crates/defra-node/src/lib.rs` (939 lines)
- **pattern:** oversized-file
- **description:** File contains 4 concerns: (1) types (StorageBackend, NodeBuilder, HttpConfig, P2PConfig ~lines 1-520), (2) NodeBuilder::build() with storage backend dispatch (~lines 520-700), (3) EmbeddedNode struct and methods (~lines 700-800), (4) HttpP2PAdapter implementation (~lines 20-200). The HttpP2PAdapter is conditionally compiled and should be a separate file.
- **training_ref:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Split into:
  - `crates/defra-node/src/lib.rs` -- re-exports, StorageBackend, NodeBuilder (est. ~300 lines)
  - `crates/defra-node/src/node.rs` -- EmbeddedNode struct and methods (est. ~200 lines)
  - `crates/defra-node/src/builder.rs` -- NodeBuilder::build() implementation (est. ~250 lines)
  - `crates/defra-node/src/http_adapter.rs` -- HttpP2PAdapter (est. ~200 lines)

---

### Finding 22
- **severity:** high
- **category:** improvement
- **crate:** cli
- **file:** `crates/cli/src/lib.rs`
- **line:** 1-31
- **pattern:** pub-should-be-pub-crate
- **description:** All 20+ adapter modules are declared `pub mod`, making every internal adapter type part of the crate's public API. These adapters (acp_adapter, backup_adapter, block_adapter, collection_mgmt_adapter, doc_acp_adapter, dump_adapter, encrypted_index_adapter, index_adapter, lens_adapter, nac_adapter, schema_adapter, sourcehub_acp_adapter, transport_doc_pusher, transport_version_syncer, txn_adapter, version_syncer, view_adapter) are implementation details used only by `commands/start/server.rs`. Only `cli`, `commands`, `config`, `error`, and `logging` should be `pub`.
- **training_ref:** rust-patterns-book ch15 "Public API Design Checklist" / "Visibility modifiers"
- **suggested_fix:** Change adapter modules to `pub(crate) mod` instead of `pub mod`. Only export modules that external consumers (the `defra` binary) actually need.

---

### Finding 23
- **severity:** high
- **category:** improvement
- **crate:** db
- **file:** `crates/db/src/lib.rs`
- **line:** 46-98
- **pattern:** pub-should-be-pub-crate
- **description:** Almost every module in the `db` crate is `pub mod`, including implementation details like `collection_loader`, `collection_cache`, `collection_snapshot`, `commit_priority_index`, `lensed_fetcher`, `lensed_auto_commit_fetcher`, `schema_loader`, `json_patch`, `lens_utils`, `txn_context`, `versioned_fetcher`, `se`, `embedding`. Many of these expose internal types that should not be part of the public API. The crate already re-exports key types at the bottom, so the modules themselves do not need to be public.
- **training_ref:** rust-patterns-book ch15 "Visibility modifiers"
- **suggested_fix:** Change internal modules to `pub(crate) mod` and only keep `pub mod` for modules whose types are part of the intended public API. Use re-exports in lib.rs for specific types that need to be public.

---

### Finding 24
- **severity:** medium
- **category:** improvement
- **crate:** query
- **file:** `crates/query/src/lib.rs`
- **line:** 21-38
- **pattern:** pub-should-be-pub-crate
- **description:** Internal modules `json_convert`, `test_utils`, `select_convert` are exposed publicly. `json_convert` is purely internal. `test_utils` should be behind `#[cfg(test)]` or a test feature flag. `select_convert` has a single public function that is already re-exported.
- **training_ref:** rust-patterns-book ch15 "Visibility modifiers"
- **suggested_fix:** Change `json_convert` to `pub(crate) mod`. Gate `test_utils` with `#[cfg(any(test, feature = "test-utils"))]`. Keep `select_convert` as-is since it is re-exported.

---

### Finding 25
- **severity:** medium
- **category:** improvement
- **crate:** acp
- **file:** `crates/acp/src/auth_error.rs`
- **line:** 7
- **pattern:** string-param-should-be-str
- **description:** `pub fn normalize_auth_error(err: String, permission: &str) -> String` takes `err` by owned `String`. Since the function only reads and reformats the error, it should accept `&str` to avoid forcing callers to allocate.
- **training_ref:** rust-patterns-book ch15 "Ergonomic Parameter Patterns"
- **suggested_fix:** Change signature to `pub fn normalize_auth_error(err: &str, permission: &str) -> String`.

---

### Finding 26
- **severity:** low
- **category:** improvement
- **crate:** crdt
- **file:** `crates/crdt/src/lww.rs`
- **line:** 153
- **pattern:** string-param-should-be-impl-into
- **description:** `pub fn new(schema_version_id: String, doc_id: &[u8], field_name: String)` takes two owned Strings. Since these are stored, `impl Into<String>` would be more ergonomic, allowing callers to pass `&str` without explicit `.to_string()`.
- **training_ref:** rust-patterns-book ch15 "Ergonomic Parameter Patterns" -- "`impl Into<T>` -- Accept Anything Convertible"
- **suggested_fix:** Change to `pub fn new(schema_version_id: impl Into<String>, doc_id: &[u8], field_name: impl Into<String>)`.

---

### Finding 27
- **severity:** low
- **category:** improvement
- **crate:** crdt
- **file:** `crates/crdt/src/composite.rs`
- **line:** 49
- **pattern:** string-param-should-be-impl-into
- **description:** `pub fn add_field_delta(&mut self, field_name: String, delta: FieldDelta)` takes an owned String. Since the field name is stored in a HashMap, `impl Into<String>` would be more ergonomic.
- **training_ref:** rust-patterns-book ch15 "Ergonomic Parameter Patterns"
- **suggested_fix:** Change to `pub fn add_field_delta(&mut self, field_name: impl Into<String>, delta: FieldDelta)`.

---

### Finding 28
- **severity:** low
- **category:** improvement
- **crate:** crdt
- **file:** `crates/crdt/src/composite.rs`
- **line:** 142-151
- **pattern:** string-param-should-be-impl-into
- **description:** `CompositeState::new(doc_id: DocId, schema_version_id: String)` and `register_lww_field(field_name: String)` take owned Strings where `impl Into<String>` would be more ergonomic.
- **training_ref:** rust-patterns-book ch15 "Ergonomic Parameter Patterns"
- **suggested_fix:** Use `impl Into<String>` for both parameters.

---

### Finding 29
- **severity:** medium
- **category:** improvement
- **crate:** crypto
- **file:** `crates/crypto/src/batch.rs`
- **line:** 33
- **pattern:** string-error-should-be-typed
- **description:** `pub fn sign_batch(cids: &[Cid], config: &SigningConfig) -> Result<BatchSignature, String>` returns `String` as the error type. The training material explicitly recommends structured error types over opaque strings: "Error type: `String` (opaque) -> `ConfigError` (structured)".
- **training_ref:** rust-patterns-book ch15 "Case Study: Designing a Public Crate API"
- **suggested_fix:** Define a `BatchSignError` enum with variants for the different failure modes (missing key, invalid key type, signing failed).

---

### Finding 30
- **severity:** high
- **category:** improvement
- **crate:** (multiple)
- **file:** (multiple lib.rs files)
- **pattern:** missing-sealed-trait
- **description:** 20+ public traits (DocumentACP, Blockstore, DocFetcher, CollectionProvider, QueryExecutor, DocMutator, TransactionRegistry, ZanzibarStore, AcpStore, Bus, Identity, FullIdentity, Key, PublicKey, PrivateKey, PlanNode, TransactionContext, MutationBatchController, RestOperations, SchemaManager, NodeAcpOperations, SourceHubProvider) are defined without the sealed trait pattern. These traits define the core extension points of the system but none use `private::Sealed` to prevent external implementations. For traits that are NOT intended to be implemented outside the workspace (most of them), this is a semver hazard -- adding a method is a breaking change.
- **training_ref:** rust-patterns-book ch15 "Seal traits you don't want users to implement"
- **suggested_fix:** For traits that should only be implemented within the workspace, add the sealed pattern: `mod private { pub trait Sealed {} }` and add `: private::Sealed` as a supertrait. Priority candidates: `Blockstore`, `DocFetcher`, `QueryExecutor`, `TransactionRegistry`, `PlanNode`, `Bus`, `DocMutator`.

---

### Finding 31
- **severity:** medium
- **category:** improvement
- **crate:** (multiple)
- **file:** (multiple)
- **pattern:** missing-non-exhaustive
- **description:** Only 3 enums in the entire codebase use `#[non_exhaustive]` (NacPermission in acp, DagSyncPlan in p2p, PeerStats in p2p). Many public enums should use it: `MergeError` (db), `RestError` (query), `QueryError` (query), `Error` (storage, blockstore, acp, db, crypto), `MergeOutcome` (p2p), `TransportEvent` (p2p), `HostEvent` (p2p). Without `#[non_exhaustive]`, adding a variant is a breaking change for downstream match statements.
- **training_ref:** rust-patterns-book ch15 "`#[non_exhaustive]` -- mark public enums"
- **suggested_fix:** Add `#[non_exhaustive]` to all public error enums and event/outcome enums that may gain variants. Priority: error types in `db`, `query`, `storage`, `acp`, `crypto`, `p2p`.

---

### Finding 32
- **severity:** low
- **category:** structure
- **crate:** query
- **file:** `crates/query/src/plan/type_join/type_join_one.rs` (938 lines)
- **pattern:** oversized-file
- **description:** TypeJoinOne plan node with inverted join logic. Contains both normal and inverted join iteration, plus FK resolution.
- **training_ref:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Extract inverted join logic into `type_join_one_inverted.rs` (est. ~400 lines), keeping normal join in `type_join_one.rs` (est. ~540 lines).

---

### Finding 33
- **severity:** low
- **category:** structure
- **crate:** query
- **file:** `crates/query/src/plan/type_join/type_join_many/plan_node.rs` (932 lines)
- **pattern:** oversized-file
- **description:** TypeJoinMany plan node with per-parent filtering, ordering, grouping, and indexed child fetch. Four distinct iteration strategies in one file.
- **training_ref:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Extract indexed child fetch logic into `indexed_child.rs` (est. ~300 lines) and grouping logic into `grouping.rs` (est. ~200 lines).

---

### Finding 34
- **severity:** low
- **category:** structure
- **crate:** embedded
- **file:** `crates/embedded/src/libp2p_adapter.rs` (896 lines)
- **pattern:** oversized-file
- **description:** Contains trait definitions (CollectionLookup, DocPusher, VersionSyncer), P2PAdapter implementation, and DbDocPusher implementation. Similar structure to cli/p2p_adapter.rs.
- **training_ref:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Split into `libp2p_adapter/traits.rs` (trait definitions ~150 lines), `libp2p_adapter/adapter.rs` (P2PAdapter ~400 lines), `libp2p_adapter/doc_pusher.rs` (DbDocPusher ~350 lines).

---

### Finding 35
- **severity:** medium
- **category:** structure
- **crate:** p2p
- **file:** `crates/p2p/src/sync/replication/mod.rs` (883 lines)
- **pattern:** oversized-file
- **description:** Contains ReplicationLoop, ReplicationConfig, ReplicationResult, parallel worker pool, batch processing, and retry logic in one file.
- **training_ref:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Split into `replication/mod.rs` (types + re-exports ~100 lines), `replication/loop.rs` (ReplicationLoop ~400 lines), `replication/worker.rs` (parallel worker pool ~380 lines).

---

### Finding 36
- **severity:** low
- **category:** structure
- **crate:** query
- **file:** `crates/query/src/runner/mutation.rs` (882 lines)
- **pattern:** oversized-file
- **description:** Mutation execution with create/update/delete/upsert handling, ACP permission checks, and batch processing all in one file.
- **training_ref:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Split ACP permission checking into `mutation_acp.rs` (est. ~200 lines) and keep mutation execution in `mutation.rs` (est. ~680 lines).

---

### Finding 37
- **severity:** low
- **category:** structure
- **crate:** query
- **file:** `crates/query/src/runner/plan.rs` (873 lines)
- **pattern:** oversized-file
- **description:** Plan execution with explain rendering (simple, execute, debug) and result collection. Explain logic is half the file.
- **training_ref:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** The `runner/explain/` directory already exists with mutation.rs and execute.rs. Move remaining explain logic from plan.rs into `runner/explain/` if not already there, reducing plan.rs to ~400 lines.

---

### Finding 38
- **severity:** medium
- **category:** structure
- **crate:** query
- **file:** `crates/query/src/rest.rs` (863 lines)
- **pattern:** oversized-file
- **description:** File contains 3 concerns: (1) RestError type and RestOperations trait (~lines 1-200), (2) RestOperationsImpl with CRUD method implementations (~lines 200-600), (3) helper methods for document get/list/truncate (~lines 600-863).
- **training_ref:** rust-patterns-book ch15 "Module Layout"
- **suggested_fix:** Split into:
  - `crates/query/src/rest/mod.rs` -- RestError, RestOperations trait, re-exports (est. ~200 lines)
  - `crates/query/src/rest/operations.rs` -- RestOperationsImpl CRUD methods (est. ~400 lines)
  - `crates/query/src/rest/helpers.rs` -- document_get_by_id, list, truncate helpers (est. ~260 lines)

---

### Finding 39
- **severity:** low
- **category:** improvement
- **crate:** db
- **file:** `crates/db/src/downsample.rs`
- **line:** 69
- **pattern:** vec-param-should-be-slice
- **description:** `GcDownsampleHistoriesOptions::with_names(names: Vec<String>)` takes owned Vec. Since this is a builder-style constructor that stores the value, this is borderline acceptable, but `impl Into<Vec<String>>` or accepting `&[impl Into<String>]` would be more flexible.
- **training_ref:** rust-patterns-book ch15 "Ergonomic Parameter Patterns"
- **suggested_fix:** Consider `pub fn with_names(names: impl Into<Vec<String>>)` to accept both `Vec<String>` and conversions from iterators.

---

### Finding 40
- **severity:** low
- **category:** improvement
- **crate:** defra-core
- **file:** `crates/defra-core/src/collection.rs`
- **line:** 20
- **pattern:** string-param-should-be-impl-into
- **description:** `pub fn new(id: CollectionId, name: String, version: u32)` takes an owned String for name. Since the value is stored, `impl Into<String>` would be more ergonomic.
- **training_ref:** rust-patterns-book ch15 "Ergonomic Parameter Patterns"
- **suggested_fix:** Change to `pub fn new(id: CollectionId, name: impl Into<String>, version: u32)`.

---

### Finding 41
- **severity:** low
- **category:** improvement
- **crate:** defra-core
- **file:** `crates/defra-core/src/signing.rs`
- **line:** 194
- **pattern:** string-param-should-be-impl-into
- **description:** `pub fn set_request_bearer_token(did: &str, token: String)` takes `token` as owned String. Since it is stored, `impl Into<String>` would allow passing `&str` without allocation at call sites that already have a `String`.
- **training_ref:** rust-patterns-book ch15 "Ergonomic Parameter Patterns"
- **suggested_fix:** Change to `pub fn set_request_bearer_token(did: &str, token: impl Into<String>)`.

---

### Finding 42
- **severity:** high
- **category:** improvement
- **crate:** defra-node
- **file:** `crates/defra-node/src/lib.rs`
- **line:** 201
- **pattern:** duplicate-trait-definitions
- **description:** The `P2POps` trait in defra-node duplicates functionality already defined in `embedded::P2POperations` and `defra_http::P2POperations`. Three near-identical P2P operation traits exist across the workspace: `defra-node::P2POps`, `embedded::P2POperations`, and `defra_http::router::P2POperations`. Each defines `local_peer_id`, `connect_peer`, `connected_peers`, `set_replicator`, etc. with slight signature variations. This trait duplication forces adapter boilerplate like `HttpP2PAdapter` which just forwards calls.
- **training_ref:** rust-patterns-book ch15 "Workspace Organization" -- "Clean dependency boundaries between components"
- **suggested_fix:** Define a single canonical `P2POperations` trait in `defra-core` or a shared crate, and have all consumers depend on that single definition. Remove the duplicate trait definitions and adapter glue.
