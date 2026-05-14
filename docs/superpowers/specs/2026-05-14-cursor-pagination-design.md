# Cursor-Based GraphQL Pagination — Design

**Date:** 2026-05-14
**Author:** brainstorm with Jack Zampolin
**Tracks Go PR:** [sourcenetwork/defradb#4617](https://github.com/sourcenetwork/defradb/pull/4617) — *"feat: Cursor based query pagination"* (OPEN)
**Supersedes:** [defradb.rs#930](https://github.com/sourcenetwork/defradb.rs/pull/930) (closed; REST limit/offset)
**Branch:** `feat/cursor-pagination`
**Worktree:** `.claude/worktrees/feat+cursor-pagination`

## Context

Go PR #4617 introduces cursor-based GraphQL pagination as a parallel `_cursor` query type with `first`/`after`/`last`/`before` args, an opaque base64url-JSON cursor token, and a `_pageInfo` sibling. defradb.rs#930 took a different direction (REST limit/offset metadata on the collection-doc-IDs endpoint) and was closed in favor of mirroring Go.

This spec defines a Rust port that matches Go's user-facing API byte-for-byte (query shape, token format, error wording) so that Go's existing 50+ integration tests under `tests/integration/query/cursor/` can serve as the parity gate via the FFI test harness.

## Decisions

The following design choices were made during brainstorming and are inputs to the rest of the spec:

| # | Decision | Why |
|---|---|---|
| D1 | **Mirror Go PR #4617 exactly**, even while it remains open and may shift | Maximum parity; we'll re-port if upstream changes |
| D2 | **Hard error on no supporting index** for cursor order fields | Matches Go; forces performant query patterns up front |
| D3 | **Byte-for-byte cross-compatible cursor tokens** between Go and Rust nodes | Tokens issued by either implementation must round-trip through the other |
| D4 | **GraphQL shape: separate `_cursor` wrapper field on the existing `Query` root** | Mirrors Go's user-facing query shape; avoids depending on multi-root GraphQL library support |
| D5 | **Token codec lives in a new `crates/cursor` workspace crate** | Mirrors Go's `internal/cursor` package; clean leaf dependency for all consumers |
| D6 | **Testing strategy: FFI runs Go's existing cursor tests against the Rust node; native Rust integration tests cover Rust-specific surfaces** | Best parity-cost ratio; FFI is the gate |
| D7 | **Land as one big PR** | Matches Go's single-PR shape; preserves semantic cohesion for review |

## Architecture

Cursor pagination is a vertical slice through the GraphQL layer, request types, planner, scan/index fetcher, and test harness. Each Go package maps to an existing Rust crate plus one new crate.

### Go → Rust crate mapping

| Go package | Rust crate / module |
|---|---|
| `client/request/cursor.go`, modifications to `consts.go`, `errors.go` | `crates/query-types/src/mapper/types.rs` (extend `Select`) + new `cursor.rs` submodule |
| `internal/cursor/` (new package) | **new `crates/cursor`** |
| `internal/request/graphql/parser/cursor.go` (new) | `crates/query-parse/src/query_parse/parser.rs` (extend) + new `cursor.rs` submodule |
| `internal/request/graphql/schema/types/cursor.go`, `schema/generate.go` (mods) | `crates/query-parse/src/schema_gen/generator.rs` (extend) + new `cursor.rs` submodule |
| `internal/planner/cursor.go` (new) | `crates/query-plan/src/plan/cursor.rs` (new) |
| `internal/planner/planner.go` (`expandCursorPlan`, `validateCursorIndex`) | `crates/query-plan/src/planner/builder/cursor.rs` (new) + wiring in `builder/groupby.rs` |
| `internal/planner/scan.go` mods | `crates/query-plan/src/plan/{scan,index_scan}.rs` + `planner/index_selection/types.rs` (`IndexScanParams` extension) |
| `internal/db/fetcher/indexer_iterators.go` mods | `crates/query-plan/src/fetcher.rs` trait + concrete impl in `crates/db` |
| `tests/integration/query/cursor/` (50+ files) | Driven via FFI through `tools/ffi-test/`; supplemented by `tools/integration-test/tests/cursor.rs` (new) |

### Runtime data flow

```
GraphQL request
  → query-parse: parser detects `_cursor` wrapper, extracts first/after/last/before,
                 builds Select with is_cursor=true and CursorParams attached
  → query-types::mapper: Select carries cursor_params + cursor_page_info bitset
  → query-plan::planner: builds plan (scan/filter/order), then expand_cursor_plan()
                 validates index coverage, decodes cursor token, sets scan.cursor_seek_key,
                 wraps top with CursorNode
  → executor: IndexScanNode seeks to cursor key via DocFetcher::get_by_index_scan(
                 IndexScanParams { cursor_seek: Some(...), .. })
  → CursorNode: probes one extra row for hasNext/hasPrev, encodes startCursor/endCursor
                 from boundary docs, emits results
  → query-parse/executor: response wraps result under `_cursor` (preserving alias)
                 and emits `_pageInfo` with only selected fields
```

## Components

### 1. `crates/cursor` — token codec

A new workspace crate. Leaf dependency: `serde`, `serde_json`, `base64`, `thiserror`. No internal workspace dependencies.

**Public API (`lib.rs`):**

```rust
pub struct Cursor {
    pub doc_id: String,
    pub keys: BTreeMap<String, serde_json::Value>,
}

impl Cursor {
    pub fn encode(&self) -> String;
    pub fn decode(token: &str) -> Result<Self, CursorError>;
    pub fn from_doc_id(doc_id: impl Into<String>) -> Self;
}
```

**Wire format:**
- JSON: `{"d": "<docID>", "k": {"<field>": <value>, ...}}`
- `k` omitted when empty (`#[serde(skip_serializing_if = "BTreeMap::is_empty")]`); matches Go's `omitempty`.
- `BTreeMap<String, _>` gives deterministic alphabetical key ordering — the crux of byte-for-byte cross-compatibility, since Go's `encoding/json` sorts map keys alphabetically.
- Outer encoding: `base64::engine::general_purpose::URL_SAFE_NO_PAD`.
- Value type is `serde_json::Value` so we round-trip whatever Go encoded (strings, numbers, bools, nulls, ISO-8601 datetime strings, etc.) without committing to a Rust-side type model. Type-specific encoding stays a concern of the caller.

**Errors (`errors.rs`):**

```rust
pub enum CursorError {
    InvalidBase64(base64::DecodeError),
    InvalidJson(serde_json::Error),
    EmptyDocId,
}
```

Wrapping via `#[derive(thiserror::Error)]` with `#[from]`.

**Cross-compat tests (`crates/cursor/tests/`):**
- `go_fixtures.rs` loads fixtures from `crates/cursor/tests/fixtures/*.json`. Each fixture: `{ "token": "<base64url string>", "decoded": { "d": "...", "k": {...} } }`.
- For each fixture, assert both directions: `Cursor::decode(token) == decoded` AND `decoded.encode() == token`.
- Fixtures generated by a small Go binary at `tools/ffi-test/cursor_fixtures/main.go` that wraps Go's `internal/cursor.Encode` against ~30 curated inputs (empty keys, single key, multi-key, datetime, float, unicode field names, large maps). Generated once; committed to the repo. Regenerate when Go's codec changes.
- Keeps the Rust build hermetic (no Go toolchain required to test the codec) and makes upstream format changes visible as fixture diffs.

**Size estimate:** `lib.rs` ~80 LOC, `errors.rs` ~25 LOC, tests ~150 LOC.

### 2. GraphQL schema generation

**File layout:**
- `crates/query-parse/src/schema_gen/cursor.rs` (new): `gen_cursor_query_type()`, `gen_page_info_type()`, `gen_cursor_collection_field()`, `cursor_args()` helper.
- `crates/query-parse/src/schema_gen/generator.rs` (extend): top-level generator registers `_cursor` field on `Query` with type `CursorQuery!`.

**New types emitted:**

```graphql
type Query {
  # ...existing per-collection fields...
  _cursor: CursorQuery!
}

type CursorQuery {
  User(
    first: Int, after: String, last: Int, before: String,
    order: [UserOrder!], filter: UserFilter, docIDs: [ID!],
    cid: String, groupBy: [UserGroupBy!], showDeleted: Boolean
  ): [User!]!
  # ...one such field per collection...
  _pageInfo: PageInfo!
}

type PageInfo {
  hasNext: Boolean!
  hasPrev: Boolean!
  startCursor: String
  endCursor: String
}
```

`PageInfo` is a singleton type — not per-collection. The cursor variant of the collection field deliberately omits `limit` and `offset` (the cursor args replace them).

**Reuse:** the cursor collection field reuses the existing `UserOrder`, `UserFilter`, etc. types. The new `(first, after, last, before)` arg block is built via a shared `cursor_args()` helper.

### 3. Parser

**File layout:**
- `crates/query-parse/src/query_parse/parser.rs` (extend): top-level field router recognizes `_cursor`.
- `crates/query-parse/src/query_parse/cursor.rs` (new): `parse_cursor_wrapper()`, `parse_cursor_collection_field()`, `parse_page_info_selection()`.

**Parse rules:**
- When the top-level selection contains a `_cursor` field (possibly aliased), descend into the wrapper.
- Inside the wrapper, expect exactly one collection field and optionally `_pageInfo`.
- Collection field args split: `(first | after | last | before)` go into `CursorParams`; everything else flows into the existing `Select` fields.

**Validation done at parse time** (defers index/token checks to planner):

| Error | Trigger |
|---|---|
| `CursorConflictingDirection` | Both `first` and `last` provided |
| `CursorMismatchedCursorArg` | `after` with `last`, or `before` with `first` |
| `CursorNegativeCount` | `first` or `last` < 0 |
| `CursorOrderRequired` | Cursor field missing `order` arg |
| `CursorWrapperShape` | Zero or multiple collection fields under `_cursor`, or unknown sibling |

Error wording is lifted verbatim from Go's `client/request/errors.go` and `internal/planner/errors.go` so FFI tests' message assertions pass.

### 4. Request types

**`crates/query-types/src/mapper/types.rs`:**

```rust
pub struct Select {
    // ...existing fields...
    pub is_cursor: bool,
    pub cursor_params: Option<CursorParams>,
    pub cursor_page_info: CursorPageInfoFields,
}

pub struct CursorParams {
    pub first: Option<u64>,
    pub after: Option<String>,   // raw token; decoded in planner
    pub last: Option<u64>,
    pub before: Option<String>,
}

pub struct CursorPageInfoFields {
    pub has_next: bool,
    pub has_prev: bool,
    pub start_cursor: bool,
    pub end_cursor: bool,
}
```

Cursor and limit/offset are mutually exclusive structurally — the cursor variant of the collection field doesn't expose `limit`/`offset` in the schema, so the existing `Select.limit: Option<Limit>` field stays untouched.

### 5. Response shaping

GraphQL response shape for cursor queries:

```json
{ "_cursor": { "User": [...], "_pageInfo": { "hasNext": true, ... } } }
```

A new wrapper step in the result encoder (likely `crates/query/src/executor.rs`) activates when `Select.is_cursor` is true:
- Emit rows under the inner collection alias (`User` or its alias).
- If `cursor_page_info` has any field set, emit `_pageInfo` with only those fields populated (selected-only emission, matches Go).
- Wrap the whole thing in the `_cursor` (or its alias) key.

`startCursor`/`endCursor`/`hasNext`/`hasPrev` values come from `CursorNode`'s execution output.

**Assumption to verify during implementation:** the GraphQL HTTP handler at `crates/http/src/handlers/graphql/query.rs::handle_graphql` is shape-agnostic and accepts the new `_cursor` schema without changes. If the executor has hardcoded field-routing for known Query fields, a small additional change there.

### 6. Planner — `CursorNode`

**File:** `crates/query-plan/src/plan/cursor.rs` (new).

```rust
pub struct CursorNode {
    inner: Box<dyn PlanNode>,
    direction: CursorDirection,         // Forward | Backward
    page_size: u64,                     // from `first` or `last`
    after: Option<Cursor>,              // decoded after-token (Forward)
    before: Option<Cursor>,             // decoded before-token (Backward)
    page_info_fields: CursorPageInfoFields,
    order_fields: Vec<OrderField>,      // to extract `keys` from boundary docs

    // execution state
    state: CursorState,                 // SkippingUntilAfter | Collecting | Probing | Drained
    buffer: VecDeque<Row>,              // Backward path; bounded to page_size + 1
    first_doc: Option<Row>,             // for startCursor
    last_doc: Option<Row>,              // for endCursor
    has_next: bool,
    has_prev: bool,
    index_seek_active: bool,            // set by planner
}
```

**Forward semantics (`async fn next() -> Result<Option<Row>>`):**

1. If `index_seek_active` is true: index has already positioned past `after`. Skip directly to `Collecting`.
2. Otherwise: pull rows from `inner` and discard until row crosses the `after` boundary (`row.doc_id > after.doc_id` under the current sort).
3. `Collecting`: pull `page_size` rows. Record `first_doc` on first emit, update `last_doc` on every emit. Yield each.
4. After yielding `page_size` rows, do one extra `inner.next()`. If it yields → `has_next = true`. Mark `Drained`.
5. `has_prev = after.is_some()` (best-effort, matches Go).

**Backward semantics:**

- **Index seek + `cursor_driven_ordering` active:** scan is iterating in reverse from the `before` boundary. Buffer up to `page_size + 1` rows. Reverse the buffer (so output is in logical forward order) and emit the last `page_size`. The +1 sets `has_prev`.
- **No index seek path:** drain rows up to the `before` boundary into a sliding window of size `page_size + 1`. Emit the window in order; the +1 sets `has_prev`.
- `has_next = before.is_some()`.

**`startCursor`/`endCursor` encoding:** `CursorNode::finalize_page_info()` takes `first_doc` and `last_doc`, extracts `(doc_id, keys)` where `keys` is a `BTreeMap<String, Value>` of `order_fields → row[field]`, builds a `cursor::Cursor`, calls `.encode()`. Cached on the node for the response encoder.

### 7. Planner expansion

**File:** `crates/query-plan/src/planner/builder/cursor.rs` (new).

Function `expand_cursor_plan(select, collection, plan) -> Result<PlanTree>` invoked from `builder/groupby.rs::apply_groupby_ordering_limit` when `select.is_cursor == true`, replacing the `LimitNode` wrap step:

```rust
if select.is_cursor {
    plan = expand_cursor_plan(select, collection, plan)?;
} else if let Some(limit) = &select.limit {
    plan = LimitNode::wrap(plan, limit);
}
```

**Steps inside `expand_cursor_plan`:**

1. **Validate index coverage** — `validate_cursor_index(collection, order_fields)`:
   - Look up `collection.indexes` from `crates/schema/src/collection.rs::CollectionVersion`.
   - Find an index whose field prefix matches `order_fields` (composite prefix rule for non-unique; more flexible for unique).
   - On no match → `PlannerError::NoSupportingIndexForCursor`. **Hard error**, no fallback.
   - If index exists but doesn't cover all order fields → `PlannerError::UnsupportedCursorCompositePrefix`.
2. **Decode cursor tokens** — `cursor::Cursor::decode(after)` and `cursor::Cursor::decode(before)`. On error → `PlannerError::InvalidCursor`.
3. **Configure the scan** — walk the plan tree to find the `IndexScanNode` and set:
   - `cursor_seek_key: Option<IndexDataStoreKey>` — built via `build_cursor_seek_key(index_desc, cursor.keys, direction)`. Forward: seek-exclusive past the boundary; Backward: seek-inclusive then iterate reversed.
   - `reversed_iteration: bool` — true for Backward when index direction is reversible.
   - `cursor_driven_ordering: bool` — records that the scan output is already ordered relative to the cursor boundary (no further sort needed).
4. **Wrap top** — build `CursorNode` with decoded tokens, page size, order fields, and `index_seek_active` flag. The planner sets `index_seek_active = true` iff it configured `cursor_seek_key` on the scan above and the cursor's `keys` map fully covered the index field prefix; the `CursorNode` reads this flag to decide between fast-path (no skip loop) and slow-path execution. `cursor_driven_ordering` is a parallel scan-side signal consumed by downstream ordering decisions; the two are typically set together but track different concerns.

### 8. `IndexScanParams` extension

**File:** `crates/query-plan/src/planner/index_selection/types.rs`.

```rust
pub struct IndexScanParams {
    // ...existing fields...
    pub cursor_seek: Option<CursorSeek>,
}

pub struct CursorSeek {
    pub seek_key: IndexDataStoreKey,
    pub inclusive: bool,   // false=Forward (skip boundary), true=Backward
    pub reversed: bool,    // iterate index in reverse direction
}
```

### 9. Fetcher

**File:** `crates/query-plan/src/fetcher.rs` (trait extension) + concrete impl in `crates/db`.

`DocFetcher::get_by_index_scan` honors `params.cursor_seek`:
- `Some(seek)`: position storage iterator at `seek.seek_key`. Use exclusive or inclusive positioning per `seek.inclusive`. If `seek.reversed`, iterate descending.
- `None`: existing behavior (iterate from index start).

The concrete implementation translates `IndexDataStoreKey` to the storage-backend key prefix and uses each backend's range-iteration API. All four backends (redb, fjall, rocksdb, memory) expose ordered range iteration; this is a new arg path, not a new capability.

**No new "indexer iterators" abstraction.** Go has one (`indexer_iterators.go`) because its fetcher is more layered. Rust's fetcher already does range iteration inline; we add a seek parameter rather than a new iterator type.

### 10. Planner errors

**File:** `crates/query-plan/src/planner/errors.rs`.

```rust
pub enum PlannerError {
    // ...existing...
    NoSupportingIndexForCursor { collection: String, order: Vec<OrderField> },
    UnsupportedCursorCompositePrefix { collection: String, index: String, order: Vec<OrderField> },
    InvalidCursor(cursor::CursorError),
}
```

`Display` impl wording is lifted verbatim from Go's `internal/planner/errors.go` so FFI test message assertions pass.

## Error handling summary

| Layer | Error variants | Source |
|---|---|---|
| Codec (`crates/cursor`) | `InvalidBase64`, `InvalidJson`, `EmptyDocId` | `CursorError` |
| Parse (`crates/query-parse`) | `CursorConflictingDirection`, `CursorMismatchedCursorArg`, `CursorNegativeCount`, `CursorOrderRequired`, `CursorWrapperShape` | `ParseError` |
| Planner (`crates/query-plan`) | `NoSupportingIndexForCursor`, `UnsupportedCursorCompositePrefix`, `InvalidCursor(_)` | `PlannerError` |

All surface in the GraphQL response as standard `errors[]` entries with strings matching Go's wording.

## Testing strategy

Three layers, ordered by cheapness:

### Layer 1 — Codec cross-compat unit tests

- Lives in `crates/cursor/tests/`. Runs as `cargo test -p cursor`.
- Loads Go-generated fixtures (`crates/cursor/tests/fixtures/*.json`).
- Asserts byte-identical encode + correct decode for each fixture.
- ~30 cases covering empty keys, single/multi-key, datetime, float, unicode field names, large maps, edge cases.
- Fastest feedback loop; runs in milliseconds.

### Layer 2 — FFI parity (primary gate)

- Existing FFI infrastructure runs Go's `tests/integration/query/cursor/*_test.go` against the Rust node.
- The Go test driver issues GraphQL queries with `_cursor { ... }` shape through the existing HTTP/GraphQL bridge — **no new FFI bindings required** because the cursor work lives entirely behind the GraphQL HTTP boundary.
- ~18 Go test files / ~5000 LOC of test code covering page sizes, forward/backward, composite indexes, datetime/float/string fields, multi-round-trip, edge cases, explain.
- All passing against the Rust node = parity gate cleared.

### Layer 3 — Rust-native integration tests

A new `[[test]]` binary in `tools/integration-test/Cargo.toml`: `cursor.rs`. Submodules:

| Module | Coverage |
|---|---|
| `smoke` | Basic forward `first`/`after`; basic backward `last`/`before`; `_pageInfo` shape |
| `error_paths` | No-index error; invalid token; conflicting direction; negative count; wrapper-shape errors |
| `storage_backends` | Same cursor query against redb / fjall / rocksdb / memory — catches backend-specific seek bugs (FFI tests typically use one backend) |
| `composite_index` | Cursor over composite index, both prefix and full-field-match scenarios |
| `subscription_interaction` | Cursor queries do not break subscription delivery (Rust-specific code path) |

~600–800 LOC of native tests. Native coverage focuses on what FFI cannot cover well (storage-backend matrix, Rust-only paths).

### Local validation order during implementation

1. `cargo test -p cursor` — codec
2. `cargo test -p query-parse` — parser + schema-gen
3. `cargo test -p query-plan` — planner
4. `cargo test -p integration-test --test cursor` — native end-to-end
5. FFI test run against Go's cursor suite — parity gate before opening PR
6. `cargo clippy --all -- -D warnings` and `cargo fmt --all`

## Size & complexity estimate

| Component | New LOC | Modified LOC |
|---|---|---|
| `crates/cursor` (codec + tests) | ~250 | 0 |
| Schema generation | ~150 | ~50 |
| Parser | ~200 | ~50 |
| Request types (`Select` extensions) | ~80 | ~30 |
| Response shaping | ~100 | ~30 |
| `CursorNode` (`plan/cursor.rs`) | ~400 | 0 |
| Planner expansion (`builder/cursor.rs`) | ~250 | ~30 (groupby wiring) |
| `IndexScanParams` / `CursorSeek` | ~30 | 0 |
| Fetcher seek path | ~150 | 0 |
| Planner errors | ~50 | 0 |
| Native integration tests | ~700 | 0 |
| **Total** | **~2360** | **~190** |

Compares to Go PR #4617's ~5400 LOC of source changes (the Go PR also contains ~5000 LOC of integration tests, which we substitute with FFI runs).

## Out of scope

- REST collection-doc-IDs pagination (was the original direction of #930; explicitly out)
- Cursor pagination for filtered/ordered arbitrary GraphQL queries beyond what Go's cursor query supports
- Changes to document CRUD routes
- Warn-and-fallback when no index exists (rejected; hard error matches Go)
- Cursor support for subscription queries (subscriptions remain unpaginated; native test only asserts the two don't break each other)

## Open assumptions

These should be validated during implementation, not after:

1. **GraphQL HTTP handler is shape-agnostic.** `crates/http/src/handlers/graphql/query.rs::handle_graphql` accepts any schema the generator emits. If the executor hardcodes Query field routing, a small executor patch will be needed (not a design change).
2. **All four storage backends support inclusive/exclusive range positioning** in the direction needed. redb and rocksdb clearly do; verify fjall and memory don't need adapter code.
3. **Go fixture generation tool can run in CI** to regenerate fixtures when Go's codec changes. If not, fixtures regenerate locally on demand and the FFI test suite catches drift.
