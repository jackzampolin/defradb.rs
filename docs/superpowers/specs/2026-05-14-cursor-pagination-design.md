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
| D2 | **Hard error on no supporting index for explicit non-docID order fields**. When `order` is absent OR orders only by `_docID`, the cursor query proceeds without requiring an index (docID-based iteration). | Matches Go (`planner.go:319`); forces performant query patterns up front for explicit field orderings while allowing the natural docID fallback. |
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
- **Fixture generator placement:** `internal/cursor` is a Go internal package (`internal/` directories are import-restricted to their parent module), so the generator **cannot** live in the Rust repo and import the package directly. Two options:
  1. **Recommended:** Add a `tools/cursor-fixtures/main.go` *inside the Go defradb repo* that imports `internal/cursor` and emits JSON to stdout. The Rust side runs `go run ./tools/cursor-fixtures > crates/cursor/tests/fixtures/all.json` against the Go repo at the path tracked by `defra-version`. Fixture output is committed to the Rust repo so the codec build remains hermetic (no Go toolchain required at `cargo test` time).
  2. **Fallback if (1) is blocked upstream:** Build a thin local generator in the Rust repo that copies the relevant `internal/cursor` files into a vendored adjunct module (not the canonical package, just a copy used for fixture generation). Sync on PR #4617 updates.
- Either way: ~30 curated inputs (empty keys, single key, multi-key, datetime, float, unicode field names, large maps). Committed to the Rust repo; regenerated when Go's codec changes. Upstream format changes appear as fixture diffs.

**Size estimate:** `lib.rs` ~80 LOC, `errors.rs` ~25 LOC, tests ~150 LOC.

### 2. GraphQL schema generation

**File layout:**
- `crates/query-parse/src/schema_gen/cursor.rs` (new): `gen_cursor_query_type()`, `gen_page_info_type()`, `gen_cursor_collection_field()`, `cursor_args()` helper.
- `crates/query-parse/src/schema_gen/generator.rs` (extend): top-level generator registers `_cursor` field on `Query` with type `CursorQuery` (nullable; see schema example below).

**New types emitted** (nullability matches Go's `schema/types/cursor.go:25-40`, `schema/generate.go:1600`, and `schema/schema.go:82` — all bare `gql.Boolean`/`gql.String`/`gql.NewList(obj)`, no `gql.NewNonNull` wrapping):

```graphql
type Query {
  # ...existing per-collection fields...
  _cursor: CursorQuery        # nullable wrapper
}

type CursorQuery {
  User(
    first: Int, after: String, last: Int, before: String,
    order: [UserOrder!], filter: UserFilter, docIDs: [ID!],
    cid: String, groupBy: [UserGroupBy!], showDeleted: Boolean
  ): [User]                   # nullable list of nullable User
  # ...one such field per collection...
  _pageInfo: PageInfo         # nullable
}

type PageInfo {
  hasNext: Boolean            # nullable; all four fields are nullable in Go
  hasPrev: Boolean
  startCursor: String
  endCursor: String
}
```

`PageInfo` is a singleton type — not per-collection. The cursor variant of the collection field deliberately omits `limit` and `offset` (the cursor args replace them). Nullability is preserved verbatim from Go so GraphQL schema introspection results match byte-for-byte.

**Reuse:** the cursor collection field reuses the existing `UserOrder`, `UserFilter`, etc. types. The new `(first, after, last, before)` arg block is built via a shared `cursor_args()` helper.

### 3. Parser

**File layout:**
- `crates/query-parse/src/query_parse/parser.rs` (extend): top-level field router recognizes `_cursor`.
- `crates/query-parse/src/query_parse/cursor.rs` (new): `parse_cursor_wrapper()`, `parse_cursor_collection_field()`, `parse_page_info_selection()`.

**Parse rules:**
- When the top-level selection contains a `_cursor` field (possibly aliased), descend into the wrapper.
- Inside the wrapper, expect exactly one collection field and optionally `_pageInfo`.
- Collection field args split: `(first | after | last | before)` go into `CursorParams`; everything else flows into the existing `Select` fields.

**Validation done at parse time** (defers index/token checks to planner). Rust internal variant names mirror Go's exported errors from `client/request/errors.go` and `internal/cursor/errors.go`; **surface messages collapse to Go's exact strings** so FFI test message assertions pass:

| Rust variant | Trigger | Surface string (Go-exact) |
|---|---|---|
| `CursorMustContainQuery` | `_cursor` wrapper has no collection field | `"_cursor block must contain exactly one collection query"` |
| `MultipleQueriesInCursor` | `_cursor` wrapper has multiple collection fields | `"_cursor block cannot contain multiple collection queries"` |
| `FirstMustBeNonNegative` | `first` is negative | `"first must be non-negative"` |
| `LastMustBeNonNegative` | `last` is negative | `"last must be non-negative"` |
| `ForwardBackwardConflict` | Any of `first`/`after` combined with any of `last`/`before` | `"forward parameters (first/after) cannot be combined with backward parameters (last/before)"` |
| `InvalidCursor` | Empty cursor string at parse time (planner reuses this for decode failures) | `"invalid cursor"` |

Go does not have a distinct "after with last" / "before with first" error — those cases fall under `ForwardBackwardConflict`. Go does not have an "order required" error — order is optional (see D2 and §7). There is no "unknown sibling field" error; unknown fields inside the `_cursor` wrapper are handled by the GraphQL library's standard "unknown field" error path.

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

### 4a. Routing: forcing the planner path

The current Rust query runner at `crates/query/src/runner/query/select.rs:287-297` routes simple queries (no nested selections, no relation filters, no ordering index, etc.) through `execute_simple_select`. Cursor queries require the planner's `CursorNode` to be installed (and, when no index is available, the docID-fallback path to be wired), so the routing predicate must force the planner branch:

```rust
let needs_planner = is_view
    || has_nested
    || filter_has_relations
    || order_has_relations
    || aggregates_have_relations
    || aggregate_filter_has_relations
    || has_secondary_relation_id
    || has_ordering_index
    || has_or_filter_index
    || has_similarity
    || has_fulltext_search
    || select.is_cursor;   // <-- added
```

Without this addition, a cursor query like `{ _cursor { User(first: 10) { name } _pageInfo { hasNext } } }` (no order, no relations, no index) would skip planner expansion and never get the `CursorNode` wrap — silently dropping cursor semantics. The Go side enforces this implicitly by routing all queries through the planner; Rust has the optimized simple path and must opt cursor queries out of it.

### 5. Response shaping

GraphQL response shape for cursor queries:

```json
{ "_cursor": { "User": [...], "_pageInfo": { "hasNext": true, ... } } }
```

Response assembly happens in the query runner, not the HTTP-side executor (`crates/query/src/executor.rs` is the HTTP/transaction wrapper; the result Map is built in the runner layer). The relevant integration points are:

- **`crates/query/src/runner/query/mod.rs::execute_query_internal_with_vars`** and **`execute_selects_internal`** (lines 79-89 and 104-114) — these iterate parsed `Select`s and merge each select's result into the top-level response Map under `select.field.output_name()`. For a cursor `Select`, this key is `_cursor` (or its alias) and the value must be a `{ <inner-collection-alias>: [...], _pageInfo: {...} }` object.
- **The per-select execution path** (`execute_select_internal` and the planner execution path it invokes for `is_cursor` selects) — emits the inner collection rows plus the `_pageInfo` payload computed by `CursorNode::finalize_page_info()` as a single Map under the `_cursor` alias.

Activation rules (when `Select.is_cursor` is true):
- Emit rows under the inner collection alias (`User` or its alias).
- If `cursor_page_info` has any field set, emit `_pageInfo` with only those fields populated (selected-only emission, matches Go).
- Wrap the whole thing in the `_cursor` (or its alias) key.

`startCursor`/`endCursor`/`hasNext`/`hasPrev` values come from `CursorNode`'s execution output.

**Assumption to verify during implementation:** the GraphQL HTTP handler at `crates/http/src/handlers/graphql/query.rs::handle_graphql` is shape-agnostic and accepts the new `_cursor` schema without changes. The handler delegates parsing and execution to the runner layer described above, so changes localize there. If the handler does any post-execution field validation, a small additional adjustment may be needed.

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
2. Otherwise (no index seek — either no index was needed because order is absent/docID-only per §7, or an index was used but the cursor lacked `keys`): pull rows from `inner` and discard until row crosses the `after` boundary (`row.doc_id > after.doc_id` under the current sort).
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

1. **Validate index coverage** — `validate_cursor_index(collection, order_fields)` (mirrors Go's `validateCursorIndex` at `planner.go:313-337`):
   - If `order_fields` is empty OR the first ordering is by `_docID`: return `Ok(reversed = false)` — **no index required**, no error, no seek. The cursor query proceeds with docID-based iteration; the natural sort order is the document key, and `CursorNode` operates in slow-path mode (skip-until-after / sliding-window).
   - Otherwise: look up `collection.indexes` from `crates/schema/src/collection.rs::CollectionVersion`. Find an index whose fields can support the requested ordering.
   - On no compatible index for explicit non-docID order → `PlannerError::NoSupportingIndexForCursor`.
   - If a candidate index exists but the ordering only covers a prefix of a non-unique composite index → `PlannerError::NoSupportingIndexForCursor` (Go uses the same error variant for this case via `isUnsupportedCursorCompositePrefix`).
   - Return whether the index supports reversed iteration so `expand_cursor_plan` can set `scan.reversed_iteration` appropriately.
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
    InvalidCursor(cursor::CursorError),
}
```

Go uses a single `ErrNoSupportingIndexForCursor` for both the "no index" and "composite prefix mismatch" cases (see `planner.go:316,324,329,333`). The Rust port follows the same collapse — one variant, one user-facing string. Internal context (which case fired) is captured in `Display` output if useful for debugging but the FFI-visible message stays Go-equivalent. Codec errors collapse to `"invalid cursor"` on the surface, matching Go's `internal/cursor/errors.go::errInvalidCursor`.

## Error handling summary

| Layer | Internal Rust variants | Surface string (Go-exact) | Source |
|---|---|---|---|
| Codec (`crates/cursor`) | `InvalidBase64`, `InvalidJson`, `EmptyDocId` | All collapse to `"invalid cursor"` (or `"failed to encode cursor"` on encode failure) | `CursorError` |
| Parse (`crates/query-parse`) | `CursorMustContainQuery`, `MultipleQueriesInCursor`, `FirstMustBeNonNegative`, `LastMustBeNonNegative`, `ForwardBackwardConflict`, `InvalidCursor` | Each maps to Go's literal string from `client/request/errors.go` (see §3 table) | `ParseError` |
| Planner (`crates/query-plan`) | `NoSupportingIndexForCursor`, `InvalidCursor(_)` | Go-equivalent strings from `internal/planner/errors.go` and `internal/cursor/errors.go` | `PlannerError` |

Surface strings are byte-identical to Go's. Internal variant names exist for Rust-side ergonomics and pattern matching but never reach GraphQL response output unchanged.

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
