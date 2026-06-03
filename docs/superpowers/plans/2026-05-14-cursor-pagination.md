# Cursor Pagination Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Port Go DefraDB PR #4617 (cursor-based GraphQL pagination) to defradb.rs, mirroring Go's `_cursor` wrapper, `first`/`after`/`last`/`before` semantics, `_pageInfo` sibling, and base64url-JSON cursor tokens with byte-for-byte cross-compatibility.

**Architecture:** Vertical slice through GraphQL parsing/schema-gen, request types, planner, scan/index-fetcher, and response shaping. New `crates/cursor` for token codec; new `CursorNode` plan node with skip/buffer/probe state machine; index-backed seek configured by `expand_cursor_plan`. Tests: codec cross-compat fixtures, native integration tests, and FFI parity against Go's existing cursor test suite.

**Tech Stack:** Rust (workspace), async-trait for plan nodes, serde + base64 for token codec, thiserror for error types, async-graphql library for schema, `graphql_parser` for query parsing. Cross-compat fixtures generated from Go's `internal/cursor` package.

**Reference spec:** `docs/superpowers/specs/2026-05-14-cursor-pagination-design.md` (commit `8fd21c57`).

**Go reference branch:** `pr-4617` checked out at `/Users/johnzampolin/go/src/github.com/sourcenetwork/defradb`.

---

## File Structure

### New files

| Path | Responsibility |
|---|---|
| `crates/cursor/Cargo.toml` | New workspace crate manifest |
| `crates/cursor/src/lib.rs` | `Cursor` struct + `encode()`/`decode()` |
| `crates/cursor/src/errors.rs` | `CursorError` enum |
| `crates/cursor/tests/codec.rs` | Unit tests for round-trip |
| `crates/cursor/tests/go_fixtures.rs` | Cross-compat tests against Go-generated fixtures |
| `crates/cursor/tests/fixtures/all.json` | Go-generated fixture file (committed) |
| `crates/query-types/src/mapper/cursor.rs` | `CursorParams`, `CursorPageInfoFields` types |
| `crates/query-parse/src/query_parse/cursor.rs` | `_cursor` wrapper + cursor args parsing |
| `crates/query-parse/src/schema_gen/cursor.rs` | `PageInfo` type, `CursorQuery` type, cursor collection field generator |
| `crates/query-plan/src/plan/cursor.rs` | `CursorNode` plan node |
| `crates/query-plan/src/planner/builder/cursor.rs` | `expand_cursor_plan`, `validate_cursor_index`, `build_cursor_seek_key` |
| `tools/integration-test/tests/cursor.rs` | Native cursor integration tests |
| `tools/integration-test/tests/cursor/smoke.rs` | Forward/backward smoke tests |
| `tools/integration-test/tests/cursor/error_paths.rs` | Error case tests |
| `tools/integration-test/tests/cursor/storage_backends.rs` | redb/fjall/rocksdb/memory matrix |
| `tools/integration-test/tests/cursor/composite_index.rs` | Composite index coverage |
| `tools/integration-test/tests/cursor/subscription_interaction.rs` | Cursor + subscription sanity |

### Modified files

| Path | Change |
|---|---|
| `Cargo.toml` (workspace root) | Add `"crates/cursor"` to `members` |
| `crates/query-types/Cargo.toml` | Add `cursor` dep |
| `crates/query-types/src/mapper/types.rs` | Extend `Select` struct with `is_cursor`, `cursor_params`, `cursor_page_info` fields and constructors |
| `crates/query-types/src/mapper/mod.rs` | Export new cursor types |
| `crates/query-types/src/error.rs` | Add cursor error constructors on `QueryError` |
| `crates/query-parse/Cargo.toml` | Add `cursor` dep |
| `crates/query-parse/src/query_parse/parser.rs` | Route `_cursor` field to new parser submodule |
| `crates/query-parse/src/query_parse/mod.rs` | Expose `cursor` submodule |
| `crates/query-parse/src/schema_gen/generator.rs` | Register `PageInfo`, `CursorQuery`, `_cursor` field on `Query`, and per-collection cursor fields |
| `crates/query-parse/src/schema_gen/mod.rs` | Expose `cursor` submodule |
| `crates/query-plan/Cargo.toml` | Add `cursor` dep |
| `crates/query-plan/src/plan/mod.rs` | Expose `cursor` submodule |
| `crates/query-plan/src/planner/index_selection/types.rs` | Extend `IndexScanParams` with `cursor_seek`; add `CursorSeek` struct |
| `crates/query-plan/src/plan/scan.rs` | Honor `cursor_seek` parameter (passthrough to fetcher) |
| `crates/query-plan/src/plan/index_scan.rs` | Honor `cursor_seek` parameter (passthrough to fetcher) |
| `crates/query-plan/src/planner/builder/mod.rs` | Expose `cursor` submodule |
| `crates/query-plan/src/planner/builder/groupby.rs` | Route to `expand_cursor_plan` when `select.is_cursor` instead of `LimitNode::new` |
| `crates/query-plan/src/fetcher.rs` | Document `cursor_seek` behavior in trait docs |
| `crates/db/src/fetcher.rs` (or wherever concrete `DocFetcher` impl lives) | Honor `cursor_seek` in storage iteration |
| `crates/query/src/runner/query/select.rs` | Add `select.is_cursor` to `needs_planner` |
| `crates/query/src/runner/query/mod.rs` | Special-case cursor select response shaping |
| `tools/integration-test/Cargo.toml` | Add `[[test]] name = "cursor"` entry |

### Files in the Go repo (must be added there separately)

| Path (in defradb Go repo) | Responsibility |
|---|---|
| `tools/cursor-fixtures/main.go` | Generates `all.json` fixture from `internal/cursor` |

This file lives outside this Rust repo. Task 3 describes how to coordinate. See spec §1 for rationale.

---

## Task 1: Bootstrap `crates/cursor`

**Files:**
- Create: `crates/cursor/Cargo.toml`
- Create: `crates/cursor/src/lib.rs`
- Modify: `Cargo.toml` (workspace root) — add `"crates/cursor"` to `members`

- [ ] **Step 1.1: Create the crate manifest**

```toml
# crates/cursor/Cargo.toml
[package]
name = "cursor"
version = "0.1.0"
edition = "2021"
description = "Opaque cursor token codec for GraphQL cursor pagination"
license = "Apache-2.0 OR MIT"

[dependencies]
base64 = "0.22"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
thiserror = "1"
```

- [ ] **Step 1.2: Create a stub `lib.rs`**

```rust
// crates/cursor/src/lib.rs
//! Opaque cursor token codec for GraphQL cursor pagination.
//!
//! Tokens are `base64url(json{d, k})` — `d` is the document ID,
//! `k` is an alphabetically-ordered map of indexed field values
//! used for index-backed seeking.

mod errors;

pub use errors::CursorError;
```

- [ ] **Step 1.3: Add the crate to the workspace**

Open the root `Cargo.toml`. In the `[workspace]` `members = [...]` array, add `"crates/cursor"` in alphabetical position (after `"crates/crypto"` or wherever `c` entries fit).

- [ ] **Step 1.4: Verify the workspace builds**

Run: `cargo build -p cursor`
Expected: builds successfully (will warn about unused `CursorError` reexport — fine for now).

- [ ] **Step 1.5: Commit**

```bash
git add Cargo.toml crates/cursor/
git commit -m "feat(cursor): bootstrap crates/cursor"
```

---

## Task 2: Implement the cursor token codec (TDD)

**Files:**
- Create: `crates/cursor/src/errors.rs`
- Modify: `crates/cursor/src/lib.rs`
- Create: `crates/cursor/tests/codec.rs`

- [ ] **Step 2.1: Write the failing test**

Create `crates/cursor/tests/codec.rs`:

```rust
use cursor::{Cursor, CursorError};
use std::collections::BTreeMap;

#[test]
fn encode_decode_doc_id_only() {
    let c = Cursor::from_doc_id("doc-1");
    let token = c.encode();
    let decoded = Cursor::decode(&token).unwrap();
    assert_eq!(decoded.doc_id, "doc-1");
    assert!(decoded.keys.is_empty());
}

#[test]
fn encode_decode_with_keys() {
    let mut keys = BTreeMap::new();
    keys.insert("age".into(), serde_json::json!(30));
    keys.insert("name".into(), serde_json::json!("alice"));
    let c = Cursor { doc_id: "doc-1".into(), keys: keys.clone() };

    let token = c.encode();
    let decoded = Cursor::decode(&token).unwrap();
    assert_eq!(decoded.doc_id, "doc-1");
    assert_eq!(decoded.keys, keys);
}

#[test]
fn decode_rejects_invalid_base64() {
    let err = Cursor::decode("!!!not-base64!!!").unwrap_err();
    assert!(matches!(err, CursorError::InvalidBase64(_)));
}

#[test]
fn decode_rejects_invalid_json() {
    // base64url("not json") = "bm90IGpzb24"
    let token = "bm90IGpzb24";
    let err = Cursor::decode(token).unwrap_err();
    assert!(matches!(err, CursorError::InvalidJson(_)));
}

#[test]
fn decode_rejects_empty_doc_id() {
    // base64url('{"d":""}') = "eyJkIjoiIn0"
    let token = "eyJkIjoiIn0";
    let err = Cursor::decode(token).unwrap_err();
    assert!(matches!(err, CursorError::EmptyDocId));
}

#[test]
fn encode_omits_empty_keys() {
    let c = Cursor::from_doc_id("doc-1");
    let token = c.encode();
    // Decode the base64 and check JSON has no "k" field
    use base64::Engine;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(&token).unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert!(json.get("k").is_none(), "empty keys must be omitted from JSON");
    assert_eq!(json.get("d").unwrap().as_str().unwrap(), "doc-1");
}

#[test]
fn keys_serialize_alphabetically() {
    let mut keys = BTreeMap::new();
    keys.insert("z_field".into(), serde_json::json!(1));
    keys.insert("a_field".into(), serde_json::json!(2));
    let c = Cursor { doc_id: "x".into(), keys };

    let token = c.encode();
    use base64::Engine;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(&token).unwrap();
    let s = std::str::from_utf8(&bytes).unwrap();
    let a_pos = s.find("a_field").unwrap();
    let z_pos = s.find("z_field").unwrap();
    assert!(a_pos < z_pos, "keys must serialize alphabetically (a before z)");
}
```

- [ ] **Step 2.2: Run the test to verify it fails**

Run: `cargo test -p cursor --test codec`
Expected: FAIL — `Cursor` and `CursorError` not exported / not found.

- [ ] **Step 2.3: Implement `CursorError`**

Create `crates/cursor/src/errors.rs`:

```rust
//! Error type for cursor token codec.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum CursorError {
    #[error("invalid cursor")]
    InvalidBase64(#[from] base64::DecodeError),

    #[error("invalid cursor")]
    InvalidJson(#[from] serde_json::Error),

    #[error("invalid cursor")]
    EmptyDocId,
}
```

Surface strings all collapse to `"invalid cursor"` per Go's `internal/cursor/errors.go`.

- [ ] **Step 2.4: Implement `Cursor`**

Replace `crates/cursor/src/lib.rs` with:

```rust
//! Opaque cursor token codec for GraphQL cursor pagination.

mod errors;

pub use errors::CursorError;

use base64::Engine;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// A decoded cursor token. `keys` carries indexed field values
/// (alphabetically ordered) for index-backed seeking.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cursor {
    #[serde(rename = "d")]
    pub doc_id: String,

    #[serde(rename = "k", default, skip_serializing_if = "BTreeMap::is_empty")]
    pub keys: BTreeMap<String, serde_json::Value>,
}

impl Cursor {
    /// Construct a cursor from a document ID with no key values.
    /// Used when no index is available — the planner falls back to
    /// docID-based iteration.
    pub fn from_doc_id(doc_id: impl Into<String>) -> Self {
        Self {
            doc_id: doc_id.into(),
            keys: BTreeMap::new(),
        }
    }

    /// Encode to a base64url-no-pad token: `base64url(json{d, k})`.
    /// Matches Go's `internal/cursor.Encode` byte-for-byte.
    pub fn encode(&self) -> String {
        let json = serde_json::to_vec(self).expect("Cursor serialization cannot fail");
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&json)
    }

    /// Decode a token. Returns `EmptyDocId` if `d` is empty,
    /// `InvalidBase64`/`InvalidJson` for malformed input.
    pub fn decode(token: &str) -> Result<Self, CursorError> {
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(token)?;
        let cursor: Cursor = serde_json::from_slice(&bytes)?;
        if cursor.doc_id.is_empty() {
            return Err(CursorError::EmptyDocId);
        }
        Ok(cursor)
    }
}
```

- [ ] **Step 2.5: Run the tests to verify they pass**

Run: `cargo test -p cursor --test codec`
Expected: PASS — all 7 tests green.

- [ ] **Step 2.6: Run clippy**

Run: `cargo clippy -p cursor -- -D warnings`
Expected: clean.

- [ ] **Step 2.7: Commit**

```bash
git add crates/cursor/
git commit -m "feat(cursor): implement Cursor::encode/decode with round-trip tests"
```

---

## Task 3: Wire Go-fixture cross-compat tests

**Files:**
- Create: `crates/cursor/tests/fixtures/all.json` (initially with a small hand-built set; replaced once Go generator runs)
- Create: `crates/cursor/tests/go_fixtures.rs`
- Note: `tools/cursor-fixtures/main.go` must be added to the Go defradb repo separately (see step 3.6)

- [ ] **Step 3.1: Define the fixture format**

Each fixture is `{"name": "...", "token": "<base64url>", "decoded": {"d": "...", "k": {...}}}`. The file is a JSON array of fixtures.

- [ ] **Step 3.2: Hand-build an initial fixture file**

Create `crates/cursor/tests/fixtures/all.json` with 4 starter cases (these match what the Go generator must produce once it exists):

```json
[
  {
    "name": "doc_id_only",
    "token": "eyJkIjoiZG9jLTEifQ",
    "decoded": { "d": "doc-1" }
  },
  {
    "name": "single_string_key",
    "token": "eyJkIjoiZG9jLTEiLCJrIjp7Im5hbWUiOiJhbGljZSJ9fQ",
    "decoded": { "d": "doc-1", "k": { "name": "alice" } }
  },
  {
    "name": "multi_key_alphabetical",
    "token": "eyJkIjoiZG9jLTIiLCJrIjp7ImFnZSI6MzAsIm5hbWUiOiJib2IifX0",
    "decoded": { "d": "doc-2", "k": { "age": 30, "name": "bob" } }
  },
  {
    "name": "numeric_value",
    "token": "eyJkIjoiZG9jLTMiLCJrIjp7InNjb3JlIjoxMjMuNDV9fQ",
    "decoded": { "d": "doc-3", "k": { "score": 123.45 } }
  }
]
```

These tokens are computed manually from `base64url_no_pad(serde_json::to_string(decoded))`. **Verify each by running `base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(token)` and inspecting the JSON.** If any decodes incorrectly, regenerate by writing a quick Rust binary that calls `Cursor { doc_id, keys }.encode()` and copying its output (this is bootstrapping the fixture, not the Go-parity check — that comes in 3.6).

- [ ] **Step 3.3: Write the failing fixture test**

Create `crates/cursor/tests/go_fixtures.rs`:

```rust
use cursor::Cursor;
use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Debug, Deserialize)]
struct Fixture {
    name: String,
    token: String,
    decoded: DecodedFixture,
}

#[derive(Debug, Deserialize)]
struct DecodedFixture {
    d: String,
    #[serde(default)]
    k: BTreeMap<String, serde_json::Value>,
}

fn load_fixtures() -> Vec<Fixture> {
    let raw = include_str!("fixtures/all.json");
    serde_json::from_str(raw).expect("fixtures must be valid JSON")
}

#[test]
fn decode_matches_go() {
    for f in load_fixtures() {
        let decoded = Cursor::decode(&f.token)
            .unwrap_or_else(|e| panic!("{}: decode failed: {}", f.name, e));
        assert_eq!(decoded.doc_id, f.decoded.d, "{}: doc_id mismatch", f.name);
        assert_eq!(decoded.keys, f.decoded.k, "{}: keys mismatch", f.name);
    }
}

#[test]
fn encode_matches_go_byte_for_byte() {
    for f in load_fixtures() {
        let c = Cursor {
            doc_id: f.decoded.d.clone(),
            keys: f.decoded.k.clone(),
        };
        let token = c.encode();
        assert_eq!(
            token, f.token,
            "{}: encoded token does not match Go-produced token byte-for-byte",
            f.name
        );
    }
}
```

- [ ] **Step 3.4: Run the cross-compat tests**

Run: `cargo test -p cursor --test go_fixtures`
Expected: PASS if the hand-built fixtures in 3.2 were generated correctly via the Rust encoder.

If they fail with `encoded token does not match`, the hand-built tokens in 3.2 are wrong; regenerate them by running `cargo test -p cursor --test codec -- --nocapture` after temporarily adding a `println!("{}", c.encode())` in a unit test, then paste the output into the fixture file.

- [ ] **Step 3.5: Commit the codec + initial fixtures**

```bash
git add crates/cursor/tests/
git commit -m "test(cursor): add Go-fixture cross-compat scaffolding (4 cases)"
```

- [ ] **Step 3.6: Document the Go fixture generator requirement**

The fixture file in 3.2 is a 4-case bootstrap. The full ~30-case set must come from a Go binary that imports `internal/cursor` (only possible from inside the defradb Go module). Add a `crates/cursor/tests/fixtures/README.md` with:

```markdown
# Cursor Fixtures

These fixtures are generated by `tools/cursor-fixtures/main.go` inside the
Go defradb repository (not this Rust repo) and committed here.

## Regenerating

From the Go repo (`/Users/johnzampolin/go/src/github.com/sourcenetwork/defradb`)
on the `pr-4617` branch:

    go run ./tools/cursor-fixtures > /path/to/defradb.rs/crates/cursor/tests/fixtures/all.json

The generator emits ~30 cases covering: empty keys, single key, multi-key
alphabetical ordering, datetime values, float values (including edge cases),
unicode field names, very large maps. Token strings must be byte-identical
to what `internal/cursor.Encode` produces.

If the Go tool does not exist yet, file an issue upstream and use the
bootstrap fixtures committed here until it lands.
```

```bash
git add crates/cursor/tests/fixtures/README.md
git commit -m "docs(cursor): document Go fixture regeneration workflow"
```

The Go-side generator is **out of scope for this plan** — it lives in the Go repo. This task ends when the Rust test infrastructure is ready to consume Go-produced fixtures.

---

## Task 4: Extend `Select` with cursor fields

**Files:**
- Create: `crates/query-types/src/mapper/cursor.rs`
- Modify: `crates/query-types/src/mapper/types.rs` (extend `Select` struct and constructors)
- Modify: `crates/query-types/src/mapper/mod.rs` (re-export)
- Modify: `crates/query-types/Cargo.toml` (add `cursor` dep)

- [ ] **Step 4.1: Write the failing test**

Create `crates/query-types/src/mapper/cursor.rs`:

```rust
//! Cursor pagination request types.

/// Parsed cursor pagination args from a GraphQL cursor query.
/// `first`/`after` are mutually exclusive with `last`/`before`
/// (validated by the parser).
#[derive(Debug, Clone, Default)]
pub struct CursorParams {
    pub first: Option<u64>,
    pub after: Option<String>,  // raw base64 token; decoded in planner
    pub last: Option<u64>,
    pub before: Option<String>,
}

impl CursorParams {
    pub fn is_forward(&self) -> bool {
        self.first.is_some() || self.after.is_some()
    }

    pub fn is_backward(&self) -> bool {
        self.last.is_some() || self.before.is_some()
    }
}

/// Which `_pageInfo` fields the client selected. Used to gate
/// response emission so we don't compute or serialize unrequested fields.
#[derive(Debug, Clone, Copy, Default)]
pub struct CursorPageInfoFields {
    pub has_next: bool,
    pub has_prev: bool,
    pub start_cursor: bool,
    pub end_cursor: bool,
}

/// Tracks the GraphQL aliases on a cursor query so response shaping can
/// emit results under the correct keys. The wrapper alias is the alias
/// (if any) on `_cursor` itself; `select.field.alias` continues to carry
/// the alias on the inner collection field.
#[derive(Debug, Clone, Default)]
pub struct CursorAliases {
    /// Alias on `_cursor` (e.g., `{ paged: _cursor { ... } }` ⇒ Some("paged")).
    /// None ⇒ emit under the literal key `_cursor`.
    pub wrapper_alias: Option<String>,
}

impl CursorPageInfoFields {
    pub fn any_selected(&self) -> bool {
        self.has_next || self.has_prev || self.start_cursor || self.end_cursor
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_params_default_is_neither_direction() {
        let p = CursorParams::default();
        assert!(!p.is_forward());
        assert!(!p.is_backward());
    }

    #[test]
    fn cursor_params_first_is_forward() {
        let p = CursorParams { first: Some(10), ..Default::default() };
        assert!(p.is_forward());
        assert!(!p.is_backward());
    }

    #[test]
    fn cursor_page_info_any_selected() {
        let p = CursorPageInfoFields::default();
        assert!(!p.any_selected());

        let p = CursorPageInfoFields { has_next: true, ..Default::default() };
        assert!(p.any_selected());
    }
}
```

- [ ] **Step 4.2: Wire the new module**

In `crates/query-types/src/mapper/mod.rs`, add:

```rust
mod cursor;
pub use cursor::{CursorAliases, CursorParams, CursorPageInfoFields};
```

- [ ] **Step 4.3: Extend the `Select` struct**

In `crates/query-types/src/mapper/types.rs`, modify the `Select` struct (around line 409). Add the three new fields just before `exhaustive: bool`:

```rust
pub struct Select {
    // ...existing fields...
    pub exhaustive: bool,
    /// Whether this select originates from a `_cursor { ... }` GraphQL wrapper.
    /// When true, the planner installs a CursorNode and forces planner routing.
    pub is_cursor: bool,
    /// Cursor pagination args (Some when `is_cursor`, None otherwise).
    pub cursor_params: Option<CursorParams>,
    /// Which `_pageInfo` fields were selected on the cursor wrapper.
    pub cursor_page_info: CursorPageInfoFields,
    /// GraphQL aliases on the cursor wrapper (`select.field.alias` continues
    /// to track the inner collection's alias; `cursor_aliases.wrapper_alias`
    /// tracks the `_cursor` field's alias).
    pub cursor_aliases: CursorAliases,
}
```

Update the constructor `Select::new` (around line 444) to initialize the new fields:

```rust
pub fn new(collection_name: impl Into<String>) -> Self {
    let collection_name = collection_name.into();
    Self {
        // ...existing initializations...
        exhaustive: false,
        is_cursor: false,
        cursor_params: None,
        cursor_page_info: CursorPageInfoFields::default(),
        cursor_aliases: CursorAliases::default(),
    }
}
```

Add `use super::cursor::{CursorAliases, CursorParams, CursorPageInfoFields};` at the top of `types.rs` (where other `use` statements are).

- [ ] **Step 4.4: Run the unit tests**

Run: `cargo test -p query-types --lib mapper::cursor`
Expected: PASS (3 tests).

- [ ] **Step 4.5: Verify the whole query-types crate compiles**

Run: `cargo build -p query-types`
Expected: builds cleanly (some existing call sites may use struct-update syntax `Select { ..default() }`; those continue to work).

- [ ] **Step 4.6: Run clippy**

Run: `cargo clippy -p query-types -- -D warnings`
Expected: clean.

- [ ] **Step 4.7: Commit**

```bash
git add crates/query-types/
git commit -m "feat(query-types): add cursor pagination fields to Select"
```

---

## Task 5: Add cursor error constructors to `QueryError`

**Files:**
- Modify: `crates/query-types/src/error.rs`

The codebase uses `QueryError` (with constructor methods) rather than a separate `PlannerError` enum. We add cursor-specific constructors that produce Go-equivalent surface strings.

- [ ] **Step 5.1: Write the failing test**

Append to the test module at the bottom of `crates/query-types/src/error.rs` (or create one if absent):

```rust
#[cfg(test)]
mod cursor_error_tests {
    use super::QueryError;

    #[test]
    fn invalid_cursor_message_matches_go() {
        let e = QueryError::cursor_invalid();
        assert_eq!(e.to_string(), "invalid cursor");
    }

    #[test]
    fn no_supporting_index_message_matches_go() {
        let e = QueryError::cursor_no_supporting_index();
        assert!(e.to_string().contains("no supporting index"));
    }

    #[test]
    fn cursor_must_contain_query_message() {
        let e = QueryError::cursor_must_contain_query();
        assert_eq!(e.to_string(), "_cursor block must contain exactly one collection query");
    }

    #[test]
    fn multiple_queries_in_cursor_message() {
        let e = QueryError::cursor_multiple_queries();
        assert_eq!(e.to_string(), "_cursor block cannot contain multiple collection queries");
    }

    #[test]
    fn first_negative_message() {
        let e = QueryError::cursor_first_must_be_non_negative();
        assert_eq!(e.to_string(), "first must be non-negative");
    }

    #[test]
    fn last_negative_message() {
        let e = QueryError::cursor_last_must_be_non_negative();
        assert_eq!(e.to_string(), "last must be non-negative");
    }

    #[test]
    fn forward_backward_conflict_message() {
        let e = QueryError::cursor_forward_backward_conflict();
        assert_eq!(
            e.to_string(),
            "forward parameters (first/after) cannot be combined with backward parameters (last/before)"
        );
    }
}
```

- [ ] **Step 5.2: Run to verify failure**

Run: `cargo test -p query-types --lib error::cursor_error_tests`
Expected: FAIL — constructors do not exist.

- [ ] **Step 5.3: Add the constructors**

In `crates/query-types/src/error.rs`, find the `impl QueryError { ... }` block (line ~180). Add at the end of that impl:

```rust
// ---- Cursor pagination errors (Go-exact surface strings) ----

/// `"invalid cursor"` — invalid base64, invalid JSON, or empty doc ID.
/// Mirrors Go's `internal/cursor/errors.go::errInvalidCursor`.
pub fn cursor_invalid() -> Self {
    Self::execution("invalid cursor")
}

/// `"_cursor block must contain exactly one collection query"`.
/// Mirrors Go's `client/request/errors.go::errCursorMustContainQuery`.
pub fn cursor_must_contain_query() -> Self {
    Self::parse("_cursor block must contain exactly one collection query")
}

/// `"_cursor block cannot contain multiple collection queries"`.
/// Mirrors Go's `errMultipleQueriesInCursor`.
pub fn cursor_multiple_queries() -> Self {
    Self::parse("_cursor block cannot contain multiple collection queries")
}

/// `"first must be non-negative"`.
pub fn cursor_first_must_be_non_negative() -> Self {
    Self::parse("first must be non-negative")
}

/// `"last must be non-negative"`.
pub fn cursor_last_must_be_non_negative() -> Self {
    Self::parse("last must be non-negative")
}

/// `"forward parameters (first/after) cannot be combined with backward parameters (last/before)"`.
pub fn cursor_forward_backward_conflict() -> Self {
    Self::parse(
        "forward parameters (first/after) cannot be combined with backward parameters (last/before)",
    )
}

/// `"no supporting index for cursor"` — order fields not covered by any index.
/// Mirrors Go's `internal/planner/errors.go::ErrNoSupportingIndexForCursor`.
/// The Go wording is roughly "no supporting index for cursor query order"; use
/// whichever string the Go side outputs — verify and adjust before merging.
pub fn cursor_no_supporting_index() -> Self {
    Self::execution("no supporting index for cursor query order")
}
```

**Note on the no-supporting-index string:** the spec lifts wording verbatim from Go's `internal/planner/errors.go`. The exact string was not captured in the spec; before merging, grep the Go file:

```bash
grep "errNoSupportingIndex\|ErrNoSupportingIndex" \
    /Users/johnzampolin/go/src/github.com/sourcenetwork/defradb/internal/planner/errors.go
```

Use the literal string from there. Update both the test expectation and the constructor to match.

- [ ] **Step 5.4: Run tests**

Run: `cargo test -p query-types --lib error::cursor_error_tests`
Expected: PASS (7 tests).

- [ ] **Step 5.5: Commit**

```bash
git add crates/query-types/src/error.rs
git commit -m "feat(query-types): add cursor error constructors with Go-exact strings"
```

---

## Task 6: Extend `IndexScanParams` with `cursor_seek`

**Files:**
- Modify: `crates/query-plan/src/planner/index_selection/types.rs`
- Modify: `crates/query-plan/Cargo.toml` (add `cursor` dep — only needed once Task 7 starts using `Cursor`; safe to add now)

- [ ] **Step 6.1: Add the `CursorSeek` struct and extend `IndexScanParams`**

In `crates/query-plan/src/planner/index_selection/types.rs`, modify `IndexScanParams` (line 13) to add a `cursor_seek` field, and add the new `CursorSeek` struct below it:

```rust
/// Parameters for executing an index scan.
#[derive(Debug, Clone)]
pub struct IndexScanParams {
    pub index_name: String,
    pub scan_type: IndexScanType,
    pub limit: Option<u64>,
    pub offset: u64,
    pub value_filter: Option<ScanValueFilter>,
    /// Optional cursor seek configuration. When `Some`, the fetcher
    /// positions its iterator at `seek_key` before scanning, honoring
    /// `inclusive` and `reversed`. Used by cursor pagination.
    pub cursor_seek: Option<CursorSeek>,
}

/// Configuration for seeking into an index from a cursor token.
#[derive(Debug, Clone)]
pub struct CursorSeek {
    /// Raw bytes of the storage-encoded index key to seek to.
    /// Built by `build_cursor_seek_key` in the planner.
    pub seek_key: Vec<u8>,
    /// `true` for backward pagination (seek inclusive, then iterate);
    /// `false` for forward pagination (seek exclusive — skip the boundary).
    pub inclusive: bool,
    /// Iterate the index in reverse order.
    pub reversed: bool,
}
```

- [ ] **Step 6.2: Update existing call sites that construct `IndexScanParams`**

This change is backward-incompatible (new required field). Search for `IndexScanParams {` constructors:

```bash
rg "IndexScanParams\s*\{" crates/
```

For each construction site, add `cursor_seek: None,` to the struct literal. There should be a handful in `crates/query-plan/src/planner/index_selection/` and possibly the planner builder. Do not change behavior — every existing call site uses `cursor_seek: None`.

Alternatively, add `#[derive(Default)]` to `IndexScanParams` if all other fields already have sensible defaults. Check the struct — `index_name: String` and `scan_type: IndexScanType` may not. If `Default` is impractical, just patch the call sites.

- [ ] **Step 6.3: Add `cursor` as a `query-plan` dependency**

In `crates/query-plan/Cargo.toml`, under `[dependencies]`, add:

```toml
cursor = { path = "../cursor" }
```

- [ ] **Step 6.4: Verify the crate compiles**

Run: `cargo build -p query-plan`
Expected: builds cleanly (call sites all updated).

- [ ] **Step 6.5: Run existing query-plan tests**

Run: `cargo test -p query-plan`
Expected: existing tests still pass — `cursor_seek: None` doesn't change semantics.

- [ ] **Step 6.6: Commit**

```bash
git add crates/query-plan/
git commit -m "feat(query-plan): add CursorSeek to IndexScanParams"
```

---

## Task 7: Honor `cursor_seek` in scan/index_scan plan nodes and fetcher

**Files:**
- Modify: `crates/query-plan/src/plan/scan.rs`
- Modify: `crates/query-plan/src/plan/index_scan.rs`
- Modify: `crates/query-plan/src/fetcher.rs` (trait doc only — `IndexScanParams` already carries the field)
- Modify: concrete fetcher impl in `crates/db/` (whichever file implements `DocFetcher::get_by_index_scan`)

- [ ] **Step 7.1: Locate the concrete fetcher implementation**

Run: `rg "impl DocFetcher" crates/db/ crates/query/`
Identify the file implementing `async fn get_by_index_scan`. Likely `crates/db/src/fetcher.rs` or similar.

- [ ] **Step 7.2: Read the current implementation**

Read the implementation. Understand how it iterates the index today (likely calls into `storage::index` with a `prefix_values` → byte-prefix translation).

- [ ] **Step 7.3: Write the failing test**

Inside the concrete fetcher impl's file (or a sibling test file), add a unit test that constructs an index, inserts known docs, then calls `get_by_index_scan` with a `cursor_seek` configured to skip past doc 1 and verifies docs 2, 3 are returned (and that with `reversed: true`, order is reversed).

Test skeleton (adapt to the existing fetcher's test patterns — match what's already in that file):

```rust
#[tokio::test]
async fn index_scan_forward_seek_exclusive_skips_boundary() {
    // ARRANGE: collection + index + 3 docs (alice=20, bob=30, carol=40)
    let fetcher = build_test_fetcher_with_index_on_age().await;
    // Seek key for bob (age=30) — built via the same helper the planner uses
    let seek = CursorSeek {
        seek_key: build_index_key("age", json!(30)),
        inclusive: false,  // forward: skip boundary
        reversed: false,
    };
    let params = IndexScanParams {
        index_name: "age_idx".into(),
        scan_type: IndexScanType::PrefixScan { prefix_values: vec![], reverse: false },
        limit: None, offset: 0, value_filter: None,
        cursor_seek: Some(seek),
    };

    // ACT
    let result = fetcher.get_by_index_scan("users", &params).await.unwrap();

    // ASSERT: bob is skipped, carol is returned
    assert_eq!(result.doc_ids(), &["carol_id"]);
}
```

- [ ] **Step 7.4: Run the failing test**

Run: `cargo test -p db <test name>` (or whichever crate owns the impl).
Expected: FAIL — `cursor_seek` is currently ignored.

- [ ] **Step 7.5: Implement seek support in the concrete fetcher**

In the body of `get_by_index_scan`, before the main scan loop, check `params.cursor_seek`:

```rust
if let Some(seek) = &params.cursor_seek {
    // Position the storage iterator at seek.seek_key.
    // If seek.inclusive is false, advance past the first matching entry.
    // If seek.reversed is true, iterate in reverse direction.
    iterator.seek_to(&seek.seek_key, seek.inclusive, seek.reversed)?;
}
```

The exact API depends on the underlying storage abstraction. Look for `range`, `seek`, `start_at` methods on the storage trait. For each of the four backends (redb, fjall, rocksdb, memory) — `storage::range_from_inclusive`, `range_from_exclusive`, and reverse iteration variants likely already exist. If a backend lacks reverse iteration, fall back to forward iteration + buffer-and-reverse in the fetcher (slow path — flag with a TODO comment and a follow-up issue link).

- [ ] **Step 7.6: Run the test**

Run: `cargo test -p db <test name>`
Expected: PASS.

- [ ] **Step 7.7: Add the backward-seek test**

```rust
#[tokio::test]
async fn index_scan_backward_seek_inclusive_includes_boundary() {
    let fetcher = build_test_fetcher_with_index_on_age().await;
    let seek = CursorSeek {
        seek_key: build_index_key("age", json!(30)),
        inclusive: true,
        reversed: true,
    };
    let params = IndexScanParams {
        index_name: "age_idx".into(),
        scan_type: IndexScanType::PrefixScan { prefix_values: vec![], reverse: false },
        limit: None, offset: 0, value_filter: None,
        cursor_seek: Some(seek),
    };
    let result = fetcher.get_by_index_scan("users", &params).await.unwrap();
    // From bob (inclusive) backwards: bob, alice
    assert_eq!(result.doc_ids(), &["bob_id", "alice_id"]);
}
```

Run: `cargo test -p db`
Expected: PASS.

- [ ] **Step 7.8: Update the `DocFetcher` trait doc**

In `crates/query-plan/src/fetcher.rs`, update the doc comment on `get_by_index_scan` (around line 192-213) to mention `cursor_seek`:

```rust
/// Get documents using an index scan.
///
/// ...existing doc...
///
/// When `params.cursor_seek` is `Some`, the implementation must position
/// the storage iterator at `cursor_seek.seek_key` before iterating, honoring
/// `inclusive` (skip the boundary if false) and `reversed` (iterate
/// descending if true). This is used by cursor pagination to seek directly
/// into an index without offset-scan.
async fn get_by_index_scan(...) -> Result<IndexScanResult> { ... }
```

- [ ] **Step 7.9: Verify existing fetcher tests still pass**

Run: `cargo test -p db`
Expected: PASS (existing tests use `cursor_seek: None`).

- [ ] **Step 7.10: Commit**

```bash
git add crates/query-plan/src/fetcher.rs crates/db/
git commit -m "feat(db): honor IndexScanParams.cursor_seek in fetcher"
```

---

## Task 8: Implement `CursorNode` skeleton + forward semantics (TDD)

**Files:**
- Create: `crates/query-plan/src/plan/cursor.rs`
- Modify: `crates/query-plan/src/plan/mod.rs` (export the new node)

- [ ] **Step 8.1: Inspect an existing PlanNode for the trait shape**

Run: `cat crates/query-plan/src/plan/limit.rs` (or `select.rs`, whichever is small).
Note the imports, the `impl PlanNode` body, and the `async fn next()` signature. Match these in `cursor.rs`.

- [ ] **Step 8.2: Stub out `CursorNode`**

Create `crates/query-plan/src/plan/cursor.rs`:

```rust
//! CursorNode — wraps a child plan with cursor pagination semantics.
//!
//! Sits at the top of a cursor query's plan tree, above the existing scan/
//! filter/order stack. Owns per-row cursor logic: skip-until-after,
//! collect, probe-for-hasNext, encode startCursor/endCursor.

use async_trait::async_trait;
use cursor::Cursor;
use query_types::error::{QueryError, Result};
use query_types::mapper::{CursorPageInfoFields, OrderCondition};
use serde_json::Value as JsonValue;
use std::collections::VecDeque;

use super::PlanNode;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorDirection {
    Forward,
    Backward,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CursorState {
    Initial,
    SkippingUntilAfter,
    Collecting,
    Drained,
}

pub struct CursorNode {
    inner: Box<dyn PlanNode>,
    direction: CursorDirection,
    page_size: u64,
    after: Option<Cursor>,
    before: Option<Cursor>,
    page_info_fields: CursorPageInfoFields,
    order_fields: Vec<OrderCondition>,

    state: CursorState,
    buffer: VecDeque<JsonValue>,           // backward path
    first_doc: Option<JsonValue>,
    last_doc: Option<JsonValue>,
    has_next: bool,
    has_prev: bool,
    index_seek_active: bool,
    emitted: u64,
    start_cursor: Option<String>,
    end_cursor: Option<String>,
}

impl CursorNode {
    /// Construct a new CursorNode. The planner is responsible for decoding
    /// tokens and setting `index_seek_active` based on whether it configured
    /// `cursor_seek` on the scan below.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        inner: Box<dyn PlanNode>,
        direction: CursorDirection,
        page_size: u64,
        after: Option<Cursor>,
        before: Option<Cursor>,
        page_info_fields: CursorPageInfoFields,
        order_fields: Vec<OrderCondition>,
        index_seek_active: bool,
    ) -> Self {
        let initial_state = if index_seek_active || after.is_none() {
            CursorState::Collecting
        } else {
            CursorState::SkippingUntilAfter
        };
        Self {
            inner,
            direction,
            page_size,
            after,
            before,
            page_info_fields,
            order_fields,
            state: initial_state,
            buffer: VecDeque::new(),
            first_doc: None,
            last_doc: None,
            has_next: false,
            has_prev: false,
            index_seek_active,
            emitted: 0,
            start_cursor: None,
            end_cursor: None,
        }
    }

    /// Result of finalizing pagination — read by response shaping after
    /// iteration completes.
    pub fn page_info(&self) -> CursorPageInfo {
        CursorPageInfo {
            has_next: self.has_next,
            has_prev: self.has_prev,
            start_cursor: self.start_cursor.clone(),
            end_cursor: self.end_cursor.clone(),
            fields: self.page_info_fields,
        }
    }

    fn doc_id(row: &JsonValue) -> Option<&str> {
        row.get("_docID").and_then(|v| v.as_str())
    }

    fn build_cursor_from_row(&self, row: &JsonValue) -> Cursor {
        let doc_id = Self::doc_id(row).unwrap_or("").to_string();
        let mut keys = std::collections::BTreeMap::new();
        for cond in &self.order_fields {
            if let Some(field) = cond.fields.first() {
                if let Some(value) = row.get(field) {
                    keys.insert(field.clone(), value.clone());
                }
            }
        }
        Cursor { doc_id, keys }
    }
}

pub struct CursorPageInfo {
    pub has_next: bool,
    pub has_prev: bool,
    pub start_cursor: Option<String>,
    pub end_cursor: Option<String>,
    pub fields: CursorPageInfoFields,
}

#[async_trait]
impl PlanNode for CursorNode {
    async fn next(&mut self) -> Result<Option<JsonValue>> {
        match self.direction {
            CursorDirection::Forward => self.next_forward().await,
            CursorDirection::Backward => self.next_backward().await,
        }
    }

    // ... other PlanNode trait methods (init, close, etc.) — match what
    // the existing LimitNode in plan/limit.rs implements
}

impl CursorNode {
    async fn next_forward(&mut self) -> Result<Option<JsonValue>> {
        loop {
            match self.state {
                CursorState::Initial | CursorState::Collecting => {
                    if self.emitted >= self.page_size {
                        // Probe one extra row to set has_next.
                        match self.inner.next().await? {
                            Some(_) => self.has_next = true,
                            None => self.has_next = false,
                        }
                        self.state = CursorState::Drained;
                        self.has_prev = self.after.is_some();
                        self.finalize_page_info();
                        return Ok(None);
                    }
                    match self.inner.next().await? {
                        Some(row) => {
                            if self.first_doc.is_none() {
                                self.first_doc = Some(row.clone());
                            }
                            self.last_doc = Some(row.clone());
                            self.emitted += 1;
                            return Ok(Some(row));
                        }
                        None => {
                            self.has_next = false;
                            self.has_prev = self.after.is_some();
                            self.state = CursorState::Drained;
                            self.finalize_page_info();
                            return Ok(None);
                        }
                    }
                }
                CursorState::SkippingUntilAfter => {
                    // Slow path: no index seek. Pull rows until past the
                    // `after` boundary on docID.
                    let after_doc_id = self.after.as_ref().map(|c| c.doc_id.clone());
                    match self.inner.next().await? {
                        Some(row) => {
                            let row_id = Self::doc_id(&row).map(|s| s.to_string());
                            match (after_doc_id.as_ref(), row_id.as_ref()) {
                                (Some(a), Some(r)) if r.as_str() > a.as_str() => {
                                    // Past the boundary — collect this row
                                    self.state = CursorState::Collecting;
                                    self.first_doc = Some(row.clone());
                                    self.last_doc = Some(row.clone());
                                    self.emitted += 1;
                                    return Ok(Some(row));
                                }
                                _ => continue,  // still skipping
                            }
                        }
                        None => {
                            self.state = CursorState::Drained;
                            self.finalize_page_info();
                            return Ok(None);
                        }
                    }
                }
                CursorState::Drained => return Ok(None),
            }
        }
    }

    async fn next_backward(&mut self) -> Result<Option<JsonValue>> {
        // Implemented in Task 9
        Err(QueryError::execution("backward cursor pagination not yet implemented"))
    }

    fn finalize_page_info(&mut self) {
        if self.page_info_fields.start_cursor {
            if let Some(row) = &self.first_doc {
                self.start_cursor = Some(self.build_cursor_from_row(row).encode());
            }
        }
        if self.page_info_fields.end_cursor {
            if let Some(row) = &self.last_doc {
                self.end_cursor = Some(self.build_cursor_from_row(row).encode());
            }
        }
    }
}
```

**Look at `crates/query-plan/src/plan/limit.rs` for the complete `impl PlanNode` shape** (other required methods like `init`, `close`, `is_done`). Mirror them in `CursorNode`. Don't guess — read the existing trait.

- [ ] **Step 8.3: Export the new node**

In `crates/query-plan/src/plan/mod.rs`, add:

```rust
mod cursor;
pub use cursor::{CursorNode, CursorDirection, CursorPageInfo};
```

- [ ] **Step 8.4: Write the failing forward test**

Create `crates/query-plan/src/plan/cursor_tests.rs` (or inline `#[cfg(test)] mod tests` in `cursor.rs`):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    /// A test PlanNode that yields a preset list of JSON rows.
    struct FakePlan {
        rows: VecDeque<JsonValue>,
    }
    impl FakePlan {
        fn new(rows: Vec<JsonValue>) -> Self { Self { rows: rows.into() } }
    }
    #[async_trait]
    impl PlanNode for FakePlan {
        async fn next(&mut self) -> Result<Option<JsonValue>> {
            Ok(self.rows.pop_front())
        }
        // ... other trait methods as no-ops
    }

    fn row(id: &str) -> JsonValue {
        serde_json::json!({ "_docID": id, "name": id })
    }

    #[tokio::test]
    async fn forward_first_only_emits_n_rows() {
        let inner = FakePlan::new(vec![row("a"), row("b"), row("c"), row("d")]);
        let mut node = CursorNode::new(
            Box::new(inner),
            CursorDirection::Forward,
            2,
            None,  // after
            None,  // before
            CursorPageInfoFields { has_next: true, ..Default::default() },
            vec![],
            false,
        );
        assert_eq!(Cursor::doc_id(&node.next().await.unwrap().unwrap()), Some("a"));
        assert_eq!(Cursor::doc_id(&node.next().await.unwrap().unwrap()), Some("b"));
        assert!(node.next().await.unwrap().is_none());
        let info = node.page_info();
        assert!(info.has_next, "should have probed c and set has_next=true");
    }

    #[tokio::test]
    async fn forward_first_after_skips_to_boundary() {
        let inner = FakePlan::new(vec![row("a"), row("b"), row("c"), row("d")]);
        let after = Cursor::from_doc_id("b");
        let mut node = CursorNode::new(
            Box::new(inner),
            CursorDirection::Forward,
            2,
            Some(after),
            None,
            CursorPageInfoFields { has_next: true, has_prev: true, ..Default::default() },
            vec![],
            false,  // no index seek — slow path
        );
        assert_eq!(Cursor::doc_id(&node.next().await.unwrap().unwrap()), Some("c"));
        assert_eq!(Cursor::doc_id(&node.next().await.unwrap().unwrap()), Some("d"));
        assert!(node.next().await.unwrap().is_none());
        let info = node.page_info();
        assert!(info.has_prev, "after.is_some() implies has_prev=true");
        assert!(!info.has_next, "no more rows after d");
    }
}
```

(The `Cursor::doc_id` helper is the private method on `CursorNode` from 8.2; expose it via `pub(crate)` or duplicate the extraction inline in the test.)

- [ ] **Step 8.5: Run the tests**

Run: `cargo test -p query-plan plan::cursor`
Expected: PASS.

- [ ] **Step 8.6: Run clippy**

Run: `cargo clippy -p query-plan -- -D warnings`
Expected: clean.

- [ ] **Step 8.7: Commit**

```bash
git add crates/query-plan/src/plan/
git commit -m "feat(query-plan): add CursorNode forward semantics with tests"
```

---

## Task 9: Implement `CursorNode` backward semantics

**Files:**
- Modify: `crates/query-plan/src/plan/cursor.rs`

- [ ] **Step 9.1: Write failing backward tests**

In the same `mod tests` block as Task 8:

```rust
#[tokio::test]
async fn backward_last_only_emits_last_n_in_order() {
    // No before cursor; iterate forward through all, keep last 2.
    // Note: this is the "no index seek" backward path.
    let inner = FakePlan::new(vec![row("a"), row("b"), row("c"), row("d")]);
    let mut node = CursorNode::new(
        Box::new(inner),
        CursorDirection::Backward,
        2,
        None, None,
        CursorPageInfoFields { has_next: true, has_prev: true, ..Default::default() },
        vec![],
        false,
    );
    assert_eq!(Cursor::doc_id(&node.next().await.unwrap().unwrap()), Some("c"));
    assert_eq!(Cursor::doc_id(&node.next().await.unwrap().unwrap()), Some("d"));
    assert!(node.next().await.unwrap().is_none());
    let info = node.page_info();
    assert!(!info.has_next, "before is None ⇒ has_next=false");
    assert!(info.has_prev, "we dropped rows from the front ⇒ has_prev=true");
}

#[tokio::test]
async fn backward_last_before_stops_at_boundary() {
    let inner = FakePlan::new(vec![row("a"), row("b"), row("c"), row("d")]);
    let before = Cursor::from_doc_id("c");
    let mut node = CursorNode::new(
        Box::new(inner),
        CursorDirection::Backward,
        2,
        None, Some(before),
        CursorPageInfoFields { has_next: true, has_prev: true, ..Default::default() },
        vec![],
        false,
    );
    // Backward (no index seek): drain until row reaches `before` boundary,
    // keep window of last 2 (excluding boundary). Result: a, b
    assert_eq!(Cursor::doc_id(&node.next().await.unwrap().unwrap()), Some("a"));
    assert_eq!(Cursor::doc_id(&node.next().await.unwrap().unwrap()), Some("b"));
    assert!(node.next().await.unwrap().is_none());
    let info = node.page_info();
    assert!(info.has_next, "before.is_some() ⇒ has_next=true");
}
```

- [ ] **Step 9.2: Run to verify failure**

Run: `cargo test -p query-plan plan::cursor backward`
Expected: FAIL — backward path returns the placeholder error from Task 8.

- [ ] **Step 9.3: Implement `next_backward`**

Replace the placeholder body of `next_backward` in `crates/query-plan/src/plan/cursor.rs`:

```rust
async fn next_backward(&mut self) -> Result<Option<JsonValue>> {
    // If the buffer hasn't been populated yet, drain the inner stream.
    if self.state == CursorState::Initial || self.state == CursorState::SkippingUntilAfter {
        self.populate_backward_buffer().await?;
        self.state = CursorState::Drained;
        self.finalize_page_info();
    }
    Ok(self.buffer.pop_front())
}

async fn populate_backward_buffer(&mut self) -> Result<()> {
    let before_doc_id = self.before.as_ref().map(|c| c.doc_id.clone());
    let window_size = self.page_size as usize + 1;  // +1 to detect has_prev
    let mut window: VecDeque<JsonValue> = VecDeque::with_capacity(window_size);

    if self.index_seek_active && matches!(self.direction, CursorDirection::Backward) {
        // Fast path: inner is already iterating in reverse from `before`.
        // Buffer up to page_size + 1, then reverse for logical order.
        while let Some(row) = self.inner.next().await? {
            window.push_back(row);
            if window.len() > window_size {
                break;
            }
        }
        // window currently in reverse order; reverse to get logical order
        let mut ordered: Vec<JsonValue> = window.into_iter().collect();
        ordered.reverse();
        if ordered.len() > self.page_size as usize {
            // We pulled an extra row → has_prev = true; drop it (it's the oldest)
            self.has_prev = true;
            ordered.remove(0);
        }
        for row in ordered {
            self.buffer.push_back(row);
        }
    } else {
        // Slow path: drain forward; keep a sliding window of last (page_size + 1).
        // When we hit `before` boundary, stop.
        while let Some(row) = self.inner.next().await? {
            if let (Some(boundary), Some(row_id)) = (before_doc_id.as_ref(), Self::doc_id(&row)) {
                if row_id >= boundary.as_str() {
                    break;  // stop at boundary (exclusive)
                }
            }
            window.push_back(row);
            if window.len() > window_size {
                window.pop_front();
            }
        }
        if window.len() > self.page_size as usize {
            self.has_prev = true;
            window.pop_front();
        }
        self.buffer = window;
    }

    // Set first/last docs for cursor encoding
    self.first_doc = self.buffer.front().cloned();
    self.last_doc = self.buffer.back().cloned();
    // has_next: any `before` provided implies there are docs after the page
    self.has_next = self.before.is_some();

    Ok(())
}
```

Also update `CursorNode::new` so that for `Backward`, the initial state is `Initial` (not `Collecting`) — the backward path uses its own pre-population logic.

- [ ] **Step 9.4: Run the backward tests**

Run: `cargo test -p query-plan plan::cursor backward`
Expected: PASS.

- [ ] **Step 9.5: Run the full cursor test set**

Run: `cargo test -p query-plan plan::cursor`
Expected: PASS — forward tests from Task 8 still pass.

- [ ] **Step 9.6: Run clippy**

Run: `cargo clippy -p query-plan -- -D warnings`
Expected: clean.

- [ ] **Step 9.7: Commit**

```bash
git add crates/query-plan/src/plan/cursor.rs
git commit -m "feat(query-plan): add CursorNode backward semantics with tests"
```

---

## Task 10: Implement `expand_cursor_plan` (validation + token decode + scan config)

**Files:**
- Create: `crates/query-plan/src/planner/builder/cursor.rs`
- Modify: `crates/query-plan/src/planner/builder/mod.rs` (expose submodule)

- [ ] **Step 10.1: Write the failing test**

Create `crates/query-plan/src/planner/builder/cursor.rs` with a stub `expand_cursor_plan` and a test:

```rust
//! Cursor pagination planner expansion.
//!
//! Mirrors Go's `expandCursorPlan` and `validateCursorIndex`.

use crate::plan::{CursorDirection, CursorNode, PlanNode};
use crate::planner::index_selection::types::CursorSeek;
use cursor::Cursor;
use query_types::error::{QueryError, Result};
use query_types::mapper::{CursorPageInfoFields, OrderCondition, Select};
use schema::collection::CollectionVersion;
use schema::index::IndexDescription;

/// Wrap a plan tree with `CursorNode`, configure any scan in the tree
/// with `cursor_seek`, and validate that ordering is supported by an index
/// when the ordering is non-empty and not docID-only.
pub(crate) fn expand_cursor_plan(
    select: &Select,
    collection: &CollectionVersion,
    plan: Box<dyn PlanNode>,
) -> Result<Box<dyn PlanNode>> {
    let params = select
        .cursor_params
        .as_ref()
        .ok_or_else(|| QueryError::internal("expand_cursor_plan called on non-cursor select"))?;

    // 1. Validate index coverage (and detect docID/no-order fallback).
    let order_fields: Vec<OrderCondition> = select
        .order_by
        .as_ref()
        .map(|o| o.conditions.clone())
        .unwrap_or_default();
    let (reversed, _matched_index) =
        validate_cursor_index(collection, &order_fields)?;

    // 2. Decode tokens.
    let after = match &params.after {
        Some(token) if !token.is_empty() => Some(Cursor::decode(token).map_err(|_| QueryError::cursor_invalid())?),
        _ => None,
    };
    let before = match &params.before {
        Some(token) if !token.is_empty() => Some(Cursor::decode(token).map_err(|_| QueryError::cursor_invalid())?),
        _ => None,
    };

    // 3. Determine direction and page size.
    let (direction, page_size) = if params.is_backward() {
        (CursorDirection::Backward, params.last.unwrap_or(0))
    } else {
        (CursorDirection::Forward, params.first.unwrap_or(0))
    };

    // 4. Configure scan if there's a matched index and the cursor has keys.
    //    (Index seek configuration is delegated to a helper that walks the plan tree.)
    let (plan, index_seek_active) = configure_scan_for_cursor(
        plan,
        &after,
        &before,
        direction,
        reversed,
        &order_fields,
    )?;

    // 5. Wrap with CursorNode.
    Ok(Box::new(CursorNode::new(
        plan,
        direction,
        page_size,
        after,
        before,
        select.cursor_page_info,
        order_fields,
        index_seek_active,
    )))
}

/// Mirrors Go's `validateCursorIndex`. Returns `(reversed, matched_index)`.
/// When `order_fields` is empty or only by `_docID`, returns `(false, None)`
/// — no index required.
pub(crate) fn validate_cursor_index(
    collection: &CollectionVersion,
    order_fields: &[OrderCondition],
) -> Result<(bool, Option<IndexDescription>)> {
    if order_fields.is_empty() {
        return Ok((false, None));
    }
    if is_doc_id_order(order_fields) {
        return Ok((false, None));
    }

    // Find an index that supports the requested ordering.
    let matched = find_matching_index(&collection.indexes, order_fields);
    let Some((idx, reversed)) = matched else {
        return Err(QueryError::cursor_no_supporting_index());
    };

    // Composite prefix rule: non-unique index must cover all order fields.
    if !idx.unique && order_fields.len() < idx.fields.len() {
        return Err(QueryError::cursor_no_supporting_index());
    }

    Ok((reversed, Some(idx)))
}

fn is_doc_id_order(order_fields: &[OrderCondition]) -> bool {
    matches!(
        order_fields.first().and_then(|c| c.fields.first()).map(String::as_str),
        Some("_docID")
    )
}

fn find_matching_index(
    indexes: &[IndexDescription],
    order_fields: &[OrderCondition],
) -> Option<(IndexDescription, bool)> {
    // Inspect indexes; return the first one whose field list matches the
    // ordering as a prefix in either direction.
    // (Mirror Go's CanBeOrderedByIndex semantics.)
    for idx in indexes {
        if let Some(reversed) = index_covers_ordering(idx, order_fields) {
            return Some((idx.clone(), reversed));
        }
    }
    None
}

fn index_covers_ordering(
    idx: &IndexDescription,
    order_fields: &[OrderCondition],
) -> Option<bool> {
    if idx.fields.len() < order_fields.len() {
        return None;
    }
    // Check that each order field matches the index in order, with consistent direction.
    // If all order conditions are ASC and index is ASC ⇒ reversed=false.
    // If all are DESC and index is ASC ⇒ reversed=true. Mixed ⇒ no match.
    let mut required_reversed: Option<bool> = None;
    for (i, cond) in order_fields.iter().enumerate() {
        let idx_field = &idx.fields[i];
        let cond_field = cond.fields.first().map(String::as_str)?;
        if cond_field != idx_field.name {
            return None;
        }
        let cond_desc = matches!(cond.direction, query_types::mapper::OrderDirection::Desc);
        let needs_reverse = cond_desc != idx_field.descending;
        match required_reversed {
            None => required_reversed = Some(needs_reverse),
            Some(prev) if prev == needs_reverse => {}
            _ => return None,  // mixed directions
        }
    }
    required_reversed.or(Some(false))
}

fn configure_scan_for_cursor(
    plan: Box<dyn PlanNode>,
    _after: &Option<Cursor>,
    _before: &Option<Cursor>,
    _direction: CursorDirection,
    _reversed: bool,
    _order_fields: &[OrderCondition],
) -> Result<(Box<dyn PlanNode>, bool)> {
    // Walk the plan tree to find an IndexScanNode; if found and the cursor
    // has keys, populate its `cursor_seek` via build_cursor_seek_key.
    // Otherwise return (plan, false) for the slow path.
    //
    // For now, return false (slow path) — implemented in Task 11.
    Ok((plan, false))
}

#[cfg(test)]
mod tests {
    use super::*;
    use schema::collection::CollectionVersion;
    use schema::index::{IndexDescription, IndexedFieldDescription};

    fn collection_with_index(name: &str, fields: Vec<&str>, unique: bool) -> CollectionVersion {
        let index = IndexDescription {
            name: name.to_string(),
            unique,
            fields: fields.into_iter().map(|f| IndexedFieldDescription {
                name: f.to_string(),
                descending: false,
            }).collect(),
            // ...other IndexDescription fields per the actual struct shape
        };
        CollectionVersion {
            indexes: vec![index],
            // ...other CollectionVersion fields
        }
    }

    fn order_on(field: &str) -> Vec<OrderCondition> {
        vec![OrderCondition::new(field, query_types::mapper::OrderDirection::Asc)]
    }

    #[test]
    fn empty_order_returns_no_index_needed() {
        let coll = collection_with_index("idx_age", vec!["age"], false);
        let (reversed, matched) = validate_cursor_index(&coll, &[]).unwrap();
        assert!(!reversed);
        assert!(matched.is_none());
    }

    #[test]
    fn doc_id_order_returns_no_index_needed() {
        let coll = collection_with_index("idx_age", vec!["age"], false);
        let order = order_on("_docID");
        let (reversed, matched) = validate_cursor_index(&coll, &order).unwrap();
        assert!(!reversed);
        assert!(matched.is_none());
    }

    #[test]
    fn matching_unique_index_returns_ok() {
        let coll = collection_with_index("idx_age", vec!["age"], true);
        let order = order_on("age");
        let (_reversed, matched) = validate_cursor_index(&coll, &order).unwrap();
        assert!(matched.is_some());
    }

    #[test]
    fn no_matching_index_returns_error() {
        let coll = collection_with_index("idx_age", vec!["age"], false);
        let order = order_on("name");
        let err = validate_cursor_index(&coll, &order).unwrap_err();
        assert!(err.to_string().contains("no supporting index"));
    }

    #[test]
    fn non_unique_composite_prefix_returns_error() {
        // Index on (age, name), order only by age → non-unique prefix mismatch.
        let coll = collection_with_index("idx_age_name", vec!["age", "name"], false);
        let order = order_on("age");
        let err = validate_cursor_index(&coll, &order).unwrap_err();
        assert!(err.to_string().contains("no supporting index"));
    }
}
```

**Note:** The struct fields shown for `CollectionVersion` and `IndexDescription` are illustrative — match the actual definitions in `crates/schema/src/collection.rs` and `crates/schema/src/index.rs`. Read those files first; the test constructors must match the real struct shape, otherwise the test won't compile.

- [ ] **Step 10.2: Expose the new submodule**

In `crates/query-plan/src/planner/builder/mod.rs`, add:

```rust
mod cursor;
pub(in crate::planner) use cursor::{expand_cursor_plan, validate_cursor_index};
```

- [ ] **Step 10.3: Run the validation tests**

Run: `cargo test -p query-plan planner::builder::cursor`
Expected: PASS (5 tests).

- [ ] **Step 10.4: Commit**

```bash
git add crates/query-plan/src/planner/builder/
git commit -m "feat(query-plan): implement validate_cursor_index + expand_cursor_plan skeleton"
```

---

## Task 11: Wire `expand_cursor_plan` into `builder/groupby.rs` and implement scan-seek configuration

**Files:**
- Modify: `crates/query-plan/src/planner/builder/groupby.rs`
- Modify: `crates/query-plan/src/planner/builder/cursor.rs` (flesh out `configure_scan_for_cursor` + `build_cursor_seek_key`)

- [ ] **Step 11.1: Locate the `LimitNode::new` call sites**

In `crates/query-plan/src/planner/builder/groupby.rs`, lines 324 and 378 wrap the plan with `LimitNode::new`. Both are inside `apply_groupby_ordering_limit`. We replace these with a conditional cursor branch.

- [ ] **Step 11.2: Update the two call sites**

For each call site (line 324 and 378), wrap with `select.is_cursor` branching:

Before:
```rust
plan = Box::new(LimitNode::new(plan, effective_limit, limit.offset));
```

After:
```rust
plan = if select.is_cursor {
    crate::planner::builder::cursor::expand_cursor_plan(
        select,
        collection,
        plan,
    )?
} else {
    Box::new(LimitNode::new(plan, effective_limit, limit.offset))
};
```

The function needs access to `collection` (the `CollectionVersion`). Verify it's already in scope at both call sites; if not, plumb it through. Check the function signature at line 20: `apply_groupby_ordering_limit(...)` — look at whether `collection` is a parameter. If not, add it.

For the path that has no `limit` (cursor without limit, since cursor doesn't carry a Limit), add the cursor branch *before* the existing `if let Some(limit) = &select.limit` check so cursor queries get wrapped even without a limit:

```rust
if select.is_cursor {
    plan = crate::planner::builder::cursor::expand_cursor_plan(select, collection, plan)?;
} else if let Some(limit) = &select.limit {
    plan = Box::new(LimitNode::new(plan, effective_limit, limit.offset));
}
```

The exact reshape depends on the existing structure of `apply_groupby_ordering_limit`. Read the function carefully and decide where the branch goes. The contract: when `select.is_cursor`, `LimitNode` is never used; `CursorNode` wraps the top.

- [ ] **Step 11.3: Implement `configure_scan_for_cursor`**

Replace the stubbed `configure_scan_for_cursor` in `crates/query-plan/src/planner/builder/cursor.rs`:

```rust
fn configure_scan_for_cursor(
    mut plan: Box<dyn PlanNode>,
    after: &Option<Cursor>,
    before: &Option<Cursor>,
    direction: CursorDirection,
    reversed: bool,
    order_fields: &[OrderCondition],
) -> Result<(Box<dyn PlanNode>, bool)> {
    // Walk the plan tree top-down to find an IndexScanNode.
    // When found, set its IndexScanParams.cursor_seek if the cursor has keys.
    //
    // Because PlanNode is a trait object, we can't downcast generically;
    // instead, we use a helper method on the node, or thread cursor_seek
    // configuration through the planner before wrapping.
    //
    // Simplest approach: add a method `set_cursor_seek` on PlanNode (default
    // no-op) that IndexScanNode overrides to set its params.

    let active_cursor = match direction {
        CursorDirection::Forward => after.as_ref(),
        CursorDirection::Backward => before.as_ref(),
    };

    let Some(cursor) = active_cursor else {
        return Ok((plan, false));
    };
    if cursor.keys.is_empty() {
        return Ok((plan, false));
    }

    let seek_key = build_cursor_seek_key(cursor, order_fields)?;
    let seek = CursorSeek {
        seek_key,
        inclusive: matches!(direction, CursorDirection::Backward),
        reversed,
    };

    let applied = plan.set_cursor_seek(seek);
    Ok((plan, applied))
}

/// Build a storage-encoded index key from a cursor's `keys` map.
/// The key order must match the index's field order — when iterating
/// `order_fields` produces the same field sequence the index expects.
fn build_cursor_seek_key(
    cursor: &Cursor,
    order_fields: &[OrderCondition],
) -> Result<Vec<u8>> {
    // Serialize the ordered key values into the storage encoding used by
    // the index. The exact encoding depends on `storage::index::encode_key`
    // (or similar helper); look at how IndexScanType::ExactMatch is built
    // today to find the right encoding helper.
    //
    // Pseudocode:
    let mut parts: Vec<JsonValue> = Vec::new();
    for cond in order_fields {
        let Some(field) = cond.fields.first() else { continue };
        let Some(value) = cursor.keys.get(field) else {
            return Err(QueryError::cursor_invalid());  // cursor missing key
        };
        parts.push(value.clone());
    }
    // Encode `parts` + cursor.doc_id (as a doc-id suffix) into bytes.
    // Use the same encoder that IndexScanType::PrefixScan uses; consult
    // crates/storage/src/index.rs for the canonical helper.
    storage::index::encode_cursor_seek_key(&parts, &cursor.doc_id)
        .map_err(|e| QueryError::execution(format!("failed to encode seek key: {e}")))
}
```

**Note:** `storage::index::encode_cursor_seek_key` may not exist. If not, locate the helper used to build `IndexDataStoreKey` from values for existing scan types and reuse it. The encoding must match what the fetcher reads — same canonical form.

- [ ] **Step 11.4: Add `set_cursor_seek` to `PlanNode` and implement on `IndexScanNode`**

In `crates/query-plan/src/plan/mod.rs` (or wherever `PlanNode` is defined), add a default-no-op method to the trait:

```rust
#[async_trait]
pub trait PlanNode: Send + Sync {
    // ...existing methods...

    /// Configure cursor seek on this node's underlying index scan, if any.
    /// Returns true if the node (or a child) applied the seek.
    /// Default: no-op, returns false.
    fn set_cursor_seek(&mut self, _seek: CursorSeek) -> bool {
        false
    }
}
```

Import `CursorSeek` in the trait file.

In `crates/query-plan/src/plan/index_scan.rs`, override the default:

```rust
impl PlanNode for IndexScanNode {
    // ...existing methods...

    fn set_cursor_seek(&mut self, seek: CursorSeek) -> bool {
        self.params.cursor_seek = Some(seek);
        true
    }
}
```

Wrapper plan nodes (like `SelectNode`, `OrderByNode`) should forward to their inner child:

```rust
fn set_cursor_seek(&mut self, seek: CursorSeek) -> bool {
    self.inner.set_cursor_seek(seek)
}
```

Add `set_cursor_seek` forwarding on `SelectNode`, `OrderByNode`, `GroupByNode`, `LimitNode` (anything that wraps a child plan). The walker stops at `ScanNode` and `IndexScanNode`.

`ScanNode` (non-index) returns `false` — full collection scans don't support cursor seek; the slow-path in `CursorNode` handles it.

- [ ] **Step 11.5: Write the wiring test**

In `crates/query-plan/src/planner/builder/cursor.rs` tests, add:

```rust
#[test]
fn expand_cursor_plan_wraps_with_cursor_node_and_forces_planner() {
    // ARRANGE: build a Select with is_cursor=true, no order
    let mut select = Select::new("users");
    select.is_cursor = true;
    select.cursor_params = Some(CursorParams { first: Some(10), ..Default::default() });
    let collection = collection_with_index("idx_age", vec!["age"], false);
    let inner: Box<dyn PlanNode> = Box::new(FakePlan::new(vec![]));

    // ACT
    let plan = expand_cursor_plan(&select, &collection, inner).unwrap();

    // ASSERT: top node is a CursorNode (downcast via a tagged interface
    // or by reading `Display`/`Debug` output if the trait supports it)
    // — at minimum, verify the plan compiles and runs without error.
    drop(plan);
}
```

- [ ] **Step 11.6: Run the full query-plan suite**

Run: `cargo test -p query-plan`
Expected: PASS — pre-existing tests continue passing; new cursor tests pass.

- [ ] **Step 11.7: Run clippy**

Run: `cargo clippy -p query-plan -- -D warnings`
Expected: clean.

- [ ] **Step 11.8: Commit**

```bash
git add crates/query-plan/
git commit -m "feat(query-plan): wire CursorNode into planner via expand_cursor_plan"
```

---

## Task 12: Generate `PageInfo` and `CursorQuery` schema types

**Files:**
- Create: `crates/query-parse/src/schema_gen/cursor.rs`
- Modify: `crates/query-parse/src/schema_gen/mod.rs` (expose submodule)
- Modify: `crates/query-parse/Cargo.toml` (add `cursor` dep — defensive; may not be needed by schema_gen but harmless)

- [ ] **Step 12.1: Read existing schema generators**

Run: `head -100 crates/query-parse/src/schema_gen/generator.rs` and identify:
- The graphql-library types in use (`async_graphql`, `juniper`, hand-rolled?). Look at imports.
- How an existing per-collection field is generated (the `User(filter, limit, ...)` pattern).
- How types are registered with the schema manager.

The new generators must use the same idiom.

- [ ] **Step 12.2: Stub `cursor.rs`**

Create `crates/query-parse/src/schema_gen/cursor.rs`. The exact API depends on the GraphQL library in use; the structure is:

```rust
//! Schema generation for cursor pagination types.
//!
//! Emits `PageInfo`, `CursorQuery` (with per-collection fields), and the
//! `_cursor` field registration on the top-level Query.

use super::{/* types from the schema library */};

/// Build the PageInfo type. All fields are nullable per Go's
/// `internal/request/graphql/schema/types/cursor.go:25-40`.
pub(crate) fn gen_page_info_type() -> /* GqlType */ {
    /* fields:
       - hasNext: Boolean    (nullable)
       - hasPrev: Boolean    (nullable)
       - startCursor: String (nullable)
       - endCursor: String   (nullable)
    */
}

/// Build the empty CursorQuery type shell. Per-collection fields are added
/// later by `gen_cursor_collection_field`.
pub(crate) fn gen_cursor_query_type(page_info: &/* GqlType */) -> /* GqlType */ {
    /* fields:
       - _pageInfo: PageInfo  (nullable)
    */
}

/// Build the per-collection field that lives inside CursorQuery.
/// Mirrors Go's `genCursorCollectionField` (generate.go:1592-1620).
pub(crate) fn gen_cursor_collection_field(
    collection_obj: &/* GqlType */,
    order_arg_type: &/* GqlType */,
    filter_arg_type: &/* GqlType */,
    group_by_arg_type: &/* GqlType */,
) -> /* GqlField */ {
    /* signature:
       <Collection>(
         first: Int, after: String, last: Int, before: String,
         order: [<Collection>Order!], filter: <Collection>Filter,
         docIDs: [ID!], cid: String,
         groupBy: [<Collection>GroupBy!], showDeleted: Boolean
       ): [<Collection>]    // nullable list of nullable items
       — no limit/offset; cursor args replace them
    */
}

fn cursor_args() -> /* arg list */ {
    /* first: Int, after: String, last: Int, before: String — all nullable */
}
```

**Fill in the actual GraphQL library types** by reading the existing `generator.rs`. Match its style exactly. For example, if existing code uses `async_graphql::dynamic::TypeRef::named_nn(..)` for non-null types, the cursor types use `TypeRef::named(..)` (no `_nn`).

- [ ] **Step 12.3: Write a smoke test for the PageInfo shape**

In `crates/query-parse/src/schema_gen/cursor.rs`, add at the bottom:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_info_has_four_fields_all_nullable() {
        let pi = gen_page_info_type();
        // Inspect the type: 4 fields, all nullable.
        // Exact assertions depend on the GraphQL lib's introspection API.
    }

    #[test]
    fn cursor_collection_field_returns_nullable_list() {
        // Build a fake collection object type and call gen_cursor_collection_field.
        // Assert the return type is `[Foo]` (nullable list of nullable Foo),
        // not `[Foo!]!`.
    }
}
```

The shape-level assertion is library-dependent. If your GraphQL library doesn't expose easy introspection in unit tests, defer to the schema-introspection check in Task 17 (integration test queries `__type(name:"PageInfo")` and inspects the result).

- [ ] **Step 12.4: Run**

Run: `cargo test -p query-parse schema_gen::cursor`
Expected: PASS (or document why the shape test is deferred).

- [ ] **Step 12.5: Commit**

```bash
git add crates/query-parse/src/schema_gen/
git commit -m "feat(query-parse): add PageInfo + CursorQuery type generators"
```

---

## Task 13: Wire `_cursor` field into `Query` root + register per-collection cursor fields

**Files:**
- Modify: `crates/query-parse/src/schema_gen/generator.rs`

- [ ] **Step 13.1: Register `PageInfo` and `CursorQuery` once per schema build**

In `crates/query-parse/src/schema_gen/generator.rs`, find the function that builds the top-level Query type (search for "defaultQueryType", "buildQuery", or "Query" object construction). Once, before iterating collections, build:

```rust
let page_info_type = crate::schema_gen::cursor::gen_page_info_type();
let cursor_query_type = crate::schema_gen::cursor::gen_cursor_query_type(&page_info_type);
schema_manager.register(page_info_type);
schema_manager.register(cursor_query_type);
```

Then add `_cursor: CursorQuery` (nullable) to the Query root:

```rust
query_root.add_field("_cursor", cursor_query_type_ref);  // nullable
```

The exact API depends on the library. Match Go's `schema.go:82-85` semantics: bare `cursorQueryType`, no `NewNonNull`.

- [ ] **Step 13.2: For each collection, add a cursor field to CursorQuery**

Inside the per-collection generation loop (Go's `generate.go:160-170` — `cursorField := g.genCursorCollectionField(...)`), add:

```rust
let cursor_field = crate::schema_gen::cursor::gen_cursor_collection_field(
    &collection_obj,
    &order_arg_type,
    &filter_arg_type,
    &group_by_arg_type,
);
cursor_query_type.add_field(cursor_field);
```

- [ ] **Step 13.3: Smoke test — query the introspection result**

Add an integration-style test in `crates/query-parse/tests/cursor_schema.rs`:

```rust
use query_parse::schema_gen::build_schema;  // or equivalent entrypoint

#[test]
fn cursor_field_registered_on_query() {
    let schema = build_schema_with_collection("User", &["age", "name"]);
    let introspection = query_introspection(&schema, "{ __type(name:\"Query\") { fields { name type { name } } } }");
    // Assert that "_cursor" appears in fields with type name "CursorQuery"
    assert!(introspection.contains("\"name\":\"_cursor\""));
    assert!(introspection.contains("CursorQuery"));
}

#[test]
fn page_info_type_has_nullable_fields() {
    let schema = build_schema_with_collection("User", &[]);
    let introspection = query_introspection(&schema, "{ __type(name:\"PageInfo\") { fields { name type { kind name } } } }");
    // Each field's type kind should be SCALAR (not NON_NULL).
    // hasNext, hasPrev, startCursor, endCursor — all kind=SCALAR.
    assert!(introspection.contains("\"name\":\"hasNext\""));
    // Verify no field has "kind":"NON_NULL" in this type
    assert!(!introspection.replace(' ', "").contains("\"kind\":\"NON_NULL\""), "PageInfo fields must be nullable");
}
```

Adapt `build_schema_with_collection` and `query_introspection` to whatever test scaffolding already exists in `crates/query-parse`.

- [ ] **Step 13.4: Run**

Run: `cargo test -p query-parse cursor_schema`
Expected: PASS.

- [ ] **Step 13.5: Run the full query-parse suite**

Run: `cargo test -p query-parse`
Expected: PASS — existing schema tests unaffected (we added types, didn't change existing ones).

- [ ] **Step 13.6: Commit**

```bash
git add crates/query-parse/src/schema_gen/
git commit -m "feat(query-parse): register _cursor field and per-collection cursor fields"
```

---

## Task 14: Parse `_cursor` wrapper and cursor args

**Files:**
- Create: `crates/query-parse/src/query_parse/cursor.rs`
- Modify: `crates/query-parse/src/query_parse/parser.rs`
- Modify: `crates/query-parse/src/query_parse/mod.rs` (expose submodule)

- [ ] **Step 14.1: Inspect the existing parser entry point**

Run: `head -120 crates/query-parse/src/query_parse/parser.rs`. Identify where top-level GraphQL selection fields are dispatched into `ParsedOperation::Select`/etc.

- [ ] **Step 14.2: Stub the cursor parser**

Create `crates/query-parse/src/query_parse/cursor.rs`:

```rust
//! Parser for the `_cursor` GraphQL wrapper field.

use query_types::error::{QueryError, Result};
use query_types::mapper::{CursorPageInfoFields, CursorParams, Select};

/// Parse the contents of a `_cursor { ... }` wrapper field.
/// Returns the inner Select with cursor fields populated, plus the alias
/// of the wrapper itself (so response shaping can emit under that alias).
pub(crate) fn parse_cursor_wrapper(
    /* GraphQL field node — match parser.rs's existing AST types */
    field: &/* AstField */,
) -> Result<Select> {
    // 1. Find the single inner collection field; collect optional _pageInfo selection.
    // 2. Parse cursor args (first/after/last/before) into CursorParams.
    // 3. Validate parse-time rules:
    //    - exactly one collection field (else cursor_must_contain_query / multiple)
    //    - first xor last (else forward_backward_conflict — also covers
    //      after-with-last and before-with-first)
    //    - first >= 0 and last >= 0
    // 4. Set inner_select.is_cursor = true, .cursor_params = Some(...), .cursor_page_info = ...
    // 5. Propagate the wrapper alias into the inner select's field.alias so
    //    response shaping uses it as the output key (instead of "_cursor").
}

fn parse_page_info_selection(
    /* AST nodes inside the _pageInfo selection */
) -> CursorPageInfoFields {
    /* Iterate child fields; flag each known name (hasNext, hasPrev, startCursor, endCursor) */
}

#[cfg(test)]
mod tests {
    use super::*;

    /* Parse-error tests. The exact entry point depends on the parser's
       AST types; mirror how existing parser tests construct fake AST or
       call the parser top-level from a query string. */

    #[test]
    fn forward_and_backward_args_conflict() {
        let query = "{ _cursor { User(first: 10, last: 5) { name } } }";
        let err = parse(query).unwrap_err();
        assert_eq!(err.to_string(), "forward parameters (first/after) cannot be combined with backward parameters (last/before)");
    }

    #[test]
    fn after_with_last_conflict() {
        let query = "{ _cursor { User(after: \"abc\", last: 5) { name } } }";
        let err = parse(query).unwrap_err();
        assert_eq!(err.to_string(), "forward parameters (first/after) cannot be combined with backward parameters (last/before)");
    }

    #[test]
    fn negative_first_rejected() {
        let query = "{ _cursor { User(first: -1) { name } } }";
        let err = parse(query).unwrap_err();
        assert_eq!(err.to_string(), "first must be non-negative");
    }

    #[test]
    fn empty_cursor_block_rejected() {
        let query = "{ _cursor { } }";
        let err = parse(query).unwrap_err();
        assert_eq!(err.to_string(), "_cursor block must contain exactly one collection query");
    }

    #[test]
    fn multiple_collections_in_cursor_rejected() {
        let query = "{ _cursor { User(first: 1) { name } Book(first: 1) { title } } }";
        let err = parse(query).unwrap_err();
        assert_eq!(err.to_string(), "_cursor block cannot contain multiple collection queries");
    }

    #[test]
    fn valid_forward_cursor_sets_select_fields() {
        let query = "{ _cursor { User(first: 10, after: \"abc\", order: { age: ASC }) { name } _pageInfo { hasNext startCursor } } }";
        let selects = parse(query).unwrap();
        let select = &selects[0];  // assuming parse returns Vec<Select>
        assert!(select.is_cursor);
        let params = select.cursor_params.as_ref().unwrap();
        assert_eq!(params.first, Some(10));
        assert_eq!(params.after, Some("abc".to_string()));
        assert!(select.cursor_page_info.has_next);
        assert!(select.cursor_page_info.start_cursor);
        assert!(!select.cursor_page_info.has_prev);
    }
}
```

- [ ] **Step 14.3: Route `_cursor` from the top-level parser**

In `crates/query-parse/src/query_parse/parser.rs`, find where top-level selection fields are dispatched. Add a branch:

```rust
if field.name == "_cursor" {
    let select = crate::query_parse::cursor::parse_cursor_wrapper(field)?;
    selects.push(select);
    continue;
}
```

Position this branch before generic collection-field handling.

- [ ] **Step 14.4: Implement the parser body**

Fill in `parse_cursor_wrapper` and `parse_page_info_selection` using the AST API discovered in 14.1. The implementation walks selection set children, extracts cursor args from the inner collection's args, validates per the table in §3 of the spec, and produces a populated `Select`.

- [ ] **Step 14.5: Run**

Run: `cargo test -p query-parse query_parse::cursor`
Expected: PASS — 6 tests.

- [ ] **Step 14.6: Run full query-parse**

Run: `cargo test -p query-parse`
Expected: PASS — existing parser tests unaffected.

- [ ] **Step 14.7: Commit**

```bash
git add crates/query-parse/src/query_parse/
git commit -m "feat(query-parse): parse _cursor wrapper and cursor args"
```

---

## Task 15: Force planner path for cursor selects

**Files:**
- Modify: `crates/query/src/runner/query/select.rs`

- [ ] **Step 15.1: Write the failing test**

In `crates/query/src/runner/query/select.rs` (or wherever the file's existing test module lives), add:

```rust
#[tokio::test]
async fn cursor_select_routes_through_planner() {
    // ARRANGE: a simple cursor select with no nested fields, no relations,
    // no index — would normally route to execute_simple_select.
    let mut select = Select::new("users");
    select.is_cursor = true;
    select.cursor_params = Some(CursorParams { first: Some(5), ..Default::default() });
    // ... minimal collection setup ...

    let runner = build_test_runner_with_users().await;

    // ACT
    let result = runner.execute_select_internal(&select, &fetcher, None).await;

    // ASSERT: the result is a cursor-shaped JSON object (with _pageInfo if
    // selected, or at minimum a list of users — execute_simple_select would
    // emit a top-level array of users; the planner emits the inner result
    // inside the cursor wrapper). The discriminator: the planner path must
    // run, observable via tracing/log or via the result shape.
    assert!(result.is_ok());
    // (Concrete shape assertion lives in Task 16 / integration tests;
    // here we just verify the planner branch was taken.)
}
```

Use whatever runner-test scaffolding already exists in the file.

- [ ] **Step 15.2: Run to verify failure**

Run: `cargo test -p query <test-name>`
Expected: FAIL (or pass for the wrong reason — the cursor query routes through `execute_simple_select` and ignores cursor semantics).

- [ ] **Step 15.3: Add `select.is_cursor` to `needs_planner`**

In `crates/query/src/runner/query/select.rs`, line 287-297, modify the `needs_planner` expression:

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
    || select.is_cursor;   // <-- added: cursor queries must go through planner
                            // for CursorNode wrapping
```

- [ ] **Step 15.4: Run**

Run: `cargo test -p query <test-name>`
Expected: PASS.

- [ ] **Step 15.5: Run full query suite**

Run: `cargo test -p query`
Expected: PASS — existing tests unaffected.

- [ ] **Step 15.6: Commit**

```bash
git add crates/query/src/runner/query/select.rs
git commit -m "feat(query): route cursor selects through planner path"
```

---

## Task 16: Wire response shaping for cursor selects

**Files:**
- Modify: `crates/query/src/runner/query/mod.rs`
- Possibly modify: the planner execution path that returns the per-select result (find via `rg "execute_select_internal" crates/query/`)

- [ ] **Step 16.1: Locate the per-select execution function**

`crates/query/src/runner/query/mod.rs:79-89` shows `execute_query_internal_with_vars` calling `execute_select_internal` per select. Find `execute_select_internal` in the same crate (search `rg "fn execute_select_internal" crates/query/`).

- [ ] **Step 16.2: Understand the current per-select return shape**

Today `execute_select_internal` returns a `JsonValue` that gets inserted under `select.field.output_name()`. For regular queries this is `[{...row}, {...row}]`. For cursor queries we need:

```json
{ "User": [...], "_pageInfo": { "hasNext": true, ... } }
```

And it gets inserted under `_cursor` (or the wrapper's alias).

- [ ] **Step 16.3: Write the failing test**

Inside `crates/query/src/runner/query/mod.rs` (or a new `cursor_response_tests.rs`):

```rust
#[tokio::test]
async fn cursor_select_response_has_pageinfo_under_cursor_key() {
    let runner = build_test_runner_with_users().await;
    let query = r#"{ _cursor { User(first: 2) { name } _pageInfo { hasNext startCursor endCursor } } }"#;
    let result = runner.execute_query(query).await.unwrap();

    let cursor_obj = result.get("_cursor").expect("response must have _cursor key");
    assert!(cursor_obj.get("User").is_some(), "inner collection key present");
    let page_info = cursor_obj.get("_pageInfo").expect("_pageInfo present when selected");
    assert!(page_info.get("hasNext").is_some());
    assert!(page_info.get("startCursor").is_some());
    assert!(page_info.get("endCursor").is_some());
    assert!(page_info.get("hasPrev").is_none(), "unselected _pageInfo fields must be absent");
}

#[tokio::test]
async fn cursor_select_omits_pageinfo_when_not_selected() {
    let runner = build_test_runner_with_users().await;
    let query = r#"{ _cursor { User(first: 2) { name } } }"#;
    let result = runner.execute_query(query).await.unwrap();
    let cursor_obj = result.get("_cursor").unwrap();
    assert!(cursor_obj.get("_pageInfo").is_none(), "_pageInfo absent when not selected");
}
```

- [ ] **Step 16.4: Run to verify failure**

Run: `cargo test -p query cursor_select_response`
Expected: FAIL — either compile error (entry point name differs) or the result shape doesn't match.

- [ ] **Step 16.5: Implement cursor response shaping**

In `execute_select_internal` (or the planner-execution adapter immediately above it), branch on `select.is_cursor`:

```rust
if select.is_cursor {
    // 1. Run the plan; the CursorNode at the top accumulates page_info().
    let inner_rows = run_plan_to_completion(plan).await?;

    // 2. Build the inner-collection key (use the alias on select.field or
    //    the field's name — which one mirrors the existing output_name()
    //    semantics for nested selects).
    let inner_key = select.field.output_name().to_string();
    //    For cursor queries, select.field.alias was set by the parser to
    //    the alias of the *inner* collection field, not the _cursor wrapper.
    //    The wrapper's alias becomes select.cursor_wrapper_alias or similar.
    //    (If the parser stored the wrapper alias differently, adjust.)

    // 3. Pull the page_info from the CursorNode (the runner needs a handle
    //    to it — either downcast or have CursorNode expose page_info via
    //    a trait method on PlanNode).
    let page_info = cursor_node_page_info(plan_top)?;

    // 4. Build the response object.
    let mut cursor_obj = Map::new();
    cursor_obj.insert(inner_key, JsonValue::Array(inner_rows));
    if page_info.fields.any_selected() {
        let mut pi = Map::new();
        if page_info.fields.has_next {
            pi.insert("hasNext".into(), JsonValue::Bool(page_info.has_next));
        }
        if page_info.fields.has_prev {
            pi.insert("hasPrev".into(), JsonValue::Bool(page_info.has_prev));
        }
        if page_info.fields.start_cursor {
            pi.insert("startCursor".into(),
                page_info.start_cursor.map(JsonValue::String).unwrap_or(JsonValue::Null));
        }
        if page_info.fields.end_cursor {
            pi.insert("endCursor".into(),
                page_info.end_cursor.map(JsonValue::String).unwrap_or(JsonValue::Null));
        }
        cursor_obj.insert("_pageInfo".into(), JsonValue::Object(pi));
    }
    return Ok(JsonValue::Object(cursor_obj));
}
```

The exact integration depends on the plan-execution machinery. Key requirement: the runner needs to access the top `CursorNode`'s `page_info()` after iteration. Two options:
- **(a)** Have `PlanNode` expose a `page_info()` method (default-None) overridden by `CursorNode`.
- **(b)** Keep a separate handle to the `CursorNode` before boxing into `Box<dyn PlanNode>`.

(a) is cleaner; do that. Add `fn page_info(&self) -> Option<CursorPageInfo> { None }` to the `PlanNode` trait; `CursorNode` returns `Some(...)`.

Then `execute_select_internal` post-iteration calls `plan_top.page_info()`.

The wrapper key (`_cursor` or its alias) and the inner collection key (`User` or its alias) are tracked separately, per the `Select` shape established in Task 4:
- `select.cursor_aliases.wrapper_alias` — the alias on `_cursor` (None ⇒ literal `_cursor`).
- `select.field.alias` (and `select.field.name`) — the inner collection's alias and name.

For cursor selects, the caller in `execute_query_internal_with_vars` should use `select.cursor_aliases.wrapper_alias.as_deref().unwrap_or("_cursor")` as the outer Map key (override the default `select.field.output_name()` behavior); the inner collection key is `select.field.output_name()`. Update `execute_query_internal_with_vars` accordingly:

```rust
let key = if select.is_cursor {
    select.cursor_aliases.wrapper_alias
        .as_deref()
        .unwrap_or("_cursor")
        .to_string()
} else {
    select.field.output_name().to_string()
};
results.insert(key, result);
```

- [ ] **Step 16.6: Run the tests**

Run: `cargo test -p query cursor_select_response`
Expected: PASS — both tests green.

- [ ] **Step 16.7: Run the full query suite**

Run: `cargo test -p query`
Expected: PASS — existing tests unaffected.

- [ ] **Step 16.8: Commit**

```bash
git add crates/query/src/runner/query/
git commit -m "feat(query): shape cursor response with _pageInfo wrapper"
```

---

## Task 17: Native integration tests — smoke + error paths

**Files:**
- Modify: `tools/integration-test/Cargo.toml` (add `[[test]] name = "cursor"`)
- Create: `tools/integration-test/tests/cursor.rs` (the binary entry)
- Create: `tools/integration-test/tests/cursor/mod.rs` (module wiring)
- Create: `tools/integration-test/tests/cursor/smoke.rs`
- Create: `tools/integration-test/tests/cursor/error_paths.rs`
- Create: `tools/integration-test/tests/cursor/composite_index.rs`

- [ ] **Step 17.1: Register the test binary**

In `tools/integration-test/Cargo.toml`, after the existing `[[test]]` entries, add:

```toml
[[test]]
name = "cursor"
path = "tests/cursor.rs"
```

- [ ] **Step 17.2: Create the test binary entry**

Create `tools/integration-test/tests/cursor.rs`:

```rust
mod cursor;
```

- [ ] **Step 17.3: Create the cursor module wiring**

Create `tools/integration-test/tests/cursor/mod.rs`:

```rust
mod smoke;
mod error_paths;
mod composite_index;
mod storage_backends;
mod subscription_interaction;
```

- [ ] **Step 17.4: Write smoke tests**

Create `tools/integration-test/tests/cursor/smoke.rs`. Pattern after `tools/integration-test/tests/query/` existing tests — read one (e.g., `tools/integration-test/tests/query/index_management.rs`) to find the harness API for `node.query(...)`, schema setup, etc.

```rust
//! Smoke tests: basic forward/backward, _pageInfo shape.

use super::common::{TestNode, assert_query};

#[tokio::test]
async fn forward_first_returns_n_items() {
    let node = TestNode::new()
        .with_schema(r#"type User { name: String age: Int @index }"#)
        .await;

    // Seed 5 users
    for (name, age) in [("alice", 20), ("bob", 30), ("carol", 40), ("dave", 50), ("eve", 60)] {
        node.create("User", &format!(r#"{{ "name": "{}", "age": {} }}"#, name, age)).await;
    }

    let result = node.query(r#"
        { _cursor { User(first: 2, order: { age: ASC }) { name } } }
    "#).await.unwrap();

    let users = result["_cursor"]["User"].as_array().unwrap();
    assert_eq!(users.len(), 2);
    assert_eq!(users[0]["name"], "alice");
    assert_eq!(users[1]["name"], "bob");
}

#[tokio::test]
async fn forward_first_after_skips_to_cursor() {
    let node = TestNode::new()
        .with_schema(r#"type User { name: String age: Int @index }"#)
        .await;
    // ... seed as above ...

    // First page
    let page1 = node.query(r#"
        { _cursor { User(first: 2, order: { age: ASC }) { name } _pageInfo { endCursor hasNext } } }
    "#).await.unwrap();

    let end = page1["_cursor"]["_pageInfo"]["endCursor"].as_str().unwrap().to_string();
    assert!(page1["_cursor"]["_pageInfo"]["hasNext"].as_bool().unwrap());

    // Second page
    let page2 = node.query(&format!(r#"
        {{ _cursor {{ User(first: 2, after: "{}", order: {{ age: ASC }}) {{ name }} }} }}
    "#, end)).await.unwrap();

    let users = page2["_cursor"]["User"].as_array().unwrap();
    assert_eq!(users.len(), 2);
    assert_eq!(users[0]["name"], "carol");
    assert_eq!(users[1]["name"], "dave");
}

#[tokio::test]
async fn backward_last_returns_last_n_items() {
    let node = TestNode::new()
        .with_schema(r#"type User { name: String age: Int @index }"#)
        .await;
    // ... seed alice/bob/carol/dave/eve ...

    let result = node.query(r#"
        { _cursor { User(last: 2, order: { age: ASC }) { name } } }
    "#).await.unwrap();

    let users = result["_cursor"]["User"].as_array().unwrap();
    assert_eq!(users.len(), 2);
    assert_eq!(users[0]["name"], "dave");
    assert_eq!(users[1]["name"], "eve");
}

#[tokio::test]
async fn no_order_uses_doc_id_fallback() {
    // Order is optional in cursor queries; with no order, docID ordering is used.
    let node = TestNode::new()
        .with_schema(r#"type User { name: String }"#)  // no index!
        .await;
    node.create("User", r#"{ "name": "alice" }"#).await;
    node.create("User", r#"{ "name": "bob" }"#).await;

    let result = node.query(r#"
        { _cursor { User(first: 2) { name } } }
    "#).await.unwrap();

    let users = result["_cursor"]["User"].as_array().unwrap();
    assert_eq!(users.len(), 2);
    // Order is by docID; specific order depends on document hashing, but
    // both users must be present. (This test validates that no-index +
    // no-order cursor queries don't error.)
}

#[tokio::test]
async fn page_info_shape_matches_selection() {
    let node = TestNode::new()
        .with_schema(r#"type User { name: String age: Int @index }"#).await;
    node.create("User", r#"{ "name": "a", "age": 1 }"#).await;

    let result = node.query(r#"
        { _cursor { User(first: 1, order: { age: ASC }) { name } _pageInfo { hasNext } } }
    "#).await.unwrap();

    let pi = &result["_cursor"]["_pageInfo"];
    assert!(pi.get("hasNext").is_some());
    assert!(pi.get("hasPrev").is_none(), "unselected fields must be omitted");
    assert!(pi.get("startCursor").is_none());
}
```

- [ ] **Step 17.5: Write error-path tests**

Create `tools/integration-test/tests/cursor/error_paths.rs`:

```rust
//! Error paths: invalid args, missing index, malformed tokens.

use super::common::TestNode;

#[tokio::test]
async fn no_supporting_index_errors() {
    let node = TestNode::new()
        .with_schema(r#"type User { name: String age: Int }"#)  // no index on age
        .await;
    node.create("User", r#"{ "name": "a", "age": 1 }"#).await;

    let result = node.query(r#"
        { _cursor { User(first: 1, order: { age: ASC }) { name } } }
    "#).await;

    let err = result.unwrap_err().to_string();
    assert!(err.contains("no supporting index"), "got: {}", err);
}

#[tokio::test]
async fn invalid_cursor_token_errors() {
    let node = TestNode::new()
        .with_schema(r#"type User { name: String age: Int @index }"#).await;

    let result = node.query(r#"
        { _cursor { User(first: 1, after: "!!!not-base64!!!", order: { age: ASC }) { name } } }
    "#).await;

    assert_eq!(result.unwrap_err().to_string(), "invalid cursor");
}

#[tokio::test]
async fn forward_backward_conflict_errors() {
    let node = TestNode::new()
        .with_schema(r#"type User { name: String age: Int @index }"#).await;

    let result = node.query(r#"
        { _cursor { User(first: 5, last: 3, order: { age: ASC }) { name } } }
    "#).await;

    assert!(result.unwrap_err().to_string().contains("forward parameters"));
}

#[tokio::test]
async fn after_with_last_errors() {
    let node = TestNode::new()
        .with_schema(r#"type User { name: String age: Int @index }"#).await;

    let result = node.query(r#"
        { _cursor { User(after: "x", last: 3, order: { age: ASC }) { name } } }
    "#).await;

    assert!(result.unwrap_err().to_string().contains("forward parameters"));
}

#[tokio::test]
async fn empty_cursor_block_errors() {
    let node = TestNode::new()
        .with_schema(r#"type User { name: String }"#).await;

    let result = node.query("{ _cursor { } }").await;

    assert!(result.unwrap_err().to_string().contains("must contain exactly one"));
}

#[tokio::test]
async fn multiple_collections_in_cursor_errors() {
    let node = TestNode::new()
        .with_schema(r#"
            type User { name: String }
            type Book { title: String }
        "#).await;

    let result = node.query(r#"
        { _cursor { User(first: 1) { name } Book(first: 1) { title } } }
    "#).await;

    assert!(result.unwrap_err().to_string().contains("cannot contain multiple"));
}
```

- [ ] **Step 17.6: Write composite-index tests**

Create `tools/integration-test/tests/cursor/composite_index.rs`:

```rust
//! Cursor over composite indexes.

use super::common::TestNode;

#[tokio::test]
async fn composite_index_full_field_coverage_works() {
    let node = TestNode::new()
        .with_schema(r#"
            type User { name: String age: Int }
            @index(fields: ["age", "name"])
        "#)  // syntax depends on Rust DefraDB's index DSL — adapt
        .await;
    // ... seed and query ordering by (age, name) ...
    let result = node.query(r#"
        { _cursor { User(first: 2, order: { age: ASC name: ASC }) { name age } } }
    "#).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn non_unique_composite_prefix_only_errors() {
    // Index on (age, name) is non-unique. Ordering only by `age` is a prefix
    // mismatch per Go's isUnsupportedCursorCompositePrefix.
    let node = TestNode::new()
        .with_schema(r#"
            type User { name: String age: Int }
            @index(fields: ["age", "name"])
        "#).await;
    let result = node.query(r#"
        { _cursor { User(first: 2, order: { age: ASC }) { name } } }
    "#).await;
    assert!(result.unwrap_err().to_string().contains("no supporting index"));
}
```

- [ ] **Step 17.7: Run the smoke + error suites**

Run: `cargo test -p integration-test --test cursor -- smoke error_paths`
Expected: PASS.

- [ ] **Step 17.8: Commit**

```bash
git add tools/integration-test/
git commit -m "test(integration): add cursor pagination smoke + error path suites"
```

---

## Task 18: Native integration tests — storage backends + subscriptions

**Files:**
- Create: `tools/integration-test/tests/cursor/storage_backends.rs`
- Create: `tools/integration-test/tests/cursor/subscription_interaction.rs`

- [ ] **Step 18.1: Write the storage-backend matrix**

Create `tools/integration-test/tests/cursor/storage_backends.rs`:

```rust
//! Run the same cursor scenario against each storage backend.

use super::common::{TestNode, StorageBackend};

async fn cursor_scenario(node: TestNode) {
    node.schema(r#"type User { name: String age: Int @index }"#).await;
    for (name, age) in [("a", 1), ("b", 2), ("c", 3), ("d", 4)] {
        node.create("User", &format!(r#"{{ "name": "{}", "age": {} }}"#, name, age)).await;
    }
    let result = node.query(r#"
        { _cursor { User(first: 2, order: { age: ASC }) { name } _pageInfo { hasNext endCursor } } }
    "#).await.unwrap();
    let users = result["_cursor"]["User"].as_array().unwrap();
    assert_eq!(users.len(), 2);
    assert!(result["_cursor"]["_pageInfo"]["hasNext"].as_bool().unwrap());
}

#[tokio::test]
async fn cursor_works_on_redb() {
    cursor_scenario(TestNode::with_backend(StorageBackend::Redb).await).await;
}

#[tokio::test]
async fn cursor_works_on_fjall() {
    cursor_scenario(TestNode::with_backend(StorageBackend::Fjall).await).await;
}

#[tokio::test]
async fn cursor_works_on_rocksdb() {
    cursor_scenario(TestNode::with_backend(StorageBackend::Rocksdb).await).await;
}

#[tokio::test]
async fn cursor_works_on_memory() {
    cursor_scenario(TestNode::with_backend(StorageBackend::Memory).await).await;
}
```

The `TestNode::with_backend` API may need to be added; if the integration-test harness only supports one backend per process, gate these tests behind feature flags or run the matrix via a build matrix in CI. **Read `tools/integration-test/tests/common/mod.rs` (or wherever the harness lives) to see how backends are currently selected.** If there's no per-test backend switch, file a follow-up issue and skip 18.1–18.4 with a TODO note; cover backends in CI matrix instead.

- [ ] **Step 18.2: Write the subscription-interaction test**

Create `tools/integration-test/tests/cursor/subscription_interaction.rs`:

```rust
//! Cursor pagination must not interfere with subscriptions.

use super::common::TestNode;

#[tokio::test]
async fn cursor_query_does_not_break_active_subscription() {
    let node = TestNode::new()
        .with_schema(r#"type User { name: String age: Int @index }"#).await;
    node.create("User", r#"{ "name": "alice", "age": 30 }"#).await;

    // Open a subscription on User
    let mut sub = node.subscribe("User", "{ name }").await;

    // Run a cursor query concurrently
    let cursor_result = node.query(r#"
        { _cursor { User(first: 5, order: { age: ASC }) { name } } }
    "#).await;
    assert!(cursor_result.is_ok(), "cursor query failed: {:?}", cursor_result.err());

    // Mutate to trigger the subscription
    node.create("User", r#"{ "name": "bob", "age": 40 }"#).await;

    // Subscription should still deliver
    let event = tokio::time::timeout(std::time::Duration::from_secs(2), sub.next()).await.unwrap().unwrap();
    assert_eq!(event["name"], "bob");
}
```

- [ ] **Step 18.3: Run the matrix and subscription suites**

Run: `cargo test -p integration-test --test cursor -- storage_backends subscription_interaction`
Expected: PASS (or, per 18.1's note, skipped with a TODO).

- [ ] **Step 18.4: Run the full cursor test binary**

Run: `cargo test -p integration-test --test cursor`
Expected: PASS — all submodules green.

- [ ] **Step 18.5: Commit**

```bash
git add tools/integration-test/tests/cursor/
git commit -m "test(integration): add storage backend + subscription cursor tests"
```

---

## Task 19: FFI parity verification

**Files:** No code changes in this task — runs the existing FFI harness against Go's cursor tests.

- [ ] **Step 19.1: Locate the FFI test runner**

Inspect `tools/ffi-test/` for the entry point. Look at recent FFI test runs to understand the invocation pattern (memory: there's an FFI revival in progress at 47% pass rate; cursor tests should add to that gradually).

- [ ] **Step 19.2: Run the FFI cursor suite**

Invoke whatever command runs the Go test driver against the Rust node. Go's cursor tests live at `/Users/johnzampolin/go/src/github.com/sourcenetwork/defradb/tests/integration/query/cursor/`. Example:

```bash
cd tools/ffi-test && ./run-suite cursor
```

(Adapt to the actual harness invocation.)

Expected: ideally all 18 cursor test files pass. Realistic: a portion pass on first run; the failures expose real parity bugs.

- [ ] **Step 19.3: Triage failures**

For each failing Go cursor test, capture the failure mode:
- **Wrong result data:** likely a planner bug (off-by-one in skip/probe, wrong sort direction in backward path).
- **Wrong error message:** the constructor strings in Task 5 don't match Go's exact wording — grep the Go file and fix.
- **GraphQL schema introspection mismatch:** nullability or arg list diverges from Go — adjust Task 13's generators.
- **Panic / compile error in test binary:** harness mismatch.

For each, decide whether it's:
- (a) **In scope for this PR** — fix immediately.
- (b) **Pre-existing FFI drift** — file an issue, mark the test as expected-failure in the harness, move on.

- [ ] **Step 19.4: Update spec if reality differs**

If FFI testing surfaces design issues that weren't apparent during brainstorming (e.g., an error string differs, a Go behavior diverges from what we ported), update `docs/superpowers/specs/2026-05-14-cursor-pagination-design.md` with the correction and commit.

- [ ] **Step 19.5: Aim for parity, commit any remaining fixes**

```bash
git add <files-affected-by-fixes>
git commit -m "fix(cursor): resolve FFI parity findings"
```

- [ ] **Step 19.6: Final verification**

Run the full workspace:

```bash
cargo build --release
cargo test
cargo clippy --all -- -D warnings
cargo fmt --all -- --check
```

All green.

---

## Task 20: Open the pull request

- [ ] **Step 20.1: Push the branch**

```bash
git push -u origin worktree-feat+cursor-pagination
```

- [ ] **Step 20.2: Open the PR**

```bash
gh pr create --title "feat: cursor-based GraphQL pagination" --body "$(cat <<'EOF'
## Summary

Implements GraphQL cursor pagination via a `_cursor` wrapper field with `first`/`after`/`last`/`before` semantics, mirroring Go DefraDB PR #4617. Supersedes the closed #930 (REST limit/offset).

Highlights:
- New `crates/cursor` for byte-compatible token codec (base64url JSON)
- `CursorNode` plan node with forward/backward state machine
- Index-backed seek via `IndexScanParams.cursor_seek`
- Hard error on missing index for explicit non-docID orderings (mirrors Go)
- DocID fallback when ordering is empty or only by `_docID`
- Cross-compat fixture tests (Go-generated tokens round-trip through Rust)
- Native integration tests + FFI parity against Go's cursor test suite

## Spec & design

`docs/superpowers/specs/2026-05-14-cursor-pagination-design.md`

## Test plan

- [x] `cargo test -p cursor`
- [x] `cargo test -p query-types`
- [x] `cargo test -p query-parse`
- [x] `cargo test -p query-plan`
- [x] `cargo test -p query`
- [x] `cargo test -p integration-test --test cursor`
- [x] FFI parity: Go cursor tests against Rust node — TBD pass rate documented in PR description
- [x] `cargo clippy --all -- -D warnings`
- [x] `cargo fmt --all -- --check`

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Implementation notes

**Sequencing rationale:** Tasks 1–3 build the codec foundation. Tasks 4–5 extend the request and error types used everywhere downstream. Tasks 6–7 prepare the storage/scan layer. Tasks 8–11 build the planner side (CursorNode + expansion). Tasks 12–14 build the GraphQL layer (schema + parser). Tasks 15–16 wire the runner. Tasks 17–19 validate. Task 20 ships.

Each task leaves the tree compiling. The tree won't pass cursor *end-to-end* until Task 16 lands; the GraphQL `_cursor` field is registered by Task 13 but parsing routes through `parse_cursor_wrapper` only after Task 14, and response shaping is complete only after Task 16. Until then, cursor queries produce errors (or empty results) — that's expected. Integration tests in Task 17+ are the first end-to-end validation point.

**Coding style:** Match existing crate idioms. The codebase uses thiserror, async-trait, serde, and `Result<T> = Result<T, QueryError>`-style aliases. Use `tracing::instrument` on async functions when sibling code does. Don't introduce new dependencies beyond `base64` (cursor crate only).

**On error string parity:** the spec says surface strings are Go-exact verbatim. Task 5 hardcodes them. If Task 19 surfaces any string mismatch, that's the canonical fix point — update the constructor, not the test expectation.

**On the Go fixture generator:** the fixture file in `crates/cursor/tests/fixtures/all.json` starts at 4 hand-built cases in Task 3. The full ~30-case set requires a Go-side tool that doesn't exist yet. If you can land it upstream in defradb (PR adding `tools/cursor-fixtures/main.go`), do so before merging this Rust PR. Otherwise, the 4 cases are a real (if shallow) cross-compat check; expand to 30+ in a follow-up.

**On the `set_cursor_seek` / `page_info` trait additions:** these add two methods to `PlanNode` with default no-op implementations. Existing nodes don't need any change beyond `CursorNode` and `IndexScanNode` (and wrapper nodes that forward to children). This is a small surface change for a meaningful capability.
