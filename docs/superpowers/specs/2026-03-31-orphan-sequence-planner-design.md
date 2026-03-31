# OrphanNode & SequenceNode Planner Design

## Goal

Implement Go-parity orphan document handling and nil FK filter indexing in the Rust query planner, fixing 47 FFI test failures (26 index + 21 explain).

## Context

When ordering by a field on a related collection (e.g., `Authors(order: {published: {name: ASC}})`), documents without any related document ("orphans") must be includable in results. Go uses `orphanNode` and `sequenceNode` plan nodes. Rust currently has an ad-hoc orphan phase inside `TypeJoinOne` that doesn't fully work and doesn't produce correct explain output.

## Architecture

Three new plan node types that implement the `PlanNode` trait, wired by the planner when `@exhaustive` + relation ordering is detected. `TypeJoinOne` is cleaned up to only handle joins — orphan logic moves entirely to dedicated nodes.

### New Plan Nodes

**`OrphanNode`** — Scans for documents without a matching relation. Two variants:

- **`PrimarySide`**: Parent stores FK. Wraps a `ScanNode` with `FK IS NULL` filter. Scans the FK index directly for NULL entries. This is the same mechanism Go uses via a cloned scanNode.

- **`SecondarySide`**: Parent doesn't store FK. Wraps a parent `ScanNode` iterator. For each parent document, performs an O(1) point lookup on the child's FK index (`datastore.Has()` with key = parent docID) to check if any child references it. Yields only parents with no referencing child.

Both variants:
- `kind()` returns `"orphanNode"`
- `explain_inner()` reports the inner scan structure
- `init()` / `start()` / `next()` / `close()` lifecycle

**`SequenceNode`** — Chains two child plan nodes sequentially. Exhausts the first child completely, then the second.

- `kind()` returns `"sequenceNode"`
- `next()`: delegates to `children[0].next()` until exhausted, then `children[1].next()`
- `explain_inner()` reports both children
- Used to concatenate orphan results with join results in the correct order:
  - ASC ordering: orphans first (NULLs sort before values)
  - DESC ordering: orphans last

### Planner Wiring

The decision logic in `planner/joins/mod.rs` follows Go's `expandTypeIndexJoinPlan()`:

1. Query has `orderBy` on a relation field
2. Child collection has an index on the ordered field
3. Join is inverted (child drives iteration via sorted index scan)
4. `@exhaustive` directive is set on the query
5. Build plan tree based on parent side:
   - **Primary side** (parent stores FK): `SequenceNode([OrphanNode::PrimarySide, TypeJoinOne])` for ASC, reversed for DESC
   - **Secondary side** (parent doesn't store FK): `SequenceNode([TypeJoinOne, OrphanNode::SecondarySide])` for DESC, reversed for ASC

Without `@exhaustive`, no orphan handling — TypeJoinOne runs alone and orphans are excluded.

### Nil FK Filter Indexing

For queries like `Devices(filter: {_ownerID: {_eq: null}})`:

- The existing `try_select_index()` in `planner/builder/index_methods.rs` already handles filter-based index selection
- `OrphanNode::PrimarySide` uses the same mechanism — FK index with NULL scan
- The `has_relation_filter` guard was already removed in a prior fix
- Remaining work: ensure the FK index is selected when the filter condition is `_eq: null` or `_neq: null` on a FK field

### Changes to TypeJoinOne

Remove from `TypeJoinOne`:
- `include_orphans: bool`
- `yielded_parent_ids: HashSet<String>`
- `orphan_phase: bool`
- `next_orphan()` method
- `with_include_orphans()` builder

TypeJoinOne becomes purely a join node — no orphan awareness.

## File Organization

**New files:**
- `crates/query/src/plan/orphan.rs` — `OrphanNode` with PrimarySide/SecondarySide variants
- `crates/query/src/plan/sequence.rs` — `SequenceNode`

**Modified files:**
- `crates/query/src/plan/type_join/type_join_one.rs` — Remove orphan phase, clean join-only
- `crates/query/src/plan/mod.rs` — Register new modules
- `crates/query/src/planner/joins/mod.rs` — Wire orphan/sequence creation
- `crates/query/src/planner/builder/index_methods.rs` — Nil FK filter index selection

## Success Criteria

- 47 FFI test failures (26 index + 21 explain) drop to under 10
- `cargo test -p query` passes
- `cargo clippy --all -- -D warnings` clean
- Explain output matches Go's node structure (orphanNode, sequenceNode, typeIndexJoin)
