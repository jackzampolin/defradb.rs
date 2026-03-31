# Orphan Runtime & Explain Parity Design

## Goal

Fix orphan document handling so that: (1) orphans are correctly included/excluded at runtime, (2) explain output matches Go's `typeIndexJoin > sequenceNode > [orphanNode, typeJoinOne]` structure exactly.

## Context

`SequenceNode` and `OrphanNode` plan nodes exist but are currently wired as external wrappers around `TypeJoinOne`. Go puts them **inside** `typeIndexJoin` as internal children. The Go explain asserter expects `sequenceNode` and `orphanNode` nested within `typeIndexJoin`, not at the top level.

## Architecture

Move `SequenceNode` and `OrphanNode` inside `TypeJoinOne`. When `@exhaustive` is set on an inverted join, `TypeJoinOne` uses a `SequenceNode` internally to chain orphan results with normal join results. The explain output naturally reports the Go-compatible structure.

### OrphanConfig

New struct held by `TypeJoinOne` when orphan handling is active:

```rust
struct OrphanConfig {
    orphan_node: OrphanNode,
    sort_direction: OrderDirection,
    sequence_active: bool,  // true once SequenceNode is driving iteration
}
```

### Runtime Behavior

When `OrphanConfig` is set, `TypeJoinOne.next()` changes:

- **Without orphans**: child-driven scan → lookup parent → merge → yield (existing behavior)
- **With orphans (ASC)**: orphan_node yields first (FK IS NULL parents), then child-driven scan
- **With orphans (DESC)**: child-driven scan first, then orphan_node yields last

The `SequenceNode` handles the chaining. TypeJoinOne creates it at init time from OrphanNode + a closure/iterator over the normal join path.

Alternatively, TypeJoinOne can manage two phases directly (orphan phase + join phase) without SequenceNode, switching between them based on sort direction. This avoids the complexity of wrapping TypeJoinOne's own iteration in a SequenceNode child. The explain output still reports `sequenceNode` structure regardless.

### Orphan Detection

**Primary side** (parent stores FK): OrphanNode clones the parent scan with FK IS NULL filter added. Delegates to scan — simple and O(filtered docs).

**Secondary side** (parent doesn't store FK): OrphanNode iterates all parents, uses `yielded_parent_ids` HashSet (populated during join phase) to skip non-orphans. This is simpler than Go's point-lookup approach but correct.

### Explain Output

`TypeJoinOne.explain_inner()` when orphan handling is active:

```json
{
  "typeIndexJoin": {
    "sequenceNode": [
      { "orphanNode": { "docFetches": N, "fieldFetches": N, "indexFetches": N, "iterations": N } },
      { "typeJoinOne": { "root": { "scanNode": {...} }, "subType": {...} } }
    ]
  }
}
```

Order reversed for DESC. This matches Go's exact output. The `sequenceNode` and `orphanNode` keys appear in explain as data, not as plan node `kind()` names at the top level.

### Planner Wiring

Revert the external `SequenceNode` wrapping in `planner/joins/mod.rs`. Instead:

```rust
if select.exhaustive {
    let orphan = create_orphan_node_for_join(/* parent scan, FK info, fetcher */);
    let direction = extract_sort_direction(&parent_order_for_child);
    join = join.with_orphan_config(orphan, direction);
}
plan = Box::new(join);
```

TypeJoinOne receives the orphan config and manages it internally.

## Files

| File | Change |
|------|--------|
| `crates/query/src/plan/type_join/type_join_one.rs` | Add `OrphanConfig`, modify `next()`/`init()`/`explain()` |
| `crates/query/src/planner/joins/mod.rs` | Revert external wrapping, pass OrphanConfig to TypeJoinOne |
| `crates/query/src/plan/orphan.rs` | No changes |
| `crates/query/src/plan/sequence.rs` | No changes (may be used internally or not) |

## Success Criteria

- 3 EXPLAIN_ASSERTER index tests pass (Go asserter recognizes structure)
- 16 DATA_MISMATCH index tests improve (orphans correctly included/excluded)
- explain/default stays at 99% (no regression)
- `cargo test -p query` passes
- `cargo clippy --all -- -D warnings` clean
