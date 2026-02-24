# Circular Type References Properly Detected

**Severity**: INFO (GREEN)
**Category**: Input Validation — Schema Safety
**Status**: Confirmed Safe

## Summary

The SDL parser uses Tarjan's Strongly Connected Components algorithm to detect circular type references. Self-references, bidirectional references, and multi-type cycles are all correctly identified and handled. There is no infinite loop risk from circular schema definitions.

## Affected Files

- `crates/query/src/sdl_parse/builder.rs:342-419` — relation graph construction
- `crates/query/src/sdl_parse/builder.rs:944-1032` — Tarjan's SCC algorithm

## Details

### Algorithm

1. **Graph construction**: Only PRIMARY-side, single-object relation edges are considered (arrays break cycles)
2. **Self-reference detection**: Types referencing themselves are tracked separately
3. **Cycle detection**: Tarjan's algorithm finds all strongly connected components
4. **Collection sets**: Multi-type cycles produce `CollectionSetDescription` with a shared CID

### What Happens on Cycle

When a cycle is detected:
- A collection set is created with all participating types
- Each type gets a `relative_id` (lexicographic position within the set)
- CIDs are generated using `generate_collection_set_cid()`
- The schema is accepted — cycles are a valid pattern (e.g., `User` ↔ `Post`)

### Key Constraint

A cycle only exists if BOTH sides of a mutual reference have PRIMARY edges. Since arrays are always secondary, `User.posts: [Post]` does not create a cycle even if `Post.author: User @primary` does.

### Query Parser Fragment Cycles

The query parser also detects circular fragment references:

```rust
// parser.rs:148-153
if visiting.contains(&spread.fragment_name) {
    return Err(QueryError::parse(format!(
        "circular fragment reference detected: '{}'", spread.fragment_name
    )));
}
```

## Test Gap

None — this is a positive finding. Cycle detection is well-tested through the collection set CID generation tests.
