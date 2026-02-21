# Directive Arguments Not Stored or Evaluated Downstream

**Severity**: INFO (GREEN)
**Category**: Input Validation — Schema Safety
**Status**: Confirmed Safe

## Summary

SDL directive arguments are parsed, validated (type-checked), and consumed during schema building — but their raw string values are never stored in a way that could be evaluated later. Unknown directive arguments are discarded entirely. There is no code path where a directive argument string could be interpreted as executable content, SQL, or a storage key injection.

## Affected Files

- `crates/query/src/sdl_parse/directives.rs` — argument whitelist per directive
- `crates/query/src/sdl_parse/fields.rs` — argument extraction and consumption

## Details

### How Directive Arguments Are Consumed

| Directive | Arguments | How Consumed |
|-----------|-----------|--------------|
| `@index` | `name`, `unique`, `direction`, `fields`, `includes` | Parsed into `IndexDescription` struct |
| `@relation` | `name` | Stored as `relation_name: String` on field |
| `@crdt` | `type` | Matched against `CType` enum (reject unknown) |
| `@policy` | `id`, `resource` | Stored in `PolicyDescription`, validated for path traversal |
| `@default` | value | Parsed into `serde_json::Value`, stored as default |
| `@constraints` | `size` | Parsed as `usize`, stored as field size |
| `@embedding` | `provider`, `model`, `url`, `fields`, `template` | Stored in `VectorEmbeddingDescription` |
| `@branchable` | `if` | Parsed as `bool`, controls branchable flag |
| `@materialized` | `if` | Parsed as `bool`, controls materialized flag |

### Key Safety Properties

1. **No eval/exec**: No directive argument is ever passed to an interpreter, shell, or query engine
2. **Type-checked consumption**: Arguments are parsed to specific Rust types (bool, String, usize) — not stored as raw strings that could be reinterpreted
3. **Policy validation**: The `@policy` arguments (`id`, `resource`) go through explicit validation in `crates/schema/src/policy.rs` that rejects path separators, `..`, and null bytes
4. **Relation names**: Used only for matching relation pairs and generating storage keys via the safe binary encoding
5. **Index names**: Used only for deduplication and display — not interpolated into queries

### Unknown Arguments

Arguments on known directives that don't match the whitelist:

```rust
// directives.rs:27-44 — argument whitelist
pub fn known_args(directive_name: &str) -> &[&str] {
    match directive_name {
        "index" => &["name", "unique", "direction", "fields", "includes"],
        "relation" => &["name"],
        // ...
    }
}
```

Unknown arguments generate `ParseWarning::UnknownDirectiveArgument` and are **discarded** — they are not stored anywhere.

## Test Gap

None needed — this is a positive finding confirming safe behavior.
