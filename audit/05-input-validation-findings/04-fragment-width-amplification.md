# Finding: Fragment Width Amplification (Non-Cyclic)

**Stream**: 05 - Input Validation
**Severity**: LOW
**Category**: Denial of Service
**Status**: CONFIRMED (cycle detection works; width amplification is theoretical)

## Summary

The parser has correct circular fragment reference detection using a `visiting` HashSet. However, non-cyclic fragment chains can amplify query width: each fragment spread duplicates all fields from the referenced fragment into the parent selection set. With many fragments and multiple spreads, a compact query can expand into thousands of effective fields. This is mitigated by the fact that DefraDB's schema limits the valid field names, so spurious fields would be rejected during planning.

## Affected Files

| File | Function | Issue |
|------|----------|-------|
| `crates/query/src/query_parse/parser.rs:147-176` | `parse_selection_to_selects()` FragmentSpread | Cycle detection correct; no spread count limit |
| `crates/query/src/query_parse/parser.rs:764-816` | `parse_selection_set()` FragmentSpread | Same: fields merged without count limit |
| `crates/query/src/query_parse/parser.rs:818-852` | `parse_selection_set()` InlineFragment | Inline fragments also unbounded |

## Details

### Cycle Detection: Working Correctly

The parser maintains a `visiting: &mut HashSet<String>` set to detect circular fragment references:

```rust
// parser.rs:148-154
Selection::FragmentSpread(spread) => {
    if visiting.contains(&spread.fragment_name) {
        return Err(QueryError::parse(format!(
            "circular fragment reference detected: '{}'",
            spread.fragment_name
        )));
    }
    visiting.insert(spread.fragment_name.clone());
    // ... process fragment ...
    visiting.remove(&spread.fragment_name);
}
```

This correctly prevents `A -> B -> A` cycles. The `remove` after processing allows the same fragment to be used in sibling positions (not a cycle), which is valid GraphQL behavior.

### Width Amplification Pattern

A fragment can be spread in multiple places. Consider:

```graphql
fragment F on User { name age email phone address }

query {
  User {
    ...F       # 5 fields
    friends {
      ...F     # 5 more fields in nested context
      colleagues {
        ...F   # 5 more fields in deeper nested context
      }
    }
  }
}
```

This is valid GraphQL and each spread expands to 5 fields. With larger fragments and more spreads, the effective query width grows linearly with the number of spreads. The fragment itself is only defined once, so the compact query text expands during parsing.

### Inline Fragment Amplification

Inline fragments (`... on Type { ... }`) are also processed recursively without count limits:

```rust
// parser.rs:818-852
Selection::InlineFragment(inline) => {
    let (inline_fields, _inline_mapping) = parse_selection_set(
        &inline.selection_set, ...
    )?;
    for inline_field in inline_fields {
        // Each field merged into parent — no count limit
        fields.push(inline_field);
    }
}
```

### Practical Mitigation

DefraDB validates field names against collection schemas during planning. A fragment containing field names that don't exist in the collection will cause planning errors. This limits the practical amplification to fields that actually exist in the schema. However, the parser still allocates `Vec<Requestable>` entries and `DocumentMapping` entries for every expanded field before the planner validates them.

### Exponential Case Does Not Apply

Classic fragment explosion (where A spreads B and C, B spreads D and E, etc.) creating 2^N expansion does NOT apply here because each spread is a simple field merge, not a multiplicative expansion. A spreads fragment F which has 5 fields = 5 fields total, not 5 * 5.

However, a query with 100 inline fragments each containing 100 fields = 10,000 fields, which is a width bomb disguised as fragments.

## Impact

**Low** - The cycle detection is correct and working. Width amplification through fragments is theoretically possible but:
1. Limited by schema validation during planning
2. Linear, not exponential, amplification
3. Equivalent to just writing a width bomb directly (no amplification advantage)

The main concern is that fragments obscure the true query complexity, making it harder for observability tools to estimate query cost from the raw query text.

## Remediation

### Optional: Add Total Field Count Limit

As part of the broader query limits (see finding 00), track total fields across all fragment expansions:

```rust
if fields.len() > MAX_TOTAL_FIELDS {
    return Err(QueryError::parse("query exceeds maximum field count"));
}
```

This addresses both direct width bombs and fragment-amplified width.

## Test Gap

No tests for:
- Fragment cycle detection (positive case — verify error is returned)
- Non-cyclic fragment chains (A -> B -> C with multiple spreads)
- Fragment expansion with many inline fragments
- Duplicate field names from fragment overlaps
