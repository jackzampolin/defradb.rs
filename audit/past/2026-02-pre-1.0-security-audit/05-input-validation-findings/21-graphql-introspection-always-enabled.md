# GraphQL Introspection Always Enabled

**Severity**: MEDIUM
**Category**: Information Disclosure — Schema Enumeration
**Status**: Confirmed

## Summary

GraphQL introspection (`__schema`, `__type` queries) is always enabled and requires no authentication when NAC is disabled. When NAC is enabled, it requires `DocumentRead` permission (same as any query). An attacker with read access can enumerate all collection names, field names, field types, directives, indexes, and the complete type system — including ACP-protected collection names.

## Affected Files

- `crates/query/src/query_parse/parser.rs:287-323` — `is_introspection_query()` detection
- `crates/query/src/runner/introspection/mod.rs` — full schema generation from collections
- `crates/http/src/handlers/graphql/query.rs:96-111` — GraphQL handler, no introspection guard

## Details

### Detection Logic

```rust
// parser.rs:287-304
fn is_introspection_query(doc: &Document<'_, String>) -> bool {
    for def in &doc.definitions {
        if let Definition::Operation(op) = def {
            let selections = match op { ... };
            for selection in selections {
                if let Selection::Field(field) = selection {
                    if field.name == "__schema" || field.name == "__type" {
                        return true;
                    }
                }
            }
        }
    }
    false
}
```

Introspection queries are **detected and routed** to a full schema generation pipeline — they are not blocked or filtered.

### Schema Generation

The introspection module (`crates/query/src/runner/introspection/mod.rs`) builds a complete async-graphql schema from all `CollectionVersion` objects. This includes:

- All collection names (including ACP-protected ones)
- All field names, types, and nullability
- All filter input types
- All mutation input types
- All index-related types
- All aggregate types
- Commit types and signature types

### Attack Scenario

Without NAC:
```graphql
{
  __schema {
    types {
      name
      fields {
        name
        type { name kind }
      }
    }
  }
}
```

This reveals the complete database schema to any unauthenticated user.

With NAC enabled, any user with `DocumentRead` permission can execute this same query and discover collections they may not have ACP access to.

### Security Assessment

**Risk is MEDIUM** because:
1. Schema enumeration is standard in GraphQL — many production systems allow it
2. Go DefraDB has the same behavior (introspection always enabled)
3. The information disclosed is the type system, not actual data
4. DefraDB is typically a local/trusted-network database, not a public API

**Risk increases if**:
- DefraDB is exposed to the internet (cloud deployment)
- Collection names contain sensitive information (e.g., `SecretProject_Users`)
- ACP policies are expected to hide collection existence, not just data access

## Remediation

Add an introspection toggle (default: enabled for Go compat, disable in production):

```rust
// In AppState or config
pub introspection_enabled: bool,

// In graphql handler
if is_introspection && !state.introspection_enabled {
    return Err(HttpError::BadRequest("introspection is disabled".into()));
}
```

## Test Gap

- No test verifies that introspection can be disabled
- No test verifies that ACP-protected collection names are hidden from introspection
- No test checks introspection behavior with NAC enabled vs disabled
