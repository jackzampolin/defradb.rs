# Finding: SDL Schema Endpoint Accepts Unbounded Input

**Stream**: 05 - Input Validation
**Severity**: MEDIUM
**Category**: Denial of Service
**Status**: CONFIRMED

## Summary

The `POST /api/v0/schema` endpoint accepts SDL text as a raw `String` body with no size limit. The `graphql_parser::parse_schema()` function (v0.4) has no built-in type count or complexity limits. An attacker can submit an SDL with thousands of type definitions, causing excessive memory allocation and CPU usage during parsing, validation, and collection building.

## Affected Files

| File | Function | Issue |
|------|----------|-------|
| `crates/http/src/handlers/schema.rs:24-39` | `add_schema()` | `body: String` extractor, no size limit |
| `crates/query/src/sdl_parse/parser.rs:122-168` | `parse_with_warnings()` | No limit on input size or type count |
| `crates/query/src/sdl_parse/parser.rs:137` | `graphql_parser::parse_schema()` | External crate, no limits |
| `crates/query/src/sdl_parse/parser.rs:141-151` | Type definition loop | Iterates all type defs without count limit |

## Details

### Unbounded String Body

The schema handler uses Axum's `String` extractor, which has **no default body size limit** (unlike `Json` which defaults to 2MB):

```rust
// schema.rs:24-28
pub async fn add_schema(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    body: String,  // No size limit — will read entire body into memory
) -> Result<Json<Vec<CollectionVersion>>, HttpError> {
```

### No Type Count Limit

The SDL parser processes every type definition in the document:

```rust
// sdl_parse/parser.rs:141-151
for def in &doc.definitions {
    match def {
        Definition::TypeDefinition(TypeDefinition::Object(obj)) => {
            self.parse_object_type(obj)?;  // No limit on number of types
        }
        Definition::TypeDefinition(TypeDefinition::Interface(iface)) => {
            self.parse_interface_type(iface)?;  // No limit on number of interfaces
        }
        _ => {}
    }
}
```

Each type definition results in:
- A `ParsedTypeDef` struct with field vectors
- HashMap entries in `type_defs` and `definition_order`
- Validation passes over all types
- Collection building for each type

### No Field Count Per Type Limit

Each type definition can have an arbitrary number of fields. The parser processes every field:

```rust
// sdl_parse/parser.rs:183-189
for field in &obj.fields {
    if field.name == EMPTY_TYPE_PLACEHOLDER { continue; }
    let parsed_field = self.parse_field(field)?;
    fields.push(parsed_field);
}
```

### Attack Payload

A malicious SDL with 10,000 types, each with 100 fields:

```graphql
type T0 { f0: String f1: String ... f99: String }
type T1 { f0: String f1: String ... f99: String }
...
type T9999 { f0: String f1: String ... f99: String }
```

This is approximately 4MB of SDL text and would create:
- 10,000 `ParsedTypeDef` entries
- 1,000,000 `ParsedField` entries
- 10,000 `CollectionVersion` objects
- Validation passes over 10,000 types (O(n^2) for relation resolution)

### graphql_parser Crate Limits

The `graphql_parser` crate (v0.4) has no:
- Input size limit
- Type count limit
- Field count limit
- Directive count limit
- Recursion depth limit for type references

It will parse any syntactically valid SDL regardless of size.

### Preprocessing Amplification

The `preprocess_empty_types()` function in `helpers.rs` uses regex replacement on the entire SDL string, which could be a secondary CPU cost for very large inputs.

## Impact

- **OOM**: A multi-gigabyte POST to `/api/v0/schema` causes OOM kill (String extractor has no limit)
- **CPU exhaustion**: 10,000+ types with cross-references trigger O(n^2) validation passes
- **Schema storage bloat**: If the schema is accepted, 10,000 collections are created in the system store
- **No authentication required**: Schema endpoint requires `CollectionPatch` NAC permission IF NAC is enabled, but NAC is off by default

## Remediation

### Immediate: Add Size Limit

Either use `DefaultBodyLimit` middleware on the schema route, or add an explicit check:

```rust
pub async fn add_schema(
    State(state): State<AppState>,
    identity: ExtractIdentity,
    body: String,
) -> Result<Json<Vec<CollectionVersion>>, HttpError> {
    if body.len() > 1_048_576 {  // 1MB
        return Err(HttpError::BadRequest("schema too large (max 1MB)".into()));
    }
    // ...
}
```

### Add Type Count Limit

In `parse_with_warnings()`:

```rust
const MAX_TYPE_DEFINITIONS: usize = 500;

if doc.definitions.len() > MAX_TYPE_DEFINITIONS {
    return Err(QueryError::parse(format!(
        "schema has {} type definitions, maximum is {}",
        doc.definitions.len(), MAX_TYPE_DEFINITIONS
    )));
}
```

### Add Field Count Per Type Limit

```rust
const MAX_FIELDS_PER_TYPE: usize = 200;

if obj.fields.len() > MAX_FIELDS_PER_TYPE {
    return Err(QueryError::parse(format!(
        "type '{}' has {} fields, maximum is {}",
        name, obj.fields.len(), MAX_FIELDS_PER_TYPE
    )));
}
```

## Test Gap

No tests for:
- Schema endpoint with large SDL input (>1MB)
- SDL with many type definitions (>100)
- SDL with many fields per type (>50)
- Body size rejection on schema endpoint
