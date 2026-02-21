# Identifiers Accept Unbounded Length

**Severity**: LOW
**Category**: Input Validation — Resource Limits
**Status**: Confirmed

## Summary

`validate_identifier()` enforces character restrictions (`[A-Za-z_][A-Za-z0-9_]*`) but has no maximum length limit. A 1,000,000-character collection name passes validation and flows into storage keys, error messages, and GraphQL type generation.

## Affected Files

- `crates/http/src/validation.rs:20-43` — `validate_identifier()`
- `crates/http/src/validation.rs:161-165` — test explicitly accepts 1000-char names

## Details

```rust
// validation.rs — test confirms no length limit
#[test]
fn test_validate_identifier_valid() {
    // ...
    let long_name = "a".repeat(1000);
    assert!(validate_identifier(&long_name).is_ok());
}
```

### Impact Areas

1. **Storage keys**: Collection names are encoded into storage keys — extremely long names increase key size
2. **Error messages**: Collection names are echoed in errors — long names produce large error responses
3. **GraphQL introspection**: Collection names become GraphQL type names — very long names generate large schemas
4. **Memory**: Processing many long-named collections consumes proportional memory

### Security Assessment

**Risk is LOW** because:
1. Creating collections requires `CollectionPatch` permission
2. The HTTP body size limits (if/when added) would bound the SDL size
3. Extremely long names would fail naturally at storage layer limits
4. This is a DoS vector, not a data integrity or access control issue

## Remediation

Add `MAX_IDENTIFIER_LENGTH = 256` to `validate_identifier()`:

```rust
const MAX_IDENTIFIER_LENGTH: usize = 256;

pub fn validate_identifier(name: &str) -> Result<(), HttpError> {
    if name.len() > MAX_IDENTIFIER_LENGTH {
        return Err(HttpError::BadRequest(format!(
            "identifier too long (max {} characters)", MAX_IDENTIFIER_LENGTH
        )));
    }
    // ... existing character validation
}
```

## Test Gap

No test verifies that excessively long identifiers are rejected.
