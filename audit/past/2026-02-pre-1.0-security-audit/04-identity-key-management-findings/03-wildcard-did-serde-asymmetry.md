# Finding 03: Wildcard DID Cannot Survive Serde Roundtrip

**Severity**: LOW
**Category**: Type Invariant / Correctness
**Status**: Confirmed (by design, but undocumented)

## Summary

`Did::wildcard()` creates a `Did("*")` that bypasses the `did:key:` prefix validation. However, the `#[serde(try_from = "String")]` attribute routes deserialization through `Did::new()`, which rejects `"*"`. This means a wildcard DID serialized to JSON cannot be deserialized back — the roundtrip fails silently.

## Affected Files

- `crates/identity/src/did.rs:28-30` — serde attributes
- `crates/identity/src/did.rs:64-66` — `wildcard()` constructor
- `crates/identity/src/did.rs:100-106` — `TryFrom<String>` used by serde

## Details

```rust
#[derive(Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Did(String);

impl Did {
    pub fn wildcard() -> Self {
        Self("*".to_string()) // Bypasses new() validation
    }
}

impl TryFrom<String> for Did {
    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::new(s) // Rejects "*" — doesn't start with "did:key:"
    }
}
```

### Demonstration

```rust
let did = Did::wildcard();
let json = serde_json::to_string(&did).unwrap(); // "\"*\""
let parsed: Result<Did, _> = serde_json::from_str(&json); // Err!
```

### Security impact

Low. Wildcard DIDs are internal constructs used in ACP permission checks and are never serialized/deserialized over the wire. The asymmetry is a correctness issue, not a security one. However, if wildcard DIDs are ever stored in a serialized format (e.g., in a database or P2P message), they would be lost.

### Same issue in zanzibar::Did

The zanzibar crate's `Did` type has the identical asymmetry — `Did::wildcard()` creates `Did("*")` but deserialization goes through `Did::new()` which rejects `"*"`.

## Remediation

Option A — Make deserialization accept wildcards:
```rust
impl TryFrom<String> for Did {
    fn try_from(s: String) -> Result<Self, Self::Error> {
        if s == "*" {
            Ok(Self::wildcard())
        } else {
            Self::new(s)
        }
    }
}
```

Option B — Document the asymmetry and add a test confirming it (if wildcard should never be serialized):
```rust
#[test]
fn test_wildcard_did_is_not_serializable() {
    let did = Did::wildcard();
    let json = serde_json::to_string(&did).unwrap();
    let result: Result<Did, _> = serde_json::from_str(&json);
    assert!(result.is_err(), "wildcard DIDs should not roundtrip through serde");
}
```

## Test Gap

- No test for wildcard DID serde roundtrip behavior
- No test documenting the intentional asymmetry
