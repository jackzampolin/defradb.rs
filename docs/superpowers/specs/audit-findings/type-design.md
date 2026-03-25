# Type Design Audit Findings

## Summary
- Total findings: 14
- Critical: 0 | High: 4 | Medium: 7 | Low: 3

## Findings

### Finding 1
- **severity:** high
- **category:** anti-pattern
- **crate:** defra-core
- **file:** crates/defra-core/src/signing.rs
- **line:** 44
- **pattern:** raw-primitive-id
- **description:** `SigningConfig.key_type` is a raw `String` that is matched against string literals ("ed25519", "secp256k1", "secp256r1", "bls") in `signature_type()` at line 84. Any typo in a string literal silently falls through to the error branch at runtime. There is already a `SignatureType` enum in `block.rs:693` -- the key type should be an enum, not a stringly-typed field. This field crosses crate boundaries (identity, FFI, HTTP) making confusion likely.
- **training_ref:** rust-patterns-book ch3 "Newtype: Zero-Cost Type Safety"
- **suggested_fix:** Replace `pub key_type: String` with a `SigningKeyType` enum:
```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SigningKeyType {
    Ed25519,
    Secp256k1,
    Secp256r1,
    Bls,
}
```
Then `signature_type()` becomes an infallible `From` conversion instead of a `Result`.

### Finding 2
- **severity:** high
- **category:** anti-pattern
- **crate:** zanzibar
- **file:** crates/zanzibar/src/engine/mod.rs
- **line:** 138-157
- **pattern:** raw-primitive-id
- **description:** `PermissionEngine::check()` takes five `&str` parameters: `policy_id`, `resource`, `object_id`, `relation`, `subject`. All five are bare `&str` at the call site. Swapping `resource` and `object_id`, or `relation` and `object_id`, compiles fine but produces wrong permission checks -- a silent security bug. The `PermissionCheckRequest` struct at line 17 has the same problem with four `&str` fields. This is the central permission check for the entire ACP system.
- **training_ref:** rust-patterns-book ch3 "Newtype: Zero-Cost Type Safety" (the `create_user(name, email, age, id)` example)
- **suggested_fix:** Introduce newtypes for the three distinct string-domain concepts:
```rust
pub struct PolicyId<'a>(&'a str);
pub struct ResourceName<'a>(&'a str);
pub struct RelationName<'a>(&'a str);
// object_id stays &str -- it is intentionally opaque
```
Then `check()` becomes `fn check(&self, policy: PolicyId, resource: ResourceName, object_id: &str, relation: RelationName, subject: &Did)`. Callers cannot accidentally swap resource and relation.

### Finding 3
- **severity:** high
- **category:** anti-pattern
- **crate:** zanzibar
- **file:** crates/zanzibar/src/types/relationship.rs
- **line:** 7-13
- **pattern:** parse-dont-validate
- **description:** `Relationship` has four public `String` fields (`resource`, `object_id`, `relation`, `subject`) with no validation at construction time. The `new()` constructor at line 16 accepts any strings. Compare with the ACP crate's `RelationTuple` at `crates/acp/src/relation.rs:32` which validates path components in `try_new()` to prevent path traversal in storage keys. `Relationship` constructs storage keys at line 39 via `format!("/rel/{}/{}/{}/{}", ...)` with no validation, meaning path traversal is possible. The ACP crate added validation for its own `RelationTuple` but the underlying zanzibar `Relationship` remains unvalidated.
- **training_ref:** type-driven-correctness-book ch7 "Validated Boundaries -- Parse, Don't Validate"
- **suggested_fix:** Add `try_new()` validation to `Relationship` matching what `RelationTuple` does, and make the fields private with accessor methods. Alternatively, make `Relationship::new()` return `Result` and validate path components.

### Finding 4
- **severity:** high
- **category:** anti-pattern
- **crate:** zanzibar
- **file:** crates/zanzibar/src/did.rs
- **line:** 36
- **pattern:** parse-dont-validate
- **description:** `Did::new_unchecked()` is `pub` (not `pub(crate)`) in the zanzibar crate. Any downstream crate can bypass DID validation by calling `Did::new_unchecked()` with an arbitrary string. Compare with `crates/identity/src/did.rs:55` where the same function is correctly scoped as `pub(crate)`. The zanzibar version breaks the "private constructor = unforgeable" principle from the capability token pattern.
- **training_ref:** type-driven-correctness-book ch4 "Zero-Sized Types as Proof Tokens" (private constructor principle)
- **suggested_fix:** Change `pub fn new_unchecked` to `pub(crate) fn new_unchecked` in `crates/zanzibar/src/did.rs:36` to match the identity crate's approach.

### Finding 5
- **severity:** medium
- **category:** improvement
- **crate:** defra-core
- **file:** crates/defra-core/src/block.rs
- **line:** 324-392
- **pattern:** raw-primitive-id
- **description:** The CRDT delta payload structs (`LwwDeltaPayload`, `CounterDeltaPayload`, `CompositeDeltaPayload`) all use `pub doc_id: Vec<u8>`, `pub schema_version_id: String`, and `pub priority: u64` as raw public fields. These are wire-format types (DAG-CBOR serialized), so newtypes add serde complexity, but the `priority` field is particularly confusing -- it is a raw `u64` while `defra-core/src/types.rs:114` defines a `Priority(pub u64)` newtype that is never used in these structs. The `status: u8` field at line 391 is a magic number (1 = active, 2 = deleted) that should be an enum.
- **training_ref:** rust-patterns-book ch3 "Newtype: Zero-Cost Type Safety"
- **suggested_fix:** Use the existing `Priority` newtype for `priority` fields. Replace `status: u8` with:
```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum DocumentStatus {
    Active = 1,
    Deleted = 2,
}
```

### Finding 6
- **severity:** medium
- **category:** improvement
- **crate:** acp
- **file:** crates/acp/src/nac/node_acp/lifecycle.rs
- **line:** 18-199
- **pattern:** runtime-state-check
- **description:** The NAC lifecycle (`enable`, `disable`, `re_enable`, `purge`) uses runtime `match` checks on `NacStatus` at the top of each method to reject invalid transitions (e.g., `enable()` rejects `NacStatus::Enabled`). The valid state machine is: `NotConfigured -> Enabled -> DisabledTemporarily -> Enabled` and `* -> NotConfigured` (via purge). This is a textbook case for type-state encoding per ch5, where `NodeACP<NotConfigured>` has `enable()`, `NodeACP<Enabled>` has `disable()`, and `NodeACP<Disabled>` has `re_enable()`. However, because the status is stored in `RwLock<NacStatus>` for async access, a full type-state encoding would require significant refactoring of the async trait dispatch. The runtime checks are reasonable given the async constraint but could still benefit from a state-machine wrapper.
- **training_ref:** type-driven-correctness-book ch5 "Protocol State Machines -- Type-State for Real Hardware"
- **suggested_fix:** This is a pragmatic trade-off. Consider adding `#[doc = "State machine: NotConfigured -> Enabled <-> DisabledTemporarily, * -> NotConfigured (purge)"]` to `NacStatus` and keep runtime checks. If the async constraint is ever relaxed, convert to type-state.

### Finding 7
- **severity:** medium
- **category:** improvement
- **crate:** defra-core
- **file:** crates/defra-core/src/types.rs
- **line:** 10-16
- **pattern:** parse-dont-validate
- **description:** `DocId::new()` accepts any string without validation. The format is documented as `"bae-<base32-encoded-bytes>"` but `DocId::new("hello")` succeeds. Meanwhile, `crates/document/src/doc_id.rs` has a `DocID` type with full parsing/validation. The `defra-core::DocId` is used throughout the core API (`Document.id`, `DocumentUpdate.id`) as a pass-through wrapper providing no guarantees. Consumers cannot trust that a `DocId` is actually valid without re-parsing.
- **training_ref:** type-driven-correctness-book ch7 "Parse, Don't Validate"
- **suggested_fix:** Either (a) add validation to `DocId::new()` matching the `bae-` prefix format, or (b) consolidate on the `document::DocID` type which already validates. If backward compatibility requires accepting arbitrary strings, add `DocId::parse(s: &str) -> Result<Self>` and deprecate `new()`.

### Finding 8
- **severity:** medium
- **category:** improvement
- **crate:** schema
- **file:** crates/schema/src/collection.rs
- **line:** 436-488
- **pattern:** missing-must-use
- **description:** `CollectionBuilder` is a builder type where calling `.field()` or `.scalar()` chains mutations, but forgetting to call `.build()` silently drops the builder and all accumulated state. The builder is not `#[must_use]`, so `CollectionBuilder::new("users", "1").scalar("1", "name", FieldKind::string());` compiles without warning -- the builder is created, a field is added, and the whole thing is silently discarded because `.build()` was never called.
- **training_ref:** type-driven-correctness-book ch3 "Single-Use Types" (builder pattern)
- **suggested_fix:** Add `#[must_use = "CollectionBuilder does nothing until .build() is called"]` to the `CollectionBuilder` struct.

### Finding 9
- **severity:** medium
- **category:** improvement
- **crate:** defra-core
- **file:** crates/defra-core/src/error.rs
- **line:** 10
- **pattern:** missing-non-exhaustive
- **description:** `defra_core::Error` is a public enum with 14 variants that any downstream crate can exhaustively match on. Adding a new error variant (e.g., `Encryption`, `Identity`) would be a breaking change for any external consumer that has a `match` without a wildcard arm. The same applies to `schema::SchemaError` (crates/schema/src/error.rs:10), `acp::Error` (crates/acp/src/error.rs:10), `zanzibar::Error` (crates/zanzibar/src/error.rs:6), `document::Error` (crates/document/src/error.rs:10), and `identity::Error` (crates/identity/src/error.rs:11). Only `NodePermission` currently has `#[non_exhaustive]`.
- **training_ref:** rust-patterns-book ch3 "The Newtype and Type-State Patterns" (implied by `#[non_exhaustive]` in validated boundary discussion)
- **suggested_fix:** Add `#[non_exhaustive]` to all public error enums across crates: `defra_core::Error`, `schema::SchemaError`, `acp::Error`, `zanzibar::Error`, `document::Error`, `identity::Error`. This is especially important for `defra_core::Error` since it is re-exported as the foundational error type.

### Finding 10
- **severity:** medium
- **category:** improvement
- **crate:** defra-core
- **file:** crates/defra-core/src/block.rs
- **line:** 220-248
- **pattern:** missing-non-exhaustive
- **description:** `CrdtDelta` is a public enum with 7 variants. New CRDT types (e.g., a Set CRDT or RGA for text) would require adding variants. Same applies to `SignatureType` at line 693 (4 variants, new algorithms will be added). Both are used across crate boundaries and should be `#[non_exhaustive]`.
- **training_ref:** rust-patterns-book ch3 "The Newtype and Type-State Patterns"
- **suggested_fix:** Add `#[non_exhaustive]` to `CrdtDelta` and `SignatureType`.

### Finding 11
- **severity:** medium
- **category:** improvement
- **crate:** defra-core
- **file:** crates/defra-core/src/encryption.rs
- **line:** 12-16
- **pattern:** raw-primitive-id
- **description:** `EncryptionConfig.encryption_key` is a raw `Vec<u8>` with no type distinction from other `Vec<u8>` fields like `doc_id` or `data`. At the call site in `derive_key()` (line 29), the key bytes, doc_id bytes, and field name bytes are concatenated -- swapping arguments would produce a wrong derived key silently. More critically, `encryption_key` is `Clone`-able and stored in a `HashMap` at line 65, meaning key material is freely duplicated in memory with no zeroization on drop.
- **training_ref:** type-driven-correctness-book ch3 "Single-Use Types -- Cryptographic Guarantees via Ownership"
- **suggested_fix:** Wrap the encryption key in a newtype that implements `Drop` with zeroization:
```rust
pub struct EncryptionKey(Vec<u8>);
impl Drop for EncryptionKey {
    fn drop(&mut self) { self.0.iter_mut().for_each(|b| *b = 0); }
}
```
Consider using the `zeroize` crate for compiler-safe zeroization.

### Finding 12
- **severity:** low
- **category:** improvement
- **crate:** schema
- **file:** crates/schema/src/collection.rs
- **line:** 38-42
- **pattern:** raw-primitive-id
- **description:** `CollectionVersion.version_id` and `CollectionVersion.collection_id` are raw `String` fields that serve as critical identifiers used for storage key construction, schema lookup, and version resolution across many crates. These are distinct domain concepts (content-addressed version hash vs stable collection identity) but are both `String` at the type level, making them interchangeable at any call site. The `defra-core` crate defines `CollectionId(u32)` and `SchemaVersion(u32)` newtypes but they are for numeric IDs, not these string identifiers.
- **training_ref:** rust-patterns-book ch3 "Newtype: Zero-Cost Type Safety"
- **suggested_fix:** Introduce `VersionId(String)` and `CollectionIdStr(String)` newtypes. This is low severity because these fields are mostly read from deserialized JSON and passed through, reducing the swap risk at call sites.

### Finding 13
- **severity:** low
- **category:** improvement
- **crate:** defra-core
- **file:** crates/defra-core/src/collection.rs
- **line:** 16
- **pattern:** raw-primitive-id
- **description:** `Collection.version` is a raw `u32` while the crate defines `SchemaVersion(u32)` at `types.rs:48`. The `Collection` struct uses `CollectionId` for the `id` field but uses a raw `u32` for `version`, inconsistently. The `Collection::new()` constructor at line 20 takes `version: u32` where it should take `SchemaVersion`.
- **training_ref:** rust-patterns-book ch3 "Newtype: Zero-Cost Type Safety"
- **suggested_fix:** Change `pub version: u32` to `pub version: SchemaVersion` and update `Collection::new()` to accept `SchemaVersion`.

### Finding 14
- **severity:** low
- **category:** improvement
- **crate:** identity, zanzibar
- **file:** crates/identity/src/did.rs and crates/zanzibar/src/did.rs
- **line:** identity:30, zanzibar:19
- **pattern:** duplicate-newtype
- **description:** There are two separate `Did` newtype implementations with identical structure and nearly identical validation logic: `identity::Did` and `zanzibar::Did`. The zanzibar crate has its own `Did` presumably to avoid a dependency on the identity crate. The ACP crate bridges between them with `to_zdid()`/`from_zdid()` conversion functions. This duplication means validation rules could diverge (and already have -- see Finding 4 where `new_unchecked` visibility differs). Having two `Did` types also means every cross-crate call site needs conversion.
- **training_ref:** type-driven-correctness-book ch7 "Validated Boundaries" (single source of truth for validation)
- **suggested_fix:** Extract `Did` into a shared micro-crate (e.g., `defra-did`) that both `identity` and `zanzibar` depend on. This eliminates the conversion layer and ensures validation is consistent.
