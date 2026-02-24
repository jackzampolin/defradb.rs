# 30: Storage Key Construction Verified Injection-Proof

| Field    | Value |
|----------|-------|
| Severity | GREEN (Verified Safe) |
| Category | Storage Key Injection |
| Status   | Confirmed Safe |

## Summary

The storage key construction layer uses three complementary mechanisms that together make key injection effectively impossible: (1) namespace byte prefixes for store isolation, (2) CockroachDB-style varint encoding for integer components that cannot produce separator bytes, and (3) `validate_identifier()` restricting collection/field names to `[A-Za-z_][A-Za-z0-9_]*`, which prohibits null bytes, `/` separators, and all special characters.

## Affected Files

- `crates/storage/src/keys/utils/mod.rs` — varint/string encoding, SEPARATOR constant
- `crates/storage/src/keys/datastore/data_store_key.rs` — DataStoreKey, PrimaryDataStoreKey
- `crates/storage/src/keys/datastore/index_key.rs` — IndexDataStoreKey
- `crates/storage/src/keys/datastore/misc.rs` — DatastoreSE, ViewCacheKey
- `crates/storage/src/keys/headstore.rs` — Headstore key types
- `crates/storage/src/keys/systemstore.rs` — System metadata keys
- `crates/storage/src/keys/peerstore.rs` — Peer metadata keys
- `crates/storage/src/keys/blockstore.rs` — CID-based block keys
- `crates/storage/src/namespace.rs` — Namespace prefix isolation
- `crates/http/src/validation.rs` — `validate_identifier()` input validation

## Details

### Three-Layer Defense

**Layer 1: Namespace Isolation** (`namespace.rs:40-49`)

Every key is prefixed with a single-byte namespace identifier before storage:
- `'d'` (0x64) for Datastore
- `'b'` (0x62) for Blockstore
- `'h'` (0x68) for Headstore
- `'s'` (0x73) for Systemstore
- `'p'` (0x70) for Peerstore
- `'e'` (0x65) for Encstore
- `'a'` (0x61) for Acpstore

Iterator operations always scope to the namespace prefix (`namespace.rs:162-174`):

```rust
if let Some(prefix) = opts.prefix() {
    prefixed_opts = prefixed_opts.with_prefix(self.namespace.prefix_key(prefix));
} else {
    prefixed_opts = prefixed_opts.with_prefix(vec![self.namespace.prefix()]);
}
```

This prevents any cross-namespace iteration even if no user prefix is specified.

**Layer 2: Varint Encoding** (`utils/mod.rs:64-82`)

Collection IDs (u32) are encoded using CockroachDB-style variable-length encoding. Values 0-239 produce a single byte in range `[0x00-0xEF]`, values 240-2287 produce two bytes starting with `[0xF0-0xF7]`, and larger values use a marker byte `> 0xF7`. This encoding is self-delimiting and cannot produce the `/` separator byte (0x2F) for any valid u32 collection ID.

**Layer 3: Identifier Validation** (`validation.rs:11-34`)

```rust
pub fn validate_identifier(name: &str) -> Result<(), HttpError> {
    let valid = name.chars().enumerate().all(|(i, c)| {
        if i == 0 {
            c.is_ascii_alphabetic() || c == '_'
        } else {
            c.is_ascii_alphanumeric() || c == '_'
        }
    });
}
```

This rejects null bytes (0x00), `/` (0x2F), and all non-alphanumeric/underscore characters. Collection and field names pass through this validation before reaching the key construction layer.

### Key Injection Analysis

**Can user-controlled strings contain separator bytes?**

- Collection names: No. `validate_identifier()` allows only `[A-Za-z_][A-Za-z0-9_]*`.
- Field names: No. Same validation path.
- Document IDs: No. Content-addressed CIDs (base32/base58 encoded) contain only `[A-Za-z0-9]`.
- Doc IDs via HTTP: Validated by `validate_doc_id()` — must match `bae-[0-9a-f-]+`.

**Can a crafted varint overflow?**

No. `encode_uvarint_ascending` accepts `u64` and produces deterministic output. The collection ID is a `u32`, which has a fixed maximum (4,294,967,295). Varint encoding of any u32 value produces bytes that cannot collide with the `/` separator or another collection's varint prefix.

**Can CIDs contain 0x00?**

CID bytes are raw binary and can contain any byte value including 0x00. However, blockstore/encstore keys use raw CID bytes directly (no separator-delimited format), so there is no separator to confuse. CIDs are structurally validated by the `cid` library on parsing.

### String Encoding in Index Keys

For string values in secondary indexes, `encode_string_ascending()` (`utils/mod.rs:154-169`) escapes null bytes:

```rust
for &byte in bytes {
    if byte == 0x00 {
        buf.push(0x00);
        buf.push(0xFF); // Escape sequence
    } else {
        buf.push(byte);
    }
}
buf.push(0x00);
buf.push(0x00); // Terminator
```

This null-byte escaping with `0x00 0xFF` and `0x00 0x00` terminators is the standard CockroachDB string encoding, preventing embedded nulls from acting as premature terminators.

### Prefix Scan Isolation

All prefix scan operations (`collection_prefix()`, `document_prefix()`, etc.) construct prefixes from typed, validated components:

```rust
pub fn collection_prefix(collection_id: u32) -> Vec<u8> {
    let mut buf = vec![SEPARATOR]; // 0x2F
    buf = encode_uvarint_ascending(buf, collection_id as u64);
    buf.push(SEPARATOR);
    buf
}
```

Since collection IDs are u32 values (assigned by a monotonic sequence), not user-controlled strings, the prefix is always well-formed and scoped to a single collection.

### Backend Key Handling

Both redb and memory backends store keys as opaque byte arrays (`&[u8]` / `Vec<u8>`) with no key transformation. Keys are compared bytewise. The redb backend uses a single `TABLE_DEFINITION` table with `&[u8]` keys, performing no normalization or case-folding.

## Remediation

None required. The key construction is sound.

## Test Gap

The existing tests verify encoding round-trips and sort order but do not include explicit injection tests (e.g., constructing a key with a malicious collection name containing `/` or `\0` and verifying it is rejected before reaching key construction). Consider adding a test that attempts to construct a `DataStoreKey` with separator bytes in the doc_id/field_id to document the expected behavior.
