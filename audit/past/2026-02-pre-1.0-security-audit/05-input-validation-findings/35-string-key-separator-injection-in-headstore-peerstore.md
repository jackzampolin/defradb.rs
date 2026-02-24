# 35: String-Based Keys Use `/` Separator Without Escaping (Headstore, Peerstore, Systemstore)

| Field    | Value |
|----------|-------|
| Severity | LOW |
| Category | Storage Key Injection |
| Status   | Confirmed (Mitigated by Input Validation) |

## Summary

Several key types in the headstore, peerstore, and systemstore use `format!()` string concatenation with `/` separators and **no escaping** of user-controlled components. If a user-controlled string (collection name, peer ID, document ID) contained a `/` character, it could create a key that aliases another key in the same namespace. This is currently mitigated by upstream input validation (`validate_identifier()`, CID format constraints, peer ID format constraints), but the key construction layer itself does not enforce safety.

## Affected Files

- `crates/storage/src/keys/headstore.rs` — All headstore key types use `format!()` with `/`
- `crates/storage/src/keys/peerstore.rs` — All peerstore key types use `format!()` with `/`
- `crates/storage/src/keys/systemstore.rs` — Collection, field, and P2P key types use `format!()` with `/`
- `crates/storage/src/keys/datastore/misc.rs` — DatastoreSE uses `format!()` with `/`
- `crates/storage/src/keys/datastore/data_store_key.rs` — PrimaryDataStoreKey uses `format!()` with `/`

## Details

### String-Concatenated Keys (No Escaping)

Unlike `DataStoreKey` which uses binary varint encoding, many key types use plain string format:

```rust
// HeadstoreFieldDefinition
impl Key for HeadstoreFieldDefinition {
    fn bytes(&self) -> Vec<u8> {
        format!("/f/{}/{}/{}", self.collection_name, self.field_name, self.cid).into_bytes()
    }
}

// PeerstoreSERetry
impl Key for PeerstoreSERetry {
    fn bytes(&self) -> Vec<u8> {
        format!("/se-retry/{}/{}/{}", self.peer_id, self.collection_id, self.doc_id).into_bytes()
    }
}

// CollectionVersionKey
impl Key for CollectionVersionKey {
    fn bytes(&self) -> Vec<u8> {
        format!("/collection/version/{}/{}", self.collection_id, self.version_id).into_bytes()
    }
}

// PrimaryDataStoreKey
impl Key for PrimaryDataStoreKey {
    fn bytes(&self) -> Vec<u8> {
        let s = format!("/{}/pk/{}", self.collection_id, self.doc_id);
        s.into_bytes()
    }
}
```

If `self.collection_name` were `"foo/bar"`, the resulting key `/f/foo/bar/field/cid` would be ambiguous — it could be collection `foo` with field `bar/field` or collection `foo/bar` with field `field`.

### Why This Is Currently Safe

1. **Collection names** pass through `validate_identifier()` which allows only `[A-Za-z_][A-Za-z0-9_]*` — no `/` allowed
2. **Document IDs** are content-addressed CIDs with base32/base58 encoding — no `/` in the encoding alphabet
3. **Peer IDs** are libp2p peer IDs (base58-encoded Ed25519 public keys) — no `/` possible
4. **Field names** come from SDL schema parsing which enforces GraphQL identifier rules — no `/` allowed
5. **Collection IDs** in `PrimaryDataStoreKey` are u32 values formatted as decimal — no `/` possible

### Contrast with DataStoreKey

`DataStoreKey` uses binary encoding with varint and `SEPARATOR` bytes:

```rust
impl Key for DataStoreKey {
    fn bytes(&self) -> Vec<u8> {
        let mut buf = vec![SEPARATOR]; // 0x2F = '/'
        buf = encode_uvarint_ascending(buf, self.collection_id as u64);
        buf.push(SEPARATOR);
        buf.push(self.instance_type.as_byte());
        buf.push(SEPARATOR);
        buf.extend_from_slice(self.doc_id.as_bytes());
        buf.push(SEPARATOR);
        buf.extend_from_slice(self.field_id.as_bytes());
        buf
    }
}
```

This is structurally safer because the varint-encoded collection ID cannot produce `0x2F` bytes, and the doc_id/field_id are user-controlled but validated upstream.

### Risk Assessment

The risk is LOW because all input paths that feed into these keys are validated. However, the defense is not defense-in-depth — the key construction layer trusts its inputs completely. If a new code path were added that bypassed input validation (e.g., P2P peer ID from a malicious peer, or a collection name from a replicated schema), the separator confusion would become exploitable.

## Remediation

1. **Assert no separator in components**: Add `debug_assert!(!component.contains('/'))` in key constructors for defense-in-depth
2. **Consider binary encoding**: For security-critical keys, consider migrating from string format to binary encoding with length prefixes or escaped separators (as `DataStoreKey` and `IndexDataStoreKey` already do)

## Test Gap

No test attempts to construct a key with a `/` in a string component and verifies rejection. The existing tests use well-formed inputs only.
