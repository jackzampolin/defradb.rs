# CID Determinism: Dual CBOR Codecs Both Verified Deterministic

**Severity:** Informational
**Category:** CID Determinism / Convergence
**Status:** Verified Clean

## Summary

CID computation uses two different CBOR libraries for two different purposes. Both produce deterministic output, and both have Go compatibility tests. The CBOR map key ordering, float encoding, and hash computation are all correct and produce the same CIDs as Go DefraDB.

## Affected Files

- `crates/document/src/encoding.rs` — `ciborium` for document CBOR (DocID computation)
- `crates/defra-core/src/block.rs` — `serde_ipld_dagcbor` v0.6.4 for block DAG-CBOR (Block CID computation)
- `crates/document/src/document.rs:388-417` — `to_cbor()` with canonical key ordering

## Details

### Two CBOR Paths, Two Libraries

| Purpose | Library | Codec | Used For |
|---------|---------|-------|----------|
| Document content encoding | `ciborium` | Canonical CBOR | DocID generation via SHA2-256 of CBOR bytes |
| Block serialization | `serde_ipld_dagcbor` | DAG-CBOR (0x71) | Block CID generation |

### Document CBOR (ciborium) — Deterministic

1. **Map key ordering**: Explicitly sorted using `canonical_cbor_key_order()` before building the CBOR map. Keys sorted by length first, then lexicographically — matches RFC 7049 Section 3.9 / RFC 8949.

2. **Float encoding**: `ciborium` uses "preferred serialization" (RFC 8949) which encodes as the shortest float representation. Verified by test: `float64(250.0)` → `f9 5bd0` (float16), matching Go's `ShortestFloat16`.

3. **Null omission**: `to_cbor()` skips nil values, matching Go's `toMap(true)` behavior.

4. **Go compatibility test**: `test_cbor_encoding_matches_go()` and `test_docid_generation_matches_go()` verify byte-for-byte CBOR and CID match with Go output.

### Block DAG-CBOR (serde_ipld_dagcbor) — Deterministic

1. **DAG-CBOR specification**: `serde_ipld_dagcbor` implements the IPLD DAG-CBOR codec which mandates deterministic encoding (sorted map keys, canonical integer/float encoding, CID link handling).

2. **Sorted heads and links**: `Block::new()` explicitly sorts heads by CID bytes and links by CID bytes before serialization, ensuring insertion order doesn't affect output.

3. **Go compatibility tests**: `crates/crypto/tests/go_compat_serialization.rs` verifies DAG-CBOR signature block byte equality with Go output.

### DocID Computation — Deterministic

```rust
// doc_id.rs:39-46
pub fn new_v0(data_cid: Cid) -> Self {
    let uuid = Uuid::new_v5(&SDN_NAMESPACE_V0, data_cid.to_string().as_bytes());
    // ...
}
```

- Namespace UUID: `SDN_NAMESPACE_V0 = "c94acbfa-dd53-40d0-97f3-29ce16c333fc"` — hardcoded constant
- Input: CID string representation (deterministic multibase encoding)
- UUID v5: SHA-1 hash of namespace + input (deterministic by specification)

### Potential Concern: `Cid::to_string()` Representation

DocID uses `data_cid.to_string().as_bytes()` as input to UUID v5. The CID string representation depends on the multibase encoding. CIDv1 uses Base32Lower by default. If the `cid` crate ever changed the default string encoding, all DocIDs would change. This is stable within a crate version but theoretically fragile across major crate updates.

### No Unicode Normalization

String values are stored as-is without Unicode normalization. The same visual text in NFC vs NFD form would produce different CBOR bytes and different CIDs. Go also does not normalize, so this is parity behavior. In practice, most applications submit strings through JSON which doesn't distinguish NFC/NFD.

## Conclusion

Both CBOR encoding paths are deterministic and Go-compatible. The CID computation chain (CBOR → SHA2-256 → multihash → CID) produces identical results across Rust and Go nodes for the same input. Test coverage confirms byte-level compatibility.
