# Block Construction: CID Correctly Computed From Serialized Bytes

**Severity:** Informational
**Category:** Data Integrity / CID Computation
**Status:** Verified Clean

## Summary

Block CID generation always computes the hash from the serialized DAG-CBOR bytes, not from in-memory structures. This eliminates the risk of CID/content mismatches caused by serialization non-determinism or struct-vs-bytes divergence. The computation is atomic — `to_dag_cbor()` serializes, then `generate_cid_from_bytes()` hashes the result.

## Affected Files

- `crates/defra-core/src/block.rs:123-127` (`Block::generate_cid()`)
- `crates/defra-core/src/block.rs:716-728` (`generate_cid_from_bytes()`)
- `crates/db/src/block_builder/compute.rs` (block construction pipeline)

## Details

### CID From Bytes, Not Structs

```rust
// block.rs:123-127
pub fn generate_cid(&self) -> Result<Cid> {
    let bytes = self.to_dag_cbor()?;  // Step 1: serialize to bytes
    generate_cid_from_bytes(&bytes)   // Step 2: hash the bytes
}

// block.rs:716-728
pub fn generate_cid_from_bytes(bytes: &[u8]) -> Result<Cid> {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    let mh = MultihashGeneric::<64>::wrap(SHA2_256_CODE, &digest)?;
    Ok(Cid::new_v1(DAG_CBOR_CODEC, mh))
}
```

### No Race Condition Possible

The CID is computed from the serialized bytes immediately after serialization, within the same function call. There is no window for the block's in-memory state to be modified between serialization and hashing. The serialization is deterministic (DAG-CBOR), so the same block always produces the same bytes and the same CID.

### Block Builder Pipeline

The block builder (`compute_document_blocks`) constructs blocks, serializes them, and stores them with their computed CIDs:

1. Build `Block` struct with sorted heads/links
2. Call `block.to_dag_cbor()` → serialized bytes
3. Call `generate_cid_from_bytes(&bytes)` → CID
4. Store `(CID, bytes)` in blockstore

The same serialized bytes used for CID computation are the bytes stored. There is no double-serialization that could produce different bytes.

### Encryption and Signature Blocks — Same Pattern

```rust
// Encryption block (block.rs:622-626)
pub fn generate_cid(&self) -> Result<Cid> {
    let bytes = self.to_dag_cbor()?;
    generate_cid_from_bytes(&bytes)
}

// Signature block (block.rs:662-666)
pub fn generate_cid(&self) -> Result<Cid> {
    let bytes = self.to_dag_cbor()?;
    generate_cid_from_bytes(&bytes)
}
```

All three block types (data, encryption, signature) use the same pattern.

## Conclusion

Block construction correctly computes CIDs from the same bytes that are stored. No struct-vs-bytes divergence, no double-serialization, no race conditions. The CID computation chain is sound.
