# Block CID Not Verified Against Content Before Merge

**Severity:** Medium
**Category:** Data Integrity / Content Substitution
**Status:** Confirmed

## Summary

When the merge handler loads a block from the blockstore, it does NOT verify that the block's content hashes to the expected CID. The blockstore's `hash_on_read` feature is disabled by default. This means if a block was stored with content that doesn't match its CID (due to a bug, storage corruption, or an attack), the merge handler will process the wrong data under a trusted CID. This enables content substitution attacks via the P2P layer.

## Affected Files

- `crates/db/src/merge_handler/mod.rs:344-345` (decode without verification)
- `crates/db/src/merge_handler/composite.rs:117-135` (parent load without verification)
- `crates/db/src/merge_handler/composite.rs:235-259` (linked block load without verification)
- `crates/blockstore/src/lib.rs:181-211` (`hash_on_read` disabled by default)

## Details

### No Verification in Merge Path

```rust
// mod.rs:344-345 — block decoded from raw bytes, CID not checked
let block = Block::from_dag_cbor(block_data)
    .map_err(|e| MergeError::BlockDecode(e.to_string()))?;
```

```rust
// composite.rs:235-259 — linked block loaded and decoded, CID not checked
let linked_block_data = match self.blockstore.get(link_cid).await {
    Ok(Some(data)) => data,
    // ...
};
let linked_block = match Block::from_dag_cbor(&linked_block_data) {
    Ok(b) => b,
    // ...
};
// <-- No verify_hash(link_cid, &linked_block_data)
```

### Blockstore Default

```rust
// lib.rs:108 — hash_on_read disabled by default
rehash: AtomicBool::new(false),
```

When disabled, `get()` returns data without any hash verification:

```rust
// lib.rs:181-211
async fn get(&self, cid: &Cid) -> Result<Option<Vec<u8>>> {
    // Cache check (no hash verification)
    // Storage read
    // Hash verification ONLY IF rehash is enabled:
    if self.rehash.load(Ordering::Relaxed) {
        if let Some(ref data) = result {
            self.verify_hash(cid, data)?;
        }
    }
    Ok(result)
}
```

### Attack Vector: Content Substitution via GossipSub

1. Attacker creates a legitimate block B1 with CID C1 (hash matches)
2. Attacker sends B1 to the target via GossipSub PushLog
3. Target stores B1 in blockstore under CID C1
4. Before merge, attacker (or corrupt storage) replaces B1's content with B2 under the same key
5. Merge handler loads "C1" → gets B2's content → processes wrong CRDT delta
6. Document state corrupted with attacker-controlled values

Step 4 is the hard part. Direct storage corruption is unlikely, but:
- A bug in the blockstore's put/get path could cause key-data mismatches
- The LRU cache in the blockstore could return stale data after an eviction race
- In multi-process scenarios, shared storage could be modified externally

### Bitswap vs GossipSub

Bitswap (used for DAG fetching) verifies CIDs as part of the protocol — the requesting node checks that received data hashes to the requested CID. However, GossipSub PushLog messages carry block data inline and are stored directly. The PushLog path in the P2P layer stores the block data in the blockstore and then triggers a merge event with the CID. If the PushLog sender sends wrong data for a CID, it gets stored and merged without verification.

### Only SHA2-256 Verified

Even when `hash_on_read` is enabled, only SHA2-256 (code 0x12) is verified:

```rust
// lib.rs:150-166
let computed_digest: Vec<u8> = match code {
    0x12 => { /* SHA2-256 verified */ }
    _ => {
        // For unsupported hash algorithms, skip verification with a warning
        return Ok(());  // <-- VERIFICATION SKIPPED for non-SHA256
    }
};
```

## Remediation

1. **Enable `hash_on_read` by default** for P2P blockstores:
   ```rust
   pub fn new(store: Arc<S>, is_p2p: bool) -> Self {
       Self {
           rehash: AtomicBool::new(is_p2p),  // verify in P2P mode
           // ...
       }
   }
   ```

2. **Verify CID at PushLog ingestion** before storing in blockstore:
   ```rust
   fn verify_block_cid(cid: &Cid, data: &[u8]) -> bool {
       let computed_cid = generate_cid_from_bytes(data).ok();
       computed_cid.as_ref() == Some(cid)
   }
   ```

3. **Add merge-time verification** as a defense-in-depth layer in the merge handler itself.

## Test Gap

No test verifies that a content-substituted block is detected:
- Unit test: store block with mismatched CID, verify merge handler detects
- Integration test: P2P peer sends block with wrong CID, receiver rejects
- Unit test: `hash_on_read` enabled, mismatched block returns error
