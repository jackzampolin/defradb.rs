# Blockstore put() Stores Blocks Without CID Verification

**Severity:** Medium
**Category:** Data Integrity / Block Injection
**Status:** Confirmed

## Summary

The blockstore's `put()` and `put_many()` methods accept arbitrary `(CID, data)` pairs without verifying that the data's hash matches the claimed CID. Combined with `hash_on_read` being disabled by default (Finding 18), a block with fabricated content can be stored, cached, and served indefinitely without detection. The P2P PushLog path is the primary attack vector: the sender controls both the CID and the block data.

## Affected Files

- `crates/blockstore/src/lib.rs:213-237` (`put()` — no hash verification)
- `crates/blockstore/src/lib.rs:239-271` (`put_many()` — no hash verification)
- `crates/p2p/src/sync/manager/process/pushlog.rs:154` (PushLog calls `put(cid, &msg.block)`)
- `crates/p2p/src/sync/coordinator/event_handler/car.rs:68` (CAR calls `put_many()`)
- `crates/p2p/src/sync/manager/process/bitswap.rs:136` (Bitswap calls `put()`)

## Details

### No Verification on Storage

```rust
// lib.rs:213-237 — put() stores data under CID without verifying hash
async fn put(&self, cid: &Cid, data: &[u8]) -> Result<()> {
    if self.cache.lock().contains(cid) {
        return Ok(());  // Dedup check — but doesn't verify existing content
    }
    if self.has(cid).await? {
        return Ok(());  // Already stored — doesn't verify content matches
    }
    // Store data directly — NO verify_hash(cid, data) call
    let mut txn = self.store.new_txn(false).await?;
    bs_txn.put_block(cid, data).await?;
    txn.commit().await?;
    // Write-through cache — poisoned data also cached
    self.cache.lock().put(*cid, data.to_vec());
    Ok(())
}
```

### Cache Write-Through Amplifies the Issue

When a bad block is `put()`, it is simultaneously written to:
1. **Persistent storage** — survives restarts
2. **LRU cache** (1M entries) — served to all readers without verification

With `hash_on_read` disabled (default), subsequent `get()` calls return the cached bad data immediately.

### P2P PushLog Attack Vector

```rust
// pushlog.rs:154 — attacker controls both cid and msg.block
if let Err(e) = self.blockstore.put(cid, &msg.block).await {
```

The PushLog message contains `cid` (parsed from `msg.cid` bytes) and `block` (raw bytes), both supplied by the remote peer. An attacker can send any content as `msg.block` paired with any `cid`. The content is stored as-is without verification.

### CAR Response Path

```rust
// car.rs:68 — blocks decoded from CAR stored without CID verification
self.manager.blockstore().as_ref()
    .put_many(&block_refs).await
```

CAR responses contain `(CID, data)` pairs decoded from the CARv1 format. No verification that each block's data hashes to its claimed CID.

### Bitswap Path (Safe)

Bitswap is safe because `iroh-bitswap` verifies that received data hashes to the requested CID as part of the protocol. The requesting node knows what CID it wants and validates the response.

### Comparison: Go DefraDB

Go's blockstore also does not verify on `Put()` by default. However, Go's `hashOnRead` flag defaults to disabled as well. This is parity behavior, but both implementations share the same weakness.

## Remediation

1. **Verify CID on P2P ingestion paths** — add `verify_block_cid()` before `put()` in PushLog and CAR handlers:
   ```rust
   fn verify_block_cid(cid: &Cid, data: &[u8]) -> Result<()> {
       let computed = generate_cid_from_bytes(data)?;
       if computed != *cid {
           return Err(Error::HashMismatch { cid: cid.to_string() });
       }
       Ok(())
   }
   ```

2. **Alternatively, verify on put()** — defense-in-depth, but adds overhead to all writes including local block construction where CID was just computed.

## Test Gap

- No test calls `put()` with data that doesn't match the CID and verifies the behavior
- No integration test sends a PushLog message with mismatched CID/data
- No test calls `put_many()` with partially mismatched blocks
