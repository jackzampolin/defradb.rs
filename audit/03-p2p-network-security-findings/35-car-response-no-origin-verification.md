# Finding: CAR Response Blocks Stored Without Origin Verification

**Stream**: 03 - P2P Network Security
**Session**: 4 — Replication Protocol Security
**Severity**: MEDIUM
**Category**: Data Integrity / Storage Abuse

## Summary

When a CAR response arrives, the handler decodes it and stores ALL blocks into the blockstore via `put_many()` without verifying that the blocks correspond to the requested root CID or that their content hashes match their claimed CIDs. An attacker responding to a CAR request can inject arbitrary blocks into the victim's blockstore.

## Affected Files

| File | Lines | Issue |
|------|-------|-------|
| `crates/p2p/src/sync/coordinator/event_handler/car.rs` | 46-80 | `handle_car_fetch_response` — stores all decoded blocks without validation |
| `crates/p2p/src/two_stream/handler/car.rs` | 41-61 | `handle_car_response_stream` — only extracts first root CID from header |

## Details

### The Handler

```rust
// car.rs event handler:46-80
pub(crate) async fn handle_car_fetch_response(
    &self, peer_id: PeerId, root_cid: Cid, car_data: Vec<u8>,
) -> Result<()> {
    let (_roots, blocks) = decode_car(&car_data)?;

    // No verification that:
    // 1. blocks' CIDs are content-addressed (hash matches data)
    // 2. blocks form a DAG rooted at root_cid
    // 3. blocks are related to any pending request
    // 4. total block count or size is reasonable

    let block_refs: Vec<(&Cid, &[u8])> =
        blocks.iter().map(|(c, d)| (c, d.as_slice())).collect();
    self.manager.blockstore().as_ref()
        .put_many(&block_refs).await
        .map_err(|e| crate::error::Error::BlockstoreError(e.to_string()))?;

    Ok(())
}
```

### Missing Validations

1. **No CID content verification**: The CID in each CAR block section is taken at face value. If block data doesn't hash to the claimed CID, the mismatch is stored anyway. (Note: Bitswap does verify CIDs because iroh-bitswap checks content hashes.)

2. **No root CID membership check**: The `root_cid` from the event is extracted from the CAR header's first root, but there's no check that any stored block is actually reachable from this root.

3. **No request correlation**: No check that we actually requested this root_cid. An attacker could send unsolicited CAR responses on the response protocol.

4. **No size limit on decoded blocks**: The CAR data itself is unbounded (Finding 00), and `decode_car` produces a `Vec<(Cid, Vec<u8>)>` of arbitrary size.

### Mitigating Factor: CAR Data Is Read Without Size Limit

The CAR response uses the same `read_stream()` function as other CAR protocol messages (`car.rs:15`), which has no `take()` limit. So the `car_data` parameter to `handle_car_fetch_response` can be arbitrarily large. The `decode_car` function does validate varint-prefixed section lengths against remaining data (preventing buffer overread), but doesn't limit total decoded size.

### collect_dag_blocks Recursive Depth (Outbound Path)

When serving a CAR request, `collect_dag_blocks` (car.rs:72-80) uses actual recursion via `collect_recursive` with `Box::pin`. The `visited` HashSet prevents cycles, but a linear DAG of depth N causes N levels of heap-allocated future recursion. This is an outbound path (we choose what to serve), so an attacker can only trigger it by requesting a CID for a deep DAG already in our blockstore — lower risk.

## Remediation

1. Verify CID content-addressing: for each `(cid, data)` in decoded blocks, check that `Cid::new_v1(cid.codec(), hash(data)) == cid`
2. Limit maximum blocks per CAR response (e.g., 10,000)
3. Verify that at least one decoded block has the requested root_cid
4. Track outstanding CAR requests and reject unsolicited responses

## Test Gap

No test sends a CAR response with blocks that don't match their CIDs or that are unrelated to the requested root.
