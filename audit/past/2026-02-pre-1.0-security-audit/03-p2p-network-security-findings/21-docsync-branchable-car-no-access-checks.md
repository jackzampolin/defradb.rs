# Finding 21: DocSync, BranchableSync, and CAR Fetch Have No Access Checks

**Severity: HIGH**
**Category: Authorization Bypass**
**Status: Confirmed**

## Summary

Three sync protocols — DocSync, BranchableSync, and CAR fetch — have no `check_access()` calls at all. Even if Finding 20 were fixed and `AccessMode::Controlled` were activated, these handlers would still allow any connected peer unrestricted access to document heads, collection heads, and full DAGs.

## Evidence

### DocSync Request Handler — No Access Check

`crates/p2p/src/sync/coordinator/event_handler/doc_sync.rs:12-81`:
```rust
pub(super) async fn handle_doc_sync_request(
    &self,
    peer_id: libp2p::PeerId,
    request: crate::message::DocSyncRequest,
) -> Result<()> {
    // NO call to self.check_access()
    // Immediately queries and returns document heads
    for doc_id in &request.doc_ids {
        match self.head_provider.get_document_heads(doc_id).await {
            Ok(heads) => { results.push(...); }
            ...
        }
    }
    // Signs and sends response to ANY requesting peer
    ...
}
```

Any connected peer can query document heads for arbitrary document IDs. This reveals:
- Whether a document exists
- The current head CIDs (which can then be fetched via Bitswap)

### BranchableSync Request Handler — No Access Check

`crates/p2p/src/sync/coordinator/event_handler/branchable_sync.rs:12-73`:
```rust
pub(super) async fn handle_branchable_sync_request(
    &self,
    peer_id: libp2p::PeerId,
    request: crate::message::BranchableSyncRequest,
) -> Result<()> {
    // NO call to self.check_access()
    // Immediately queries and returns collection heads
    let heads = self.head_provider
        .get_collection_heads(&request.collection_id)
        .await;
    ...
}
```

Any connected peer can query all document heads for an entire collection. This is worse than DocSync because it reveals every document in a collection.

### CAR Fetch Request Handler — No Access Check

`crates/p2p/src/sync/coordinator/event_handler/car.rs:13-43`:
```rust
pub(crate) async fn handle_car_fetch_request(
    &self,
    peer_id: PeerId,
    root_cid: Cid,
) -> Result<()> {
    // NO call to self.check_access()
    // Note: CAR requests don't even carry collection_id, so there's
    // nothing to check against even if the code were added
    let blocks = collect_dag_blocks(..., &root_cid).await?;
    let car_data = encode_car(&[root_cid], &block_refs)?;
    self.host.send_car_response(peer_id, car_data).await?;
    Ok(())
}
```

Any connected peer can fetch an entire DAG by CID. CAR requests don't carry a `collection_id` field, so even adding a `check_access()` call would require a lookup from CID → collection first.

### Contrast with PushLog and GossipSub

PushLog (`pushlog.rs:26`) and GossipSub (`gossip.rs:26`) both call `check_access()` as their first operation. The three handlers above were either written after the access control model was designed, or the access check was forgotten.

## Impact

**Even with AccessMode::Controlled:**
- Any peer can enumerate all documents in any collection (BranchableSync)
- Any peer can check existence and get head CIDs for any document (DocSync)
- Any peer can download entire DAGs of any block (CAR fetch)
- These bypass the replicator registry entirely

## Recommendation

Add `check_access()` calls at the start of all three handlers. For CAR fetch, either:
1. Add a `collection_id` field to the CAR request message, or
2. Perform a CID → collection lookup before serving
