# Finding 22: Bitswap Serves Blocks Without Collection-Level Access Checks

**Severity: MEDIUM**
**Category: Information Disclosure**
**Status: Confirmed (by design, but assumptions are incomplete)**

## Summary

The `BitswapStoreAdapter` exposes the blockstore to any connected peer via the iroh-bitswap `Store` trait. The security model documented in `access.rs` claims this is safe because "Bitswap inherently only serves blocks that passed the coordinator's access check." This assumption is incomplete — blocks can enter the blockstore via local writes, initial data loading, or other paths that never went through the coordinator.

## Evidence

### BitswapStoreAdapter — No Access Control

`crates/p2p/src/bitswap/store.rs:57-86`:
```rust
impl<B: Blockstore + Debug + 'static> Store for BitswapStoreAdapter<B> {
    async fn get(&self, cid: &Cid) -> Result<Block> {
        // Returns ANY block by CID to ANY requesting peer
        let data = self.blockstore.get(cid).await...;
        Ok(Block::new(Bytes::from(data), *cid))
    }

    async fn has(&self, cid: &Cid) -> Result<bool> {
        // Reveals block existence to ANY peer
        self.blockstore.has(cid).await...
    }
}
```

### Documented Assumption

`crates/p2p/src/bitswap/access.rs:8-18`:
```
//! Access control is enforced at the **SyncCoordinator level**, not at the Bitswap level.
//! ...
//! 1. Unauthorized peers cannot push blocks to this node
//! 2. Bitswap inherently only serves blocks that passed the coordinator's access check
//! 3. Per-collection authorization is enforced
```

Claim #2 is false. The blockstore contains:
- **Locally created documents** — blocks from the node's own database operations
- **Blocks from CAR fetches** — stored without access checks (Finding 21)
- **Blocks from initial data loading** — pre-existing data at node startup
- **Blocks from DocSync/BranchableSync** — fetched without access checks (Finding 21)

### Cross-Collection CID Guessing

Even if access control were properly enforced at the coordinator level:
- A peer authorized for collection A could guess/enumerate CIDs from collection B
- Bitswap requests carry only CIDs, not collection IDs
- The Store trait has no mechanism to check which collection a block belongs to

### This Is By Design — But the Design Has Gaps

The two-tier security model (coordinator for ingress, blockstore for Bitswap serving) is a reasonable architecture. The issue is that the ingress side has gaps (Finding 21), making the "blocks only enter through the coordinator" assumption false.

## Impact

- Cross-collection information disclosure via CID guessing
- Full blockstore exposure when combined with the CAR fetch bypass (Finding 21)
- The documented security model creates a false sense of security

## Recommendation

Accept this as a design tradeoff but:
1. Fix the ingress gaps (Finding 21) so the assumption becomes more defensible
2. Update the documentation to acknowledge the limitations (local blocks, CID guessing)
3. Consider adding a CID → collection_id reverse index for future Bitswap-level checks
