# P2P PushLog Stores Blocks Without CID Content Verification

**Severity:** Medium
**Category:** Data Integrity / P2P Security
**Status:** Confirmed

## Summary

When a PushLog broadcast arrives from a P2P peer, the block data is stored in the blockstore under the peer-supplied CID without verifying that the data actually hashes to that CID. The CID is parsed from `msg.cid` and the block is `msg.block` — both controlled by the sender. An attacker can send arbitrary block content paired with any syntactically valid CID, and the target will store and eventually merge the fabricated content.

This is distinct from Finding 18 (hash_on_read disabled during merge) and Finding 23 (put() itself doesn't verify). This finding focuses on the P2P ingestion path specifically, where the attack is most practical.

## Affected Files

- `crates/p2p/src/sync/manager/process/pushlog.rs:35-277` (`process_pushlog()`)
- `crates/p2p/src/sync/manager/process/pushlog.rs:154` (`blockstore.put(cid, &msg.block)`)

## Details

### PushLog Processing Flow (No Verification Points)

```
PushLog broadcast received
    ↓
1. Parse CID from msg.cid bytes           ← attacker controls
2. Check if already merged                 ← dedup only
3. blockstore.put(cid, &msg.block)         ← NO VERIFICATION
4. Check for missing links                 ← trusts block content
5. Emit BlockReceived event                ← triggers merge
```

At no point in this flow is the block's content verified against the CID.

### Attack Scenario: CRDT State Corruption

1. Attacker observes that collection `Users` has document `doc-123`
2. Attacker constructs a malicious LWW delta that sets `name` to "Pwned" with high priority
3. Attacker serializes this as a valid `Block` → gets bytes `B_evil`
4. Attacker takes an existing legitimate CID `C_legit` for a different block
5. Attacker sends PushLog: `{cid: C_legit, block: B_evil, doc_id: "doc-123", ...}`
6. Target stores `B_evil` under key `C_legit` in blockstore
7. If `C_legit` was already stored, `put()` returns early (dedup) — attack blocked for existing CIDs
8. If `C_legit` is new, `B_evil` is stored and merged

More practically, the attacker computes their own `C_evil = generate_cid_from_bytes(B_evil)` and sends that:
1. Attacker creates block `B_evil` with malicious delta
2. Attacker sends PushLog: `{cid: C_evil, block: B_evil, ...}` — CID matches content
3. Target stores and merges — this is indistinguishable from a legitimate update

In this second case, CID verification would pass because the attacker computed the correct CID. The defense here is signature verification (Finding 18 in ACP stream), not CID verification. But for the first case (wrong CID + wrong content), CID verification would detect the mismatch.

### Why CID Verification Still Matters

Even though a sophisticated attacker would compute the correct CID for their payload, CID verification defends against:
- **Accidental corruption**: network bit flips, storage errors
- **Unsophisticated attacks**: fuzzed PushLog messages
- **Implementation bugs**: code paths that accidentally swap block data between messages
- **Cache coherence**: ensures cached data is the data that was hashed

### Contrast: Bitswap Is Verified

Bitswap requests specific CIDs and the `iroh-bitswap` library verifies that received data matches the requested CID. PushLog is push-based — the sender chooses what to send, and the receiver trusts both CID and content.

## Remediation

Add CID verification in `process_block_inner()` before `put()`:

```rust
// After parsing CID, before storing:
let computed_cid = defra_core::block::generate_cid_from_bytes(&msg.block)
    .map_err(|e| Error::InvalidBlock(format!("Failed to compute CID: {}", e)))?;
if computed_cid != cid {
    return Err(Error::InvalidBlock(format!(
        "Block content does not match CID: expected {}, got {}",
        cid, computed_cid
    )));
}
```

## Test Gap

- No integration test sends a PushLog with mismatched CID/data and verifies rejection
- No unit test for `process_pushlog()` with fabricated block content
- No test verifies that Bitswap blocks are verified while PushLog blocks are not
