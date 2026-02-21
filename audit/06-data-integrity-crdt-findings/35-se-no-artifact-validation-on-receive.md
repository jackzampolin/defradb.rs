# Finding: No SE Artifact Validation on P2P Receive Path

**Stream**: 06 - Data Integrity & CRDT Correctness
**Session**: 4 - Searchable Encryption Deep-Dive
**Severity**: MEDIUM (when receiver is implemented, validation must be added to prevent index corruption)
**Category**: Searchable Encryption / P2P Security
**Status**: NEW (blocked on Finding 34 — receiver not yet implemented)

## Summary

The SE artifact send path (`push_docs.rs`) sends artifacts via fire-and-forget without signing. When the SE receive path is implemented (Finding 34), there is no validation framework in place: no signature verification on SE artifacts, no collection-level access checks, no tag format validation. A malicious peer could inject fake artifacts that corrupt the SE index, causing false positives or denials of service.

## Evidence

### SE Artifacts Sent Fire-and-Forget Without Signing

`crates/p2p/src/two_stream/handler/branchable_se.rs:84-111`:

```rust
pub async fn send_se_artifacts_fire_and_forget(
    &mut self,
    peer_id: PeerId,
    request: PushSEArtifactsRequest,
) -> Result<()> {
    // ...
    write_message(&mut stream, &request).await?;
    // No response awaited
    Ok(())
}
```

Contrast with PushLog requests which ARE signed:

`crates/db/src/push_docs.rs:169-173`:
```rust
if let Err(e) = p2p::signing::sign_message(handle.keypair(), &mut field_req) {
    tracing::warn!(error = %e, "Failed to sign field block PushLog request");
    continue;
}
```

### SE Request Has Metadata but No Signing in Practice

`crates/p2p/src/message/se.rs:278-301` — `PushSEArtifactsRequest` has `MetaData` (which includes signature fields), but the `push_docs.rs` code never calls `sign_message` on the SE request.

### No Validation Framework for Receiver

When Finding 34 is resolved and the receiver is implemented, it must add:

1. **Tag format validation**: Tags must be exactly 16 bytes
2. **Collection access check**: Sender must be authorized to push artifacts for this collection
3. **Sender authentication**: Either via P2P identity or message signature
4. **Rate limiting**: A peer should not be able to flood the SE index with fake artifacts
5. **Artifact count limit**: A single request should not contain unbounded artifacts

### Attack Scenarios (When Receiver Is Implemented)

| Attack | Method | Impact |
|--------|--------|--------|
| False positive injection | Insert fake (tag, docID) pairs | Queries return wrong documents |
| Tag flooding | Insert millions of fake tags | Slow down or DoS SE queries |
| Collection confusion | Send artifacts for wrong collection | Corrupt cross-collection isolation |
| Tag overwrite | Insert conflicting tags for real docIDs | Break SE query correctness |

## Impact

This is a forward-looking finding. Currently, the receiver discards all artifacts (Finding 34), so there's nothing to corrupt. However, when the receiver is implemented, the lack of validation must be addressed simultaneously to prevent SE index corruption.

## Affected Code

- `crates/db/src/push_docs.rs:304-316` — SE artifacts sent without signing
- `crates/p2p/src/two_stream/runner.rs:144-158` — placeholder receiver (no validation)

## Remediation

When implementing the SE receiver:

1. **Sign SE requests**: Call `sign_message` on `PushSEArtifactsRequest` before sending
2. **Verify signature on receive**: Check message signature against known peer identity
3. **Validate tag length**: Reject artifacts where `search_tag.len() != 16`
4. **Check collection authorization**: Only accept artifacts for collections the peer is authorized to replicate
5. **Bound artifact count**: Reject requests with more than a configurable maximum artifacts

## Cross-References

- Finding 34: SE receiver not implemented
- Stream 3 Finding 12: Two-stream no signature verification
- Stream 3 Finding 17: GossipSub no application signature check
