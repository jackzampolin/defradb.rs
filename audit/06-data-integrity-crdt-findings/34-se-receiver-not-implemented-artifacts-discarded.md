# Finding: SE Artifact Receiver Not Implemented — Incoming Artifacts Silently Discarded

**Stream**: 06 - Data Integrity & CRDT Correctness
**Session**: 4 - Searchable Encryption Deep-Dive
**Severity**: HIGH (SE queries on Rust replicator nodes will return empty results)
**Category**: Searchable Encryption / P2P Integration
**Status**: NEW (1.0 gap)

## Summary

When a Rust node receives SE artifacts from a peer (Go or Rust), the two-stream runner reads the bytes and logs a message but does not parse, validate, or store the artifacts. This means a Rust node acting as a replicator cannot serve SE queries — all queries will return empty results.

## Evidence

### SE Request Stream Handler — Read and Discard

`crates/p2p/src/two_stream/runner.rs:144-158`:

```rust
// Handle incoming SE request streams (Rust receiving SE artifacts - log for now)
Some((peer_id, mut stream)) = self.se_request_streams.next() => {
    tokio::spawn(async move {
        use futures::AsyncReadExt;
        let mut buf = Vec::new();
        if let Err(e) = stream.read_to_end(&mut buf).await {
            tracing::warn!(peer_id = %peer_id, error = %e, "Failed to read SE request stream");
            return;
        }
        tracing::info!(
            peer_id = %peer_id,
            buf_len = buf.len(),
            "Received SE request stream (Rust as receiver not yet implemented)"
        );
    });
}
```

The handler:
1. Reads the entire stream into a buffer (`read_to_end`)
2. Logs the buffer length
3. Drops the buffer — no CBOR deserialization, no validation, no storage
4. No response is sent (the sender uses fire-and-forget, so this is tolerated)

### SE Response Stream Handler — Also Discarded

`crates/p2p/src/two_stream/runner.rs:160-175`:

```rust
// Handle incoming SE response streams (replies to our SE pushes)
Some((peer_id, mut stream)) = self.se_response_streams.next() => {
    tokio::spawn(async move {
        use futures::AsyncReadExt;
        let mut buf = Vec::new();
        if let Err(e) = stream.read_to_end(&mut buf).await {
            tracing::warn!(peer_id = %peer_id, error = %e, "Failed to read SE response stream");
            return;
        }
        tracing::debug!(
            peer_id = %peer_id,
            buf_len = buf.len(),
            "Received SE response (acknowledgement)"
        );
    });
}
```

### SE Artifact Sending IS Implemented

`crates/p2p/src/two_stream/handler/branchable_se.rs:84-111` — The Rust node CAN send SE artifacts to peers (used when a Go node is the replicator). The send path is complete and working.

### read_to_end — No Size Limit

Both handlers use `stream.read_to_end(&mut buf)` which reads the entire stream into memory with no size limit. Combined with the two-stream finding that there's no message size limit (Stream 3, Finding 00), a malicious peer could send an arbitrarily large SE request to exhaust memory.

## Impact

### SE Queries on Rust Replicators Return Empty

In a deployment where:
1. Node A creates documents with encrypted indexes
2. Node B (Rust) is configured as a replicator
3. Node A pushes SE artifacts to Node B
4. Node A later queries Node B for matching documents

Node B silently discards the artifacts, so the query returns no results. This breaks the SE query workflow for Rust-as-replicator scenarios.

### Memory Exhaustion via Unbounded Read

`read_to_end` on the SE request stream has no size limit. A peer can send an arbitrarily large "SE artifact" payload that consumes all available memory. This is the same class of vulnerability as Stream 3 Finding 00 (no message size limit).

## Affected Code

- `crates/p2p/src/two_stream/runner.rs:144-175` — SE request/response handlers

## Remediation

### Phase 1: Implement SE Artifact Storage

```rust
Some((peer_id, mut stream)) = self.se_request_streams.next() => {
    let event_tx = self.event_tx.clone();
    tokio::spawn(async move {
        match TwoStreamHandler::handle_se_request_stream(peer_id, stream).await {
            Ok(event) => { event_tx.send(event).await.ok(); }
            Err(e) => { tracing::warn!(?e, "Failed to handle SE request"); }
        }
    });
}
```

Where `handle_se_request_stream` deserializes the CBOR, validates the artifacts, and stores them via `se::storage::store_artifacts`.

### Phase 2: Add Size Limits

Replace `read_to_end` with a size-limited read (e.g., `take(MAX_SE_MESSAGE_SIZE)`) to prevent memory exhaustion.

## Test Gap

- No integration test exercises a Rust node as SE artifact receiver
- `encrypted_index.rs` tests only exercise the producer side (creating/listing/deleting indexes)
- No test verifies SE query results returned by a Rust replicator
