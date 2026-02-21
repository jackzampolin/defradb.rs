# Finding: DocSyncRequest.doc_ids Is an Unbounded Array

**Stream**: 03 - P2P Network Security
**Session**: 4 — Replication Protocol Security
**Severity**: HIGH
**Category**: Denial of Service / Amplification

## Summary

`DocSyncRequest.doc_ids` is a `Vec<String>` with no length limit. The handler iterates over every element, performing a database lookup for each. An attacker can send a request with millions of document IDs, causing unbounded CPU consumption and a proportionally large response. Combined with Finding 00 (no message size limit on the two-stream path), there is no wire-level protection either.

## Affected Files

| File | Lines | Issue |
|------|-------|-------|
| `crates/p2p/src/message/docsync.rs` | 21 | `doc_ids: Vec<String>` — no max length |
| `crates/p2p/src/sync/coordinator/event_handler/doc_sync.rs` | 25-49 | Iterates all doc_ids with database lookup per item |

## Details

### The Message Type

```rust
// message/docsync.rs:14-22
pub struct DocSyncRequest {
    #[serde(flatten)]
    pub metadata: MetaData,

    #[serde(rename = "DocIDs")]
    pub doc_ids: Vec<String>,
}
```

No `#[serde(deserialize_with)]` or post-deserialization validation limits the array length.

### The Handler

```rust
// doc_sync.rs:24-49
let mut results: Vec<DocSyncItem> = Vec::new();
for doc_id in &request.doc_ids {
    match self.head_provider.get_document_heads(doc_id).await {
        Ok(heads) => {
            if !heads.is_empty() {
                results.push(DocSyncItem {
                    doc_id: doc_id.clone(),
                    heads: heads.iter().map(|cid| cid.to_bytes()).collect(),
                });
            }
        }
        Err(e) => { /* warn and continue */ }
    }
}
```

For each doc_id:
1. Database lookup via `head_provider.get_document_heads()`
2. If found, CID bytes are collected into the response
3. Response size grows proportionally

### Attack Scenario

1. Attacker sends DocSyncRequest with 1,000,000 doc_ids (all random strings)
2. Handler performs 1,000,000 database lookups (CPU + I/O exhaustion)
3. Even though most won't match, the iteration itself is O(n)
4. If some DO match, the response `Vec<DocSyncItem>` grows unbounded
5. Response is serialized and sent back — potentially very large

### No Access Check

Per Finding 21, DocSync requests have NO access control check. Any connected peer can send this request regardless of replicator authorization.

### Other Unbounded Arrays

Similar patterns exist in reply types:

| Type | Field | Risk |
|------|-------|------|
| `DocSyncReply.results` | `Vec<DocSyncItem>` | Each item contains `Vec<Vec<u8>>` of head CIDs |
| `BranchableSyncReply.heads` | `Vec<Vec<u8>>` | Unbounded head CID list |
| `PushLogRequest.block` | `Vec<u8>` | Unbounded block data |
| `PushLogRequest.cid` | `Vec<u8>` | Unbounded CID bytes |
| `PushLogRequest.creator` | `String` | Unbounded string |
| `PushLogRequest.collection_id` | `String` | Unbounded string |

None of these fields have length validation before or after deserialization.

## Remediation

1. Add a `MAX_DOC_IDS` constant (e.g., 1000) and reject requests exceeding it
2. Add field-level validation after deserialization for all string and array fields
3. Consider a custom serde deserializer that rejects oversized arrays during parsing

## Test Gap

No test sends a DocSyncRequest with a large doc_ids array and verifies it is rejected or rate-limited.
