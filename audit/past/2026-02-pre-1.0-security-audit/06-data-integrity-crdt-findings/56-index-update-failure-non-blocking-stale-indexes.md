# Index Update Failure Does Not Block Transaction Commit

**Severity:** Medium
**Category:** Data Integrity / Index Consistency
**Status:** Confirmed
**Session:** 6 of 6

## Summary

When the merge handler processes a P2P composite delta, document storage and index updates occur within the same transaction. However, if the index update fails, the failure is logged as a warning but does NOT prevent the transaction from committing. This leaves the document stored without corresponding index entries, causing silent query inconsistency.

## Affected Files

- `crates/db/src/merge_handler/composite.rs` lines 509-536 (standard merge path)
- `crates/db/src/merge_handler/composite.rs` lines 1031-1065 (batch merge path)

## Details

### Standard Merge Path (lines 509-536)

```rust
if let Err(e) = collection.save_with_datastore(&datastore, &doc).await {
    process_error = Some(MergeError::Database(e));  // ← Blocks commit
} else {
    // Index update
    let index_result = match &old_doc {
        Some(old) => index_manager.on_document_update(&datastore, old, &doc, ...).await,
        None => index_manager.on_document_create(&datastore, &doc, ...).await,
    };
    if let Err(e) = index_result {
        tracing::warn!(doc_id = %doc_id_str, error = %e,
            "Failed to update indexes after merge");
        // ← NO process_error set. Transaction commits without indexes.
    }
}
```

### Batch Merge Path (lines 1031-1065)

Same pattern — index failure only logged, not set as `process_error`.

### Delete Path — Even Worse (lines 418-431, 973-980)

```rust
// Standard path
if let Err(e) = index_manager.on_document_delete(&datastore, &old_doc, ...).await {
    tracing::warn!("Failed to delete indexes after merge");
    // Transaction continues → deletion marker set but index entries remain
}

// Batch path — error completely discarded
let _ = index_manager.on_document_delete(&datastore, &old_doc, ...).await;
```

### Impact

1. **Missing index entries**: Document stored but not indexed → index queries miss the document
2. **Orphaned index entries on delete**: Document deleted but old index entries remain → index queries return phantom results for deleted documents
3. **No self-healing**: Once stale, indexes remain stale permanently

### Attack Vector

A malicious peer could craft blocks that cause index update failures (e.g., via corrupted field values that fail index encoding) while the document itself stores successfully. Over time, this degrades index reliability without any visible error.

## Remediation

Set `process_error` on index update failure so the transaction rolls back:

```rust
if let Err(e) = index_result {
    process_error = Some(MergeError::Database(
        crate::error::Error::IndexUpdateFailed(e.to_string())
    ));
}
```

This ensures document storage and index updates are truly atomic.

## Test Gap

No integration test verifies index consistency after a P2P merge where the index update fails. No test injects index update failures during merge.
