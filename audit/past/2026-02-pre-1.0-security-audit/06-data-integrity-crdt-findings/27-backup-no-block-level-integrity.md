# Backup Import Operates at Document Level, Not Block Level

**Severity:** Low
**Category:** Data Integrity / Backup
**Status:** Confirmed (By Design)

## Summary

The backup/import system operates at the document level using JSON and GraphQL mutations, not at the block level. Imported documents have their CIDs regenerated from content, so a tampered backup cannot inject blocks with fabricated CIDs. However, the backup file itself has no integrity protection (no checksum or signature), meaning a tampered backup could introduce documents with silently modified field values.

## Affected Files

- `crates/db/src/backup/import.rs:30-203` (`import_database()` — GraphQL mutations)
- `crates/db/src/backup/mod.rs:86-115` (`compute_doc_id_new()` — CID regeneration)

## Details

### Import Path

```rust
// import.rs — documents created via GraphQL mutations, not raw block insertion
let mutation = format!(
    "mutation {{ create_{}(input: {}) {{ _docID }} }}",
    collection_name, input
);
let response = runner.execute(request).await;
```

Imported documents go through the full document creation pipeline:
1. JSON parsed to document fields
2. GraphQL mutation executed
3. Document CBOR computed → SHA-256 → CID → DocID generated
4. CRDT blocks built from document via block_builder
5. Blocks stored with computed CIDs

This means:
- CIDs are always recomputed from content — a tampered backup cannot inject blocks with pre-computed CIDs
- Documents that fail schema validation are rejected
- Foreign key references are validated against existing documents

### No Backup File Integrity Protection

The backup file is plain JSON:
```json
{
    "User": [{"_docID": "...", "name": "John", "age": 30}],
    "Address": [{"_docID": "...", "street": "...", "city": "..."}]
}
```

No checksum, HMAC, or signature protects the file. A tampering attack can:
- Modify field values (e.g., change `"age": 30` to `"age": 99`)
- Add or remove documents
- The import will succeed with the modified data
- New (different) CIDs will be computed for modified documents

### `_docID` and `_docIDNew` Ignored

```rust
// import.rs:110-111
doc_map.remove("_docID");
doc_map.remove("_docIDNew");
```

The original DocIDs from the backup are stripped. New DocIDs are generated from the imported content. This means a backup/restore cycle changes all DocIDs (the new DocID reflects the content at import time with the current collection schema).

### Export Path

Export creates the JSON by querying documents through the normal query pipeline. No raw block access.

## Remediation

1. **Add optional integrity checksum** — append a SHA-256 hash or HMAC to the backup file, verify on import. This would detect accidental corruption or intentional tampering.

2. **Document the DocID regeneration behavior** — users should understand that DocIDs change on import, which affects any external references to documents by ID.

## Test Gap

- No test imports a tampered backup and verifies the resulting documents have different CIDs
- No test verifies that imported documents' CIDs differ from exported documents' CIDs (due to `_docID` removal and regeneration)
