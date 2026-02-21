# 33: Lens Transform Output Not Validated Against Schema

| Field    | Value |
|----------|-------|
| Severity | MEDIUM |
| Category | Data Integrity |
| Status   | Confirmed |

## Summary

WASM lens transforms receive and produce `LensDoc` values (JSON `Map<String, Value>`), but the output is not validated against the destination schema. A malicious or buggy transform can return documents with wrong field types, extra fields, missing required fields, modified document IDs, or fields belonging to a different collection. The pipeline trusts transform output completely.

## Affected Files

- `crates/lens/src/wasm.rs:633-635` — Output deserialized and returned without validation
- `crates/lens/src/pipeline.rs:348-411` — `apply_transform()` returns output directly
- `crates/lens/src/doc.rs` — `LensDoc = serde_json::Map<String, serde_json::Value>`

## Details

### No Output Validation

After WASM execution, the output document is deserialized from JSON and returned directly:

```rust
let doc: LensDoc = serde_json::from_slice(&result_bytes)
    .map_err(|e| Error::WasmExecution(e.to_string()))?;
output_docs.push(doc);
```

The only validation is that the bytes are valid JSON and deserialize to a `Map<String, Value>`. There is no check that:

1. **Field types match the destination schema** — A transform could change `age: 25` (Int) to `age: "twenty-five"` (String)
2. **Document ID is preserved** — A transform could modify `_docID`, pointing the document at a different storage key
3. **Required fields are present** — A transform could drop fields
4. **No extra unauthorized fields** — A transform could inject fields not in the schema
5. **Output size is reasonable** — A transform could return a multi-megabyte document from a small input

### LensDoc is Untyped

```rust
pub type LensDoc = serde_json::Map<String, serde_json::Value>;
```

This is a completely untyped JSON object. The `_docID` and `_deleted` fields are reserved by convention only — nothing prevents a transform from modifying them.

### Impact

- **Data corruption**: Documents stored after a buggy transform may have wrong field types, causing query errors or silent data loss
- **Document ID manipulation**: If `_docID` is modified, the document could overwrite or shadow another document
- **Schema bypass**: A transform could introduce fields that bypass schema validation (since the document is already "inside" the system)
- **Amplification**: A 1→N transform with no output cap could generate thousands of documents from one input

## Remediation

1. **Validate output against destination schema**: After transform execution, verify field types match the destination collection's schema definition
2. **Preserve document ID**: Assert that `_docID` in the output matches the input (or is absent, to be filled by the pipeline)
3. **Output size limit**: Cap the total output bytes per transform invocation
4. **Output document count**: Cap the number of output documents per batch (addresses the amplification concern from finding 31)

## Test Gap

No tests verify that a transform's output matches the destination schema. The `MemoryTransformStore` used in tests passes documents through unchanged, so schema mismatches are never exercised. No test attempts to modify `_docID` through a lens transform.
