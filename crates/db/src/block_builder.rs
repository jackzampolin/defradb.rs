//! IPLD block builder for document mutations.
//!
//! Creates proper Block structures with CRDT delta payloads for P2P synchronization.
//! Matches Go DefraDB's block format for wire compatibility.
//!
//! This module provides two main functions:
//! - `build_blocks_from_document`: For P2P broadcast (uses external blockstore)
//! - `write_document_blocks`: For FFI/local storage (uses transaction stores)

use blockstore::Blockstore;
use cid::Cid;
use datastore::NamespaceView;
use defra_core::block::{Block, CompositeDeltaPayload, CrdtDelta, DAGLink, LwwDeltaPayload};
use document::{Document, NormalValue};
use std::sync::Arc;
use storage::corekv::Key;
use storage::keys::headstore::HeadstoreDocKey;

/// Result of building blocks from a document mutation.
#[derive(Debug, Clone)]
pub struct BlockResult {
    /// The CID of the composite (root) block
    pub cid: Cid,
    /// The raw composite block bytes (DAG-CBOR encoded)
    pub block: Vec<u8>,
    /// The document ID
    pub doc_id: String,
    /// CIDs of all field blocks created
    pub field_cids: Vec<Cid>,
}

/// Build IPLD blocks from a document for P2P sync.
///
/// This creates the proper Block structure that Go DefraDB expects:
/// 1. An LWW field block for each document field
/// 2. A Composite block linking all field blocks
///
/// # Arguments
///
/// * `doc` - The document to build blocks from
/// * `schema_version_id` - The schema version ID for CRDT deltas
/// * `blockstore` - Blockstore to store field blocks
///
/// # Returns
///
/// Returns `BlockResult` containing the composite block and metadata.
pub async fn build_blocks_from_document<B: Blockstore>(
    doc: &Document,
    schema_version_id: &str,
    blockstore: &Arc<B>,
) -> Result<BlockResult, String> {
    let doc_id = doc
        .id()
        .ok_or_else(|| "Document must have an ID".to_string())?;
    let doc_id_str = doc_id.to_string();
    let doc_id_bytes = doc_id_str.as_bytes().to_vec();

    let mut field_links: Vec<DAGLink> = Vec::new();
    let mut field_cids: Vec<Cid> = Vec::new();

    // Create an LWW block for each field
    for (field_name, field_value) in doc.values() {
        // Skip internal fields like _docID
        if field_name.starts_with('_') {
            continue;
        }

        // Encode the field value as CBOR
        let value_bytes = encode_value_as_cbor(field_value.value())?;

        // Create LWW delta payload
        let lww_payload = LwwDeltaPayload {
            doc_id: doc_id_bytes.clone(),
            field_name: field_name.clone(),
            priority: 1, // Initial priority for new documents
            schema_version_id: schema_version_id.to_string(),
            data: value_bytes,
        };

        // Create the field block
        let field_block = Block::new(CrdtDelta::Lww(lww_payload), vec![], vec![]);

        // Serialize and generate CID
        let field_block_bytes = field_block
            .to_dag_cbor()
            .map_err(|e| format!("Failed to encode field block: {}", e))?;
        let field_cid = field_block
            .generate_cid()
            .map_err(|e| format!("Failed to generate field CID: {}", e))?;

        // Store the field block in the blockstore
        blockstore
            .put(&field_cid, &field_block_bytes)
            .await
            .map_err(|e| format!("Failed to store field block: {}", e))?;

        tracing::debug!(
            field_name = %field_name,
            cid = %field_cid,
            "Stored LWW field block"
        );

        // Add link to composite
        field_links.push(DAGLink::new(field_name.clone(), field_cid));
        field_cids.push(field_cid);
    }

    // Create the Composite delta payload
    let composite_payload = CompositeDeltaPayload {
        doc_id: doc_id_bytes,
        schema_version_id: schema_version_id.to_string(),
        priority: 1, // Initial priority
        status: 1,   // Active document
    };

    // Create the composite block with links to all field blocks
    let composite_block = Block::new(CrdtDelta::Composite(composite_payload), vec![], field_links);

    // Serialize the composite block
    let composite_bytes = composite_block
        .to_dag_cbor()
        .map_err(|e| format!("Failed to encode composite block: {}", e))?;
    let composite_cid = composite_block
        .generate_cid()
        .map_err(|e| format!("Failed to generate composite CID: {}", e))?;

    // Store the composite block
    blockstore
        .put(&composite_cid, &composite_bytes)
        .await
        .map_err(|e| format!("Failed to store composite block: {}", e))?;

    tracing::info!(
        doc_id = %doc_id_str,
        cid = %composite_cid,
        field_count = field_cids.len(),
        "Built composite block with field links"
    );

    Ok(BlockResult {
        cid: composite_cid,
        block: composite_bytes,
        doc_id: doc_id_str,
        field_cids,
    })
}

/// Encode a NormalValue as CBOR bytes.
fn encode_value_as_cbor(value: &NormalValue) -> Result<Vec<u8>, String> {
    let mut bytes = Vec::new();
    ciborium::into_writer(value, &mut bytes)
        .map_err(|e| format!("Failed to encode value as CBOR: {}", e))?;
    Ok(bytes)
}

/// Encode a priority as a varint (matching Go's binary.PutUvarint).
fn encode_priority_varint(priority: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(10); // Max varint64 is 10 bytes
    let mut n = priority;
    while n >= 0x80 {
        buf.push((n as u8) | 0x80);
        n >>= 7;
    }
    buf.push(n as u8);
    buf
}

/// Decode a varint to priority (matching Go's binary.Uvarint).
fn decode_priority_varint(buf: &[u8]) -> u64 {
    let mut n: u64 = 0;
    let mut shift: u32 = 0;
    for &byte in buf {
        if shift >= 64 {
            return 0; // Overflow protection
        }
        n |= ((byte & 0x7f) as u64) << shift;
        if byte < 0x80 {
            return n;
        }
        shift += 7;
    }
    n
}

/// Information about existing heads for a document field.
struct FieldHeadInfo {
    /// The CID of the existing head (if any)
    cid: Option<Cid>,
    /// The priority of the existing head (0 if none)
    #[allow(dead_code)] // May be used for priority-based conflict resolution
    priority: u64,
    /// The full key (for deletion when replacing)
    key: Option<Vec<u8>>,
}

/// Get existing head info for a specific field of a document.
///
/// Returns the current head CID and priority for the field.
/// Used to build proper head links when creating update blocks.
async fn get_field_head(
    headstore: &NamespaceView,
    doc_id: &str,
    field_id: &str,
) -> Result<FieldHeadInfo, String> {
    use storage::corekv::IterOptions;

    let prefix = HeadstoreDocKey::field_prefix(doc_id, field_id);
    let opts = IterOptions::new().with_prefix(prefix);

    let mut iter = headstore
        .iterator(opts)
        .await
        .map_err(|e| format!("Failed to create headstore iterator: {}", e))?;

    // There should be at most one head per field
    if let Some(kv_pair) = iter
        .next()
        .await
        .map_err(|e| format!("Failed to iterate headstore: {}", e))?
    {
        let priority = decode_priority_varint(&kv_pair.value);
        // Parse CID from key: /d/{doc_id}/{field_id}/{cid}
        let key_str = String::from_utf8_lossy(&kv_pair.key);
        let parts: Vec<&str> = key_str.split('/').collect();
        if let Some(cid_str) = parts.last() {
            if let Ok(cid) = cid_str.parse::<Cid>() {
                return Ok(FieldHeadInfo {
                    cid: Some(cid),
                    priority,
                    key: Some(kv_pair.key.clone()),
                });
            }
        }
    }

    Ok(FieldHeadInfo {
        cid: None,
        priority: 0,
        key: None,
    })
}

/// Get the maximum priority from existing heads for a document.
///
/// Scans the headstore for all existing heads of the given document
/// and returns the maximum priority found. Returns 0 if no heads exist.
async fn get_max_priority(headstore: &NamespaceView, doc_id: &str) -> Result<u64, String> {
    use storage::corekv::IterOptions;

    let prefix = HeadstoreDocKey::document_prefix(doc_id);
    let opts = IterOptions::new().with_prefix(prefix);

    let mut iter = headstore
        .iterator(opts)
        .await
        .map_err(|e| format!("Failed to create headstore iterator: {}", e))?;

    let mut max_priority: u64 = 0;
    while let Some(kv_pair) = iter
        .next()
        .await
        .map_err(|e| format!("Failed to iterate headstore: {}", e))?
    {
        let priority = decode_priority_varint(&kv_pair.value);
        if priority > max_priority {
            max_priority = priority;
        }
    }

    Ok(max_priority)
}

/// Write document blocks to blockstore and heads to headstore.
///
/// This function creates CRDT blocks for a document and writes:
/// 1. LWW field blocks to blockstore
/// 2. Composite block to blockstore
/// 3. Head CIDs to headstore (for _commits queries)
///
/// This matches Go DefraDB's ProcessBlock → updateHeads flow.
///
/// # Arguments
///
/// * `blockstore` - Transaction blockstore view
/// * `headstore` - Transaction headstore view
/// * `doc` - The document to build blocks from
/// * `schema_version_id` - The schema version ID for CRDT deltas
/// * `modified_fields` - Optional set of field names that were modified.
///   If Some, only these fields will have new blocks created.
///   If None (for create operations), all fields get new blocks.
///
/// # Returns
///
/// Returns `BlockResult` containing the composite block and metadata.
pub async fn write_document_blocks(
    blockstore: &NamespaceView,
    headstore: &NamespaceView,
    doc: &Document,
    schema_version_id: &str,
    modified_fields: Option<&std::collections::HashSet<String>>,
) -> Result<BlockResult, String> {
    let doc_id = doc
        .id()
        .ok_or_else(|| "Document must have an ID".to_string())?;
    let doc_id_str = doc_id.to_string();
    let doc_id_bytes = doc_id_str.as_bytes().to_vec();

    let mut field_links: Vec<DAGLink> = Vec::new();
    let mut field_cids: Vec<Cid> = Vec::new();

    // Get max existing priority for this document and increment by 1.
    // For new documents, max is 0, so priority becomes 1.
    // For updates, priority = max_existing + 1 (matches Go behavior).
    let max_priority = get_max_priority(headstore, &doc_id_str).await?;
    let priority: u64 = max_priority + 1;

    // Create LWW blocks for each field
    // For updates with modified_fields, only create blocks for changed fields.
    // For unchanged fields, use their existing head CID in the composite links.
    for (field_name, field_value) in doc.values() {
        // Skip internal fields like _docID
        if field_name.starts_with('_') {
            continue;
        }

        // Check if this field should have a new block created
        let should_create_block = match modified_fields {
            None => true, // Create operation: all fields get new blocks
            Some(fields) => fields.contains(field_name), // Update: only modified fields
        };

        // Get existing head for this field (if any)
        let field_head = get_field_head(headstore, &doc_id_str, field_name).await?;

        if should_create_block {
            // Create new LWW block for this field
            let heads: Vec<Cid> = field_head.cid.into_iter().collect();

            // Encode the field value as CBOR
            let value_bytes = encode_value_as_cbor(field_value.value())?;

            // Create LWW delta payload
            let lww_payload = LwwDeltaPayload {
                doc_id: doc_id_bytes.clone(),
                field_name: field_name.clone(),
                priority,
                schema_version_id: schema_version_id.to_string(),
                data: value_bytes,
            };

            // Create the field block with heads linking to previous version
            let field_block = Block::new(CrdtDelta::Lww(lww_payload), heads, vec![]);

            // Serialize and generate CID
            let field_block_bytes = field_block
                .to_dag_cbor()
                .map_err(|e| format!("Failed to encode field block: {}", e))?;
            let field_cid = field_block
                .generate_cid()
                .map_err(|e| format!("Failed to generate field CID: {}", e))?;

            // Store the field block in blockstore
            blockstore
                .set(&field_cid.to_bytes(), &field_block_bytes)
                .await
                .map_err(|e| format!("Failed to store field block: {}", e))?;

            // Delete old head entry if it exists (replace, not accumulate)
            if let Some(old_key) = field_head.key {
                headstore
                    .delete(&old_key)
                    .await
                    .map_err(|e| format!("Failed to delete old field head: {}", e))?;
            }

            // Write new head CID to headstore: /d/{doc_id}/{field_name}/{cid} → priority
            let head_key = HeadstoreDocKey::new(&doc_id_str, field_name, field_cid);
            let priority_bytes = encode_priority_varint(priority);
            headstore
                .set(&head_key.bytes(), &priority_bytes)
                .await
                .map_err(|e| format!("Failed to write field head: {}", e))?;

            tracing::debug!(
                field_name = %field_name,
                cid = %field_cid,
                has_prev_head = field_head.cid.is_some(),
                "Stored LWW field block and head"
            );

            // Add link to composite - only new blocks get linked
            field_links.push(DAGLink::new(field_name.clone(), field_cid));
            field_cids.push(field_cid);
        }
        // Note: Unchanged fields are NOT added to composite links.
        // Go only includes newly created field blocks in the composite's links array.
    }

    // Get existing composite head (if any) to build proper DAG links
    // "C" is the marker for composite/document-level commits (matches Go)
    let composite_head = get_field_head(headstore, &doc_id_str, "C").await?;
    let composite_heads: Vec<Cid> = composite_head.cid.into_iter().collect();

    // Create the Composite delta payload
    let composite_payload = CompositeDeltaPayload {
        doc_id: doc_id_bytes,
        schema_version_id: schema_version_id.to_string(),
        priority,
        status: 1, // Active document
    };

    // Create the composite block with heads linking to previous version
    let composite_block = Block::new(
        CrdtDelta::Composite(composite_payload),
        composite_heads,
        field_links,
    );

    // Serialize the composite block
    let composite_bytes = composite_block
        .to_dag_cbor()
        .map_err(|e| format!("Failed to encode composite block: {}", e))?;
    let composite_cid = composite_block
        .generate_cid()
        .map_err(|e| format!("Failed to generate composite CID: {}", e))?;

    // Store the composite block in blockstore
    blockstore
        .set(&composite_cid.to_bytes(), &composite_bytes)
        .await
        .map_err(|e| format!("Failed to store composite block: {}", e))?;

    // Delete old composite head entry if it exists (replace, not accumulate)
    if let Some(old_key) = composite_head.key {
        headstore
            .delete(&old_key)
            .await
            .map_err(|e| format!("Failed to delete old composite head: {}", e))?;
    }

    // Write new composite head to headstore: /d/{doc_id}/C/{cid} → priority
    let composite_head_key = HeadstoreDocKey::new(&doc_id_str, "C", composite_cid);
    let priority_bytes = encode_priority_varint(priority);
    headstore
        .set(&composite_head_key.bytes(), &priority_bytes)
        .await
        .map_err(|e| format!("Failed to write composite head: {}", e))?;

    tracing::info!(
        doc_id = %doc_id_str,
        cid = %composite_cid,
        field_count = field_cids.len(),
        has_prev_head = composite_head.cid.is_some(),
        "Built composite block with field links and wrote heads"
    );

    Ok(BlockResult {
        cid: composite_cid,
        block: composite_bytes,
        doc_id: doc_id_str,
        field_cids,
    })
}

// === Legacy function for backwards compatibility ===

/// Build an IPLD block from a document (legacy - DO NOT USE for P2P).
///
/// **WARNING**: This creates raw document CBOR, not a proper Block structure.
/// Go DefraDB cannot parse this format. Use `build_blocks_from_document` instead.
///
/// This function is kept for backward compatibility with non-P2P code paths.
#[deprecated(since = "0.1.0", note = "Use build_blocks_from_document for P2P sync")]
pub fn build_block_from_document(doc: &Document) -> Result<BlockResult, String> {
    use multihash::MultihashGeneric;
    use sha2::{Digest, Sha256};

    const DAG_CBOR_CODEC: u64 = 0x71;
    const SHA2_256_CODE: u64 = 0x12;

    let doc_id = doc
        .id()
        .ok_or_else(|| "Document must have an ID".to_string())?
        .to_string();

    // Encode document as CBOR (raw - NOT a Block structure)
    let block = doc
        .to_cbor()
        .map_err(|e| format!("Failed to encode document: {}", e))?;

    // Create CID from block
    let mut hasher = Sha256::new();
    hasher.update(&block);
    let digest = hasher.finalize();

    let mh = MultihashGeneric::<64>::wrap(SHA2_256_CODE, &digest)
        .map_err(|e| format!("Failed to create multihash: {}", e))?;

    let cid = Cid::new_v1(DAG_CBOR_CODEC, mh);

    Ok(BlockResult {
        cid,
        block,
        doc_id,
        field_cids: vec![],
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use blockstore::DefraBlockstore;
    use storage::backends::MemoryStore;

    fn make_test_blockstore() -> Arc<DefraBlockstore<MemoryStore>> {
        let store = Arc::new(MemoryStore::new());
        Arc::new(DefraBlockstore::new(store, false))
    }

    #[tokio::test]
    async fn test_build_blocks_creates_proper_structure() {
        let mut doc = Document::new();
        doc.generate_and_set_doc_id().unwrap();
        doc.set("name", NormalValue::String("Alice".to_string()));
        doc.set("age", NormalValue::Int(30));

        let blockstore = make_test_blockstore();
        let schema_version_id = "bafyreihsneodeja4lfer5puptim3lkwvketyckrmkhfpgxm67ch5wenjwq";

        let result = build_blocks_from_document(&doc, schema_version_id, &blockstore)
            .await
            .unwrap();

        // Should have created 2 field blocks (name, age)
        assert_eq!(result.field_cids.len(), 2);
        assert!(!result.doc_id.is_empty());

        // Composite block should be in blockstore
        let stored = blockstore.get(&result.cid).await.unwrap();
        assert!(stored.is_some());

        // Each field block should be in blockstore
        for field_cid in &result.field_cids {
            let stored = blockstore.get(field_cid).await.unwrap();
            assert!(stored.is_some());
        }
    }

    #[tokio::test]
    async fn test_build_blocks_requires_doc_id() {
        let doc = Document::new();
        let blockstore = make_test_blockstore();

        let result = build_blocks_from_document(&doc, "schema-v1", &blockstore).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("must have an ID"));
    }

    #[tokio::test]
    async fn test_field_block_contains_lww_delta() {
        let mut doc = Document::new();
        doc.generate_and_set_doc_id().unwrap();
        doc.set("name", NormalValue::String("Bob".to_string()));

        let blockstore = make_test_blockstore();
        let schema_version_id = "schema-v1";

        let result = build_blocks_from_document(&doc, schema_version_id, &blockstore)
            .await
            .unwrap();

        // Get the field block
        let field_cid = &result.field_cids[0];
        let field_bytes = blockstore.get(field_cid).await.unwrap().unwrap();

        // Decode and verify it's an LWW block
        let field_block = Block::from_dag_cbor(&field_bytes).unwrap();
        match &field_block.delta {
            CrdtDelta::Lww(payload) => {
                assert_eq!(payload.field_name, "name");
                assert_eq!(payload.schema_version_id, schema_version_id);
                assert_eq!(payload.priority, 1);
            }
            _ => panic!("Expected LWW delta"),
        }
    }

    #[tokio::test]
    async fn test_composite_block_has_field_links() {
        let mut doc = Document::new();
        doc.generate_and_set_doc_id().unwrap();
        doc.set("name", NormalValue::String("Charlie".to_string()));
        doc.set("age", NormalValue::Int(25));

        let blockstore = make_test_blockstore();

        let result = build_blocks_from_document(&doc, "schema-v1", &blockstore)
            .await
            .unwrap();

        // Decode the composite block
        let composite_block = Block::from_dag_cbor(&result.block).unwrap();

        // Verify it's a Composite delta
        match &composite_block.delta {
            CrdtDelta::Composite(payload) => {
                assert_eq!(payload.status, 1); // Active
                assert_eq!(payload.priority, 1);
            }
            _ => panic!("Expected Composite delta"),
        }

        // Verify links to field blocks
        let links = composite_block.links.as_ref().expect("Should have links");
        assert_eq!(links.len(), 2);

        // Links should reference field CIDs
        let link_cids: Vec<Cid> = links.iter().map(|l| l.link).collect();
        for field_cid in &result.field_cids {
            assert!(link_cids.contains(field_cid));
        }
    }
}
