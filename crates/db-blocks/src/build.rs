use super::*;
use blockstore::Blockstore;
use defra_core::block::generate_cid_from_bytes;
use std::sync::Arc;

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

    // Create a block for each field (LWW or Counter depending on CRDT type)
    for (field_name, field_value) in doc.values() {
        // Skip _docID (stored as the block key, not a CRDT field).
        // Other _-prefixed fields like _ownerID (foreign keys) DO get blocks.
        if field_name == "_docID" {
            continue;
        }

        // Encode the field value as CBOR
        let value_bytes = encode_value_as_cbor(field_value.value())?;

        // Check if this field is a Counter type
        let is_counter = doc
            .fields()
            .get(field_name)
            .map(|f| f.crdt_type().is_counter())
            .unwrap_or(false);

        // Create the appropriate delta based on CRDT type
        let delta = if is_counter {
            CrdtDelta::Counter(CounterDeltaPayload {
                doc_id: doc_id_bytes.clone(),
                field_name: field_name.clone(),
                priority: 1, // Initial priority for new documents
                nonce: 0,    // Go uses nonce=0 for initial creation (deterministic CID)
                schema_version_id: schema_version_id.to_string(),
                data: value_bytes,
            })
        } else {
            CrdtDelta::Lww(LwwDeltaPayload {
                doc_id: doc_id_bytes.clone(),
                field_name: field_name.clone(),
                priority: 1, // Initial priority for new documents
                schema_version_id: schema_version_id.to_string(),
                data: value_bytes,
            })
        };

        // Create the field block
        let field_block = Block::new(delta, vec![], vec![]);

        // Serialize once, then derive CID from the same bytes
        let field_block_bytes = field_block
            .to_dag_cbor()
            .map_err(|e| format!("Failed to encode field block: {}", e))?;
        let field_cid = generate_cid_from_bytes(&field_block_bytes)
            .map_err(|e| format!("Failed to generate field CID: {}", e))?;

        // Store the field block in the blockstore
        blockstore
            .put(&field_cid, &field_block_bytes)
            .await
            .map_err(|e| format!("Failed to store field block: {}", e))?;

        tracing::debug!(
            field_name = %field_name,
            cid = %field_cid,
            is_counter = is_counter,
            "Stored field block"
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

    // Serialize once, then derive CID from the same bytes
    let composite_bytes = composite_block
        .to_dag_cbor()
        .map_err(|e| format!("Failed to encode composite block: {}", e))?;
    let composite_cid = generate_cid_from_bytes(&composite_bytes)
        .map_err(|e| format!("Failed to generate composite CID: {}", e))?;

    // Store the composite block
    blockstore
        .put(&composite_cid, &composite_bytes)
        .await
        .map_err(|e| format!("Failed to store composite block: {}", e))?;

    tracing::debug!(
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

/// **WARNING**: This creates raw document CBOR, not a proper Block structure.
/// Go DefraDB cannot parse this format. Use `build_blocks_from_document` instead.
///
/// This function is kept for backward compatibility with non-P2P code paths.
#[deprecated(since = "0.1.0", note = "Use build_blocks_from_document for P2P sync")]
pub fn build_block_from_document(doc: &Document) -> Result<BlockResult, String> {
    use multihash::Multihash;
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

    let mh = Multihash::wrap(SHA2_256_CODE, &digest)
        .map_err(|e| format!("Failed to create multihash: {}", e))?;

    let cid = Cid::new_v1(DAG_CBOR_CODEC, mh);

    Ok(BlockResult {
        cid,
        block,
        doc_id,
        field_cids: vec![],
    })
}
