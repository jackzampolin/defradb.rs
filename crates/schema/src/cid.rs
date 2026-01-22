//! CID (Content Identifier) generation for schema definitions.
//!
//! This module generates CIDs compatible with Go DefraDB's IPLD block format.
//! Uses defra-core's Block type with serde_ipld_dagcbor for proper DAG-CBOR encoding.

use cid::Cid;
use defra_core::{
    Block, CollectionDefinitionDeltaPayload, CrdtDelta, DAGLink, FieldDefinitionDeltaPayload,
    DAG_CBOR_CODEC, SHA2_256_CODE,
};
use multihash::MultihashGeneric;
use sha2::{Digest, Sha256};

use crate::{FieldDescription, FieldKind};

/// Generates a CID for a field definition with priority=1.
///
/// This matches Go's field definition block structure using defra-core's Block type.
/// For collections with multiple fields, use `generate_field_cid_with_priority` instead
/// to match Go's behavior of incrementing priorities (1, 2, 3, ...).
pub fn generate_field_cid(field: &FieldDescription) -> crate::Result<Cid> {
    generate_field_cid_with_priority(field, 1)
}

/// Generates a CID for a field definition with a specific priority.
///
/// Go assigns incrementing priorities to each field block (1 for first, 2 for second, etc).
/// The priority affects the CID, so it must match Go's assignment order.
pub fn generate_field_cid_with_priority(
    field: &FieldDescription,
    priority: u64,
) -> crate::Result<Cid> {
    let delta = field_to_delta_with_priority(field, priority)?;
    let block = Block::new(CrdtDelta::FieldDefinition(delta), vec![], vec![]);
    generate_block_cid(&block)
}

/// Generates a CID for a collection definition with priority = num_fields + 1.
///
/// This matches Go's collection definition block structure using defra-core's Block type.
/// Field CIDs are included as DAGLinks (matching Go's behavior where collection blocks
/// link to their field definition blocks).
///
/// NOTE: This calculates priority as (num_fields + 1). For Go AddSchema compatibility,
/// use `generate_collection_cid_with_priority` with priority=1 instead.
pub fn generate_collection_cid(name: &str, field_cids: &[Cid]) -> crate::Result<Cid> {
    // Priority = num_fields + 1 (legacy behavior)
    let priority = (field_cids.len() as u64) + 1;
    generate_collection_cid_with_priority(name, field_cids, priority)
}

/// Generates a CID for a collection definition with a specific priority.
///
/// This matches Go's collection definition block structure using defra-core's Block type.
/// Field CIDs are included as DAGLinks (matching Go's behavior where collection blocks
/// link to their field definition blocks).
///
/// IMPORTANT: Go's actual AddSchema uses priority=1 for ALL blocks (both field and collection),
/// not incrementing priorities. Use priority=1 for Go interoperability.
pub fn generate_collection_cid_with_priority(
    name: &str,
    field_cids: &[Cid],
    priority: u64,
) -> crate::Result<Cid> {
    let delta = CollectionDefinitionDeltaPayload::new(priority).with_name(name);

    // Convert field CIDs to DAGLinks (Go uses empty string as the link name for field definitions)
    let links: Vec<DAGLink> = field_cids
        .iter()
        .map(|cid| DAGLink::new("", *cid))
        .collect();

    let block = Block::new(CrdtDelta::CollectionDefinition(delta), vec![], links);
    generate_block_cid(&block)
}

/// Generates a CID from a Block using DAG-CBOR encoding.
fn generate_block_cid(block: &Block) -> crate::Result<Cid> {
    // Serialize to DAG-CBOR using serde_ipld_dagcbor
    let cbor_bytes = block
        .to_dag_cbor()
        .map_err(|e| crate::SchemaError::CidGeneration(e.to_string()))?;

    // Hash with SHA2-256
    let mut hasher = Sha256::new();
    hasher.update(&cbor_bytes);
    let hash_bytes = hasher.finalize();

    // Create multihash (CID crate uses MultihashGeneric<64>)
    let mh: MultihashGeneric<64> = MultihashGeneric::wrap(SHA2_256_CODE, &hash_bytes)
        .map_err(|e| crate::SchemaError::CidGeneration(e.to_string()))?;

    // Create CIDv1 with DAG-CBOR codec
    let cid = Cid::new_v1(DAG_CBOR_CODEC, mh);
    Ok(cid)
}

/// Convert a FieldDescription to a FieldDefinitionDeltaPayload with a specific priority
fn field_to_delta_with_priority(
    field: &FieldDescription,
    priority: u64,
) -> crate::Result<FieldDefinitionDeltaPayload> {
    let mut delta = FieldDefinitionDeltaPayload::new(priority)
        .with_name(&field.name)
        .with_crdt(field.crdt_type as u8);

    match &field.kind {
        FieldKind::Scalar(k) => {
            delta = delta.with_scalar_kind(*k as u8);
        }
        FieldKind::ScalarArray(k) => {
            delta = delta.with_scalar_kind(*k as u8);
        }
        FieldKind::Relation { collection_id, .. } => {
            delta = delta.with_collection_id(collection_id);
        }
        FieldKind::SelfRef { relative_id, .. } => {
            if !relative_id.is_empty() {
                let rel_id = relative_id.parse::<i32>().map_err(|e| {
                    crate::SchemaError::CidGeneration(format!(
                        "Invalid relative_id '{}': {}",
                        relative_id, e
                    ))
                })?;
                delta = delta.with_relative_id(rel_id);
            }
        }
        FieldKind::Named { .. } => {}
    }

    Ok(delta)
}
