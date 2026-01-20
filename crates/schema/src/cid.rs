//! CID (Content Identifier) generation for schema definitions.
//!
//! This module generates CIDs compatible with Go DefraDB's IPLD block format.
//! Uses defra-core's Block type with serde_ipld_dagcbor for proper DAG-CBOR encoding.

use cid::Cid;
use defra_core::{
    Block, CollectionDefinitionDeltaPayload, CrdtDelta, FieldDefinitionDeltaPayload,
    DAG_CBOR_CODEC, SHA2_256_CODE,
};
use multihash::MultihashGeneric;
use sha2::{Digest, Sha256};

use crate::{FieldDescription, FieldKind};

/// Generates a CID for a field definition.
///
/// This matches Go's field definition block structure using defra-core's Block type.
pub fn generate_field_cid(field: &FieldDescription) -> crate::Result<Cid> {
    let delta = field_to_delta(field)?;
    let block = Block::new(CrdtDelta::FieldDefinition(delta), vec![], vec![]);
    generate_block_cid(&block)
}

/// Generates a CID for a collection definition.
///
/// This matches Go's collection definition block structure using defra-core's Block type.
pub fn generate_collection_cid(name: &str, _field_cids: &[Cid]) -> crate::Result<Cid> {
    let delta = CollectionDefinitionDeltaPayload::new(1).with_name(name);
    let block = Block::new(CrdtDelta::CollectionDefinition(delta), vec![], vec![]);
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

/// Convert a FieldDescription to a FieldDefinitionDeltaPayload
fn field_to_delta(field: &FieldDescription) -> crate::Result<FieldDefinitionDeltaPayload> {
    let mut delta = FieldDefinitionDeltaPayload::new(1)
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
