//! CID (Content Identifier) generation for schema definitions.
//!
//! This module generates CIDs compatible with Go DefraDB's IPLD block format.
//! The format uses DAG-CBOR encoding with CIDv1 and SHA2-256 hashing.
//!
//! Key compatibility requirements:
//! - CBOR field ordering must be deterministic (lowercase, alphabetical)
//! - IPLD schema field names use lowercase (not PascalCase like JSON)
//! - Block structure wraps delta with optional links/heads

use cid::Cid;
use multihash::Multihash;
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::{FieldDescription, FieldKind};

/// CID codec for DAG-CBOR (matches Go's multicodec.DagCbor)
const DAG_CBOR_CODEC: u64 = 0x71;

/// Multihash code for SHA2-256 (matches Go's multicodec.Sha2_256)
const SHA2_256_CODE: u64 = 0x12;

/// Generates a CID for a field definition.
///
/// This matches Go's field definition block structure:
/// - Block { delta: CRDT { FieldDefinitionDelta }, heads: null, links: null }
pub fn generate_field_cid(field: &FieldDescription) -> crate::Result<Cid> {
    let delta = FieldDefinitionDelta::from_field(field);
    let block = Block::new_field_definition(delta);
    generate_cid(&block)
}

/// Generates a CID for a collection definition.
///
/// This matches Go's collection definition block structure:
/// - Block { delta: CRDT { CollectionDefinitionDelta }, heads: null, links: [field CIDs] }
pub fn generate_collection_cid(
    name: &str,
    field_cids: &[Cid],
) -> crate::Result<Cid> {
    let delta = CollectionDefinitionDelta::new(name);
    let links: Vec<DagLink> = field_cids
        .iter()
        .map(|cid| DagLink {
            name: String::new(),
            link: CidLink { cid: cid.to_string() },
        })
        .collect();
    let block = Block::new_collection_definition(delta, links);
    generate_cid(&block)
}

/// Generates a CID from a serializable block.
fn generate_cid<T: Serialize>(block: &T) -> crate::Result<Cid> {
    // Serialize to CBOR
    let cbor_bytes = serde_cbor::to_vec(block)
        .map_err(|e| crate::SchemaError::CidGeneration(e.to_string()))?;

    // Hash with SHA2-256
    let mut hasher = Sha256::new();
    hasher.update(&cbor_bytes);
    let hash_bytes = hasher.finalize();

    // Create multihash (CID crate uses Multihash<64>)
    let mh: Multihash<64> = Multihash::wrap(SHA2_256_CODE, &hash_bytes)
        .map_err(|e| crate::SchemaError::CidGeneration(e.to_string()))?;

    // Create CIDv1 with DAG-CBOR codec
    let cid = Cid::new_v1(DAG_CBOR_CODEC, mh);
    Ok(cid)
}

/// IPLD Block structure matching Go's coreblock.Block.
/// Field names must be lowercase to match Go's IPLD schema.
#[derive(Debug, Clone, Serialize)]
struct Block {
    delta: CrdtWrapper,
    #[serde(skip_serializing_if = "Option::is_none")]
    heads: Option<Vec<CidLink>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    links: Option<Vec<DagLink>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    encryption: Option<CidLink>,
    #[serde(skip_serializing_if = "Option::is_none")]
    signature: Option<CidLink>,
}

impl Block {
    fn new_field_definition(delta: FieldDefinitionDelta) -> Self {
        Self {
            delta: CrdtWrapper::FieldDefinitionDelta(delta),
            heads: None,
            links: None,
            encryption: None,
            signature: None,
        }
    }

    fn new_collection_definition(delta: CollectionDefinitionDelta, links: Vec<DagLink>) -> Self {
        Self {
            delta: CrdtWrapper::CollectionDefinitionDelta(delta),
            heads: None,
            links: if links.is_empty() { None } else { Some(links) },
            encryption: None,
            signature: None,
        }
    }
}

/// CRDT union type matching Go's crdt.CRDT.
/// Go uses IPLD schema union representation.
#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
enum CrdtWrapper {
    FieldDefinitionDelta(FieldDefinitionDelta),
    CollectionDefinitionDelta(CollectionDefinitionDelta),
}

/// DAG link structure matching Go's coreblock.DAGLink.
#[derive(Debug, Clone, Serialize)]
struct DagLink {
    name: String,
    link: CidLink,
}

/// CID link structure for IPLD.
#[derive(Debug, Clone, Serialize)]
struct CidLink {
    #[serde(rename = "/")]
    cid: String,
}

/// Field definition delta matching Go's crdt.FieldDefinitionDelta.
/// IPLD schema field names are lowercase.
#[derive(Debug, Clone, Serialize)]
struct FieldDefinitionDelta {
    priority: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    crdt: Option<u8>,
    #[serde(rename = "scalarKind", skip_serializing_if = "Option::is_none")]
    scalar_kind: Option<u8>,
    #[serde(rename = "collectionID", skip_serializing_if = "Option::is_none")]
    collection_id: Option<String>,
    #[serde(rename = "relativeID", skip_serializing_if = "Option::is_none")]
    relative_id: Option<i32>,
}

impl FieldDefinitionDelta {
    fn from_field(field: &FieldDescription) -> Self {
        let (scalar_kind, collection_id, relative_id) = match &field.kind {
            FieldKind::Scalar(k) => (Some(*k as u8), None, None),
            FieldKind::ScalarArray(k) => (Some(*k as u8), None, None),
            FieldKind::Relation { collection_id, .. } => {
                (None, Some(collection_id.clone()), None)
            }
            FieldKind::SelfRef { relative_id, .. } => {
                let rel_id = if relative_id.is_empty() {
                    None
                } else {
                    relative_id.parse::<i32>().ok()
                };
                (None, None, rel_id)
            }
            FieldKind::Named { .. } => (None, None, None),
        };

        Self {
            priority: 1, // Default priority for new fields
            name: Some(field.name.clone()),
            crdt: Some(field.crdt_type as u8),
            scalar_kind,
            collection_id,
            relative_id,
        }
    }
}

/// Collection definition delta matching Go's crdt.CollectionDefinitionDelta.
#[derive(Debug, Clone, Serialize)]
struct CollectionDefinitionDelta {
    priority: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(rename = "querySelect", skip_serializing_if = "Option::is_none")]
    query_select: Option<Vec<u8>>,
    #[serde(rename = "queryTransform", skip_serializing_if = "Option::is_none")]
    query_transform: Option<CidLink>,
}

impl CollectionDefinitionDelta {
    fn new(name: &str) -> Self {
        Self {
            priority: 1,
            name: Some(name.to_string()),
            query_select: None,
            query_transform: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CType;

    #[test]
    fn test_field_cid_generation() {
        let field = FieldDescription::new("1", "_docID", FieldKind::doc_id());
        let cid = generate_field_cid(&field).unwrap();

        // CID should be a valid CIDv1
        assert_eq!(cid.version(), cid::Version::V1);
        assert_eq!(cid.codec(), DAG_CBOR_CODEC);

        // Same field should produce same CID (deterministic)
        let cid2 = generate_field_cid(&field).unwrap();
        assert_eq!(cid, cid2);
    }

    #[test]
    fn test_collection_cid_generation() {
        let field1 = FieldDescription::new("1", "_docID", FieldKind::doc_id());
        let field2 = FieldDescription::new("2", "name", FieldKind::string());

        let cid1 = generate_field_cid(&field1).unwrap();
        let cid2 = generate_field_cid(&field2).unwrap();

        let collection_cid = generate_collection_cid("users", &[cid1, cid2]).unwrap();

        assert_eq!(collection_cid.version(), cid::Version::V1);
        assert_eq!(collection_cid.codec(), DAG_CBOR_CODEC);
    }

    #[test]
    fn test_different_fields_produce_different_cids() {
        let field1 = FieldDescription::new("1", "name", FieldKind::string());
        let field2 = FieldDescription::new("2", "age", FieldKind::int());

        let cid1 = generate_field_cid(&field1).unwrap();
        let cid2 = generate_field_cid(&field2).unwrap();

        assert_ne!(cid1, cid2);
    }

    #[test]
    fn test_relation_field_cid() {
        let field = FieldDescription::new("1", "author", FieldKind::relation("users-v1", false))
            .with_crdt_type(CType::Object);

        let cid = generate_field_cid(&field).unwrap();
        assert_eq!(cid.version(), cid::Version::V1);
    }

    #[test]
    fn test_self_ref_field_cid() {
        let kind = FieldKind::SelfRef {
            relative_id: "0".to_string(),
            is_array: false,
        };
        let field = FieldDescription::new("1", "parent", kind)
            .with_crdt_type(CType::Object);

        let cid = generate_field_cid(&field).unwrap();
        assert_eq!(cid.version(), cid::Version::V1);
    }

    #[test]
    fn test_cid_string_format() {
        let field = FieldDescription::new("1", "_docID", FieldKind::doc_id());
        let cid = generate_field_cid(&field).unwrap();

        let cid_str = cid.to_string();
        // CIDv1 base32 strings start with 'bafy'
        assert!(cid_str.starts_with("bafy"), "CID should be base32 encoded: {}", cid_str);
    }
}
