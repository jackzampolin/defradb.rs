//! CID (Content Identifier) generation for schema definitions.
//!
//! This module generates CIDs compatible with Go DefraDB's IPLD block format.
//! The format uses DAG-CBOR encoding with CIDv1 and SHA2-256 hashing.
//!
//! Key compatibility requirements:
//! - CBOR map keys must be in alphabetical order (serde_cbor does this)
//! - IPLD uses keyed union representation: {"fieldDefinition": {...}}
//! - Block structure: {"delta": {"fieldDefinition": {...}}}

use cid::Cid;
use multihash::Multihash;
use serde::ser::{SerializeMap, Serializer};
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
    let block = FieldBlock { delta };
    generate_cid(&block)
}

/// Generates a CID for a collection definition.
///
/// This matches Go's collection definition block structure:
/// - Block { delta: CRDT { CollectionDefinitionDelta }, heads: null, links: null }
pub fn generate_collection_cid(name: &str, _field_cids: &[Cid]) -> crate::Result<Cid> {
    let delta = CollectionDefinitionDelta::new(name);
    let block = CollectionBlock { delta };
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

/// Block containing a field definition delta.
/// Serializes to: {"delta": {"fieldDefinition": {...}}}
struct FieldBlock {
    delta: FieldDefinitionDelta,
}

impl Serialize for FieldBlock {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Outer block: {"delta": ...}
        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry("delta", &FieldCrdtWrapper(&self.delta))?;
        map.end()
    }
}

/// CRDT keyed union wrapper for field definition.
/// Serializes to: {"fieldDefinition": {...}}
struct FieldCrdtWrapper<'a>(&'a FieldDefinitionDelta);

impl Serialize for FieldCrdtWrapper<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry("fieldDefinition", self.0)?;
        map.end()
    }
}

/// Block containing a collection definition delta.
/// Serializes to: {"delta": {"collectionDefinition": {...}}}
struct CollectionBlock {
    delta: CollectionDefinitionDelta,
}

impl Serialize for CollectionBlock {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry("delta", &CollectionCrdtWrapper(&self.delta))?;
        map.end()
    }
}

/// CRDT keyed union wrapper for collection definition.
/// Serializes to: {"collectionDefinition": {...}}
struct CollectionCrdtWrapper<'a>(&'a CollectionDefinitionDelta);

impl Serialize for CollectionCrdtWrapper<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry("collectionDefinition", self.0)?;
        map.end()
    }
}

/// Field definition delta matching Go's crdt.FieldDefinitionDelta.
/// Fields must serialize in alphabetical order (crdt, collectionID, name, priority, relativeID, scalarKind).
#[derive(Debug, Clone)]
struct FieldDefinitionDelta {
    crdt: Option<u8>,
    collection_id: Option<String>,
    name: Option<String>,
    priority: u64,
    relative_id: Option<i32>,
    scalar_kind: Option<u8>,
}

impl Serialize for FieldDefinitionDelta {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Count non-None fields plus priority (always present)
        let mut count = 1; // priority is always present
        if self.crdt.is_some() {
            count += 1;
        }
        if self.collection_id.is_some() {
            count += 1;
        }
        if self.name.is_some() {
            count += 1;
        }
        if self.relative_id.is_some() {
            count += 1;
        }
        if self.scalar_kind.is_some() {
            count += 1;
        }

        let mut map = serializer.serialize_map(Some(count))?;

        // CBOR canonical ordering: shorter keys first, then lexicographic within same length
        // Key lengths: crdt(4), name(4), priority(8), relativeID(10), scalarKind(10), collectionID(12)
        // Order: crdt, name, priority, relativeID, scalarKind, collectionID

        // Length 4: crdt, name (lexicographic: crdt < name)
        if let Some(crdt) = self.crdt {
            map.serialize_entry("crdt", &crdt)?;
        }
        if let Some(ref name) = self.name {
            map.serialize_entry("name", name)?;
        }
        // Length 8: priority
        map.serialize_entry("priority", &self.priority)?;
        // Length 10: relativeID, scalarKind (lexicographic: relativeID < scalarKind)
        if let Some(rid) = self.relative_id {
            map.serialize_entry("relativeID", &rid)?;
        }
        if let Some(sk) = self.scalar_kind {
            map.serialize_entry("scalarKind", &sk)?;
        }
        // Length 12: collectionID
        if let Some(ref cid) = self.collection_id {
            map.serialize_entry("collectionID", cid)?;
        }

        map.end()
    }
}

impl FieldDefinitionDelta {
    fn from_field(field: &FieldDescription) -> Self {
        let (scalar_kind, collection_id, relative_id) = match &field.kind {
            FieldKind::Scalar(k) => (Some(*k as u8), None, None),
            FieldKind::ScalarArray(k) => (Some(*k as u8), None, None),
            FieldKind::Relation { collection_id, .. } => (None, Some(collection_id.clone()), None),
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
#[derive(Debug, Clone)]
struct CollectionDefinitionDelta {
    name: Option<String>,
    priority: u64,
}

impl Serialize for CollectionDefinitionDelta {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut count = 1; // priority always present
        if self.name.is_some() {
            count += 1;
        }

        let mut map = serializer.serialize_map(Some(count))?;

        // Alphabetical: name, priority
        if let Some(ref name) = self.name {
            map.serialize_entry("name", name)?;
        }
        map.serialize_entry("priority", &self.priority)?;

        map.end()
    }
}

impl CollectionDefinitionDelta {
    fn new(name: &str) -> Self {
        Self {
            priority: 1,
            name: Some(name.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CType;

    // Go-generated CID fixtures for verification
    // Generated by: go test -v -run TestGenerateCIDFixtures ./client/
    const GO_CID_FIELD_DOCID: &str = "bafyreibmf7lqchqcal3j6zupiwo5fkn2ax7n25wszdygsibv7ybgdi6cny";
    const GO_CID_FIELD_STRING: &str = "bafyreiaezc5g33yzhyzcgbyiv476lovyztyoliotzksdfogoep5ktgpedq";
    const GO_CID_FIELD_INT: &str = "bafyreiefugdbpc563kqcjoe4pjrgavtv3ysbhpzp5smdwb4stxeldmronm";
    const GO_CID_FIELD_RELATION: &str = "bafyreibzyoyhnkjp3byipqvvpplbgxq5c5aeiy5zdu773gb7zbxdmsvlym";
    const GO_CID_COLLECTION_USERS: &str = "bafyreiazcyzp3lzapqxuf3b6pw6himmcun65ljkaxmgsbxngdfrcbcfg7e";

    #[test]
    fn test_field_docid_cid_matches_go() {
        let field = FieldDescription::new("1", "_docID", FieldKind::doc_id());
        let cid = generate_field_cid(&field).unwrap();
        assert_eq!(
            cid.to_string(),
            GO_CID_FIELD_DOCID,
            "Rust CID for _docID field should match Go"
        );
    }

    #[test]
    fn test_field_string_cid_matches_go() {
        let field = FieldDescription::new("1", "name", FieldKind::string());
        let cid = generate_field_cid(&field).unwrap();
        assert_eq!(
            cid.to_string(),
            GO_CID_FIELD_STRING,
            "Rust CID for string field should match Go"
        );
    }

    #[test]
    fn test_field_int_cid_matches_go() {
        let field = FieldDescription::new("1", "age", FieldKind::int());
        let cid = generate_field_cid(&field).unwrap();
        assert_eq!(
            cid.to_string(),
            GO_CID_FIELD_INT,
            "Rust CID for int field should match Go"
        );
    }

    #[test]
    fn test_field_relation_cid_matches_go() {
        let field =
            FieldDescription::new("1", "author", FieldKind::relation("bafkreiusers123", false))
                .with_crdt_type(CType::Object);
        let cid = generate_field_cid(&field).unwrap();
        assert_eq!(
            cid.to_string(),
            GO_CID_FIELD_RELATION,
            "Rust CID for relation field should match Go"
        );
    }

    #[test]
    fn test_collection_cid_matches_go() {
        let cid = generate_collection_cid("users", &[]).unwrap();
        assert_eq!(
            cid.to_string(),
            GO_CID_COLLECTION_USERS,
            "Rust CID for collection should match Go"
        );
    }

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
    fn test_self_ref_field_cid() {
        let kind = FieldKind::SelfRef {
            relative_id: "0".to_string(),
            is_array: false,
        };
        let field = FieldDescription::new("1", "parent", kind).with_crdt_type(CType::Object);

        let cid = generate_field_cid(&field).unwrap();
        assert_eq!(cid.version(), cid::Version::V1);
    }

    #[test]
    fn test_cid_string_format() {
        let field = FieldDescription::new("1", "_docID", FieldKind::doc_id());
        let cid = generate_field_cid(&field).unwrap();

        let cid_str = cid.to_string();
        // CIDv1 base32 strings start with 'bafy'
        assert!(
            cid_str.starts_with("bafy"),
            "CID should be base32 encoded: {}",
            cid_str
        );
    }

    #[test]
    fn test_cbor_output_for_debugging() {
        // This test outputs CBOR hex for debugging purposes
        let delta = FieldDefinitionDelta {
            priority: 1,
            name: Some("_docID".to_string()),
            crdt: Some(1),
            scalar_kind: Some(1),
            collection_id: None,
            relative_id: None,
        };
        let block = FieldBlock { delta };
        let cbor_bytes = serde_cbor::to_vec(&block).unwrap();
        let hex: String = cbor_bytes.iter().map(|b| format!("{:02x}", b)).collect();
        println!("Rust CBOR hex: {}", hex);

        // Go's expected hex for comparison:
        // a16564656c7461a16f6669656c64446566696e6974696f6ea4646372647401646e616d65665f646f634944687072696f72697479016a7363616c61724b696e6401
    }
}
