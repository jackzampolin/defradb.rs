//! IPLD Block types and operations for DefraDB
//!
//! This module provides Go-compatible Block structures with DAG-CBOR serialization.
//! All types match the Go implementation in `internal/core/block/` for wire compatibility.

use cid::Cid;
use multihash::Multihash;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{Error, Result};

/// DAG-CBOR codec identifier (multicodec 0x71)
pub const DAG_CBOR_CODEC: u64 = 0x71;

/// SHA2-256 multihash code
pub const SHA2_256_CODE: u64 = 0x12;

// ============================================================================
// Block - The fundamental unit of content-addressed storage
// ============================================================================

/// IPLD Block for DefraDB - content-addressed data with CRDT deltas
///
/// Matches Go's `internal/core/block/block.go:Block` structure exactly for
/// wire compatibility. Uses DAG-CBOR serialization with deterministic field ordering.
///
/// # Serialization
///
/// - Empty slices become `None` for space efficiency (omitted in CBOR)
/// - Heads and links are sorted lexicographically by CID string
/// - Uses CIDv1 with DAG-CBOR codec (0x71) and SHA2-256 hash
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Block {
    /// CRDT delta payload
    pub delta: CrdtDelta,

    /// Previous block CIDs (sorted lexicographically by string)
    ///
    /// `None` = nil (omitted in CBOR), `Some(vec![])` would be empty array
    /// but is normalized to `None` in constructor for space efficiency.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub heads: Option<Vec<Cid>>,

    /// Named links to other blocks (sorted lexicographically by CID)
    ///
    /// Used for field-level links in composite blocks. Empty for field-level blocks.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub links: Option<Vec<DAGLink>>,

    /// Optional link to encryption metadata block
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encryption: Option<Cid>,

    /// Optional link to signature block
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<Cid>,
}

impl Block {
    /// Create a new block with sorted heads and links
    ///
    /// Matches Go's `New()` behavior:
    /// - Sorts heads lexicographically by CID string
    /// - Sorts links lexicographically by CID string
    /// - Converts empty slices to `None` for space efficiency
    pub fn new(delta: CrdtDelta, heads: Vec<Cid>, links: Vec<DAGLink>) -> Self {
        // Sort and normalize heads
        let mut sorted_heads = heads;
        sorted_heads.sort_by_key(|a| a.to_string());
        let heads = if sorted_heads.is_empty() {
            None
        } else {
            Some(sorted_heads)
        };

        // Sort and normalize links
        let mut sorted_links = links;
        sorted_links.sort();
        let links = if sorted_links.is_empty() {
            None
        } else {
            Some(sorted_links)
        };

        Self {
            delta,
            heads,
            links,
            encryption: None,
            signature: None,
        }
    }

    /// Create a block with encryption and/or signature
    pub fn new_with_options(
        delta: CrdtDelta,
        heads: Vec<Cid>,
        links: Vec<DAGLink>,
        encryption: Option<Cid>,
        signature: Option<Cid>,
    ) -> Self {
        let mut block = Self::new(delta, heads, links);
        block.encryption = encryption;
        block.signature = signature;
        block
    }

    /// Serialize to DAG-CBOR bytes
    pub fn to_dag_cbor(&self) -> Result<Vec<u8>> {
        serde_ipld_dagcbor::to_vec(self).map_err(|e| Error::Serialization(e.to_string()))
    }

    /// Deserialize from DAG-CBOR bytes
    pub fn from_dag_cbor(bytes: &[u8]) -> Result<Self> {
        serde_ipld_dagcbor::from_slice(bytes).map_err(|e| Error::Serialization(e.to_string()))
    }

    /// Generate CID from block content
    ///
    /// Uses CIDv1 with DAG-CBOR codec (0x71) and SHA2-256 hash.
    /// Equivalent to Go's `GenerateLink()`.
    pub fn generate_cid(&self) -> Result<Cid> {
        let bytes = self.to_dag_cbor()?;
        generate_cid_from_bytes(&bytes)
    }

    /// Get all CID links in this block (heads + named links)
    ///
    /// Returns heads first, then named links (in order).
    /// Matches Go's `AllLinks()` method.
    pub fn all_links(&self) -> Vec<Cid> {
        let mut links = Vec::new();

        if let Some(ref heads) = self.heads {
            links.extend(heads.iter().cloned());
        }

        if let Some(ref dag_links) = self.links {
            links.extend(dag_links.iter().map(|l| l.link));
        }

        links
    }

    /// Get a specific named link by name
    ///
    /// Matches Go's `GetLinkByName()` method.
    pub fn get_link_by_name(&self, name: &str) -> Option<&Cid> {
        self.links
            .as_ref()?
            .iter()
            .find(|l| l.name == name)
            .map(|l| &l.link)
    }

    /// Check if this block has encryption
    pub fn is_encrypted(&self) -> bool {
        self.encryption.is_some()
    }

    /// Check if this block is signed
    pub fn is_signed(&self) -> bool {
        self.signature.is_some()
    }
}

// ============================================================================
// DAGLink - Named link to another block
// ============================================================================

/// Link to another block with a name
///
/// Matches Go's `internal/core/block/block.go:DAGLink`.
/// The name is typically a field name or "_head" for composite head links.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DAGLink {
    /// Field name or "_head" for composite head
    pub name: String,

    /// CID of the linked block
    pub link: Cid,
}

impl DAGLink {
    /// Create a new DAGLink
    pub fn new(name: impl Into<String>, link: Cid) -> Self {
        Self {
            name: name.into(),
            link,
        }
    }
}

impl PartialOrd for DAGLink {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DAGLink {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Sort by link CID string (matching Go behavior)
        self.link.to_string().cmp(&other.link.to_string())
    }
}

// ============================================================================
// CrdtDelta - CRDT delta payloads embedded in blocks
// ============================================================================

/// CRDT delta types that can be embedded in a Block
///
/// Matches Go's `crdt.CRDT` IPLD union with "keyed" representation.
/// Serializes as `{"lww": {...}}` or `{"counter": {...}}` etc.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CrdtDelta {
    /// LWW Register delta
    #[serde(rename = "lww")]
    Lww(LwwDeltaPayload),

    /// Counter delta
    #[serde(rename = "counter")]
    Counter(CounterDeltaPayload),

    /// Document composite delta
    #[serde(rename = "composite")]
    Composite(CompositeDeltaPayload),

    /// Collection delta
    #[serde(rename = "collection")]
    Collection(CollectionDeltaPayload),
}

impl CrdtDelta {
    /// Get the priority of this delta
    pub fn priority(&self) -> u64 {
        match self {
            CrdtDelta::Lww(d) => d.priority,
            CrdtDelta::Counter(d) => d.priority,
            CrdtDelta::Composite(d) => d.priority,
            CrdtDelta::Collection(d) => d.priority,
        }
    }

    /// Set the priority of this delta
    pub fn set_priority(&mut self, priority: u64) {
        match self {
            CrdtDelta::Lww(d) => d.priority = priority,
            CrdtDelta::Counter(d) => d.priority = priority,
            CrdtDelta::Composite(d) => d.priority = priority,
            CrdtDelta::Collection(d) => d.priority = priority,
        }
    }

    /// Get the document ID (if present)
    ///
    /// Note: CollectionDelta does not have a doc_id, so returns None for that type.
    pub fn doc_id(&self) -> Option<&[u8]> {
        match self {
            CrdtDelta::Lww(d) => Some(&d.doc_id),
            CrdtDelta::Counter(d) => Some(&d.doc_id),
            CrdtDelta::Composite(d) => Some(&d.doc_id),
            CrdtDelta::Collection(_) => None,
        }
    }
}

/// LWW Register delta payload for block embedding
///
/// Matches Go's `crdt.LWWDelta` structure.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LwwDeltaPayload {
    /// Document ID
    #[serde(rename = "docID", with = "serde_bytes")]
    pub doc_id: Vec<u8>,

    /// Field name
    #[serde(rename = "fieldName")]
    pub field_name: String,

    /// Priority for conflict resolution
    pub priority: u64,

    /// Schema version identifier
    #[serde(rename = "schemaVersionID")]
    pub schema_version_id: String,

    /// The value data (empty = deletion/tombstone)
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
}

/// Counter delta payload for block embedding
///
/// Matches Go's `crdt.CounterDelta` structure.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CounterDeltaPayload {
    /// Document ID
    #[serde(rename = "docID", with = "serde_bytes")]
    pub doc_id: Vec<u8>,

    /// Field name
    #[serde(rename = "fieldName")]
    pub field_name: String,

    /// Priority
    pub priority: u64,

    /// Nonce for idempotency
    pub nonce: i64,

    /// Schema version identifier
    #[serde(rename = "schemaVersionID")]
    pub schema_version_id: String,

    /// Increment/decrement value (encoded)
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
}

/// Composite delta payload for block embedding
///
/// Matches Go's `crdt.DocCompositeDelta` structure.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompositeDeltaPayload {
    /// Document ID
    #[serde(rename = "docID", with = "serde_bytes")]
    pub doc_id: Vec<u8>,

    /// Schema version identifier
    #[serde(rename = "schemaVersionID")]
    pub schema_version_id: String,

    /// Priority
    pub priority: u64,

    /// Document status (1 = active, 2 = deleted)
    #[serde(default)]
    pub status: u8,
}

/// Collection delta payload for block embedding
///
/// Matches Go's `crdt.CollectionDelta` structure.
/// Note: CollectionDelta does NOT have a docID field (unlike other delta types).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CollectionDeltaPayload {
    /// Schema version identifier
    #[serde(rename = "schemaVersionID")]
    pub schema_version_id: String,

    /// Priority
    pub priority: u64,
}

// ============================================================================
// Encryption - Encryption metadata block
// ============================================================================

/// Encryption metadata block
///
/// Matches Go's `internal/core/block/encryption.go:Encryption`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Encryption {
    /// Document ID bytes
    #[serde(rename = "docID", with = "serde_bytes")]
    pub doc_id: Vec<u8>,

    /// Field name (None for document-level encryption)
    #[serde(rename = "fieldName", default, skip_serializing_if = "Option::is_none")]
    pub field_name: Option<String>,

    /// Encryption key
    #[serde(with = "serde_bytes")]
    pub key: Vec<u8>,
}

impl Encryption {
    /// Create a new encryption block
    pub fn new(doc_id: Vec<u8>, key: Vec<u8>) -> Self {
        Self {
            doc_id,
            field_name: None,
            key,
        }
    }

    /// Create a field-level encryption block
    pub fn new_for_field(doc_id: Vec<u8>, field_name: String, key: Vec<u8>) -> Self {
        Self {
            doc_id,
            field_name: Some(field_name),
            key,
        }
    }

    /// Serialize to DAG-CBOR bytes
    pub fn to_dag_cbor(&self) -> Result<Vec<u8>> {
        serde_ipld_dagcbor::to_vec(self).map_err(|e| Error::Serialization(e.to_string()))
    }

    /// Deserialize from DAG-CBOR bytes
    pub fn from_dag_cbor(bytes: &[u8]) -> Result<Self> {
        serde_ipld_dagcbor::from_slice(bytes).map_err(|e| Error::Serialization(e.to_string()))
    }

    /// Generate CID for this encryption block
    pub fn generate_cid(&self) -> Result<Cid> {
        let bytes = self.to_dag_cbor()?;
        generate_cid_from_bytes(&bytes)
    }
}

// ============================================================================
// Signature - Block signature
// ============================================================================

/// Signature block
///
/// Matches Go's `internal/core/block/signature.go:Signature`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Signature {
    /// Signature header with algorithm and identity
    pub header: SignatureHeader,

    /// Signature value bytes
    #[serde(with = "serde_bytes")]
    pub value: Vec<u8>,
}

impl Signature {
    /// Create a new signature
    pub fn new(header: SignatureHeader, value: Vec<u8>) -> Self {
        Self { header, value }
    }

    /// Serialize to DAG-CBOR bytes
    pub fn to_dag_cbor(&self) -> Result<Vec<u8>> {
        serde_ipld_dagcbor::to_vec(self).map_err(|e| Error::Serialization(e.to_string()))
    }

    /// Deserialize from DAG-CBOR bytes
    pub fn from_dag_cbor(bytes: &[u8]) -> Result<Self> {
        serde_ipld_dagcbor::from_slice(bytes).map_err(|e| Error::Serialization(e.to_string()))
    }

    /// Generate CID for this signature block
    pub fn generate_cid(&self) -> Result<Cid> {
        let bytes = self.to_dag_cbor()?;
        generate_cid_from_bytes(&bytes)
    }
}

/// Signature header with algorithm type and identity
///
/// Matches Go's `internal/core/block/signature.go:SignatureHeader`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SignatureHeader {
    /// Algorithm type: "ES256K" (secp256k1) or "EdDSA" (Ed25519)
    #[serde(rename = "type")]
    pub sig_type: SignatureType,

    /// Signer identity (public key bytes)
    #[serde(with = "serde_bytes")]
    pub identity: Vec<u8>,
}

impl SignatureHeader {
    /// Create a new signature header
    pub fn new(sig_type: SignatureType, identity: Vec<u8>) -> Self {
        Self { sig_type, identity }
    }
}

/// Signature algorithm types
///
/// Matches Go's signature type constants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SignatureType {
    /// ECDSA with secp256k1 curve
    #[serde(rename = "ES256K")]
    ES256K,

    /// EdDSA with Ed25519 curve
    EdDSA,
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Generate CID from raw bytes using DAG-CBOR codec and SHA2-256
fn generate_cid_from_bytes(bytes: &[u8]) -> Result<Cid> {
    // Hash with SHA2-256
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();

    // Create multihash
    let mh = Multihash::<64>::wrap(SHA2_256_CODE, &digest)
        .map_err(|e| Error::BlockError(format!("Failed to create multihash: {}", e)))?;

    // Create CIDv1 with DAG-CBOR codec
    Ok(Cid::new_v1(DAG_CBOR_CODEC, mh))
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn test_cid() -> Cid {
        Cid::from_str("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi").unwrap()
    }

    fn test_cid2() -> Cid {
        Cid::from_str("bafkreigh2akiscaildcqabsyg3dfr6chu3fgpregiymsck7e7aqa4s52zy").unwrap()
    }

    fn test_lww_delta() -> CrdtDelta {
        CrdtDelta::Lww(LwwDeltaPayload {
            doc_id: b"doc1".to_vec(),
            field_name: "name".to_string(),
            priority: 1,
            schema_version_id: "schema1".to_string(),
            data: b"John".to_vec(),
        })
    }

    #[test]
    fn test_block_dag_cbor_roundtrip() {
        let block = Block::new(test_lww_delta(), vec![], vec![]);

        let bytes = block.to_dag_cbor().unwrap();
        let restored = Block::from_dag_cbor(&bytes).unwrap();

        assert_eq!(block, restored);
    }

    // ========================================================================
    // Go Wire Compatibility Golden Tests (Issue #15)
    // Test vectors generated from Go DefraDB implementation
    // ========================================================================

    // Go test vector: Simple LWW Block
    const GO_LWW_SIMPLE_BYTES: &[u8] = &[
        0xA1, 0x65, 0x64, 0x65, 0x6C, 0x74, 0x61, 0xA1, 0x63, 0x6C, 0x77, 0x77, 0xA5, 0x64, 0x64,
        0x61, 0x74, 0x61, 0x44, 0x4A, 0x6F, 0x68, 0x6E, 0x65, 0x64, 0x6F, 0x63, 0x49, 0x44, 0x44,
        0x64, 0x6F, 0x63, 0x31, 0x68, 0x70, 0x72, 0x69, 0x6F, 0x72, 0x69, 0x74, 0x79, 0x01, 0x69,
        0x66, 0x69, 0x65, 0x6C, 0x64, 0x4E, 0x61, 0x6D, 0x65, 0x64, 0x6E, 0x61, 0x6D, 0x65, 0x6F,
        0x73, 0x63, 0x68, 0x65, 0x6D, 0x61, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6F, 0x6E, 0x49, 0x44,
        0x67, 0x73, 0x63, 0x68, 0x65, 0x6D, 0x61, 0x31,
    ];
    const GO_LWW_SIMPLE_CID: &str = "bafyreigzutct4sl23hifnebryxvgdehhmsh3m5aexej2e2jo3wstq7glxi";

    // Go test vector: LWW Block with higher priority
    const GO_LWW_HIGH_PRIORITY_BYTES: &[u8] = &[
        0xA1, 0x65, 0x64, 0x65, 0x6C, 0x74, 0x61, 0xA1, 0x63, 0x6C, 0x77, 0x77, 0xA5, 0x64, 0x64,
        0x61, 0x74, 0x61, 0x42, 0x18, 0x1E, 0x65, 0x64, 0x6F, 0x63, 0x49, 0x44, 0x44, 0x64, 0x6F,
        0x63, 0x31, 0x68, 0x70, 0x72, 0x69, 0x6F, 0x72, 0x69, 0x74, 0x79, 0x18, 0x64, 0x69, 0x66,
        0x69, 0x65, 0x6C, 0x64, 0x4E, 0x61, 0x6D, 0x65, 0x63, 0x61, 0x67, 0x65, 0x6F, 0x73, 0x63,
        0x68, 0x65, 0x6D, 0x61, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6F, 0x6E, 0x49, 0x44, 0x67, 0x73,
        0x63, 0x68, 0x65, 0x6D, 0x61, 0x31,
    ];
    const GO_LWW_HIGH_PRIORITY_CID: &str =
        "bafyreifj3pxwi7jf2n2qpoetqj2m72sirocaiy4tx4zjle5pttqcttolry";

    // Go test vector: Counter Block
    const GO_COUNTER_BYTES: &[u8] = &[
        0xA1, 0x65, 0x64, 0x65, 0x6C, 0x74, 0x61, 0xA1, 0x67, 0x63, 0x6F, 0x75, 0x6E, 0x74, 0x65,
        0x72, 0xA6, 0x64, 0x64, 0x61, 0x74, 0x61, 0x41, 0x0A, 0x65, 0x64, 0x6F, 0x63, 0x49, 0x44,
        0x44, 0x64, 0x6F, 0x63, 0x31, 0x65, 0x6E, 0x6F, 0x6E, 0x63, 0x65, 0x19, 0x30, 0x39, 0x68,
        0x70, 0x72, 0x69, 0x6F, 0x72, 0x69, 0x74, 0x79, 0x01, 0x69, 0x66, 0x69, 0x65, 0x6C, 0x64,
        0x4E, 0x61, 0x6D, 0x65, 0x65, 0x63, 0x6F, 0x75, 0x6E, 0x74, 0x6F, 0x73, 0x63, 0x68, 0x65,
        0x6D, 0x61, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6F, 0x6E, 0x49, 0x44, 0x67, 0x73, 0x63, 0x68,
        0x65, 0x6D, 0x61, 0x31,
    ];
    const GO_COUNTER_CID: &str = "bafyreiavbuhwh23hcfh2pvgvtnuup6gbtvqamkil3wca46l4xofmzjszn4";

    // Go test vector: Composite Block (active document)
    const GO_COMPOSITE_ACTIVE_BYTES: &[u8] = &[
        0xA1, 0x65, 0x64, 0x65, 0x6C, 0x74, 0x61, 0xA1, 0x69, 0x63, 0x6F, 0x6D, 0x70, 0x6F, 0x73,
        0x69, 0x74, 0x65, 0xA4, 0x65, 0x64, 0x6F, 0x63, 0x49, 0x44, 0x44, 0x64, 0x6F, 0x63, 0x31,
        0x66, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x01, 0x68, 0x70, 0x72, 0x69, 0x6F, 0x72, 0x69,
        0x74, 0x79, 0x01, 0x6F, 0x73, 0x63, 0x68, 0x65, 0x6D, 0x61, 0x56, 0x65, 0x72, 0x73, 0x69,
        0x6F, 0x6E, 0x49, 0x44, 0x67, 0x73, 0x63, 0x68, 0x65, 0x6D, 0x61, 0x31,
    ];
    const GO_COMPOSITE_ACTIVE_CID: &str =
        "bafyreia3owq65zslwtr5qpewkwjbvn3w4pulyo4dy4qdrulm3s5jwthzgm";

    // Go test vector: Composite Block (deleted document)
    const GO_COMPOSITE_DELETED_BYTES: &[u8] = &[
        0xA1, 0x65, 0x64, 0x65, 0x6C, 0x74, 0x61, 0xA1, 0x69, 0x63, 0x6F, 0x6D, 0x70, 0x6F, 0x73,
        0x69, 0x74, 0x65, 0xA4, 0x65, 0x64, 0x6F, 0x63, 0x49, 0x44, 0x44, 0x64, 0x6F, 0x63, 0x31,
        0x66, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x02, 0x68, 0x70, 0x72, 0x69, 0x6F, 0x72, 0x69,
        0x74, 0x79, 0x02, 0x6F, 0x73, 0x63, 0x68, 0x65, 0x6D, 0x61, 0x56, 0x65, 0x72, 0x73, 0x69,
        0x6F, 0x6E, 0x49, 0x44, 0x67, 0x73, 0x63, 0x68, 0x65, 0x6D, 0x61, 0x31,
    ];
    const GO_COMPOSITE_DELETED_CID: &str =
        "bafyreif76cgwj4cuokkk564uniizeqbeqe6tqr3h6nsieidbtjbzecwutu";

    // Go test vector: Collection Block
    const GO_COLLECTION_BYTES: &[u8] = &[
        0xA1, 0x65, 0x64, 0x65, 0x6C, 0x74, 0x61, 0xA1, 0x6A, 0x63, 0x6F, 0x6C, 0x6C, 0x65, 0x63,
        0x74, 0x69, 0x6F, 0x6E, 0xA2, 0x68, 0x70, 0x72, 0x69, 0x6F, 0x72, 0x69, 0x74, 0x79, 0x01,
        0x6F, 0x73, 0x63, 0x68, 0x65, 0x6D, 0x61, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6F, 0x6E, 0x49,
        0x44, 0x67, 0x73, 0x63, 0x68, 0x65, 0x6D, 0x61, 0x31,
    ];
    const GO_COLLECTION_CID: &str = "bafyreigf5dhfj5gxgij5bvycbn5p7jkarmedjqk3bwcst3am3lp5yyqnsi";

    // Go test vector: LWW Block with empty data (deletion)
    const GO_LWW_DELETION_BYTES: &[u8] = &[
        0xA1, 0x65, 0x64, 0x65, 0x6C, 0x74, 0x61, 0xA1, 0x63, 0x6C, 0x77, 0x77, 0xA5, 0x64, 0x64,
        0x61, 0x74, 0x61, 0x40, 0x65, 0x64, 0x6F, 0x63, 0x49, 0x44, 0x44, 0x64, 0x6F, 0x63, 0x31,
        0x68, 0x70, 0x72, 0x69, 0x6F, 0x72, 0x69, 0x74, 0x79, 0x02, 0x69, 0x66, 0x69, 0x65, 0x6C,
        0x64, 0x4E, 0x61, 0x6D, 0x65, 0x64, 0x6E, 0x61, 0x6D, 0x65, 0x6F, 0x73, 0x63, 0x68, 0x65,
        0x6D, 0x61, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6F, 0x6E, 0x49, 0x44, 0x67, 0x73, 0x63, 0x68,
        0x65, 0x6D, 0x61, 0x31,
    ];
    const GO_LWW_DELETION_CID: &str = "bafyreihqlzggsqqcokhhugjneworlnh2jpiin4x4gxtvrnmfqtz5kkrio4";

    #[test]
    fn test_go_wire_compat_lww_simple() {
        // Deserialize Go bytes
        let block = Block::from_dag_cbor(GO_LWW_SIMPLE_BYTES).unwrap();

        // Re-serialize and verify byte-identical output
        let rust_bytes = block.to_dag_cbor().unwrap();
        assert_eq!(
            rust_bytes.as_slice(),
            GO_LWW_SIMPLE_BYTES,
            "Rust serialization should match Go bytes"
        );

        // Verify CID matches
        assert_eq!(
            block.generate_cid().unwrap().to_string(),
            GO_LWW_SIMPLE_CID,
            "CID should match Go's CID"
        );

        // Verify content
        if let CrdtDelta::Lww(lww) = &block.delta {
            assert_eq!(lww.doc_id, b"doc1");
            assert_eq!(lww.field_name, "name");
            assert_eq!(lww.priority, 1);
            assert_eq!(lww.schema_version_id, "schema1");
            assert_eq!(lww.data, b"John");
        } else {
            panic!("Expected LWW delta");
        }
    }

    #[test]
    fn test_go_wire_compat_lww_high_priority() {
        let block = Block::from_dag_cbor(GO_LWW_HIGH_PRIORITY_BYTES).unwrap();
        let rust_bytes = block.to_dag_cbor().unwrap();

        assert_eq!(rust_bytes.as_slice(), GO_LWW_HIGH_PRIORITY_BYTES);
        assert_eq!(
            block.generate_cid().unwrap().to_string(),
            GO_LWW_HIGH_PRIORITY_CID
        );

        if let CrdtDelta::Lww(lww) = &block.delta {
            assert_eq!(lww.priority, 100);
            assert_eq!(lww.field_name, "age");
        } else {
            panic!("Expected LWW delta");
        }
    }

    #[test]
    fn test_go_wire_compat_counter() {
        let block = Block::from_dag_cbor(GO_COUNTER_BYTES).unwrap();
        let rust_bytes = block.to_dag_cbor().unwrap();

        assert_eq!(rust_bytes.as_slice(), GO_COUNTER_BYTES);
        assert_eq!(block.generate_cid().unwrap().to_string(), GO_COUNTER_CID);

        if let CrdtDelta::Counter(counter) = &block.delta {
            assert_eq!(counter.doc_id, b"doc1");
            assert_eq!(counter.field_name, "count");
            assert_eq!(counter.priority, 1);
            assert_eq!(counter.nonce, 12345);
            assert_eq!(counter.data, &[0x0A]); // CBOR integer 10
        } else {
            panic!("Expected Counter delta");
        }
    }

    #[test]
    fn test_go_wire_compat_composite_active() {
        let block = Block::from_dag_cbor(GO_COMPOSITE_ACTIVE_BYTES).unwrap();
        let rust_bytes = block.to_dag_cbor().unwrap();

        assert_eq!(rust_bytes.as_slice(), GO_COMPOSITE_ACTIVE_BYTES);
        assert_eq!(
            block.generate_cid().unwrap().to_string(),
            GO_COMPOSITE_ACTIVE_CID
        );

        if let CrdtDelta::Composite(composite) = &block.delta {
            assert_eq!(composite.doc_id, b"doc1");
            assert_eq!(composite.priority, 1);
            assert_eq!(composite.status, 1); // Active
        } else {
            panic!("Expected Composite delta");
        }
    }

    #[test]
    fn test_go_wire_compat_composite_deleted() {
        let block = Block::from_dag_cbor(GO_COMPOSITE_DELETED_BYTES).unwrap();
        let rust_bytes = block.to_dag_cbor().unwrap();

        assert_eq!(rust_bytes.as_slice(), GO_COMPOSITE_DELETED_BYTES);
        assert_eq!(
            block.generate_cid().unwrap().to_string(),
            GO_COMPOSITE_DELETED_CID
        );

        if let CrdtDelta::Composite(composite) = &block.delta {
            assert_eq!(composite.priority, 2);
            assert_eq!(composite.status, 2); // Deleted
        } else {
            panic!("Expected Composite delta");
        }
    }

    #[test]
    fn test_go_wire_compat_collection() {
        let block = Block::from_dag_cbor(GO_COLLECTION_BYTES).unwrap();
        let rust_bytes = block.to_dag_cbor().unwrap();

        assert_eq!(rust_bytes.as_slice(), GO_COLLECTION_BYTES);
        assert_eq!(block.generate_cid().unwrap().to_string(), GO_COLLECTION_CID);

        if let CrdtDelta::Collection(collection) = &block.delta {
            assert_eq!(collection.priority, 1);
            assert_eq!(collection.schema_version_id, "schema1");
        } else {
            panic!("Expected Collection delta");
        }
    }

    #[test]
    fn test_go_wire_compat_lww_deletion() {
        let block = Block::from_dag_cbor(GO_LWW_DELETION_BYTES).unwrap();
        let rust_bytes = block.to_dag_cbor().unwrap();

        assert_eq!(rust_bytes.as_slice(), GO_LWW_DELETION_BYTES);
        assert_eq!(
            block.generate_cid().unwrap().to_string(),
            GO_LWW_DELETION_CID
        );

        if let CrdtDelta::Lww(lww) = &block.delta {
            assert_eq!(lww.priority, 2);
            assert!(lww.data.is_empty(), "Deletion should have empty data");
        } else {
            panic!("Expected LWW delta");
        }
    }

    #[test]
    fn test_rust_produces_go_compatible_lww() {
        // Create same block structure as Go test
        let block = Block::new(test_lww_delta(), vec![], vec![]);
        let bytes = block.to_dag_cbor().unwrap();

        // Should produce identical bytes to Go
        assert_eq!(
            bytes.as_slice(),
            GO_LWW_SIMPLE_BYTES,
            "Rust-created block should produce Go-compatible bytes"
        );
        assert_eq!(
            block.generate_cid().unwrap().to_string(),
            GO_LWW_SIMPLE_CID,
            "CID should match Go"
        );
    }

    #[test]
    fn test_block_cid_generation_deterministic() {
        let block = Block::new(test_lww_delta(), vec![], vec![]);

        let cid1 = block.generate_cid().unwrap();
        let cid2 = block.generate_cid().unwrap();

        assert_eq!(cid1, cid2);
    }

    #[test]
    fn test_block_cid_uses_dag_cbor_codec() {
        let block = Block::new(test_lww_delta(), vec![], vec![]);
        let cid = block.generate_cid().unwrap();

        assert_eq!(cid.codec(), DAG_CBOR_CODEC);
    }

    #[test]
    fn test_heads_sorted_lexicographically() {
        let cid_z = test_cid(); // bafybeig...
        let cid_a = test_cid2(); // bafkreig... (comes after bafybeig)

        // Note: bafkreig > bafybeig lexicographically
        let block = Block::new(test_lww_delta(), vec![cid_a, cid_z], vec![]);

        let heads = block.heads.unwrap();
        assert!(
            heads[0].to_string() < heads[1].to_string(),
            "Heads should be sorted: {} < {}",
            heads[0],
            heads[1]
        );
    }

    #[test]
    fn test_empty_heads_becomes_none() {
        let block = Block::new(test_lww_delta(), vec![], vec![]);
        assert!(block.heads.is_none());
    }

    #[test]
    fn test_empty_links_becomes_none() {
        let block = Block::new(test_lww_delta(), vec![], vec![]);
        assert!(block.links.is_none());
    }

    #[test]
    fn test_all_links_returns_heads_then_links() {
        let head1 = test_cid();
        let head2 = test_cid2();
        let link1 = DAGLink::new("field1", test_cid());
        let link2 = DAGLink::new("field2", test_cid2());

        let block = Block::new(test_lww_delta(), vec![head1, head2], vec![link1, link2]);

        let all = block.all_links();
        assert_eq!(all.len(), 4);
        // First two should be heads (sorted)
        // Last two should be links (sorted)
    }

    #[test]
    fn test_get_link_by_name() {
        let link = DAGLink::new("myfield", test_cid());
        let block = Block::new(test_lww_delta(), vec![], vec![link.clone()]);

        assert_eq!(block.get_link_by_name("myfield"), Some(&link.link));
        assert_eq!(block.get_link_by_name("nonexistent"), None);
    }

    #[test]
    fn test_is_encrypted_and_signed() {
        let mut block = Block::new(test_lww_delta(), vec![], vec![]);
        assert!(!block.is_encrypted());
        assert!(!block.is_signed());

        block.encryption = Some(test_cid());
        assert!(block.is_encrypted());

        block.signature = Some(test_cid2());
        assert!(block.is_signed());
    }

    #[test]
    fn test_dag_link_ordering() {
        let link_a = DAGLink::new("a", test_cid());
        let link_b = DAGLink::new("b", test_cid2());

        // Ordering is by CID string, not by name
        let mut links = vec![link_b.clone(), link_a.clone()];
        links.sort();

        // bafkreig < bafybeig (k < y in ASCII)
        assert_eq!(links[0].link, test_cid2());
        assert_eq!(links[1].link, test_cid());
    }

    #[test]
    fn test_encryption_roundtrip() {
        let enc = Encryption::new(b"doc1".to_vec(), b"key123".to_vec());

        let bytes = enc.to_dag_cbor().unwrap();
        let restored = Encryption::from_dag_cbor(&bytes).unwrap();

        assert_eq!(enc, restored);
    }

    #[test]
    fn test_encryption_with_field_name() {
        let enc = Encryption::new_for_field(
            b"doc1".to_vec(),
            "secret_field".to_string(),
            b"key".to_vec(),
        );

        let bytes = enc.to_dag_cbor().unwrap();
        let restored = Encryption::from_dag_cbor(&bytes).unwrap();

        assert_eq!(restored.field_name, Some("secret_field".to_string()));
    }

    #[test]
    fn test_signature_roundtrip() {
        let sig = Signature::new(
            SignatureHeader::new(SignatureType::EdDSA, b"pubkey".to_vec()),
            b"signature_value".to_vec(),
        );

        let bytes = sig.to_dag_cbor().unwrap();
        let restored = Signature::from_dag_cbor(&bytes).unwrap();

        assert_eq!(sig, restored);
    }

    #[test]
    fn test_signature_type_serialization() {
        let header_ed = SignatureHeader::new(SignatureType::EdDSA, vec![]);
        let header_ec = SignatureHeader::new(SignatureType::ES256K, vec![]);

        // Verify the type serializes correctly
        let bytes_ed = serde_ipld_dagcbor::to_vec(&header_ed).unwrap();
        let bytes_ec = serde_ipld_dagcbor::to_vec(&header_ec).unwrap();

        let restored_ed: SignatureHeader = serde_ipld_dagcbor::from_slice(&bytes_ed).unwrap();
        let restored_ec: SignatureHeader = serde_ipld_dagcbor::from_slice(&bytes_ec).unwrap();

        assert_eq!(restored_ed.sig_type, SignatureType::EdDSA);
        assert_eq!(restored_ec.sig_type, SignatureType::ES256K);
    }

    #[test]
    fn test_crdt_delta_priority() {
        let mut delta = test_lww_delta();
        assert_eq!(delta.priority(), 1);

        delta.set_priority(42);
        assert_eq!(delta.priority(), 42);
    }

    #[test]
    fn test_composite_delta_roundtrip() {
        let delta = CrdtDelta::Composite(CompositeDeltaPayload {
            doc_id: b"doc1".to_vec(),
            schema_version_id: "v1".to_string(),
            priority: 5,
            status: 1,
        });

        let block = Block::new(delta, vec![], vec![]);
        let bytes = block.to_dag_cbor().unwrap();
        let restored = Block::from_dag_cbor(&bytes).unwrap();

        if let CrdtDelta::Composite(c) = &restored.delta {
            assert_eq!(c.status, 1);
            assert_eq!(c.priority, 5);
        } else {
            panic!("Expected Composite delta");
        }
    }

    #[test]
    fn test_counter_delta_roundtrip() {
        let delta = CrdtDelta::Counter(CounterDeltaPayload {
            doc_id: b"doc1".to_vec(),
            field_name: "counter_field".to_string(),
            priority: 3,
            nonce: -42,
            schema_version_id: "v1".to_string(),
            data: vec![1, 2, 3, 4],
        });

        let block = Block::new(delta, vec![], vec![]);
        let bytes = block.to_dag_cbor().unwrap();
        let restored = Block::from_dag_cbor(&bytes).unwrap();

        if let CrdtDelta::Counter(c) = &restored.delta {
            assert_eq!(c.nonce, -42);
            assert_eq!(c.field_name, "counter_field");
            assert_eq!(c.data, vec![1, 2, 3, 4]);
        } else {
            panic!("Expected Counter delta");
        }
    }

    #[test]
    fn test_collection_delta_roundtrip() {
        let delta = CrdtDelta::Collection(CollectionDeltaPayload {
            schema_version_id: "v2".to_string(),
            priority: 10,
        });

        let block = Block::new(delta, vec![], vec![]);
        let bytes = block.to_dag_cbor().unwrap();
        let restored = Block::from_dag_cbor(&bytes).unwrap();

        if let CrdtDelta::Collection(c) = &restored.delta {
            assert_eq!(c.priority, 10);
            assert_eq!(c.schema_version_id, "v2");
        } else {
            panic!("Expected Collection delta");
        }
    }

    #[test]
    fn test_block_with_heads_and_links() {
        let head = test_cid();
        let link = DAGLink::new("field", test_cid2());

        let block = Block::new_with_options(
            test_lww_delta(),
            vec![head],
            vec![link],
            Some(test_cid()),
            Some(test_cid2()),
        );

        assert!(block.heads.is_some());
        assert!(block.links.is_some());
        assert!(block.is_encrypted());
        assert!(block.is_signed());

        // Roundtrip
        let bytes = block.to_dag_cbor().unwrap();
        let restored = Block::from_dag_cbor(&bytes).unwrap();
        assert_eq!(block, restored);
    }

    // ========================================================================
    // Deserialization Error Handling Tests (Issue #16)
    // ========================================================================

    #[test]
    fn test_from_dag_cbor_rejects_invalid_cbor() {
        // Completely invalid bytes - not valid CBOR at all
        let result = Block::from_dag_cbor(&[0xFF, 0xFF, 0xFF]);
        assert!(result.is_err());
    }

    #[test]
    fn test_from_dag_cbor_rejects_empty_input() {
        let result = Block::from_dag_cbor(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_from_dag_cbor_rejects_truncated_data() {
        let block = Block::new(test_lww_delta(), vec![], vec![]);
        let bytes = block.to_dag_cbor().unwrap();

        // Try various truncation points
        for truncate_at in [1, bytes.len() / 4, bytes.len() / 2, bytes.len() - 1] {
            let result = Block::from_dag_cbor(&bytes[..truncate_at]);
            assert!(
                result.is_err(),
                "Should reject truncated data at {} bytes",
                truncate_at
            );
        }
    }

    #[test]
    fn test_from_dag_cbor_rejects_wrong_type_integer() {
        // Valid CBOR integer (42), but not a Block structure
        let cbor_integer = serde_ipld_dagcbor::to_vec(&42u64).unwrap();
        let result = Block::from_dag_cbor(&cbor_integer);
        assert!(result.is_err());
    }

    #[test]
    fn test_from_dag_cbor_rejects_wrong_type_string() {
        // Valid CBOR string, but not a Block structure
        let cbor_string = serde_ipld_dagcbor::to_vec(&"not a block").unwrap();
        let result = Block::from_dag_cbor(&cbor_string);
        assert!(result.is_err());
    }

    #[test]
    fn test_from_dag_cbor_rejects_wrong_type_array() {
        // Valid CBOR array, but Block expects a map
        let cbor_array = serde_ipld_dagcbor::to_vec(&vec![1, 2, 3]).unwrap();
        let result = Block::from_dag_cbor(&cbor_array);
        assert!(result.is_err());
    }

    #[test]
    fn test_from_dag_cbor_rejects_empty_map() {
        // Valid CBOR map but missing required 'delta' field
        use std::collections::BTreeMap;
        let empty_map: BTreeMap<String, String> = BTreeMap::new();
        let cbor_empty_map = serde_ipld_dagcbor::to_vec(&empty_map).unwrap();
        let result = Block::from_dag_cbor(&cbor_empty_map);
        assert!(result.is_err());
    }

    #[test]
    fn test_from_dag_cbor_rejects_map_with_wrong_delta_type() {
        // Map with 'delta' field but wrong type (string instead of CrdtDelta)
        use std::collections::BTreeMap;
        let mut map: BTreeMap<String, String> = BTreeMap::new();
        map.insert("delta".to_string(), "not a delta".to_string());
        let cbor_map = serde_ipld_dagcbor::to_vec(&map).unwrap();
        let result = Block::from_dag_cbor(&cbor_map);
        assert!(result.is_err());
    }

    #[test]
    fn test_from_dag_cbor_rejects_corrupted_block() {
        // Take valid block bytes and corrupt them in the middle
        let block = Block::new(test_lww_delta(), vec![test_cid()], vec![]);
        let mut bytes = block.to_dag_cbor().unwrap();

        // Corrupt bytes in the middle (should break structure)
        if bytes.len() > 20 {
            bytes[15] = 0xFF;
            bytes[16] = 0xFF;
            bytes[17] = 0xFF;
        }

        let result = Block::from_dag_cbor(&bytes);
        assert!(result.is_err());
    }

    #[test]
    fn test_encryption_from_dag_cbor_rejects_invalid() {
        let result = Encryption::from_dag_cbor(&[0xFF, 0xFF]);
        assert!(result.is_err());
    }

    #[test]
    fn test_encryption_from_dag_cbor_rejects_truncated() {
        let enc = Encryption::new(b"doc".to_vec(), b"key".to_vec());
        let bytes = enc.to_dag_cbor().unwrap();
        let result = Encryption::from_dag_cbor(&bytes[..bytes.len() / 2]);
        assert!(result.is_err());
    }

    #[test]
    fn test_encryption_from_dag_cbor_rejects_missing_fields() {
        use std::collections::BTreeMap;
        // Missing 'key' field
        let mut map: BTreeMap<String, Vec<u8>> = BTreeMap::new();
        map.insert("docID".to_string(), b"doc".to_vec());
        let cbor = serde_ipld_dagcbor::to_vec(&map).unwrap();
        let result = Encryption::from_dag_cbor(&cbor);
        assert!(result.is_err());
    }

    #[test]
    fn test_signature_from_dag_cbor_rejects_invalid() {
        let result = Signature::from_dag_cbor(&[0xFF, 0xFF]);
        assert!(result.is_err());
    }

    #[test]
    fn test_signature_from_dag_cbor_rejects_truncated() {
        let sig = Signature::new(
            SignatureHeader::new(SignatureType::EdDSA, b"pk".to_vec()),
            b"sig".to_vec(),
        );
        let bytes = sig.to_dag_cbor().unwrap();
        let result = Signature::from_dag_cbor(&bytes[..bytes.len() / 2]);
        assert!(result.is_err());
    }

    #[test]
    fn test_signature_from_dag_cbor_rejects_corrupted() {
        // Take valid signature bytes and corrupt them
        let sig = Signature::new(
            SignatureHeader::new(SignatureType::EdDSA, b"pubkey".to_vec()),
            b"signature".to_vec(),
        );
        let mut bytes = sig.to_dag_cbor().unwrap();

        // Corrupt in the middle
        if bytes.len() > 10 {
            bytes[5] = 0xFF;
            bytes[6] = 0xFF;
        }

        let result = Signature::from_dag_cbor(&bytes);
        assert!(result.is_err());
    }

    #[test]
    fn test_block_with_corrupted_cid_in_heads() {
        // Create a valid block then corrupt the CID bytes
        let block = Block::new(test_lww_delta(), vec![test_cid()], vec![]);
        let mut bytes = block.to_dag_cbor().unwrap();

        // Find and corrupt CID bytes (CIDs have a specific structure)
        // The corruption should cause deserialization to fail
        for i in (bytes.len() / 2)..bytes.len() {
            bytes[i] = 0x00;
        }

        let result = Block::from_dag_cbor(&bytes);
        assert!(result.is_err());
    }
}
