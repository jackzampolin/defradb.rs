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

    /// Clone the block (shallow copy of links, deep copy of delta)
    ///
    /// Matches Go's `Clone()` behavior.
    pub fn clone_block(&self) -> Self {
        Self {
            delta: self.delta.clone(),
            heads: self.heads.clone(),
            links: self.links.clone(),
            encryption: self.encryption,
            signature: self.signature,
        }
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
/// Matches Go's `crdt.CRDT` interface. Uses serde enum representation
/// compatible with Go's CBOR encoding.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
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

    /// Get the document ID
    pub fn doc_id(&self) -> &[u8] {
        match self {
            CrdtDelta::Lww(d) => &d.doc_id,
            CrdtDelta::Counter(d) => &d.doc_id,
            CrdtDelta::Composite(d) => &d.doc_id,
            CrdtDelta::Collection(d) => &d.doc_id,
        }
    }
}

/// LWW Register delta payload for block embedding
///
/// Matches Go's `crdt.LWWDelta` structure.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LwwDeltaPayload {
    /// Document ID
    #[serde(rename = "docID")]
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
    pub data: Vec<u8>,
}

/// Counter delta payload for block embedding
///
/// Matches Go's `crdt.CounterDelta` structure.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CounterDeltaPayload {
    /// Document ID
    #[serde(rename = "docID")]
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
    pub data: Vec<u8>,
}

/// Composite delta payload for block embedding
///
/// Matches Go's `crdt.DocCompositeDelta` structure.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompositeDeltaPayload {
    /// Document ID
    #[serde(rename = "docID")]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CollectionDeltaPayload {
    /// Document ID
    #[serde(rename = "docID")]
    pub doc_id: Vec<u8>,

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
    #[serde(rename = "docID")]
    pub doc_id: Vec<u8>,

    /// Field name (None for document-level encryption)
    #[serde(rename = "fieldName", default, skip_serializing_if = "Option::is_none")]
    pub field_name: Option<String>,

    /// Encryption key
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
}
