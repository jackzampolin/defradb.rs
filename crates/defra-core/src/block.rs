//! IPLD Block types and operations for DefraDB
//!
//! This module provides Go-compatible Block structures with DAG-CBOR serialization.
//! All types match the Go implementation in `internal/core/block/` for wire compatibility.

use cid::Cid;
use multihash::MultihashGeneric;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{Error, Result};

// Re-export delta payload types
pub use crate::block_delta::{
    CollectionDefinitionDeltaPayload, CollectionDeltaPayload, CollectionSetDeltaPayload,
    CompositeDeltaPayload, CounterDeltaPayload, FieldDefinitionDeltaPayload, LwwDeltaPayload,
};

// Re-export signature/encryption types
pub use crate::block_signature::{
    DocumentStatus, Encryption, Signature, SignatureHeader, SignatureType,
};

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
        sorted_heads.sort_by_key(|a| a.to_bytes());
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
    ///
    /// For IPLD-based traversal with visitor pattern, use `ipld::collect_block_links()`
    /// or `ipld::walk_ipld()` with a custom `IpldVisitor`.
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
        self.link.to_bytes().cmp(&other.link.to_bytes())
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
#[non_exhaustive]
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

    /// Collection set delta (circular relation groups)
    #[serde(rename = "collectionSet")]
    CollectionSet(CollectionSetDeltaPayload),

    /// Field definition delta (schema versioning)
    #[serde(rename = "fieldDefinition")]
    FieldDefinition(FieldDefinitionDeltaPayload),

    /// Collection definition delta (schema versioning)
    #[serde(rename = "collectionDefinition")]
    CollectionDefinition(CollectionDefinitionDeltaPayload),
}

impl CrdtDelta {
    /// Get the priority of this delta
    pub fn priority(&self) -> u64 {
        match self {
            CrdtDelta::Lww(d) => d.priority,
            CrdtDelta::Counter(d) => d.priority,
            CrdtDelta::Composite(d) => d.priority,
            CrdtDelta::Collection(d) => d.priority,
            CrdtDelta::CollectionSet(d) => d.priority,
            CrdtDelta::FieldDefinition(d) => d.priority,
            CrdtDelta::CollectionDefinition(d) => d.priority,
        }
    }

    /// Set the priority of this delta
    pub fn set_priority(&mut self, priority: u64) {
        match self {
            CrdtDelta::Lww(d) => d.priority = priority,
            CrdtDelta::Counter(d) => d.priority = priority,
            CrdtDelta::Composite(d) => d.priority = priority,
            CrdtDelta::Collection(d) => d.priority = priority,
            CrdtDelta::CollectionSet(d) => d.priority = priority,
            CrdtDelta::FieldDefinition(d) => d.priority = priority,
            CrdtDelta::CollectionDefinition(d) => d.priority = priority,
        }
    }

    /// Get the document ID (if present)
    ///
    /// Note: Collection, FieldDefinition, and CollectionDefinition deltas
    /// do not have a doc_id, so return None for those types.
    pub fn doc_id(&self) -> Option<&[u8]> {
        match self {
            CrdtDelta::Lww(d) => Some(&d.doc_id),
            CrdtDelta::Counter(d) => Some(&d.doc_id),
            CrdtDelta::Composite(d) => Some(&d.doc_id),
            CrdtDelta::Collection(_) => None,
            CrdtDelta::CollectionSet(_) => None,
            CrdtDelta::FieldDefinition(_) => None,
            CrdtDelta::CollectionDefinition(_) => None,
        }
    }

    /// Get the schema version ID / collection version ID (if present)
    ///
    /// Note: FieldDefinition and CollectionDefinition deltas do not have
    /// a schema_version_id, so return None for those types.
    pub fn schema_version_id(&self) -> Option<&str> {
        match self {
            CrdtDelta::Lww(d) => Some(&d.schema_version_id),
            CrdtDelta::Counter(d) => Some(&d.schema_version_id),
            CrdtDelta::Composite(d) => Some(&d.schema_version_id),
            CrdtDelta::Collection(d) => Some(&d.schema_version_id),
            CrdtDelta::CollectionSet(_) => None,
            CrdtDelta::FieldDefinition(_) => None,
            CrdtDelta::CollectionDefinition(_) => None,
        }
    }

    /// Check if this is a schema definition delta
    pub fn is_definition(&self) -> bool {
        matches!(
            self,
            CrdtDelta::FieldDefinition(_)
                | CrdtDelta::CollectionDefinition(_)
                | CrdtDelta::CollectionSet(_)
        )
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Generate CID from raw DAG-CBOR bytes using SHA2-256.
///
/// Use this when you already have the serialized bytes to avoid
/// double-serializing (e.g., when you called `to_dag_cbor()` separately).
pub fn generate_cid_from_bytes(bytes: &[u8]) -> Result<Cid> {
    // Hash with SHA2-256
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();

    // Create multihash
    let mh = MultihashGeneric::<64>::wrap(SHA2_256_CODE, &digest)
        .map_err(|e| Error::BlockError(format!("Failed to create multihash: {}", e)))?;

    // Create CIDv1 with DAG-CBOR codec
    Ok(Cid::new_v1(DAG_CBOR_CODEC, mh))
}
