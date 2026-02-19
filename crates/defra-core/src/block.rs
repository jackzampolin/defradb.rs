//! IPLD Block types and operations for DefraDB
//!
//! This module provides Go-compatible Block structures with DAG-CBOR serialization.
//! All types match the Go implementation in `internal/core/block/` for wire compatibility.

use cid::Cid;
use multihash::MultihashGeneric;
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

    /// Collection version identifier
    #[serde(rename = "collectionVersionID")]
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

    /// Collection version identifier
    #[serde(rename = "collectionVersionID")]
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

    /// Collection version identifier
    #[serde(rename = "collectionVersionID")]
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
    /// Collection version identifier
    #[serde(rename = "collectionVersionID")]
    pub schema_version_id: String,

    /// Priority
    pub priority: u64,
}

/// Field definition delta payload for schema versioning
///
/// Matches Go's `crdt.FieldDefinitionDelta` structure.
/// Used to track changes to field definitions across schema versions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FieldDefinitionDeltaPayload {
    /// Priority
    pub priority: u64,

    /// Field name (optional for updates)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    /// CRDT type for this field
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub crdt: Option<u8>,

    /// Scalar kind (for scalar fields)
    #[serde(
        rename = "scalarKind",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub scalar_kind: Option<u8>,

    /// Related collection ID (for relation fields)
    #[serde(
        rename = "collectionID",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub collection_id: Option<String>,

    /// Relative ID (for self-referencing fields)
    #[serde(
        rename = "relativeID",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub relative_id: Option<i32>,
}

impl FieldDefinitionDeltaPayload {
    /// Create a new field definition delta
    pub fn new(priority: u64) -> Self {
        Self {
            priority,
            name: None,
            crdt: None,
            scalar_kind: None,
            collection_id: None,
            relative_id: None,
        }
    }

    /// Set the field name
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Set the CRDT type
    pub fn with_crdt(mut self, crdt: u8) -> Self {
        self.crdt = Some(crdt);
        self
    }

    /// Set the scalar kind
    pub fn with_scalar_kind(mut self, kind: u8) -> Self {
        self.scalar_kind = Some(kind);
        self
    }

    /// Set the related collection ID
    pub fn with_collection_id(mut self, id: impl Into<String>) -> Self {
        self.collection_id = Some(id.into());
        self
    }

    /// Set the relative ID
    pub fn with_relative_id(mut self, id: i32) -> Self {
        self.relative_id = Some(id);
        self
    }
}

/// Collection definition delta payload for schema versioning
///
/// Matches Go's `crdt.CollectionDefinitionDelta` structure.
/// Used to track changes to collection definitions across schema versions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CollectionDefinitionDeltaPayload {
    /// Priority
    pub priority: u64,

    /// Collection name (optional for updates)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    /// Query select for view collections (JSON-encoded query definition)
    #[serde(
        rename = "querySelect",
        default,
        skip_serializing_if = "Option::is_none",
        with = "serde_bytes"
    )]
    pub query_select: Option<Vec<u8>>,

    /// Query transform CID for view collections (link to lens transform)
    #[serde(
        rename = "queryTransform",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub query_transform: Option<Cid>,
}

impl CollectionDefinitionDeltaPayload {
    /// Create a new collection definition delta
    pub fn new(priority: u64) -> Self {
        Self {
            priority,
            name: None,
            query_select: None,
            query_transform: None,
        }
    }

    /// Set the collection name
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Set the query select (for view collections)
    pub fn with_query_select(mut self, query: Vec<u8>) -> Self {
        self.query_select = Some(query);
        self
    }

    /// Set the query transform CID (for view collections with lens transforms)
    pub fn with_query_transform(mut self, transform_cid: Cid) -> Self {
        self.query_transform = Some(transform_cid);
        self
    }
}

/// Collection set delta payload for schema versioning.
///
/// Matches Go's `crdt.CollectionSetDelta` structure.
/// Used to generate a CollectionSetID CID for circular relation groups.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CollectionSetDeltaPayload {
    pub priority: u64,
}

impl CollectionSetDeltaPayload {
    pub fn new(priority: u64) -> Self {
        Self { priority }
    }
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

    /// ECDSA with secp256r1 (P-256) curve
    ES256,

    /// EdDSA with Ed25519 curve
    EdDSA,

    /// Threshold BLS12-381 (Orbis ring signing)
    BLS,
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
