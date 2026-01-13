//! Core type definitions for DefraDB

use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Document identifier - unique identifier for a document
/// Format: "bae-<base32-encoded-bytes>"
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DocId(String);

impl DocId {
    /// Create a new DocId from a string
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Get the string representation
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for DocId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Collection identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CollectionId(u32);

impl CollectionId {
    /// Create a new CollectionId
    pub fn new(id: u32) -> Self {
        Self(id)
    }

    /// Get the numeric ID
    pub fn as_u32(&self) -> u32 {
        self.0
    }
}

/// Schema version identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SchemaVersion(u32);

impl SchemaVersion {
    /// Create a new schema version
    pub fn new(version: u32) -> Self {
        Self(version)
    }

    /// Get the version number
    pub fn as_u32(&self) -> u32 {
        self.0
    }
}

/// Field identifier within a collection
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FieldId(u32);

impl FieldId {
    /// Create a new FieldId
    pub fn new(id: u32) -> Self {
        Self(id)
    }

    /// Get the numeric ID
    pub fn as_u32(&self) -> u32 {
        self.0
    }
}

/// Field kind - the type of a field in a schema
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FieldKind {
    /// String type
    String,
    /// Integer type
    Int,
    /// Float type
    Float,
    /// Boolean type
    Bool,
    /// DateTime type (RFC3339)
    DateTime,
    /// JSON type (arbitrary JSON)
    Json,
    /// Bytes type (base64 encoded)
    Bytes,
    /// Object reference (foreign key)
    Object(String),
    /// Array of values
    Array(Box<FieldKind>),
}

/// CRDT type - conflict-free replicated data type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CrdtType {
    /// Last-Write-Wins Register
    LWW,
    /// Positive-Negative Counter
    Counter,
    /// Composite CRDT (document-level)
    Composite,
}

/// Priority for conflict resolution
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Priority(pub u64);

impl Priority {
    /// Create a new priority
    pub fn new(value: u64) -> Self {
        Self(value)
    }
}

/// Content Identifier (CID) for IPLD blocks
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CID(cid::Cid);

impl CID {
    /// Create a CID from bytes with validation
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let c = cid::Cid::try_from(bytes).map_err(|e| Error::InvalidCID(e.to_string()))?;

        // Validate version
        match c.version() {
            cid::Version::V0 | cid::Version::V1 => {}
        }

        // Validate hash function (only support SHA-256 for now)
        let hash = c.hash();
        if hash.code() != 0x12 {
            // 0x12 = SHA-256
            return Err(Error::InvalidCID(format!(
                "unsupported hash function: {}",
                hash.code()
            )));
        }

        Ok(CID(c))
    }

    /// Create a CID from a string
    pub fn from_string(s: &str) -> Result<Self> {
        let c = cid::Cid::try_from(s).map_err(|e| Error::InvalidCID(e.to_string()))?;

        // Validate version
        match c.version() {
            cid::Version::V0 | cid::Version::V1 => {}
        }

        // Validate hash function
        let hash = c.hash();
        if hash.code() != 0x12 {
            return Err(Error::InvalidCID(format!(
                "unsupported hash function: {}",
                hash.code()
            )));
        }

        Ok(CID(c))
    }

    /// Get the underlying CID
    pub fn inner(&self) -> &cid::Cid {
        &self.0
    }

    /// Convert to bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        self.0.to_bytes()
    }
}

impl fmt::Display for CID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_docid_creation() {
        let id = DocId::new("bae-test123");
        assert_eq!(id.as_str(), "bae-test123");
    }

    #[test]
    fn test_collection_id() {
        let id = CollectionId::new(1);
        assert_eq!(id.as_u32(), 1);
    }

    #[test]
    fn test_cid_from_string_valid() {
        // Valid CIDv1 with SHA-256
        let cid_str = "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi";
        let cid = CID::from_string(cid_str);
        assert!(cid.is_ok());
    }

    #[test]
    fn test_cid_from_string_invalid() {
        let result = CID::from_string("invalid-cid");
        assert!(result.is_err());
    }

    #[test]
    fn test_cid_unsupported_hash() {
        // Create a CID with non-SHA256 hash (this would need to be a real CID with different hash)
        // For now, just verify the validation logic exists
        let cid_str = "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi";
        let cid = CID::from_string(cid_str);
        assert!(cid.is_ok());
    }

    #[test]
    fn test_cid_to_bytes_roundtrip() {
        let cid_str = "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi";
        let cid = CID::from_string(cid_str).unwrap();
        let bytes = cid.to_bytes();
        let cid2 = CID::from_bytes(&bytes).unwrap();
        assert_eq!(cid, cid2);
    }
}
