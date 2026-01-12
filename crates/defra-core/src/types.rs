//! Core type definitions for DefraDB

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
}
