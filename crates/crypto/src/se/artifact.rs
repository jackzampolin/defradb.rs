//! Searchable encryption artifact types.
//!
//! Artifacts represent the cryptographic search metadata that is replicated
//! to remote nodes for enabling privacy-preserving search queries.
//!
//! Matches Go's internal/se/core/artifact.go

use serde::{Deserialize, Serialize};

/// Type of SE artifact.
///
/// Currently only equality tags are supported, but this enum allows
/// for future extension to other search types (range, prefix, etc.).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArtifactType {
    /// Equality search tag - enables exact match queries.
    #[serde(rename = "equality_tag")]
    #[default]
    EqualityTag,
}

impl std::fmt::Display for ArtifactType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ArtifactType::EqualityTag => write!(f, "equality_tag"),
        }
    }
}

/// Type of operation for artifact changes.
///
/// Used to indicate whether an artifact should be added to or removed
/// from the remote node's index.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperationType {
    /// Add the artifact to the index.
    #[serde(rename = "add")]
    #[default]
    Add,
    /// Remove the artifact from the index.
    #[serde(rename = "delete")]
    Delete,
}

impl std::fmt::Display for OperationType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OperationType::Add => write!(f, "add"),
            OperationType::Delete => write!(f, "delete"),
        }
    }
}

/// A searchable encryption artifact to be replicated to remote nodes.
///
/// Contains the cryptographic search tag and metadata needed to store and query
/// encrypted indexes on untrusted replicator nodes.
///
/// # Storage Key Format
///
/// The remote node stores this artifact at:
/// ```text
/// /se/<CollectionID>/<IndexID>/<SearchTag>/<DocID>
/// ```
///
/// When querying, the client computes the same tag for the search value
/// and the remote node returns all DocIDs with matching tags.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Artifact {
    /// Unique identifier of the collection.
    #[serde(rename = "CollectionID")]
    pub collection_id: String,

    /// Unique document identifier.
    #[serde(rename = "DocID")]
    pub doc_id: String,

    /// Unique identifier of the encrypted index.
    /// Used as a domain separator in search tag computation.
    /// Typically the field name being indexed.
    #[serde(rename = "IndexID")]
    pub index_id: String,

    /// The deterministic cryptographic tag used for searching.
    #[serde(rename = "SearchTag", with = "serde_bytes")]
    pub search_tag: Vec<u8>,
}

impl Artifact {
    /// Create a new artifact.
    ///
    /// # Arguments
    ///
    /// * `collection_id` - Collection version ID
    /// * `doc_id` - Document ID
    /// * `index_id` - Encrypted index ID (typically field name)
    /// * `search_tag` - 16-byte search tag from `generate_equality_tag`
    pub fn new(
        collection_id: impl Into<String>,
        doc_id: impl Into<String>,
        index_id: impl Into<String>,
        search_tag: Vec<u8>,
    ) -> Self {
        Self {
            collection_id: collection_id.into(),
            doc_id: doc_id.into(),
            index_id: index_id.into(),
            search_tag,
        }
    }
}

/// A batch of artifacts to be pushed to a replicator.
///
/// Used for efficient network transmission of multiple artifacts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactBatch {
    /// Collection ID these artifacts belong to.
    #[serde(rename = "CollectionID")]
    pub collection_id: String,

    /// The artifacts to push.
    #[serde(rename = "Artifacts")]
    pub artifacts: Vec<Artifact>,

    /// Operation type (add or delete).
    #[serde(rename = "Operation", default)]
    pub operation: OperationType,
}

impl ArtifactBatch {
    /// Create a new artifact batch for adding artifacts.
    pub fn new_add(collection_id: impl Into<String>, artifacts: Vec<Artifact>) -> Self {
        Self {
            collection_id: collection_id.into(),
            artifacts,
            operation: OperationType::Add,
        }
    }

    /// Create a new artifact batch for deleting artifacts.
    pub fn new_delete(collection_id: impl Into<String>, artifacts: Vec<Artifact>) -> Self {
        Self {
            collection_id: collection_id.into(),
            artifacts,
            operation: OperationType::Delete,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_artifact_creation() {
        let artifact = Artifact::new("col_v1", "bae123", "age", vec![0x01, 0x02, 0x03]);

        assert_eq!(artifact.collection_id, "col_v1");
        assert_eq!(artifact.doc_id, "bae123");
        assert_eq!(artifact.index_id, "age");
        assert_eq!(artifact.search_tag, vec![0x01, 0x02, 0x03]);
    }

    #[test]
    fn test_artifact_type_display() {
        assert_eq!(ArtifactType::EqualityTag.to_string(), "equality_tag");
    }

    #[test]
    fn test_operation_type_display() {
        assert_eq!(OperationType::Add.to_string(), "add");
        assert_eq!(OperationType::Delete.to_string(), "delete");
    }

    #[test]
    fn test_artifact_batch_add() {
        let artifacts = vec![Artifact::new("col", "doc1", "field", vec![1, 2, 3])];
        let batch = ArtifactBatch::new_add("col", artifacts);

        assert_eq!(batch.operation, OperationType::Add);
        assert_eq!(batch.artifacts.len(), 1);
    }

    #[test]
    fn test_artifact_batch_delete() {
        let artifacts = vec![Artifact::new("col", "doc1", "field", vec![1, 2, 3])];
        let batch = ArtifactBatch::new_delete("col", artifacts);

        assert_eq!(batch.operation, OperationType::Delete);
    }

    #[test]
    fn test_artifact_serialization() {
        let artifact = Artifact::new("col", "doc", "field", vec![0xab, 0xcd]);
        let json = serde_json::to_string(&artifact).unwrap();

        assert!(json.contains("\"CollectionID\""));
        assert!(json.contains("\"DocID\""));
        assert!(json.contains("\"IndexID\""));
        assert!(json.contains("\"SearchTag\""));
    }

    #[test]
    fn test_artifact_type_serialization() {
        let art_type = ArtifactType::EqualityTag;
        let json = serde_json::to_string(&art_type).unwrap();
        assert_eq!(json, "\"equality_tag\"");

        let parsed: ArtifactType = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, ArtifactType::EqualityTag);
    }

    #[test]
    fn test_operation_type_serialization() {
        let op_add = OperationType::Add;
        assert_eq!(serde_json::to_string(&op_add).unwrap(), "\"add\"");

        let op_delete = OperationType::Delete;
        assert_eq!(serde_json::to_string(&op_delete).unwrap(), "\"delete\"");
    }
}
