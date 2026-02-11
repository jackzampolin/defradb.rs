//! Relationship and ObjectRef types.

use identity::Did;
use serde::{Deserialize, Serialize};

use super::subject::Subject;

/// A stored relationship tuple.
///
/// Represents: subject has relation to resource:object_id
/// Example: "did:key:alice" has "owner" relation to "document:doc123"
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Relationship {
    /// The resource type
    pub resource: String,

    /// The object ID within the resource
    pub object_id: String,

    /// The relation name
    pub relation: String,

    /// The subject (who has the relation)
    pub subject: Subject,
}

impl Relationship {
    /// Create a new relationship.
    pub fn new(
        resource: impl Into<String>,
        object_id: impl Into<String>,
        relation: impl Into<String>,
        subject: Subject,
    ) -> Self {
        Self {
            resource: resource.into(),
            object_id: object_id.into(),
            relation: relation.into(),
            subject,
        }
    }

    /// Create a relationship with a direct entity subject.
    pub fn with_entity(
        resource: impl Into<String>,
        object_id: impl Into<String>,
        relation: impl Into<String>,
        did: Did,
    ) -> Self {
        Self::new(resource, object_id, relation, Subject::Entity(did))
    }

    /// Get the storage key for this relationship.
    ///
    /// Key format: /rel/{resource}/{object_id}/{relation}/{subject_hash}
    pub fn storage_key(&self) -> String {
        format!(
            "/rel/{}/{}/{}/{}",
            self.resource,
            self.object_id,
            self.relation,
            self.subject.storage_hash()
        )
    }

    /// Get the prefix for scanning all relationships on an object.
    pub fn object_prefix(resource: &str, object_id: &str) -> String {
        format!("/rel/{}/{}/", resource, object_id)
    }

    /// Get the prefix for scanning all relationships with a specific relation.
    pub fn relation_prefix(resource: &str, object_id: &str, relation: &str) -> String {
        format!("/rel/{}/{}/{}/", resource, object_id, relation)
    }
}

impl std::fmt::Display for Relationship {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}:{}#{}@{}",
            self.resource, self.object_id, self.relation, self.subject
        )
    }
}

/// Reference to an object (resource + object_id).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ObjectRef {
    pub resource: String,
    pub object_id: String,
}

impl ObjectRef {
    pub fn new(resource: impl Into<String>, object_id: impl Into<String>) -> Self {
        Self {
            resource: resource.into(),
            object_id: object_id.into(),
        }
    }
}
