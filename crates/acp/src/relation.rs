//! Relation tuple types for ACP.
//!
//! A relation tuple represents a subject having a relation to an object.
//! For example: "did:key:abc123 is owner of doc:users/doc456"

use identity::Did;
use serde::{Deserialize, Serialize};

/// The required relation that represents document ownership.
/// Every document MUST have an owner relation as per DPI rules.
pub const OWNER_RELATION: &str = "owner";

/// Common relation name for read access
pub const READER_RELATION: &str = "reader";

/// Common relation name for update access
pub const UPDATER_RELATION: &str = "updater";

/// Common relation name for delete access
pub const DELETER_RELATION: &str = "deleter";

/// A relation tuple: subject has relation to object.
///
/// The object is identified by collection_id and doc_id.
/// Relations are policy-defined strings like "owner", "reader", etc.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RelationTuple {
    /// The subject (identity) that has the relation
    pub subject: Did,

    /// The relation name (e.g., "owner", "reader", "updater")
    pub relation: String,

    /// The collection ID (resource type)
    pub collection_id: String,

    /// The document ID within the collection
    pub doc_id: String,
}

impl RelationTuple {
    /// Create a new relation tuple.
    pub fn new(
        subject: Did,
        relation: impl Into<String>,
        collection_id: impl Into<String>,
        doc_id: impl Into<String>,
    ) -> Self {
        Self {
            subject,
            relation: relation.into(),
            collection_id: collection_id.into(),
            doc_id: doc_id.into(),
        }
    }

    /// Create an owner relation tuple.
    pub fn owner(subject: Did, collection_id: impl Into<String>, doc_id: impl Into<String>) -> Self {
        Self::new(subject, OWNER_RELATION, collection_id, doc_id)
    }

    /// Check if this is an owner relation.
    pub fn is_owner(&self) -> bool {
        self.relation == OWNER_RELATION
    }

    /// Get the storage key for this tuple.
    ///
    /// Key format: `/acp/{collection_id}/{doc_id}/{relation}/{subject_did}`
    pub fn storage_key(&self) -> String {
        format!(
            "/acp/{}/{}/{}/{}",
            self.collection_id, self.doc_id, self.relation, self.subject
        )
    }

    /// Get the prefix for scanning all relations of a document.
    ///
    /// Key format: `/acp/{collection_id}/{doc_id}/`
    pub fn doc_prefix(collection_id: &str, doc_id: &str) -> String {
        format!("/acp/{}/{}/", collection_id, doc_id)
    }

    /// Get the prefix for scanning all tuples with a specific relation.
    ///
    /// Key format: `/acp/{collection_id}/{doc_id}/{relation}/`
    pub fn relation_prefix(collection_id: &str, doc_id: &str, relation: &str) -> String {
        format!("/acp/{}/{}/{}/", collection_id, doc_id, relation)
    }
}

impl std::fmt::Display for RelationTuple {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}@{}:{}#{}",
            self.subject, self.collection_id, self.doc_id, self.relation
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_did() -> Did {
        Did::new("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK").unwrap()
    }

    #[test]
    fn test_relation_tuple_new() {
        let did = test_did();
        let tuple = RelationTuple::new(did.clone(), "reader", "users", "doc123");

        assert_eq!(tuple.subject, did);
        assert_eq!(tuple.relation, "reader");
        assert_eq!(tuple.collection_id, "users");
        assert_eq!(tuple.doc_id, "doc123");
    }

    #[test]
    fn test_relation_tuple_owner() {
        let did = test_did();
        let tuple = RelationTuple::owner(did.clone(), "users", "doc123");

        assert_eq!(tuple.relation, OWNER_RELATION);
        assert!(tuple.is_owner());
    }

    #[test]
    fn test_relation_tuple_storage_key() {
        let did = test_did();
        let tuple = RelationTuple::new(did.clone(), "reader", "users", "doc123");

        let key = tuple.storage_key();
        assert!(key.starts_with("/acp/"));
        assert!(key.contains("users"));
        assert!(key.contains("doc123"));
        assert!(key.contains("reader"));
        assert!(key.contains(did.as_str()));
    }

    #[test]
    fn test_doc_prefix() {
        let prefix = RelationTuple::doc_prefix("users", "doc123");
        assert_eq!(prefix, "/acp/users/doc123/");
    }

    #[test]
    fn test_relation_prefix() {
        let prefix = RelationTuple::relation_prefix("users", "doc123", "owner");
        assert_eq!(prefix, "/acp/users/doc123/owner/");
    }

    #[test]
    fn test_relation_tuple_display() {
        let did = test_did();
        let tuple = RelationTuple::new(did, "reader", "users", "doc123");
        let display = format!("{}", tuple);
        assert!(display.contains("reader"));
        assert!(display.contains("users"));
        assert!(display.contains("doc123"));
    }

    #[test]
    fn test_relation_tuple_serde() {
        let did = test_did();
        let tuple = RelationTuple::new(did, "owner", "users", "doc123");
        let json = serde_json::to_string(&tuple).unwrap();
        let parsed: RelationTuple = serde_json::from_str(&json).unwrap();
        assert_eq!(tuple, parsed);
    }
}
