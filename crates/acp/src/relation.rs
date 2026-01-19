//! Relation tuple types for ACP.
//!
//! A relation tuple represents a subject having a relation to an object.
//! For example: "did:key:abc123 is owner of doc:users/doc456"

use identity::Did;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

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
///
/// All path components are validated to prevent path traversal attacks.
/// Use `try_new` to construct instances from untrusted input.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct RelationTuple {
    /// The subject (identity) that has the relation
    subject: Did,

    /// The relation name (e.g., "owner", "reader", "updater")
    relation: String,

    /// The collection ID (resource type)
    collection_id: String,

    /// The document ID within the collection
    doc_id: String,
}

/// Validate a path component for storage key safety.
/// Rejects empty strings and strings containing path separators.
fn validate_path_component(value: &str, field_name: &str) -> Result<()> {
    if value.is_empty() {
        return Err(Error::InvalidPolicy(format!(
            "{} cannot be empty",
            field_name
        )));
    }
    if value.contains('/') || value.contains('\\') {
        return Err(Error::InvalidPolicy(format!(
            "{} cannot contain path separators: {}",
            field_name, value
        )));
    }
    Ok(())
}

impl RelationTuple {
    /// Create a new relation tuple with validation.
    ///
    /// Returns an error if any path component (relation, collection_id, doc_id)
    /// contains path separators or is empty.
    pub fn try_new(
        subject: Did,
        relation: impl Into<String>,
        collection_id: impl Into<String>,
        doc_id: impl Into<String>,
    ) -> Result<Self> {
        let relation = relation.into();
        let collection_id = collection_id.into();
        let doc_id = doc_id.into();

        validate_path_component(&relation, "relation")?;
        validate_path_component(&collection_id, "collection_id")?;
        validate_path_component(&doc_id, "doc_id")?;

        Ok(Self {
            subject,
            relation,
            collection_id,
            doc_id,
        })
    }

    /// Create a new relation tuple without validation.
    ///
    /// This is crate-internal for use where inputs are already validated
    /// (e.g., from database storage or internal code paths).
    ///
    /// External code should use `try_new` to ensure path traversal safety.
    pub(crate) fn new(
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

    /// Get the subject DID.
    pub fn subject(&self) -> &Did {
        &self.subject
    }

    /// Get the relation name.
    pub fn relation(&self) -> &str {
        &self.relation
    }

    /// Get the collection ID.
    pub fn collection_id(&self) -> &str {
        &self.collection_id
    }

    /// Get the document ID.
    pub fn doc_id(&self) -> &str {
        &self.doc_id
    }

    /// Create an owner relation tuple.
    pub fn owner(
        subject: Did,
        collection_id: impl Into<String>,
        doc_id: impl Into<String>,
    ) -> Self {
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

    /// Validate a storage key prefix.
    ///
    /// Returns an error if any component contains path separators.
    pub fn validate_prefix(collection_id: &str, doc_id: &str) -> Result<()> {
        validate_path_component(collection_id, "collection_id")?;
        validate_path_component(doc_id, "doc_id")?;
        Ok(())
    }

    /// Validate a relation prefix.
    ///
    /// Returns an error if any component contains path separators.
    pub fn validate_relation_prefix(
        collection_id: &str,
        doc_id: &str,
        relation: &str,
    ) -> Result<()> {
        validate_path_component(collection_id, "collection_id")?;
        validate_path_component(doc_id, "doc_id")?;
        validate_path_component(relation, "relation")?;
        Ok(())
    }

    /// Get the prefix for scanning all relations of a document.
    ///
    /// Key format: `/acp/{collection_id}/{doc_id}/`
    ///
    /// # Security
    ///
    /// This method constructs a storage key from the inputs. Callers MUST validate
    /// inputs using [`Self::validate_prefix`] before calling this method with
    /// untrusted input to prevent path traversal attacks.
    ///
    /// For trusted internal calls where inputs are known to be safe, validation
    /// can be skipped.
    pub fn doc_prefix(collection_id: &str, doc_id: &str) -> String {
        format!("/acp/{}/{}/", collection_id, doc_id)
    }

    /// Get the prefix for scanning all relations of a document with validation.
    ///
    /// This is the safe version that validates inputs before constructing the prefix.
    /// Use this when constructing prefixes from potentially untrusted input.
    pub fn doc_prefix_validated(collection_id: &str, doc_id: &str) -> Result<String> {
        Self::validate_prefix(collection_id, doc_id)?;
        Ok(Self::doc_prefix(collection_id, doc_id))
    }

    /// Get the prefix for scanning all tuples with a specific relation.
    ///
    /// Key format: `/acp/{collection_id}/{doc_id}/{relation}/`
    ///
    /// # Security
    ///
    /// This method constructs a storage key from the inputs. Callers MUST validate
    /// inputs using [`Self::validate_relation_prefix`] before calling this method
    /// with untrusted input to prevent path traversal attacks.
    pub fn relation_prefix(collection_id: &str, doc_id: &str, relation: &str) -> String {
        format!("/acp/{}/{}/{}/", collection_id, doc_id, relation)
    }

    /// Get the prefix for scanning tuples with a specific relation, with validation.
    ///
    /// This is the safe version that validates inputs before constructing the prefix.
    /// Use this when constructing prefixes from potentially untrusted input.
    pub fn relation_prefix_validated(
        collection_id: &str,
        doc_id: &str,
        relation: &str,
    ) -> Result<String> {
        Self::validate_relation_prefix(collection_id, doc_id, relation)?;
        Ok(Self::relation_prefix(collection_id, doc_id, relation))
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

/// Custom Deserialize implementation that validates path components.
/// This prevents path traversal attacks via deserialized data.
impl<'de> Deserialize<'de> for RelationTuple {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error;

        // Deserialize into a helper struct first
        #[derive(Deserialize)]
        struct RelationTupleRaw {
            subject: Did,
            relation: String,
            collection_id: String,
            doc_id: String,
        }

        let raw = RelationTupleRaw::deserialize(deserializer)?;

        // Validate path components
        RelationTuple::try_new(raw.subject, raw.relation, raw.collection_id, raw.doc_id)
            .map_err(|e| D::Error::custom(e.to_string()))
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

        assert_eq!(tuple.subject(), &did);
        assert_eq!(tuple.relation(), "reader");
        assert_eq!(tuple.collection_id(), "users");
        assert_eq!(tuple.doc_id(), "doc123");
    }

    #[test]
    fn test_relation_tuple_owner() {
        let did = test_did();
        let tuple = RelationTuple::owner(did.clone(), "users", "doc123");

        assert_eq!(tuple.relation(), OWNER_RELATION);
        assert!(tuple.is_owner());
    }

    #[test]
    fn test_try_new_validates_path_components() {
        let did = test_did();

        // Valid components should work
        assert!(RelationTuple::try_new(did.clone(), "reader", "users", "doc123").is_ok());

        // Relation with slash should fail
        assert!(RelationTuple::try_new(did.clone(), "reader/admin", "users", "doc123").is_err());

        // Collection ID with slash should fail
        assert!(RelationTuple::try_new(did.clone(), "reader", "users/internal", "doc123").is_err());

        // Doc ID with slash should fail
        assert!(RelationTuple::try_new(did.clone(), "reader", "users", "doc/123").is_err());

        // Backslash should also fail
        assert!(RelationTuple::try_new(did.clone(), "reader\\admin", "users", "doc123").is_err());

        // Empty relation should fail
        assert!(RelationTuple::try_new(did.clone(), "", "users", "doc123").is_err());

        // Empty collection_id should fail
        assert!(RelationTuple::try_new(did.clone(), "reader", "", "doc123").is_err());

        // Empty doc_id should fail
        assert!(RelationTuple::try_new(did.clone(), "reader", "users", "").is_err());
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

    #[test]
    fn test_deserialize_rejects_path_traversal() {
        // Craft JSON with path separator in relation field
        let malicious_json = r#"{
            "subject": "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK",
            "relation": "reader/../admin",
            "collection_id": "users",
            "doc_id": "doc123"
        }"#;

        let result: std::result::Result<RelationTuple, _> = serde_json::from_str(malicious_json);
        assert!(
            result.is_err(),
            "should reject path traversal in relation field"
        );

        // Craft JSON with path separator in collection_id
        let malicious_json = r#"{
            "subject": "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK",
            "relation": "reader",
            "collection_id": "users/../secrets",
            "doc_id": "doc123"
        }"#;

        let result: std::result::Result<RelationTuple, _> = serde_json::from_str(malicious_json);
        assert!(
            result.is_err(),
            "should reject path traversal in collection_id field"
        );

        // Craft JSON with path separator in doc_id
        let malicious_json = r#"{
            "subject": "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK",
            "relation": "reader",
            "collection_id": "users",
            "doc_id": "doc/123"
        }"#;

        let result: std::result::Result<RelationTuple, _> = serde_json::from_str(malicious_json);
        assert!(
            result.is_err(),
            "should reject path traversal in doc_id field"
        );
    }
}
