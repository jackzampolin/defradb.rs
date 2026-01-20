//! ACP tuple storage trait.
//!
//! Defines the interface for storing and retrieving relation tuples.

use async_trait::async_trait;
use identity::Did;

use crate::error::Result;
use crate::relation::RelationTuple;

/// Trait for storing and querying relation tuples.
///
/// This abstraction allows different storage backends to be used
/// (in-memory for testing, RocksDB for production, etc.)
///
/// # Security: Input Validation
///
/// **CRITICAL**: Implementations MUST validate all string inputs (collection_id, doc_id,
/// relation) to prevent path traversal attacks. Use [`RelationTuple::validate_prefix`]
/// or [`RelationTuple::validate_relation_prefix`] before constructing storage keys.
///
/// Inputs containing path separators (`/` or `\`) MUST be rejected with an error.
/// Never construct storage keys from unvalidated user input.
///
/// # Security: Error Handling
///
/// All operations that check permissions MUST follow a fail-closed pattern:
/// - On success: return the actual result
/// - On error: callers MUST treat the error as "access denied"
///
/// Never allow errors to result in unauthorized access.
#[async_trait]
pub trait AcpStore: Send + Sync {
    /// Store a relation tuple.
    ///
    /// The tuple has already been validated by construction.
    async fn put_tuple(&self, tuple: &RelationTuple) -> Result<()>;

    /// Delete a relation tuple.
    ///
    /// The tuple has already been validated by construction.
    async fn delete_tuple(&self, tuple: &RelationTuple) -> Result<()>;

    /// Check if a tuple exists.
    ///
    /// The tuple has already been validated by construction.
    async fn has_tuple(&self, tuple: &RelationTuple) -> Result<bool>;

    /// Get all tuples for a document.
    ///
    /// # Security
    ///
    /// Implementations MUST validate `collection_id` and `doc_id` using
    /// [`RelationTuple::validate_prefix`] before constructing storage keys.
    async fn get_doc_tuples(&self, collection_id: &str, doc_id: &str)
        -> Result<Vec<RelationTuple>>;

    /// Get all subjects with a specific relation to a document.
    ///
    /// # Security
    ///
    /// Implementations MUST validate `collection_id`, `doc_id`, and `relation` using
    /// [`RelationTuple::validate_relation_prefix`] before constructing storage keys.
    async fn get_relation_subjects(
        &self,
        collection_id: &str,
        doc_id: &str,
        relation: &str,
    ) -> Result<Vec<Did>>;

    /// Get all relations a subject has to a document.
    ///
    /// # Security
    ///
    /// Implementations MUST validate `collection_id` and `doc_id` using
    /// [`RelationTuple::validate_prefix`] before constructing storage keys.
    async fn get_subject_relations(
        &self,
        subject: &Did,
        collection_id: &str,
        doc_id: &str,
    ) -> Result<Vec<String>>;

    /// Delete all tuples for a document.
    ///
    /// # Security
    ///
    /// Implementations MUST validate `collection_id` and `doc_id` using
    /// [`RelationTuple::validate_prefix`] before constructing storage keys.
    async fn delete_doc_tuples(&self, collection_id: &str, doc_id: &str) -> Result<()>;

    /// Check if a document has any tuples (i.e., is registered with ACP).
    ///
    /// # Security
    ///
    /// Implementations MUST validate `collection_id` and `doc_id` using
    /// [`RelationTuple::validate_prefix`] before constructing storage keys.
    async fn is_doc_registered(&self, collection_id: &str, doc_id: &str) -> Result<bool>;
}
