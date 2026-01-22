//! Document mutator abstraction for mutation execution.
//!
//! This module provides the `DocMutator` trait which abstracts storage write
//! operations for mutation execution, following the same pattern as `DocFetcher`.

use async_trait::async_trait;
use document::{DocID, Document};

use crate::error::Result;

/// Status of P2P broadcast after a mutation.
///
/// This allows callers to know whether changes were successfully broadcast
/// to the P2P network, enabling appropriate handling of partial success.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BroadcastStatus {
    /// Broadcast succeeded
    Success,
    /// Broadcast failed with the given error message
    Failed(String),
    /// Broadcast was not attempted (P2P not enabled or not applicable)
    NotAttempted,
}

impl BroadcastStatus {
    /// Returns true if the broadcast succeeded.
    pub fn is_success(&self) -> bool {
        matches!(self, BroadcastStatus::Success)
    }

    /// Returns true if the broadcast failed.
    pub fn is_failed(&self) -> bool {
        matches!(self, BroadcastStatus::Failed(_))
    }

    /// Returns the error message if the broadcast failed.
    pub fn error(&self) -> Option<&str> {
        match self {
            BroadcastStatus::Failed(msg) => Some(msg),
            _ => None,
        }
    }
}

impl Default for BroadcastStatus {
    fn default() -> Self {
        BroadcastStatus::NotAttempted
    }
}

/// Result of a create mutation, including the generated DocID and the document.
#[derive(Debug, Clone)]
pub struct CreateResult {
    /// The generated document ID
    pub doc_id: DocID,
    /// The created document (with ID set)
    pub document: Document,
    /// Status of P2P broadcast (if applicable)
    pub broadcast_status: BroadcastStatus,
}

impl CreateResult {
    /// Create a new result.
    pub fn new(doc_id: DocID, document: Document) -> Self {
        Self {
            doc_id,
            document,
            broadcast_status: BroadcastStatus::NotAttempted,
        }
    }

    /// Create a result with broadcast status.
    pub fn with_broadcast(doc_id: DocID, document: Document, broadcast_status: BroadcastStatus) -> Self {
        Self {
            doc_id,
            document,
            broadcast_status,
        }
    }
}

/// Result of an update mutation.
#[derive(Debug, Clone)]
pub struct UpdateResult {
    /// The updated document
    pub document: Document,
    /// Number of fields that were modified
    pub fields_modified: usize,
    /// Status of P2P broadcast (if applicable)
    pub broadcast_status: BroadcastStatus,
}

impl UpdateResult {
    /// Create a new result.
    pub fn new(document: Document, fields_modified: usize) -> Self {
        Self {
            document,
            fields_modified,
            broadcast_status: BroadcastStatus::NotAttempted,
        }
    }

    /// Create a result with broadcast status.
    pub fn with_broadcast(document: Document, fields_modified: usize, broadcast_status: BroadcastStatus) -> Self {
        Self {
            document,
            fields_modified,
            broadcast_status,
        }
    }
}

/// Result of a delete mutation.
#[derive(Debug, Clone)]
pub struct DeleteResult {
    /// The document ID that was deleted
    pub doc_id: DocID,
    /// Whether the document existed before deletion
    pub existed: bool,
    /// Status of P2P broadcast (if applicable)
    pub broadcast_status: BroadcastStatus,
}

impl DeleteResult {
    /// Create a new result.
    pub fn new(doc_id: DocID, existed: bool) -> Self {
        Self {
            doc_id,
            existed,
            broadcast_status: BroadcastStatus::NotAttempted,
        }
    }

    /// Create a result with broadcast status.
    pub fn with_broadcast(doc_id: DocID, existed: bool, broadcast_status: BroadcastStatus) -> Self {
        Self {
            doc_id,
            existed,
            broadcast_status,
        }
    }
}

/// Storage abstraction for mutating documents.
///
/// This trait provides write operations for mutations, complementing `DocFetcher`
/// which provides read operations. Implementations are expected to be
/// transaction-scoped, meaning all operations occur within a single transaction.
///
/// # Transaction Semantics
///
/// All mutations performed through a `DocMutator` should be atomic within
/// the transaction context. The caller is responsible for committing or
/// rolling back the transaction after mutations complete.
///
/// # Example
///
/// ```ignore
/// async fn create_user(mutator: &dyn DocMutator, name: &str, age: i64) -> Result<DocID> {
///     let mut doc = Document::new();
///     doc.set("name", name);
///     doc.set("age", age);
///
///     let result = mutator.create("Users", doc).await?;
///     Ok(result.doc_id)
/// }
/// ```
#[async_trait]
pub trait DocMutator: Send + Sync {
    /// Create a new document in a collection.
    ///
    /// The document should NOT have an ID set - the mutator will generate
    /// a content-based DocID and set it on the document before persisting.
    ///
    /// # Arguments
    ///
    /// * `collection_name` - The name of the collection to create the document in
    /// * `doc` - The document to create (ID will be generated)
    ///
    /// # Returns
    ///
    /// The generated DocID and the document with ID set.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The collection does not exist
    /// - The document fails schema validation
    /// - A document with the same content already exists
    async fn create(&self, collection_name: &str, doc: Document) -> Result<CreateResult>;

    /// Update an existing document.
    ///
    /// The document must have a valid DocID set. Only dirty (modified) fields
    /// will be updated in storage.
    ///
    /// # Arguments
    ///
    /// * `collection_name` - The name of the collection
    /// * `doc` - The document with updated fields
    ///
    /// # Returns
    ///
    /// The updated document and count of modified fields.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The collection does not exist
    /// - The document does not have an ID
    /// - The document does not exist in the collection
    /// - The document fails schema validation
    async fn update(&self, collection_name: &str, doc: Document) -> Result<UpdateResult>;

    /// Delete a document by ID.
    ///
    /// # Arguments
    ///
    /// * `collection_name` - The name of the collection
    /// * `doc_id` - The ID of the document to delete
    ///
    /// # Returns
    ///
    /// Whether the document existed before deletion.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The collection does not exist
    /// - A storage error occurs
    async fn delete(&self, collection_name: &str, doc_id: &DocID) -> Result<DeleteResult>;

    /// Check if a document exists.
    ///
    /// # Arguments
    ///
    /// * `collection_name` - The name of the collection
    /// * `doc_id` - The ID of the document to check
    ///
    /// # Returns
    ///
    /// `true` if the document exists, `false` otherwise.
    async fn exists(&self, collection_name: &str, doc_id: &DocID) -> Result<bool>;

    /// Get a document by ID for updating.
    ///
    /// This is a convenience method for fetching a document before updating it.
    /// The returned document will have dirty tracking reset.
    ///
    /// # Arguments
    ///
    /// * `collection_name` - The name of the collection
    /// * `doc_id` - The ID of the document to fetch
    ///
    /// # Returns
    ///
    /// The document if it exists, `None` otherwise.
    async fn get_for_update(
        &self,
        collection_name: &str,
        doc_id: &DocID,
    ) -> Result<Option<Document>>;
}
