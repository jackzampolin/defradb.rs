//! Document mutator abstraction for mutation execution.
//!
//! This module provides the `DocMutator` trait which abstracts storage write
//! operations for mutation execution, following the same pattern as `DocFetcher`.

use async_trait::async_trait;
use cid::Cid;
use document::{DocID, Document};
use std::sync::Arc;
use storage::corekv::MaybeSendSync;

use crate::error::Result;

/// Status of P2P broadcast after a mutation.
///
/// This allows callers to know whether changes were successfully broadcast
/// to the P2P network, enabling appropriate handling of partial success.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum BroadcastStatus {
    /// Broadcast succeeded
    Success,
    /// Broadcast failed with the given error message
    Failed(String),
    /// Broadcast was not attempted (P2P not enabled or not applicable)
    #[default]
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

/// Result of a create mutation, including the generated DocID and the document.
#[derive(Debug, Clone)]
pub struct CreateResult {
    /// The generated document ID
    pub doc_id: DocID,
    /// The created document (with ID set)
    pub document: Document,
    /// Status of P2P broadcast (if applicable)
    pub broadcast_status: BroadcastStatus,
    /// The CID of the commit block (for _version queries)
    /// This is the dag-cbor encoded Block CID, not the document data CID.
    pub commit_cid: Option<Cid>,
    /// The raw bytes of the committed composite block (for P2P broadcast)
    pub commit_block: Option<Vec<u8>>,
    /// For branchable collections: the collection block CID to broadcast instead of composite.
    pub broadcast_cid: Option<Cid>,
    /// For branchable collections: the collection block bytes to broadcast.
    pub broadcast_block: Option<Vec<u8>>,
}

impl CreateResult {
    /// Create a new result.
    pub fn new(doc_id: DocID, document: Document) -> Self {
        Self {
            doc_id,
            document,
            broadcast_status: BroadcastStatus::NotAttempted,
            commit_cid: None,
            commit_block: None,
            broadcast_cid: None,
            broadcast_block: None,
        }
    }

    /// Create a result with commit CID and block data (for _version + P2P broadcast).
    pub fn with_commit(
        doc_id: DocID,
        document: Document,
        commit_cid: Cid,
        commit_block: Vec<u8>,
    ) -> Self {
        Self {
            doc_id,
            document,
            broadcast_status: BroadcastStatus::NotAttempted,
            commit_cid: Some(commit_cid),
            commit_block: Some(commit_block),
            broadcast_cid: None,
            broadcast_block: None,
        }
    }

    /// Create a result with broadcast status.
    pub fn with_broadcast(
        doc_id: DocID,
        document: Document,
        broadcast_status: BroadcastStatus,
    ) -> Self {
        Self {
            doc_id,
            document,
            broadcast_status,
            commit_cid: None,
            commit_block: None,
            broadcast_cid: None,
            broadcast_block: None,
        }
    }

    /// Create a result with commit CID and broadcast status.
    pub fn with_commit_and_broadcast(
        doc_id: DocID,
        document: Document,
        commit_cid: Cid,
        commit_block: Vec<u8>,
        broadcast_status: BroadcastStatus,
    ) -> Self {
        Self {
            doc_id,
            document,
            broadcast_status,
            commit_cid: Some(commit_cid),
            commit_block: Some(commit_block),
            broadcast_cid: None,
            broadcast_block: None,
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
    /// The CID of the committed composite block (for P2P broadcast)
    pub commit_cid: Option<Cid>,
    /// The raw bytes of the committed composite block (for P2P broadcast)
    pub commit_block: Option<Vec<u8>>,
    /// For branchable collections: the collection block CID to broadcast instead of composite.
    pub broadcast_cid: Option<Cid>,
    /// For branchable collections: the collection block bytes to broadcast.
    pub broadcast_block: Option<Vec<u8>>,
}

impl UpdateResult {
    /// Create a new result.
    pub fn new(document: Document, fields_modified: usize) -> Self {
        Self {
            document,
            fields_modified,
            broadcast_status: BroadcastStatus::NotAttempted,
            commit_cid: None,
            commit_block: None,
            broadcast_cid: None,
            broadcast_block: None,
        }
    }

    /// Create a result with committed block data (for P2P broadcast).
    pub fn with_commit(
        document: Document,
        fields_modified: usize,
        commit_cid: Cid,
        commit_block: Vec<u8>,
    ) -> Self {
        Self {
            document,
            fields_modified,
            broadcast_status: BroadcastStatus::NotAttempted,
            commit_cid: Some(commit_cid),
            commit_block: Some(commit_block),
            broadcast_cid: None,
            broadcast_block: None,
        }
    }

    /// Create a result with broadcast status.
    pub fn with_broadcast(
        document: Document,
        fields_modified: usize,
        broadcast_status: BroadcastStatus,
    ) -> Self {
        Self {
            document,
            fields_modified,
            broadcast_status,
            commit_cid: None,
            commit_block: None,
            broadcast_cid: None,
            broadcast_block: None,
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
    /// The CID of the committed composite delete block (for P2P broadcast)
    pub commit_cid: Option<Cid>,
    /// The raw bytes of the committed composite delete block (for P2P broadcast)
    pub commit_block: Option<Vec<u8>>,
}

impl DeleteResult {
    /// Create a new result.
    pub fn new(doc_id: DocID, existed: bool) -> Self {
        Self {
            doc_id,
            existed,
            broadcast_status: BroadcastStatus::NotAttempted,
            commit_cid: None,
            commit_block: None,
        }
    }

    /// Create a result with committed block data (for P2P broadcast).
    pub fn with_commit(
        doc_id: DocID,
        existed: bool,
        commit_cid: Cid,
        commit_block: Vec<u8>,
    ) -> Self {
        Self {
            doc_id,
            existed,
            broadcast_status: BroadcastStatus::NotAttempted,
            commit_cid: Some(commit_cid),
            commit_block: Some(commit_block),
        }
    }

    /// Create a result with broadcast status.
    pub fn with_broadcast(doc_id: DocID, existed: bool, broadcast_status: BroadcastStatus) -> Self {
        Self {
            doc_id,
            existed,
            broadcast_status,
            commit_cid: None,
            commit_block: None,
        }
    }
}

/// Controller for a request-scoped mutation batch.
///
/// Implementations own the shared transaction lifecycle for an implicit
/// GraphQL mutation request and are responsible for committing or rolling
/// back all writes performed through the paired mutator/fetcher.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait MutationBatchController: MaybeSendSync {
    /// Commit the shared transaction backing this batch.
    async fn commit(&self) -> Result<()>;

    /// Roll back the shared transaction backing this batch.
    async fn rollback(&self) -> Result<()>;
}

/// A request-scoped mutation batch with a shared mutator and fetcher.
pub struct MutationBatch {
    mutator: Arc<dyn DocMutator>,
    fetcher: Arc<dyn crate::fetcher::DocFetcher>,
    controller: Arc<dyn MutationBatchController>,
}

impl MutationBatch {
    /// Create a new mutation batch wrapper.
    pub fn new(
        mutator: Arc<dyn DocMutator>,
        fetcher: Arc<dyn crate::fetcher::DocFetcher>,
        controller: Arc<dyn MutationBatchController>,
    ) -> Self {
        Self {
            mutator,
            fetcher,
            controller,
        }
    }

    /// Get the shared mutator for this batch.
    pub fn mutator(&self) -> Arc<dyn DocMutator> {
        self.mutator.clone()
    }

    /// Get the shared fetcher for this batch.
    pub fn fetcher(&self) -> Arc<dyn crate::fetcher::DocFetcher> {
        self.fetcher.clone()
    }

    /// Commit the batch transaction.
    pub async fn commit(&self) -> Result<()> {
        self.controller.commit().await
    }

    /// Roll back the batch transaction.
    pub async fn rollback(&self) -> Result<()> {
        self.controller.rollback().await
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
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait DocMutator: MaybeSendSync {
    /// Begin a request-scoped mutation batch.
    ///
    /// Implementations can override this to provide a shared transaction for
    /// multiple top-level GraphQL mutations in one request. The default
    /// implementation disables batching.
    async fn begin_batch(&self) -> Result<Option<MutationBatch>> {
        Ok(None)
    }

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

    /// Create multiple documents in a single transaction.
    ///
    /// The default implementation calls `create()` in a loop. Implementations
    /// can override for single-transaction batching (one commit/fsync for N docs).
    async fn create_many(
        &self,
        collection_name: &str,
        docs: Vec<Document>,
    ) -> Result<Vec<CreateResult>> {
        let mut results = Vec::with_capacity(docs.len());
        for doc in docs {
            results.push(self.create(collection_name, doc).await?);
        }
        Ok(results)
    }

    /// Update an existing document.
    ///
    /// The document must have a valid DocID set. Only dirty (modified) fields
    /// will be updated in storage.
    ///
    /// # Arguments
    ///
    /// * `collection_name` - The name of the collection
    /// * `doc` - The document with updated fields
    /// * `modified_fields` - The set of field names that were modified
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
    async fn update(
        &self,
        collection_name: &str,
        doc: Document,
        modified_fields: std::collections::HashSet<String>,
    ) -> Result<UpdateResult>;

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
