//! Document fetcher abstraction for query execution.
//!
//! This module provides the `DocFetcher` trait which abstracts storage access
//! for query execution, along with result types for handling partial fetches.

use async_trait::async_trait;
use document::Document;
use schema::CollectionVersion;
use std::collections::HashMap;
use std::sync::Arc;

use crate::error::Result;

/// Result of fetching documents by ID, including information about missing documents.
#[derive(Debug, Clone)]
pub struct FetchByIdsResult {
    docs: Vec<Document>,
    missing_ids: Vec<String>,
}

impl FetchByIdsResult {
    /// Create a new result with no missing IDs.
    pub fn all_found(docs: Vec<Document>) -> Self {
        Self {
            docs,
            missing_ids: Vec::new(),
        }
    }

    /// Create a new result with some missing IDs.
    pub fn partial(docs: Vec<Document>, missing_ids: Vec<String>) -> Self {
        Self { docs, missing_ids }
    }

    /// Check if all requested documents were found.
    pub fn is_complete(&self) -> bool {
        self.missing_ids.is_empty()
    }

    /// Get the number of documents found.
    pub fn found_count(&self) -> usize {
        self.docs.len()
    }

    /// Get the number of missing documents.
    pub fn missing_count(&self) -> usize {
        self.missing_ids.len()
    }

    /// Get the found documents.
    pub fn docs(&self) -> &[Document] {
        &self.docs
    }

    /// Take ownership of the found documents.
    pub fn into_docs(self) -> Vec<Document> {
        self.docs
    }

    /// Get the IDs that were not found.
    pub fn missing_ids(&self) -> &[String] {
        &self.missing_ids
    }
}

/// Storage abstraction for fetching documents.
#[async_trait]
pub trait DocFetcher: Send + Sync {
    /// Get all documents from a collection.
    async fn get_all(&self, collection_name: &str) -> Result<Vec<Document>>;

    /// Get documents by their IDs.
    ///
    /// Returns both the found documents and the IDs that were not found.
    /// This allows callers to handle missing documents appropriately.
    async fn get_by_ids(
        &self,
        collection_name: &str,
        doc_ids: &[String],
    ) -> Result<FetchByIdsResult>;

    /// Get documents by a field value (for FK lookups).
    ///
    /// This method is optimized for type joins - it looks up documents where
    /// a specific field equals a given value. Implementations may use indexes
    /// for efficient lookups when available.
    ///
    /// # Arguments
    ///
    /// * `collection_name` - The collection to search
    /// * `field_name` - The field to match (e.g., "author_id" for FK lookups)
    /// * `value` - The value to match against
    ///
    /// # Returns
    ///
    /// All documents where the field equals the given value.
    async fn get_by_field_value(
        &self,
        collection_name: &str,
        field_name: &str,
        value: &str,
    ) -> Result<Vec<Document>>;

    /// Fetch commits from the _commits system collection.
    ///
    /// This method fetches commit history from the headstore and blockstore.
    /// Default implementation returns an error - implementations that support
    /// commits queries should override this.
    ///
    /// # Arguments
    ///
    /// * `options` - Query options (docID, cid, depth, fieldName filters)
    ///
    /// # Returns
    ///
    /// Commit documents with fields: cid, height, fieldName, docID, delta,
    /// collectionVersionId, links, heads, signature.
    async fn get_commits(&self, options: &CommitsQueryOptions) -> Result<Vec<Document>> {
        let _ = options;
        Err(crate::error::QueryError::execution(
            "_commits queries are not supported by this fetcher".to_string(),
        ))
    }
}

/// Options for _commits queries
#[derive(Debug, Clone, Default)]
pub struct CommitsQueryOptions {
    /// Filter by document ID
    pub doc_id: Option<String>,
    /// Filter by specific CID
    pub cid: Option<String>,
    /// Maximum depth to traverse (None = unlimited)
    pub depth: Option<u64>,
    /// Filter by field name
    pub field_name: Option<String>,
}

/// Provides collection schemas on-demand.
///
/// This trait abstracts collection resolution, allowing the QueryRunner to
/// resolve collections from the database at query time instead of using a
/// static cache. This eliminates synchronization issues when schemas are
/// added or modified.
#[async_trait]
pub trait CollectionProvider: Send + Sync {
    /// Get a collection schema by name.
    async fn get_collection(&self, name: &str) -> Result<Option<Arc<CollectionVersion>>>;

    /// List all collection names.
    async fn list_collections(&self) -> Result<Vec<String>>;
}

/// Static collection provider for tests and backward compatibility.
///
/// This provider holds a static HashMap of collections, set at construction
/// time. Use this for tests or when collections won't change during runtime.
pub struct StaticCollectionProvider {
    collections: HashMap<String, Arc<CollectionVersion>>,
}

impl StaticCollectionProvider {
    /// Create a new static collection provider from a list of collection schemas.
    pub fn new(collections: Vec<CollectionVersion>) -> Self {
        let map = collections
            .into_iter()
            .map(|c| (c.name.clone(), Arc::new(c)))
            .collect();
        Self { collections: map }
    }

    /// Create a new static collection provider from an existing HashMap.
    pub fn from_map(collections: HashMap<String, Arc<CollectionVersion>>) -> Self {
        Self { collections }
    }
}

#[async_trait]
impl CollectionProvider for StaticCollectionProvider {
    async fn get_collection(&self, name: &str) -> Result<Option<Arc<CollectionVersion>>> {
        Ok(self.collections.get(name).cloned())
    }

    async fn list_collections(&self) -> Result<Vec<String>> {
        Ok(self.collections.keys().cloned().collect())
    }
}
