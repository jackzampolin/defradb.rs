//! Document fetcher abstraction for query execution.
//!
//! This module provides the `DocFetcher` trait which abstracts storage access
//! for query execution, along with result types for handling partial fetches.

use async_trait::async_trait;
use document::Document;

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
}
