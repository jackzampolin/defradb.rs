//! Document head provider trait for DocSync.
//!
//! This trait allows the coordinator to query document heads from the database
//! without having a direct dependency on the DB crate.

use async_trait::async_trait;
use cid::Cid;

/// Trait for providing document and collection head CIDs.
///
/// This is used by the SyncCoordinator to respond to DocSync and BranchableSync requests.
/// The implementation should query the headstore for composite heads.
#[async_trait]
pub trait DocumentHeadProvider: Send + Sync {
    /// Get the composite head CIDs for a document.
    ///
    /// Returns the CIDs stored at /d/{doc_id}/C/{cid} in the headstore.
    /// Returns an empty vector if the document doesn't exist.
    async fn get_document_heads(&self, doc_id: &str) -> Result<Vec<Cid>, String>;

    /// Get the head CIDs for a branchable collection.
    ///
    /// Returns the CIDs stored at /c/{collection_short_id}/{cid} in the headstore.
    /// Returns an empty vector if the collection has no heads.
    async fn get_collection_heads(&self, collection_id: &str) -> Result<Vec<Cid>, String>;
}

/// No-op implementation that returns empty heads.
///
/// Use this when head lookup is not needed.
pub struct NoOpHeadProvider;

#[async_trait]
impl DocumentHeadProvider for NoOpHeadProvider {
    async fn get_document_heads(&self, _doc_id: &str) -> Result<Vec<Cid>, String> {
        Ok(Vec::new())
    }

    async fn get_collection_heads(&self, _collection_id: &str) -> Result<Vec<Cid>, String> {
        Ok(Vec::new())
    }
}
