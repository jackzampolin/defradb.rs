//! CollectionIndex trait definition

use async_trait::async_trait;
use document::NormalValue;
use schema::IndexDescription;

use crate::corekv::{MaybeSend, MaybeSendSync, Reader, Result, Writer};

/// Trait for collection index implementations.
///
/// Indexes maintain secondary lookup structures for efficient querying
/// of documents by field values other than the primary key.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait CollectionIndex: MaybeSendSync {
    /// Returns the index description (metadata).
    fn description(&self) -> &IndexDescription;

    /// Save adds a new document to the index.
    ///
    /// Called when a new document is created in the collection.
    /// The values slice contains the field values in index field order.
    async fn save<T: Reader + Writer + MaybeSend>(
        &self,
        txn: &mut T,
        doc_id: &str,
        values: &[NormalValue],
    ) -> Result<()>;

    /// Update modifies an existing document's index entry.
    ///
    /// Called when a document is updated. Removes the old entry
    /// and adds a new one with the updated values.
    async fn update<T: Reader + Writer + MaybeSend>(
        &self,
        txn: &mut T,
        doc_id: &str,
        old_values: &[NormalValue],
        new_values: &[NormalValue],
    ) -> Result<()>;

    /// Delete removes a document from the index.
    ///
    /// Called when a document is deleted from the collection.
    async fn delete<T: Reader + Writer + MaybeSend>(
        &self,
        txn: &mut T,
        doc_id: &str,
        values: &[NormalValue],
    ) -> Result<()>;

    /// RemoveAll removes all entries for this index.
    ///
    /// Called when the index is dropped from the collection.
    async fn remove_all<T: Reader + Writer + MaybeSend>(&self, txn: &mut T) -> Result<()>;
}
