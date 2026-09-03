use document::NormalValue;
use schema::IndexDescription;

use storage::corekv::{MaybeSend, Reader, Result, Writer};
use storage::index::{
    Bound, CollectionIndex, ExactMatchIterator, FullTextIndex, RangeIterator, SimpleIndex,
    UniqueIndex,
};

use crate::index::vector::index::VectorIndex;

/// Which concrete index a collection entry is, and the dispatch over them.
///
/// An enum rather than `dyn CollectionIndex`, because that trait's methods are
/// generic over the transaction type and so not object-safe. It lives beside
/// the manager that dispatches on it rather than in `storage` with the
/// implementations, so a kind whose engine lives here can be a variant.
#[non_exhaustive]
pub enum IndexType {
    Simple(SimpleIndex),
    Unique(UniqueIndex),
    FullText(FullTextIndex),
    Vector(VectorIndex),
}

impl IndexType {
    /// Create the appropriate index type based on description.
    ///
    /// A vector description reaches [`IndexType::try_new`] instead, because a
    /// vector index can be rejected (out-of-range parameters) and this cannot
    /// report that. A vector description here falls back to an ordered index
    /// rather than silently mis-indexing.
    pub fn new(collection_short_id: u32, desc: IndexDescription) -> Self {
        if desc.is_vector() {
            if let Ok(index) = VectorIndex::try_new(collection_short_id, desc.clone()) {
                return IndexType::Vector(index);
            }
        }
        if desc.resolved_unique() {
            IndexType::Unique(UniqueIndex::new(collection_short_id, desc))
        } else {
            IndexType::Simple(SimpleIndex::new(collection_short_id, desc))
        }
    }

    /// Like [`IndexType::new`], but surfaces why a vector description was
    /// refused rather than quietly building something else.
    pub fn try_new(
        collection_short_id: u32,
        desc: IndexDescription,
    ) -> crate::index::error::Result<Self> {
        if desc.is_vector() {
            return Ok(IndexType::Vector(VectorIndex::try_new(
                collection_short_id,
                desc,
            )?));
        }
        Ok(Self::new(collection_short_id, desc))
    }

    /// Get a reference to the VectorIndex if this is one.
    pub fn as_vector(&self) -> Option<&VectorIndex> {
        match self {
            IndexType::Vector(idx) => Some(idx),
            _ => None,
        }
    }

    /// Get the index description.
    pub fn description(&self) -> &IndexDescription {
        match self {
            IndexType::Simple(idx) => idx.description(),
            IndexType::Unique(idx) => idx.description(),
            IndexType::FullText(idx) => idx.description(),
            IndexType::Vector(idx) => idx.description(),
        }
    }

    /// Get a reference to the FullTextIndex if this is one.
    pub fn as_fulltext(&self) -> Option<&FullTextIndex> {
        match self {
            IndexType::FullText(idx) => Some(idx),
            _ => None,
        }
    }

    /// Save adds a new document to the index.
    pub async fn save<T: Reader + Writer + MaybeSend>(
        &self,
        txn: &mut T,
        doc_short_id: u64,
        values: &[NormalValue],
    ) -> Result<()> {
        match self {
            IndexType::Simple(idx) => idx.save(txn, doc_short_id, values).await,
            IndexType::Unique(idx) => idx.save(txn, doc_short_id, values).await,
            IndexType::FullText(idx) => idx.save(txn, doc_short_id, values).await,
            IndexType::Vector(idx) => idx.save(txn, doc_short_id, values).await,
        }
    }

    /// Update modifies an existing document's index entry.
    pub async fn update<T: Reader + Writer + MaybeSend>(
        &self,
        txn: &mut T,
        doc_short_id: u64,
        old_values: &[NormalValue],
        new_values: &[NormalValue],
    ) -> Result<()> {
        match self {
            IndexType::Simple(idx) => idx.update(txn, doc_short_id, old_values, new_values).await,
            IndexType::Unique(idx) => idx.update(txn, doc_short_id, old_values, new_values).await,
            IndexType::FullText(idx) => idx.update(txn, doc_short_id, old_values, new_values).await,
            IndexType::Vector(idx) => idx.update(txn, doc_short_id, old_values, new_values).await,
        }
    }

    /// Delete removes a document from the index.
    pub async fn delete<T: Reader + Writer + MaybeSend>(
        &self,
        txn: &mut T,
        doc_short_id: u64,
        values: &[NormalValue],
    ) -> Result<()> {
        match self {
            IndexType::Simple(idx) => idx.delete(txn, doc_short_id, values).await,
            IndexType::Unique(idx) => idx.delete(txn, doc_short_id, values).await,
            IndexType::FullText(idx) => idx.delete(txn, doc_short_id, values).await,
            IndexType::Vector(idx) => idx.delete(txn, doc_short_id, values).await,
        }
    }

    /// Gives a vector index one chance to train and build, if it should. A
    /// no-op for every other index kind, which has nothing to train.
    pub async fn build_if_needed<T: Reader + Writer + MaybeSend>(&self, txn: &mut T) -> Result<()> {
        match self {
            IndexType::Simple(_) | IndexType::Unique(_) | IndexType::FullText(_) => Ok(()),
            IndexType::Vector(idx) => idx.build_if_needed(txn).await,
        }
    }

    /// RemoveAll removes all entries for this index.
    pub async fn remove_all<T: Reader + Writer + MaybeSend>(&self, txn: &mut T) -> Result<()> {
        match self {
            IndexType::Simple(idx) => idx.remove_all(txn).await,
            IndexType::Unique(idx) => idx.remove_all(txn).await,
            IndexType::FullText(idx) => idx.remove_all(txn).await,
            IndexType::Vector(idx) => idx.remove_all(txn).await,
        }
    }

    /// Get all entries with exact field values.
    pub async fn get<R: Reader + MaybeSend>(
        &self,
        txn: &R,
        values: &[NormalValue],
    ) -> Result<ExactMatchIterator> {
        match self {
            IndexType::Simple(idx) => idx.get(txn, values).await,
            IndexType::Unique(idx) => idx.get(txn, values).await,
            IndexType::FullText(_) | IndexType::Vector(_) => Err(storage::corekv::Error::Other(
                "exact match get is not supported on full-text or vector indexes: neither \
                 stores entries in key order"
                    .to_string(),
            )),
        }
    }

    /// Scan all entries in the index.
    pub async fn scan<R: Reader + MaybeSend>(
        &self,
        txn: &R,
        reverse: bool,
    ) -> Result<RangeIterator> {
        match self {
            IndexType::Simple(idx) => idx.scan(txn, reverse).await,
            IndexType::Unique(idx) => idx.scan(txn, reverse).await,
            IndexType::FullText(_) | IndexType::Vector(_) => Err(storage::corekv::Error::Other(
                "scan is not supported on full-text or vector indexes: neither \
                 stores entries in key order"
                    .to_string(),
            )),
        }
    }

    /// Scan entries with a prefix match on the first N fields.
    pub async fn scan_prefix<R: Reader + MaybeSend>(
        &self,
        txn: &R,
        prefix_values: &[NormalValue],
        reverse: bool,
    ) -> Result<RangeIterator> {
        match self {
            IndexType::Simple(idx) => idx.scan_prefix(txn, prefix_values, reverse).await,
            IndexType::Unique(idx) => idx.scan_prefix(txn, prefix_values, reverse).await,
            IndexType::FullText(_) | IndexType::Vector(_) => Err(storage::corekv::Error::Other(
                "scan_prefix is not supported on full-text or vector indexes: neither \
                 stores entries in key order"
                    .to_string(),
            )),
        }
    }

    /// Scan entries within a range on a field.
    pub async fn scan_range<R: Reader + MaybeSend>(
        &self,
        txn: &R,
        prefix_values: &[NormalValue],
        lower: Bound,
        upper: Bound,
        reverse: bool,
    ) -> Result<RangeIterator> {
        match self {
            IndexType::Simple(idx) => {
                idx.scan_range(txn, prefix_values, lower, upper, reverse)
                    .await
            }
            IndexType::Unique(idx) => {
                idx.scan_range(txn, prefix_values, lower, upper, reverse)
                    .await
            }
            IndexType::FullText(_) | IndexType::Vector(_) => Err(storage::corekv::Error::Other(
                "scan_range is not supported on full-text or vector indexes: neither \
                 stores entries in key order"
                    .to_string(),
            )),
        }
    }
}
