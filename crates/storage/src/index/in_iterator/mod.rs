//! IN operator iterator for multi-value index lookups
//!
//! Provides efficient iteration over multiple exact match values,
//! supporting the _in filter operator.

use std::collections::HashSet;

use async_trait::async_trait;
use document::NormalValue;
use schema::IndexDescription;

use super::eq_iterator::ExactMatchIterator;
use super::iterator::{IndexEntry, IndexIterator};
use crate::corekv::{MaybeSend, Reader, Result};

#[cfg(test)]
mod tests;

/// Iterator for IN operator queries on an index.
///
/// Efficiently looks up multiple values by creating and iterating through
/// ExactMatchIterators for each value in the set. Results are pre-fetched
/// and cached to avoid holding transaction references.
pub struct InIterator {
    /// The values to look up
    values: Vec<NormalValue>,
    /// Index description for creating iterators
    desc: IndexDescription,
    /// Whether this is a unique index
    is_unique: bool,
    /// Set of already-seen doc short IDs for deduplication
    seen_doc_ids: HashSet<u64>,
    /// Whether the iterator has been exhausted
    exhausted: bool,
    /// Cached results since we can't hold a reference to the transaction
    cached_results: Vec<IndexEntry>,
    /// Current position in cached results
    cache_position: usize,
}

impl InIterator {
    /// Create a new IN iterator for a simple (non-unique) index.
    pub async fn new_simple<R: Reader + MaybeSend + ?Sized>(
        txn: &R,
        collection_short_id: u32,
        desc: &IndexDescription,
        values: &[NormalValue],
    ) -> Result<Self> {
        let mut iter = Self {
            values: values.to_vec(),
            desc: desc.clone(),
            is_unique: false,
            seen_doc_ids: HashSet::new(),
            exhausted: false,
            cached_results: Vec::new(),
            cache_position: 0,
        };

        // Pre-fetch all results since we can't hold the transaction reference
        iter.prefetch_all(txn, collection_short_id).await?;
        Ok(iter)
    }

    /// Create a new IN iterator for a unique index.
    pub async fn new_unique<R: Reader + MaybeSend + ?Sized>(
        txn: &R,
        collection_short_id: u32,
        desc: &IndexDescription,
        values: &[NormalValue],
    ) -> Result<Self> {
        let mut iter = Self {
            values: values.to_vec(),
            desc: desc.clone(),
            is_unique: true,
            seen_doc_ids: HashSet::new(),
            exhausted: false,
            cached_results: Vec::new(),
            cache_position: 0,
        };

        // Pre-fetch all results
        iter.prefetch_all(txn, collection_short_id).await?;
        Ok(iter)
    }

    /// Pre-fetch all results into the cache.
    async fn prefetch_all<R: Reader + MaybeSend + ?Sized>(
        &mut self,
        txn: &R,
        collection_short_id: u32,
    ) -> Result<()> {
        for value in &self.values {
            let mut iter = if self.is_unique {
                ExactMatchIterator::new_unique(
                    txn,
                    collection_short_id,
                    &self.desc,
                    std::slice::from_ref(value),
                )
                .await?
            } else {
                ExactMatchIterator::new_simple(
                    txn,
                    collection_short_id,
                    &self.desc,
                    std::slice::from_ref(value),
                )
                .await?
            };

            while let Some(entry) = iter.next().await? {
                // Deduplicate by doc short ID
                if self.seen_doc_ids.insert(entry.doc_short_id) {
                    self.cached_results.push(entry);
                }
            }

            // Explicitly close the iterator per Iterator trait contract
            iter.close().await?;
        }

        Ok(())
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl IndexIterator for InIterator {
    async fn next(&mut self) -> Result<Option<IndexEntry>> {
        if self.exhausted || self.cache_position >= self.cached_results.len() {
            self.exhausted = true;
            return Ok(None);
        }

        let entry = self.cached_results[self.cache_position].clone();
        self.cache_position += 1;
        Ok(Some(entry))
    }

    async fn close(&mut self) -> Result<()> {
        self.exhausted = true;
        self.cached_results.clear();
        Ok(())
    }

    async fn reset(&mut self) -> Result<()> {
        self.cache_position = 0;
        self.exhausted = false;
        Ok(())
    }
}
