// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
use crate::corekv::{Reader, Result};

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
    /// Set of already-seen doc_ids for deduplication
    seen_doc_ids: HashSet<String>,
    /// Whether the iterator has been exhausted
    exhausted: bool,
    /// Cached results since we can't hold a reference to the transaction
    cached_results: Vec<IndexEntry>,
    /// Current position in cached results
    cache_position: usize,
}

impl InIterator {
    /// Create a new IN iterator for a simple (non-unique) index.
    pub async fn new_simple<R: Reader + Send + ?Sized>(
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
    pub async fn new_unique<R: Reader + Send + ?Sized>(
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
    async fn prefetch_all<R: Reader + Send + ?Sized>(
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
                    &[value.clone()],
                )
                .await?
            } else {
                ExactMatchIterator::new_simple(
                    txn,
                    collection_short_id,
                    &self.desc,
                    &[value.clone()],
                )
                .await?
            };

            while let Some(entry) = iter.next().await? {
                // Deduplicate by doc_id
                if self.seen_doc_ids.insert(entry.doc_id.clone()) {
                    self.cached_results.push(entry);
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::MemoryStore;
    use crate::corekv::Store;
    use crate::index::{CollectionIndex, SimpleIndex, UniqueIndex};
    use schema::IndexedFieldDescription;

    fn test_index_description(unique: bool) -> IndexDescription {
        IndexDescription {
            id: 1,
            name: "test_index".to_string(),
            unique,
            fields: vec![IndexedFieldDescription {
                name: "name".to_string(),
                descending: false,
            }],
        }
    }

    #[tokio::test]
    async fn test_in_iterator_simple_finds_multiple_values() {
        let store = MemoryStore::new();
        let mut txn = store.new_txn(false).await.unwrap();

        let desc = test_index_description(false);
        let index = SimpleIndex::new(1, desc.clone());

        // Insert documents with different values
        index
            .save(
                &mut txn,
                "doc1",
                &[NormalValue::String("alice".to_string())],
            )
            .await
            .unwrap();
        index
            .save(&mut txn, "doc2", &[NormalValue::String("bob".to_string())])
            .await
            .unwrap();
        index
            .save(
                &mut txn,
                "doc3",
                &[NormalValue::String("charlie".to_string())],
            )
            .await
            .unwrap();
        index
            .save(
                &mut txn,
                "doc4",
                &[NormalValue::String("david".to_string())],
            )
            .await
            .unwrap();
        txn.commit().await.unwrap();

        let txn = store.new_txn(true).await.unwrap();
        let mut iter = InIterator::new_simple(
            txn.as_ref(),
            1,
            &desc,
            &[
                NormalValue::String("alice".to_string()),
                NormalValue::String("charlie".to_string()),
            ],
        )
        .await
        .unwrap();

        let entries = iter.collect_all().await.unwrap();
        assert_eq!(entries.len(), 2);

        let doc_ids: Vec<&str> = entries.iter().map(|e| e.doc_id.as_str()).collect();
        assert!(doc_ids.contains(&"doc1"));
        assert!(doc_ids.contains(&"doc3"));
    }

    #[tokio::test]
    async fn test_in_iterator_simple_handles_duplicates_same_value() {
        let store = MemoryStore::new();
        let mut txn = store.new_txn(false).await.unwrap();

        let desc = test_index_description(false);
        let index = SimpleIndex::new(1, desc.clone());

        // Insert multiple documents with same indexed value
        index
            .save(
                &mut txn,
                "doc1",
                &[NormalValue::String("alice".to_string())],
            )
            .await
            .unwrap();
        index
            .save(
                &mut txn,
                "doc2",
                &[NormalValue::String("alice".to_string())],
            )
            .await
            .unwrap();
        index
            .save(&mut txn, "doc3", &[NormalValue::String("bob".to_string())])
            .await
            .unwrap();
        txn.commit().await.unwrap();

        let txn = store.new_txn(true).await.unwrap();
        let mut iter = InIterator::new_simple(
            txn.as_ref(),
            1,
            &desc,
            &[NormalValue::String("alice".to_string())],
        )
        .await
        .unwrap();

        let entries = iter.collect_all().await.unwrap();
        assert_eq!(entries.len(), 2);

        let doc_ids: Vec<&str> = entries.iter().map(|e| e.doc_id.as_str()).collect();
        assert!(doc_ids.contains(&"doc1"));
        assert!(doc_ids.contains(&"doc2"));
    }

    #[tokio::test]
    async fn test_in_iterator_unique_finds_values() {
        let store = MemoryStore::new();
        let mut txn = store.new_txn(false).await.unwrap();

        let desc = test_index_description(true);
        let index = UniqueIndex::new(1, desc.clone());

        index
            .save(
                &mut txn,
                "doc1",
                &[NormalValue::String("alice".to_string())],
            )
            .await
            .unwrap();
        index
            .save(&mut txn, "doc2", &[NormalValue::String("bob".to_string())])
            .await
            .unwrap();
        index
            .save(
                &mut txn,
                "doc3",
                &[NormalValue::String("charlie".to_string())],
            )
            .await
            .unwrap();
        txn.commit().await.unwrap();

        let txn = store.new_txn(true).await.unwrap();
        let mut iter = InIterator::new_unique(
            txn.as_ref(),
            1,
            &desc,
            &[
                NormalValue::String("alice".to_string()),
                NormalValue::String("bob".to_string()),
            ],
        )
        .await
        .unwrap();

        let entries = iter.collect_all().await.unwrap();
        assert_eq!(entries.len(), 2);
    }

    #[tokio::test]
    async fn test_in_iterator_empty_result() {
        let store = MemoryStore::new();
        let mut txn = store.new_txn(false).await.unwrap();

        let desc = test_index_description(false);
        let index = SimpleIndex::new(1, desc.clone());

        index
            .save(
                &mut txn,
                "doc1",
                &[NormalValue::String("alice".to_string())],
            )
            .await
            .unwrap();
        txn.commit().await.unwrap();

        let txn = store.new_txn(true).await.unwrap();
        let mut iter = InIterator::new_simple(
            txn.as_ref(),
            1,
            &desc,
            &[
                NormalValue::String("bob".to_string()),
                NormalValue::String("charlie".to_string()),
            ],
        )
        .await
        .unwrap();

        let entries = iter.collect_all().await.unwrap();
        assert_eq!(entries.len(), 0);
    }

    #[tokio::test]
    async fn test_in_iterator_reset() {
        let store = MemoryStore::new();
        let mut txn = store.new_txn(false).await.unwrap();

        let desc = test_index_description(false);
        let index = SimpleIndex::new(1, desc.clone());

        index
            .save(
                &mut txn,
                "doc1",
                &[NormalValue::String("alice".to_string())],
            )
            .await
            .unwrap();
        index
            .save(&mut txn, "doc2", &[NormalValue::String("bob".to_string())])
            .await
            .unwrap();
        txn.commit().await.unwrap();

        let txn = store.new_txn(true).await.unwrap();
        let mut iter = InIterator::new_simple(
            txn.as_ref(),
            1,
            &desc,
            &[
                NormalValue::String("alice".to_string()),
                NormalValue::String("bob".to_string()),
            ],
        )
        .await
        .unwrap();

        // Consume all entries
        let entries1 = iter.collect_all().await.unwrap();
        assert_eq!(entries1.len(), 2);

        // Reset and collect again
        iter.reset().await.unwrap();
        let entries2 = iter.collect_all().await.unwrap();
        assert_eq!(entries2.len(), 2);
    }

    #[tokio::test]
    async fn test_in_iterator_with_null_values() {
        let store = MemoryStore::new();
        let mut txn = store.new_txn(false).await.unwrap();

        let desc = test_index_description(false);
        let index = SimpleIndex::new(1, desc.clone());

        index
            .save(&mut txn, "doc1", &[NormalValue::Null])
            .await
            .unwrap();
        index
            .save(
                &mut txn,
                "doc2",
                &[NormalValue::String("alice".to_string())],
            )
            .await
            .unwrap();
        txn.commit().await.unwrap();

        let txn = store.new_txn(true).await.unwrap();
        let mut iter = InIterator::new_simple(
            txn.as_ref(),
            1,
            &desc,
            &[NormalValue::Null, NormalValue::String("alice".to_string())],
        )
        .await
        .unwrap();

        let entries = iter.collect_all().await.unwrap();
        assert_eq!(entries.len(), 2);
    }

    #[tokio::test]
    async fn test_in_iterator_integer_values() {
        let store = MemoryStore::new();
        let mut txn = store.new_txn(false).await.unwrap();

        let desc = IndexDescription {
            id: 1,
            name: "age_index".to_string(),
            unique: false,
            fields: vec![IndexedFieldDescription {
                name: "age".to_string(),
                descending: false,
            }],
        };
        let index = SimpleIndex::new(1, desc.clone());

        index
            .save(&mut txn, "doc1", &[NormalValue::Int(25)])
            .await
            .unwrap();
        index
            .save(&mut txn, "doc2", &[NormalValue::Int(30)])
            .await
            .unwrap();
        index
            .save(&mut txn, "doc3", &[NormalValue::Int(35)])
            .await
            .unwrap();
        index
            .save(&mut txn, "doc4", &[NormalValue::Int(40)])
            .await
            .unwrap();
        txn.commit().await.unwrap();

        let txn = store.new_txn(true).await.unwrap();
        let mut iter = InIterator::new_simple(
            txn.as_ref(),
            1,
            &desc,
            &[NormalValue::Int(25), NormalValue::Int(35)],
        )
        .await
        .unwrap();

        let entries = iter.collect_all().await.unwrap();
        assert_eq!(entries.len(), 2);

        let doc_ids: Vec<&str> = entries.iter().map(|e| e.doc_id.as_str()).collect();
        assert!(doc_ids.contains(&"doc1"));
        assert!(doc_ids.contains(&"doc3"));
    }
}
