// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! Secondary index implementations for DefraDB
//!
//! This module provides the `CollectionIndex` trait and implementations
//! for managing secondary indexes on collections.
//!
//! # Index Types
//!
//! - `SimpleIndex`: Non-unique index that appends document ID to the key
//! - `UniqueIndex`: Unique index that stores document ID in the value,
//!   enforcing uniqueness on the indexed field(s)
//!
//! # Key Structure
//!
//! Index keys are structured as:
//! ```text
//! /[CollectionShortID]/[IndexID]/[EncodedFieldValue1][EncodedFieldValue2]...([DocID])
//! ```
//!
//! For SimpleIndex, the document ID is appended to the key.
//! For UniqueIndex, the document ID is stored as the value.
//!
//! # Query Execution
//!
//! Index iterators support:
//! - Exact match (`get`): Find entries with exact field values
//! - Prefix scan (`scan_prefix`): Find entries matching first N fields
//! - Range scan (`scan_range`): Find entries within a range of values
//! - Full scan (`scan`): Iterate all index entries

mod eq_iterator;
mod in_iterator;
mod iterator;
mod matcher;
mod range_iterator;
mod simple;
mod traits;
mod unique;

pub use eq_iterator::ExactMatchIterator;
pub use in_iterator::InIterator;
pub use iterator::{Bound, IndexEntry, IndexIterator};
pub use matcher::{
    EqMatcher, GtMatcher, InMatcher, IndexMatcher, LikeMatcher, LtMatcher, NeMatcher, NinMatcher,
    NlikeMatcher,
};
pub use range_iterator::RangeIterator;
pub use simple::SimpleIndex;
pub use traits::CollectionIndex;
pub use unique::UniqueIndex;

use document::NormalValue;
use schema::IndexDescription;

use crate::corekv::{Reader, Result, Writer};

/// Validate that a document ID is valid for use in index keys.
///
/// Checks that the doc_id is:
/// - Not empty
/// - Valid UTF-8 (guaranteed by &str type parameter)
pub(crate) fn validate_doc_id(doc_id: &str, index_name: &str) -> Result<()> {
    if doc_id.is_empty() {
        return Err(crate::corekv::Error::Other(format!(
            "index '{}': doc_id cannot be empty",
            index_name
        )));
    }
    Ok(())
}

/// Enum for index types (avoids dyn trait issues).
pub enum IndexType {
    Simple(SimpleIndex),
    Unique(UniqueIndex),
}

impl IndexType {
    /// Create the appropriate index type based on description.
    pub fn new(collection_short_id: u32, desc: IndexDescription) -> Self {
        if desc.unique {
            IndexType::Unique(UniqueIndex::new(collection_short_id, desc))
        } else {
            IndexType::Simple(SimpleIndex::new(collection_short_id, desc))
        }
    }

    /// Get the index description.
    pub fn description(&self) -> &IndexDescription {
        match self {
            IndexType::Simple(idx) => idx.description(),
            IndexType::Unique(idx) => idx.description(),
        }
    }

    /// Save adds a new document to the index.
    pub async fn save<T: Reader + Writer + Send>(
        &self,
        txn: &mut T,
        doc_id: &str,
        values: &[NormalValue],
    ) -> Result<()> {
        match self {
            IndexType::Simple(idx) => idx.save(txn, doc_id, values).await,
            IndexType::Unique(idx) => idx.save(txn, doc_id, values).await,
        }
    }

    /// Update modifies an existing document's index entry.
    pub async fn update<T: Reader + Writer + Send>(
        &self,
        txn: &mut T,
        doc_id: &str,
        old_values: &[NormalValue],
        new_values: &[NormalValue],
    ) -> Result<()> {
        match self {
            IndexType::Simple(idx) => idx.update(txn, doc_id, old_values, new_values).await,
            IndexType::Unique(idx) => idx.update(txn, doc_id, old_values, new_values).await,
        }
    }

    /// Delete removes a document from the index.
    pub async fn delete<T: Reader + Writer + Send>(
        &self,
        txn: &mut T,
        doc_id: &str,
        values: &[NormalValue],
    ) -> Result<()> {
        match self {
            IndexType::Simple(idx) => idx.delete(txn, doc_id, values).await,
            IndexType::Unique(idx) => idx.delete(txn, doc_id, values).await,
        }
    }

    /// RemoveAll removes all entries for this index.
    pub async fn remove_all<T: Reader + Writer + Send>(&self, txn: &mut T) -> Result<()> {
        match self {
            IndexType::Simple(idx) => idx.remove_all(txn).await,
            IndexType::Unique(idx) => idx.remove_all(txn).await,
        }
    }

    /// Get all entries with exact field values.
    pub async fn get<R: Reader + Send>(
        &self,
        txn: &R,
        values: &[NormalValue],
    ) -> Result<ExactMatchIterator> {
        match self {
            IndexType::Simple(idx) => idx.get(txn, values).await,
            IndexType::Unique(idx) => idx.get(txn, values).await,
        }
    }

    /// Scan all entries in the index.
    pub async fn scan<R: Reader + Send>(&self, txn: &R, reverse: bool) -> Result<RangeIterator> {
        match self {
            IndexType::Simple(idx) => idx.scan(txn, reverse).await,
            IndexType::Unique(idx) => idx.scan(txn, reverse).await,
        }
    }

    /// Scan entries with a prefix match on the first N fields.
    pub async fn scan_prefix<R: Reader + Send>(
        &self,
        txn: &R,
        prefix_values: &[NormalValue],
        reverse: bool,
    ) -> Result<RangeIterator> {
        match self {
            IndexType::Simple(idx) => idx.scan_prefix(txn, prefix_values, reverse).await,
            IndexType::Unique(idx) => idx.scan_prefix(txn, prefix_values, reverse).await,
        }
    }

    /// Scan entries within a range on a field.
    pub async fn scan_range<R: Reader + Send>(
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::MemoryStore;
    use crate::corekv::{IterOptions, Store};
    use crate::keys::IndexDataStoreKey;
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

    fn composite_index_description(unique: bool) -> IndexDescription {
        IndexDescription {
            id: 2,
            name: "composite_index".to_string(),
            unique,
            fields: vec![
                IndexedFieldDescription {
                    name: "category".to_string(),
                    descending: false,
                },
                IndexedFieldDescription {
                    name: "created_at".to_string(),
                    descending: true,
                },
            ],
        }
    }

    /// Helper to count entries with a prefix
    async fn count_entries(txn: &dyn Reader, prefix: &[u8]) -> usize {
        let opts = IterOptions::default().with_prefix(prefix.to_vec());
        let mut iter = txn.iterator(opts).await.unwrap();
        iter.count().await.unwrap()
    }

    /// Helper to get entries with a prefix
    async fn get_entries(txn: &dyn Reader, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        let opts = IterOptions::default().with_prefix(prefix.to_vec());
        let mut iter = txn.iterator(opts).await.unwrap();
        let items = iter.collect_all().await.unwrap();
        items.into_iter().map(|kv| (kv.key, kv.value)).collect()
    }

    #[tokio::test]
    async fn test_simple_index_save() {
        let store = MemoryStore::new();
        let mut txn = store.new_txn(false).await.unwrap();

        let index = SimpleIndex::new(1, test_index_description(false));
        let values = vec![NormalValue::String("alice".to_string())];

        index.save(&mut txn, "doc1", &values).await.unwrap();
        txn.commit().await.unwrap();

        // Verify entry exists
        let txn = store.new_txn(true).await.unwrap();
        let prefix = IndexDataStoreKey::index_prefix(1, 1);
        let count = count_entries(txn.as_ref(), &prefix).await;
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn test_simple_index_allows_duplicates() {
        let store = MemoryStore::new();
        let mut txn = store.new_txn(false).await.unwrap();

        let index = SimpleIndex::new(1, test_index_description(false));
        let values = vec![NormalValue::String("alice".to_string())];

        // Same value, different doc IDs - should work
        index.save(&mut txn, "doc1", &values).await.unwrap();
        index.save(&mut txn, "doc2", &values).await.unwrap();
        txn.commit().await.unwrap();

        // Verify both entries exist
        let txn = store.new_txn(true).await.unwrap();
        let prefix = IndexDataStoreKey::index_prefix(1, 1);
        let count = count_entries(txn.as_ref(), &prefix).await;
        assert_eq!(count, 2);
    }

    #[tokio::test]
    async fn test_simple_index_update() {
        let store = MemoryStore::new();
        let mut txn = store.new_txn(false).await.unwrap();

        let index = SimpleIndex::new(1, test_index_description(false));
        let old_values = vec![NormalValue::String("alice".to_string())];
        let new_values = vec![NormalValue::String("bob".to_string())];

        index.save(&mut txn, "doc1", &old_values).await.unwrap();
        index
            .update(&mut txn, "doc1", &old_values, &new_values)
            .await
            .unwrap();
        txn.commit().await.unwrap();

        // Verify only one entry exists (with new value)
        let txn = store.new_txn(true).await.unwrap();
        let prefix = IndexDataStoreKey::index_prefix(1, 1);
        let count = count_entries(txn.as_ref(), &prefix).await;
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn test_simple_index_delete() {
        let store = MemoryStore::new();
        let mut txn = store.new_txn(false).await.unwrap();

        let index = SimpleIndex::new(1, test_index_description(false));
        let values = vec![NormalValue::String("alice".to_string())];

        index.save(&mut txn, "doc1", &values).await.unwrap();
        index.delete(&mut txn, "doc1", &values).await.unwrap();
        txn.commit().await.unwrap();

        // Verify no entries exist
        let txn = store.new_txn(true).await.unwrap();
        let prefix = IndexDataStoreKey::index_prefix(1, 1);
        let count = count_entries(txn.as_ref(), &prefix).await;
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_simple_index_remove_all() {
        let store = MemoryStore::new();
        let mut txn = store.new_txn(false).await.unwrap();

        let index = SimpleIndex::new(1, test_index_description(false));

        index
            .save(&mut txn, "doc1", &[NormalValue::String("a".to_string())])
            .await
            .unwrap();
        index
            .save(&mut txn, "doc2", &[NormalValue::String("b".to_string())])
            .await
            .unwrap();
        index
            .save(&mut txn, "doc3", &[NormalValue::String("c".to_string())])
            .await
            .unwrap();
        index.remove_all(&mut txn).await.unwrap();
        txn.commit().await.unwrap();

        // Verify all entries removed
        let txn = store.new_txn(true).await.unwrap();
        let prefix = IndexDataStoreKey::index_prefix(1, 1);
        let count = count_entries(txn.as_ref(), &prefix).await;
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_unique_index_save() {
        let store = MemoryStore::new();
        let mut txn = store.new_txn(false).await.unwrap();

        let index = UniqueIndex::new(1, test_index_description(true));
        let values = vec![NormalValue::String("alice".to_string())];

        index.save(&mut txn, "doc1", &values).await.unwrap();
        txn.commit().await.unwrap();

        // Verify entry exists with doc_id in value
        let txn = store.new_txn(true).await.unwrap();
        let prefix = IndexDataStoreKey::index_prefix(1, 1);
        let entries = get_entries(txn.as_ref(), &prefix).await;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].1, "doc1".as_bytes());
    }

    #[tokio::test]
    async fn test_unique_index_rejects_duplicates() {
        let store = MemoryStore::new();
        let mut txn = store.new_txn(false).await.unwrap();

        let index = UniqueIndex::new(1, test_index_description(true));
        let values = vec![NormalValue::String("alice".to_string())];

        index.save(&mut txn, "doc1", &values).await.unwrap();

        // Same value, different doc ID - should fail
        let result = index.save(&mut txn, "doc2", &values).await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("constraint violation"),
            "error should mention constraint violation: {}",
            err_msg
        );
    }

    #[tokio::test]
    async fn test_unique_index_allows_same_doc_update() {
        let store = MemoryStore::new();
        let mut txn = store.new_txn(false).await.unwrap();

        let index = UniqueIndex::new(1, test_index_description(true));
        let values = vec![NormalValue::String("alice".to_string())];

        index.save(&mut txn, "doc1", &values).await.unwrap();

        // Same value, same doc ID - should work (idempotent)
        index.save(&mut txn, "doc1", &values).await.unwrap();
    }

    #[tokio::test]
    async fn test_unique_index_null_allows_duplicates() {
        let store = MemoryStore::new();
        let mut txn = store.new_txn(false).await.unwrap();

        let index = UniqueIndex::new(1, test_index_description(true));
        let null_values = vec![NormalValue::Null];

        // Multiple docs with NULL value - should be allowed
        index.save(&mut txn, "doc1", &null_values).await.unwrap();
        index.save(&mut txn, "doc2", &null_values).await.unwrap();
        txn.commit().await.unwrap();

        // Verify both entries exist
        let txn = store.new_txn(true).await.unwrap();
        let prefix = IndexDataStoreKey::index_prefix(1, 1);
        let count = count_entries(txn.as_ref(), &prefix).await;
        assert_eq!(count, 2);
    }

    #[tokio::test]
    async fn test_unique_index_update() {
        let store = MemoryStore::new();
        let mut txn = store.new_txn(false).await.unwrap();

        let index = UniqueIndex::new(1, test_index_description(true));
        let old_values = vec![NormalValue::String("alice".to_string())];
        let new_values = vec![NormalValue::String("bob".to_string())];

        index.save(&mut txn, "doc1", &old_values).await.unwrap();
        index
            .update(&mut txn, "doc1", &old_values, &new_values)
            .await
            .unwrap();
        txn.commit().await.unwrap();

        // Verify only new entry exists
        let txn = store.new_txn(true).await.unwrap();
        let prefix = IndexDataStoreKey::index_prefix(1, 1);
        let entries = get_entries(txn.as_ref(), &prefix).await;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].1, "doc1".as_bytes());
    }

    #[tokio::test]
    async fn test_unique_index_delete() {
        let store = MemoryStore::new();
        let mut txn = store.new_txn(false).await.unwrap();

        let index = UniqueIndex::new(1, test_index_description(true));
        let values = vec![NormalValue::String("alice".to_string())];

        index.save(&mut txn, "doc1", &values).await.unwrap();
        index.delete(&mut txn, "doc1", &values).await.unwrap();
        txn.commit().await.unwrap();

        // Verify no entries exist
        let txn = store.new_txn(true).await.unwrap();
        let prefix = IndexDataStoreKey::index_prefix(1, 1);
        let count = count_entries(txn.as_ref(), &prefix).await;
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_composite_index() {
        let store = MemoryStore::new();
        let mut txn = store.new_txn(false).await.unwrap();

        let index = SimpleIndex::new(1, composite_index_description(false));
        let values = vec![
            NormalValue::String("electronics".to_string()),
            NormalValue::Int(1705000000),
        ];

        index.save(&mut txn, "doc1", &values).await.unwrap();
        txn.commit().await.unwrap();

        let txn = store.new_txn(true).await.unwrap();
        let prefix = IndexDataStoreKey::index_prefix(1, 2);
        let count = count_entries(txn.as_ref(), &prefix).await;
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn test_index_type_factory() {
        let simple_desc = test_index_description(false);
        let unique_desc = test_index_description(true);

        let simple_index = IndexType::new(1, simple_desc);
        assert!(!simple_index.description().unique);

        let unique_index = IndexType::new(1, unique_desc);
        assert!(unique_index.description().unique);
    }

    #[tokio::test]
    async fn test_index_sort_order() {
        let store = MemoryStore::new();
        let mut txn = store.new_txn(false).await.unwrap();

        let index = SimpleIndex::new(1, test_index_description(false));

        // Insert in non-sorted order
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

        // Verify sorted iteration
        let txn = store.new_txn(true).await.unwrap();
        let prefix = IndexDataStoreKey::index_prefix(1, 1);
        let entries = get_entries(txn.as_ref(), &prefix).await;
        assert_eq!(entries.len(), 3);

        // Keys should be in lexicographic order of encoded values
        // alice < bob < charlie
        let keys: Vec<Vec<u8>> = entries.iter().map(|(k, _)| k.clone()).collect();
        assert!(keys[0] < keys[1], "alice key should be < bob key");
        assert!(keys[1] < keys[2], "bob key should be < charlie key");
    }

    #[tokio::test]
    async fn test_unique_index_update_to_existing_value_fails() {
        let store = MemoryStore::new();
        let mut txn = store.new_txn(false).await.unwrap();

        let index = UniqueIndex::new(1, test_index_description(true));

        // Save two documents with different values
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

        // Try to update doc1 to have the same value as doc2 - should fail
        let result = index
            .update(
                &mut txn,
                "doc1",
                &[NormalValue::String("alice".to_string())],
                &[NormalValue::String("bob".to_string())],
            )
            .await;

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("constraint violation"),
            "error should mention constraint violation: {}",
            err_msg
        );
        assert!(
            err_msg.contains("doc2"),
            "error should mention conflicting document: {}",
            err_msg
        );
    }

    #[tokio::test]
    async fn test_composite_index_sort_order() {
        let store = MemoryStore::new();
        let mut txn = store.new_txn(false).await.unwrap();

        let index = SimpleIndex::new(1, composite_index_description(false));

        // Insert documents with same first field, different second field
        // composite_index_description has first field ascending, second descending
        index
            .save(
                &mut txn,
                "doc1",
                &[
                    NormalValue::String("electronics".to_string()),
                    NormalValue::Int(100),
                ],
            )
            .await
            .unwrap();
        index
            .save(
                &mut txn,
                "doc2",
                &[
                    NormalValue::String("electronics".to_string()),
                    NormalValue::Int(300),
                ],
            )
            .await
            .unwrap();
        index
            .save(
                &mut txn,
                "doc3",
                &[
                    NormalValue::String("electronics".to_string()),
                    NormalValue::Int(200),
                ],
            )
            .await
            .unwrap();
        txn.commit().await.unwrap();

        // Verify entries
        let txn = store.new_txn(true).await.unwrap();
        let prefix = IndexDataStoreKey::index_prefix(1, 2);
        let entries = get_entries(txn.as_ref(), &prefix).await;
        assert_eq!(entries.len(), 3);

        // Since second field is descending, keys should be ordered:
        // electronics/300 < electronics/200 < electronics/100
        // (higher int values should have smaller byte sequences for descending)
        let keys: Vec<Vec<u8>> = entries.iter().map(|(k, _)| k.clone()).collect();
        assert!(
            keys[0] < keys[1] && keys[1] < keys[2],
            "composite index should maintain correct sort order"
        );
    }
}
