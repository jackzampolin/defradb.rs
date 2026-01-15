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

use async_trait::async_trait;
use document::NormalValue;
use schema::IndexDescription;

use crate::corekv::{IterOptions, Key, Reader, Result, Writer};
use crate::keys::datastore::IndexedField;
use crate::keys::IndexDataStoreKey;

/// Trait for collection index implementations.
///
/// Indexes maintain secondary lookup structures for efficient querying
/// of documents by field values other than the primary key.
#[async_trait]
pub trait CollectionIndex: Send + Sync {
    /// Returns the index description (metadata).
    fn description(&self) -> &IndexDescription;

    /// Save adds a new document to the index.
    ///
    /// Called when a new document is created in the collection.
    /// The values slice contains the field values in index field order.
    async fn save<T: Reader + Writer + Send>(
        &self,
        txn: &mut T,
        doc_id: &str,
        values: &[NormalValue],
    ) -> Result<()>;

    /// Update modifies an existing document's index entry.
    ///
    /// Called when a document is updated. Removes the old entry
    /// and adds a new one with the updated values.
    async fn update<T: Reader + Writer + Send>(
        &self,
        txn: &mut T,
        doc_id: &str,
        old_values: &[NormalValue],
        new_values: &[NormalValue],
    ) -> Result<()>;

    /// Delete removes a document from the index.
    ///
    /// Called when a document is deleted from the collection.
    async fn delete<T: Reader + Writer + Send>(
        &self,
        txn: &mut T,
        doc_id: &str,
        values: &[NormalValue],
    ) -> Result<()>;

    /// RemoveAll removes all entries for this index.
    ///
    /// Called when the index is dropped from the collection.
    async fn remove_all<T: Reader + Writer + Send>(&self, txn: &mut T) -> Result<()>;
}

/// A simple (non-unique) index implementation.
///
/// SimpleIndex stores document IDs in the key itself, allowing
/// multiple documents to have the same indexed field values.
///
/// Key format: /[ColID]/[IdxID]/[EncodedFields][DocID]
/// Value: empty
pub struct SimpleIndex {
    /// The collection's short ID
    collection_short_id: u32,
    /// Index description from schema
    desc: IndexDescription,
}

impl SimpleIndex {
    /// Create a new SimpleIndex.
    pub fn new(collection_short_id: u32, desc: IndexDescription) -> Self {
        Self {
            collection_short_id,
            desc,
        }
    }

    /// Get the index ID
    pub fn id(&self) -> u32 {
        self.desc.id
    }

    /// Build the index key for a document with the given field values.
    fn build_key(&self, values: &[NormalValue], doc_id: &str) -> Vec<u8> {
        let fields = self.build_indexed_fields(values);
        let key = IndexDataStoreKey::new(self.collection_short_id, self.desc.id, fields);

        // For simple index, append doc_id to make key unique
        let mut key_bytes = key.bytes();
        key_bytes.extend_from_slice(doc_id.as_bytes());
        key_bytes
    }

    /// Build IndexedField structs from values and index description.
    fn build_indexed_fields(&self, values: &[NormalValue]) -> Vec<IndexedField> {
        values
            .iter()
            .zip(self.desc.fields.iter())
            .map(|(value, field_desc)| IndexedField::new(value.clone(), field_desc.descending))
            .collect()
    }
}

#[async_trait]
impl CollectionIndex for SimpleIndex {
    fn description(&self) -> &IndexDescription {
        &self.desc
    }

    async fn save<T: Reader + Writer + Send>(
        &self,
        txn: &mut T,
        doc_id: &str,
        values: &[NormalValue],
    ) -> Result<()> {
        let key = self.build_key(values, doc_id);
        txn.set(&key, &[]).await
    }

    async fn update<T: Reader + Writer + Send>(
        &self,
        txn: &mut T,
        doc_id: &str,
        old_values: &[NormalValue],
        new_values: &[NormalValue],
    ) -> Result<()> {
        // Delete old entry
        let old_key = self.build_key(old_values, doc_id);
        txn.delete(&old_key).await?;

        // Insert new entry
        let new_key = self.build_key(new_values, doc_id);
        txn.set(&new_key, &[]).await
    }

    async fn delete<T: Reader + Writer + Send>(
        &self,
        txn: &mut T,
        doc_id: &str,
        values: &[NormalValue],
    ) -> Result<()> {
        let key = self.build_key(values, doc_id);
        txn.delete(&key).await
    }

    async fn remove_all<T: Reader + Writer + Send>(&self, txn: &mut T) -> Result<()> {
        let prefix = IndexDataStoreKey::index_prefix(self.collection_short_id, self.desc.id);
        // Iterate over all keys with this prefix and delete them
        let opts = IterOptions::default().with_prefix(prefix.clone());
        let mut iter = txn.iterator(opts).await?;

        // Collect keys first using the async collect_all method
        let items = iter.collect_all().await?;
        let keys_to_delete: Vec<Vec<u8>> = items.into_iter().map(|kv| kv.key).collect();

        for key in keys_to_delete {
            txn.delete(&key).await?;
        }
        Ok(())
    }
}

/// A unique index implementation.
///
/// UniqueIndex stores document IDs in the value, enforcing that
/// each indexed field value combination can only appear once.
///
/// Key format: /[ColID]/[IdxID]/[EncodedFields]
/// Value: [DocID] (or empty for NULL values)
///
/// For fields that allow NULL, NULL values are stored specially
/// to allow multiple documents with NULL in the indexed field.
pub struct UniqueIndex {
    /// The collection's short ID
    collection_short_id: u32,
    /// Index description from schema
    desc: IndexDescription,
}

impl UniqueIndex {
    /// Create a new UniqueIndex.
    pub fn new(collection_short_id: u32, desc: IndexDescription) -> Self {
        Self {
            collection_short_id,
            desc,
        }
    }

    /// Get the index ID
    pub fn id(&self) -> u32 {
        self.desc.id
    }

    /// Check if all values are nil (special case for unique index with NULL).
    fn all_nil(values: &[NormalValue]) -> bool {
        values.iter().all(|v| v.is_nil())
    }

    /// Build the index key for given field values.
    ///
    /// For unique indexes, the doc_id is NOT part of the key (it's in the value).
    fn build_key(&self, values: &[NormalValue]) -> Vec<u8> {
        let fields = self.build_indexed_fields(values);
        IndexDataStoreKey::new(self.collection_short_id, self.desc.id, fields).bytes()
    }

    /// Build the key with doc_id appended (for NULL case).
    fn build_key_with_doc_id(&self, values: &[NormalValue], doc_id: &str) -> Vec<u8> {
        let mut key = self.build_key(values);
        key.extend_from_slice(doc_id.as_bytes());
        key
    }

    /// Build IndexedField structs from values and index description.
    fn build_indexed_fields(&self, values: &[NormalValue]) -> Vec<IndexedField> {
        values
            .iter()
            .zip(self.desc.fields.iter())
            .map(|(value, field_desc)| IndexedField::new(value.clone(), field_desc.descending))
            .collect()
    }
}

#[async_trait]
impl CollectionIndex for UniqueIndex {
    fn description(&self) -> &IndexDescription {
        &self.desc
    }

    async fn save<T: Reader + Writer + Send>(
        &self,
        txn: &mut T,
        doc_id: &str,
        values: &[NormalValue],
    ) -> Result<()> {
        // Special case: if all values are nil, allow multiple entries
        // by appending doc_id to the key (like SimpleIndex)
        if Self::all_nil(values) {
            let key = self.build_key_with_doc_id(values, doc_id);
            return txn.set(&key, &[]).await;
        }

        let key = self.build_key(values);

        // Check for existing entry (uniqueness constraint)
        if let Some(existing) = txn.get(&key).await? {
            let existing_doc_id =
                String::from_utf8(existing).map_err(|e| crate::corekv::Error::Other(e.to_string()))?;
            if existing_doc_id != doc_id {
                return Err(crate::corekv::Error::Other(format!(
                    "unique index constraint violation: value already exists for document '{}'",
                    existing_doc_id
                )));
            }
        }

        // Store doc_id as the value
        txn.set(&key, doc_id.as_bytes()).await
    }

    async fn update<T: Reader + Writer + Send>(
        &self,
        txn: &mut T,
        doc_id: &str,
        old_values: &[NormalValue],
        new_values: &[NormalValue],
    ) -> Result<()> {
        // Delete old entry
        if Self::all_nil(old_values) {
            let old_key = self.build_key_with_doc_id(old_values, doc_id);
            txn.delete(&old_key).await?;
        } else {
            let old_key = self.build_key(old_values);
            txn.delete(&old_key).await?;
        }

        // Insert new entry (with uniqueness check)
        self.save(txn, doc_id, new_values).await
    }

    async fn delete<T: Reader + Writer + Send>(
        &self,
        txn: &mut T,
        doc_id: &str,
        values: &[NormalValue],
    ) -> Result<()> {
        if Self::all_nil(values) {
            let key = self.build_key_with_doc_id(values, doc_id);
            txn.delete(&key).await
        } else {
            let key = self.build_key(values);
            txn.delete(&key).await
        }
    }

    async fn remove_all<T: Reader + Writer + Send>(&self, txn: &mut T) -> Result<()> {
        let prefix = IndexDataStoreKey::index_prefix(self.collection_short_id, self.desc.id);
        // Iterate over all keys with this prefix and delete them
        let opts = IterOptions::default().with_prefix(prefix.clone());
        let mut iter = txn.iterator(opts).await?;

        // Collect keys first using the async collect_all method
        let items = iter.collect_all().await?;
        let keys_to_delete: Vec<Vec<u8>> = items.into_iter().map(|kv| kv.key).collect();

        for key in keys_to_delete {
            txn.delete(&key).await?;
        }
        Ok(())
    }
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::MemoryStore;
    use crate::corekv::Store;
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
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("unique index constraint violation"));
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
            .save(&mut txn, "doc3", &[NormalValue::String("charlie".to_string())])
            .await
            .unwrap();
        index
            .save(&mut txn, "doc1", &[NormalValue::String("alice".to_string())])
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
}
