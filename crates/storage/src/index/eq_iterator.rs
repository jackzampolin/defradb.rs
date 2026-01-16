// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! Exact match iterator for index lookups
//!
//! Provides iteration over index entries that exactly match specific field values.
//! Maps to Go's `eqSingleIndexIterator` and supports both unique and non-unique indexes.

use async_trait::async_trait;
use document::NormalValue;
use schema::IndexDescription;

use super::iterator::{IndexEntry, IndexIterator};
use crate::corekv::{IterOptions, Iterator, Reader, Result};
use crate::keys::datastore::IndexedField;
use crate::keys::IndexDataStoreKey;

/// Iterator for exact match queries on an index.
///
/// For SimpleIndex: scans all entries with the exact encoded value prefix,
/// extracting document IDs from the key suffix.
///
/// For UniqueIndex: performs a direct lookup for non-NULL values (single result),
/// or prefix scan for NULL values (multiple results allowed).
pub struct ExactMatchIterator {
    /// The underlying KV iterator (None after exhaustion or for unique index after first result)
    inner: Option<Box<dyn Iterator>>,
    /// The key prefix (encoded field values without doc_id)
    key_prefix: Vec<u8>,
    /// The exact field values being matched
    values: Vec<NormalValue>,
    /// Whether this is a unique index (reserved for future use in value decoding)
    #[allow(dead_code)]
    is_unique: bool,
    /// Single result for unique index direct lookup (consumed on first next())
    unique_result: Option<IndexEntry>,
    /// Whether the iterator has been exhausted
    exhausted: bool,
}

impl ExactMatchIterator {
    /// Create a new exact match iterator for a simple (non-unique) index.
    ///
    /// Scans all index entries with the given field values.
    pub async fn new_simple<R: Reader + Send + ?Sized>(
        txn: &R,
        collection_short_id: u32,
        desc: &IndexDescription,
        values: &[NormalValue],
    ) -> Result<Self> {
        let fields = build_indexed_fields(values, desc);
        let key_prefix =
            IndexDataStoreKey::new(collection_short_id, desc.id, fields).try_bytes()?;

        let opts = IterOptions::default().with_prefix(key_prefix.clone());
        let inner = txn.iterator(opts).await?;

        Ok(Self {
            inner: Some(inner),
            key_prefix,
            values: values.to_vec(),
            is_unique: false,
            unique_result: None,
            exhausted: false,
        })
    }

    /// Create a new exact match iterator for a unique index.
    ///
    /// For non-NULL values: performs direct key lookup (single result).
    /// For NULL values: performs prefix scan (multiple results allowed).
    pub async fn new_unique<R: Reader + Send + ?Sized>(
        txn: &R,
        collection_short_id: u32,
        desc: &IndexDescription,
        values: &[NormalValue],
    ) -> Result<Self> {
        let fields = build_indexed_fields(values, desc);
        let key_prefix =
            IndexDataStoreKey::new(collection_short_id, desc.id, fields).try_bytes()?;

        // Check if any value is nil - if so, use prefix scan like SimpleIndex
        let has_nil = values.iter().any(|v| v.is_nil());

        if has_nil {
            // NULL values: use prefix scan (multiple documents can have NULL)
            let opts = IterOptions::default().with_prefix(key_prefix.clone());
            let inner = txn.iterator(opts).await?;

            Ok(Self {
                inner: Some(inner),
                key_prefix,
                values: values.to_vec(),
                is_unique: true,
                unique_result: None,
                exhausted: false,
            })
        } else {
            // Non-NULL values: direct lookup, doc_id stored in value
            let unique_result = if let Some(value_bytes) = txn.get(&key_prefix).await? {
                let doc_id = String::from_utf8(value_bytes)
                    .map_err(|e| crate::corekv::Error::Other(e.to_string()))?;
                Some(IndexEntry::new(doc_id, values.to_vec()))
            } else {
                None
            };

            Ok(Self {
                inner: None, // No iterator needed for direct lookup
                key_prefix,
                values: values.to_vec(),
                is_unique: true,
                unique_result,
                exhausted: false,
            })
        }
    }

    /// Extract the document ID from an index key.
    ///
    /// For simple index keys, the doc_id is appended after the encoded field values.
    fn extract_doc_id(&self, key: &[u8]) -> Result<String> {
        // The key is: [prefix_bytes][doc_id_bytes]
        // where prefix_bytes is what we already have in key_prefix
        if key.len() <= self.key_prefix.len() {
            return Err(crate::corekv::Error::Other(
                "index key too short to contain doc_id".to_string(),
            ));
        }
        let doc_id_bytes = &key[self.key_prefix.len()..];
        String::from_utf8(doc_id_bytes.to_vec())
            .map_err(|e| crate::corekv::Error::Other(format!("invalid doc_id in index key: {}", e)))
    }
}

#[async_trait]
impl IndexIterator for ExactMatchIterator {
    async fn next(&mut self) -> Result<Option<IndexEntry>> {
        if self.exhausted {
            return Ok(None);
        }

        // Check for single unique result first
        if let Some(entry) = self.unique_result.take() {
            self.exhausted = true;
            return Ok(Some(entry));
        }

        // Use the inner iterator if available
        if let Some(ref mut iter) = self.inner {
            if let Some(kv) = iter.next().await? {
                // For unique index with NULL: doc_id is in key suffix
                // For simple index: doc_id is in key suffix
                // For unique index without NULL: handled above via unique_result
                let doc_id = self.extract_doc_id(&kv.key)?;
                return Ok(Some(IndexEntry::new(doc_id, self.values.clone())));
            }
        }

        self.exhausted = true;
        Ok(None)
    }

    async fn close(&mut self) -> Result<()> {
        if let Some(ref mut iter) = self.inner {
            iter.close().await?;
        }
        self.inner = None;
        self.exhausted = true;
        Ok(())
    }

    async fn reset(&mut self) -> Result<()> {
        if let Some(ref mut iter) = self.inner {
            iter.reset().await?;
        }
        self.exhausted = false;
        Ok(())
    }
}

/// Build IndexedField structs from values and index description.
fn build_indexed_fields(values: &[NormalValue], desc: &IndexDescription) -> Vec<IndexedField> {
    values
        .iter()
        .zip(desc.fields.iter())
        .map(|(value, field_desc)| IndexedField::new(value.clone(), field_desc.descending))
        .collect()
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
    async fn test_exact_match_simple_single_result() {
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
        let mut iter = ExactMatchIterator::new_simple(
            txn.as_ref(),
            1,
            &desc,
            &[NormalValue::String("alice".to_string())],
        )
        .await
        .unwrap();

        let entry = iter.next().await.unwrap();
        assert!(entry.is_some());
        let entry = entry.unwrap();
        assert_eq!(entry.doc_id, "doc1");

        let entry = iter.next().await.unwrap();
        assert!(entry.is_none());
    }

    #[tokio::test]
    async fn test_exact_match_simple_multiple_results() {
        let store = MemoryStore::new();
        let mut txn = store.new_txn(false).await.unwrap();

        let desc = test_index_description(false);
        let index = SimpleIndex::new(1, desc.clone());

        // Same value, multiple documents
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
        let mut iter = ExactMatchIterator::new_simple(
            txn.as_ref(),
            1,
            &desc,
            &[NormalValue::String("alice".to_string())],
        )
        .await
        .unwrap();

        let entries = iter.collect_all().await.unwrap();
        assert_eq!(entries.len(), 2);
        assert!(entries.iter().any(|e| e.doc_id == "doc1"));
        assert!(entries.iter().any(|e| e.doc_id == "doc2"));
    }

    #[tokio::test]
    async fn test_exact_match_unique_single_result() {
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
        txn.commit().await.unwrap();

        let txn = store.new_txn(true).await.unwrap();
        let mut iter = ExactMatchIterator::new_unique(
            txn.as_ref(),
            1,
            &desc,
            &[NormalValue::String("alice".to_string())],
        )
        .await
        .unwrap();

        let entry = iter.next().await.unwrap();
        assert!(entry.is_some());
        let entry = entry.unwrap();
        assert_eq!(entry.doc_id, "doc1");

        let entry = iter.next().await.unwrap();
        assert!(entry.is_none());
    }

    #[tokio::test]
    async fn test_exact_match_unique_not_found() {
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
        txn.commit().await.unwrap();

        let txn = store.new_txn(true).await.unwrap();
        let mut iter = ExactMatchIterator::new_unique(
            txn.as_ref(),
            1,
            &desc,
            &[NormalValue::String("bob".to_string())],
        )
        .await
        .unwrap();

        let entry = iter.next().await.unwrap();
        assert!(entry.is_none());
    }

    #[tokio::test]
    async fn test_exact_match_unique_null_multiple() {
        let store = MemoryStore::new();
        let mut txn = store.new_txn(false).await.unwrap();

        let desc = test_index_description(true);
        let index = UniqueIndex::new(1, desc.clone());

        // Multiple documents with NULL - allowed for unique index
        index
            .save(&mut txn, "doc1", &[NormalValue::Null])
            .await
            .unwrap();
        index
            .save(&mut txn, "doc2", &[NormalValue::Null])
            .await
            .unwrap();
        txn.commit().await.unwrap();

        let txn = store.new_txn(true).await.unwrap();
        let mut iter = ExactMatchIterator::new_unique(txn.as_ref(), 1, &desc, &[NormalValue::Null])
            .await
            .unwrap();

        let entries = iter.collect_all().await.unwrap();
        assert_eq!(entries.len(), 2);
    }
}
