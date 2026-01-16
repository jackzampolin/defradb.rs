// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! UniqueIndex implementation for unique indexes

use async_trait::async_trait;
use document::NormalValue;
use schema::IndexDescription;

use super::CollectionIndex;
use crate::corekv::{IterOptions, Reader, Result, Writer};
use crate::keys::datastore::IndexedField;
use crate::keys::IndexDataStoreKey;

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
    ///
    /// # Panics
    ///
    /// Panics if the index description has `unique = false`. Use `SimpleIndex`
    /// for non-unique indexes.
    pub fn new(collection_short_id: u32, desc: IndexDescription) -> Self {
        assert!(
            desc.unique,
            "UniqueIndex requires unique index, got unique=false for index '{}'",
            desc.name
        );
        Self {
            collection_short_id,
            desc,
        }
    }

    /// Create a new UniqueIndex, returning an error if the description is invalid.
    pub fn try_new(collection_short_id: u32, desc: IndexDescription) -> Result<Self> {
        if !desc.unique {
            return Err(crate::corekv::Error::Other(format!(
                "UniqueIndex requires unique index, got unique=false for index '{}'",
                desc.name
            )));
        }
        Ok(Self {
            collection_short_id,
            desc,
        })
    }

    /// Get the index ID
    pub fn id(&self) -> u32 {
        self.desc.id
    }

    /// Validate that the number of values matches the index field count.
    fn validate_field_count(&self, values: &[NormalValue], doc_id: &str) -> Result<()> {
        if values.len() != self.desc.fields.len() {
            return Err(crate::corekv::Error::Other(format!(
                "index '{}' field count mismatch for document '{}': expected {} fields, got {}",
                self.desc.name,
                doc_id,
                self.desc.fields.len(),
                values.len()
            )));
        }
        Ok(())
    }

    /// Check if any value is nil (special case for unique index with NULL).
    ///
    /// Matches Go's `hasIndexKeyNilField()` behavior: if ANY field is NULL,
    /// the uniqueness constraint is bypassed, allowing multiple documents
    /// with the same partial values.
    fn has_nil_field(values: &[NormalValue]) -> bool {
        values.iter().any(|v| v.is_nil())
    }

    /// Build the index key for given field values.
    ///
    /// For unique indexes, the doc_id is NOT part of the key (it's in the value).
    fn build_key(&self, values: &[NormalValue]) -> Result<Vec<u8>> {
        let fields = self.build_indexed_fields(values);
        IndexDataStoreKey::new(self.collection_short_id, self.desc.id, fields).try_bytes()
    }

    /// Build the key with doc_id appended (for NULL case).
    fn build_key_with_doc_id(&self, values: &[NormalValue], doc_id: &str) -> Result<Vec<u8>> {
        let mut key = self.build_key(values)?;
        key.extend_from_slice(doc_id.as_bytes());
        Ok(key)
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
        self.validate_field_count(values, doc_id)?;

        // Special case: if all values are nil, allow multiple entries
        // by appending doc_id to the key (like SimpleIndex)
        if Self::has_nil_field(values) {
            let key = self.build_key_with_doc_id(values, doc_id)?;
            return txn.set(&key, &[]).await;
        }

        let key = self.build_key(values)?;

        // Check for existing entry (uniqueness constraint)
        if let Some(existing) = txn.get(&key).await? {
            let existing_doc_id = String::from_utf8(existing)
                .map_err(|e| crate::corekv::Error::Other(e.to_string()))?;
            if existing_doc_id != doc_id {
                return Err(crate::corekv::Error::Other(format!(
                    "unique index '{}' constraint violation: value already exists for document '{}'",
                    self.desc.name,
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
        self.validate_field_count(old_values, doc_id)?;
        self.validate_field_count(new_values, doc_id)?;

        // Check uniqueness of new values BEFORE deleting old entry
        // This prevents data loss if the uniqueness check fails
        if !Self::has_nil_field(new_values) {
            let new_key = self.build_key(new_values)?;
            if let Some(existing) = txn.get(&new_key).await? {
                let existing_doc_id = String::from_utf8(existing)
                    .map_err(|e| crate::corekv::Error::Other(e.to_string()))?;
                if existing_doc_id != doc_id {
                    return Err(crate::corekv::Error::Other(format!(
                        "unique index '{}' constraint violation: value already exists for document '{}'",
                        self.desc.name,
                        existing_doc_id
                    )));
                }
            }
        }

        // Delete old entry (safe now that we've validated the new values)
        if Self::has_nil_field(old_values) {
            let old_key = self.build_key_with_doc_id(old_values, doc_id)?;
            txn.delete(&old_key).await?;
        } else {
            let old_key = self.build_key(old_values)?;
            txn.delete(&old_key).await?;
        }

        // Insert new entry
        if Self::has_nil_field(new_values) {
            let key = self.build_key_with_doc_id(new_values, doc_id)?;
            txn.set(&key, &[]).await
        } else {
            let key = self.build_key(new_values)?;
            txn.set(&key, doc_id.as_bytes()).await
        }
    }

    async fn delete<T: Reader + Writer + Send>(
        &self,
        txn: &mut T,
        doc_id: &str,
        values: &[NormalValue],
    ) -> Result<()> {
        self.validate_field_count(values, doc_id)?;

        if Self::has_nil_field(values) {
            let key = self.build_key_with_doc_id(values, doc_id)?;
            txn.delete(&key).await
        } else {
            let key = self.build_key(values)?;
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
