// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! SimpleIndex implementation for non-unique indexes

use async_trait::async_trait;
use document::NormalValue;
use schema::IndexDescription;

use super::CollectionIndex;
use crate::corekv::{IterOptions, Reader, Result, Writer};
use crate::keys::datastore::IndexedField;
use crate::keys::IndexDataStoreKey;

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

    /// Build the index key for a document with the given field values.
    fn build_key(&self, values: &[NormalValue], doc_id: &str) -> Result<Vec<u8>> {
        let fields = self.build_indexed_fields(values);
        let key = IndexDataStoreKey::new(self.collection_short_id, self.desc.id, fields);

        // For simple index, append doc_id to make key unique
        let mut key_bytes = key.try_bytes()?;
        key_bytes.extend_from_slice(doc_id.as_bytes());
        Ok(key_bytes)
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
        self.validate_field_count(values, doc_id)?;
        let key = self.build_key(values, doc_id)?;
        txn.set(&key, &[]).await
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

        // Delete old entry
        let old_key = self.build_key(old_values, doc_id)?;
        txn.delete(&old_key).await?;

        // Insert new entry
        let new_key = self.build_key(new_values, doc_id)?;
        txn.set(&new_key, &[]).await
    }

    async fn delete<T: Reader + Writer + Send>(
        &self,
        txn: &mut T,
        doc_id: &str,
        values: &[NormalValue],
    ) -> Result<()> {
        self.validate_field_count(values, doc_id)?;
        let key = self.build_key(values, doc_id)?;
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
