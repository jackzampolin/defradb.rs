//! Range iterator for index scanning with bounds
//!
//! Provides iteration over index entries within a range of field values.
//! Supports:
//! - Prefix scanning (match first N fields exactly)
//! - Range bounds (_gt, _ge, _lt, _le)
//! - Forward and reverse iteration

use async_trait::async_trait;
use document::NormalValue;
use schema::{FieldKind, IndexDescription};
use tracing::trace;

use super::iterator::{Bound, IndexEntry, IndexIterator};
use crate::corekv::{MaybeSend, IterOptions, Iterator, Reader, Result};
use crate::field_value::{decode_field_value, encode_field_value};
use crate::keys::IndexDataStoreKey;

/// Iterator for range queries on an index.
///
/// Supports:
/// - Prefix iteration: scan all entries matching first N field values
/// - Bounded iteration: scan entries where a field is within a range
/// - Reverse iteration: scan in descending order
pub struct RangeIterator {
    /// The underlying KV iterator
    inner: Box<dyn Iterator>,
    /// Length of the index prefix (collection_id + index_id only, without field values).
    /// Used to find where encoded field values start in a key.
    index_prefix_len: usize,
    /// Index description for decoding field values
    desc: IndexDescription,
    /// Whether this is a unique index
    is_unique: bool,
    /// Encoded lower bound key (for comparison)
    lower_bound_key: Option<Vec<u8>>,
    /// Encoded upper bound key (for comparison)
    upper_bound_key: Option<Vec<u8>>,
    /// Whether lower bound is inclusive
    lower_inclusive: bool,
    /// Whether upper bound is inclusive
    upper_inclusive: bool,
    /// Whether the iterator has been exhausted
    exhausted: bool,
    /// Whether iterating in reverse
    reverse: bool,
}

impl RangeIterator {
    /// Create a new range iterator that scans all entries in an index.
    ///
    /// This is equivalent to a full index scan with optional ordering.
    pub async fn new_scan<R: Reader + MaybeSend + ?Sized>(
        txn: &R,
        collection_short_id: u32,
        desc: &IndexDescription,
        is_unique: bool,
        reverse: bool,
    ) -> Result<Self> {
        let key_prefix = IndexDataStoreKey::index_prefix(collection_short_id, desc.id);
        let index_prefix_len = key_prefix.len();

        let opts = IterOptions::default()
            .with_prefix(key_prefix.clone())
            .with_reverse(reverse);
        let inner = txn.iterator(opts).await?;

        Ok(Self {
            inner,
            index_prefix_len,
            desc: desc.clone(),
            is_unique,
            lower_bound_key: None,
            upper_bound_key: None,
            lower_inclusive: true,
            upper_inclusive: true,
            exhausted: false,
            reverse,
        })
    }

    /// Create a new range iterator with prefix matching.
    ///
    /// Scans all entries where the first `prefix_values.len()` fields match exactly.
    /// Useful for composite indexes where you want to filter on the first field(s).
    pub async fn new_prefix<R: Reader + MaybeSend + ?Sized>(
        txn: &R,
        collection_short_id: u32,
        desc: &IndexDescription,
        is_unique: bool,
        prefix_values: &[NormalValue],
        reverse: bool,
    ) -> Result<Self> {
        // Build key prefix with the exact field values
        let mut key_prefix = IndexDataStoreKey::index_prefix(collection_short_id, desc.id);
        let index_prefix_len = key_prefix.len();

        // Encode prefix field values
        for (i, value) in prefix_values.iter().enumerate() {
            let descending = desc.fields.get(i).map(|f| f.descending).unwrap_or(false);
            key_prefix = encode_field_value(key_prefix, value, descending)?;
        }

        let opts = IterOptions::default()
            .with_prefix(key_prefix.clone())
            .with_reverse(reverse);
        let inner = txn.iterator(opts).await?;

        Ok(Self {
            inner,
            index_prefix_len,
            desc: desc.clone(),
            is_unique,
            lower_bound_key: None,
            upper_bound_key: None,
            lower_inclusive: true,
            upper_inclusive: true,
            exhausted: false,
            reverse,
        })
    }

    /// Create a new range iterator with bounds on a field.
    ///
    /// Optionally match first `prefix_values.len()` fields exactly, then apply
    /// range bounds on the next field.
    #[allow(clippy::too_many_arguments)]
    pub async fn new_range<R: Reader + MaybeSend + ?Sized>(
        txn: &R,
        collection_short_id: u32,
        desc: &IndexDescription,
        is_unique: bool,
        prefix_values: &[NormalValue],
        lower: Bound,
        upper: Bound,
        reverse: bool,
    ) -> Result<Self> {
        // Build base key prefix with collection/index and prefix values
        let mut key_prefix = IndexDataStoreKey::index_prefix(collection_short_id, desc.id);
        let index_prefix_len = key_prefix.len();

        // Encode prefix field values
        for (i, value) in prefix_values.iter().enumerate() {
            let descending = desc.fields.get(i).map(|f| f.descending).unwrap_or(false);
            key_prefix = encode_field_value(key_prefix, value, descending)?;
        }

        let range_field_idx = prefix_values.len();
        let range_descending = desc
            .fields
            .get(range_field_idx)
            .map(|f| f.descending)
            .unwrap_or(false);

        // Build encoded bound keys for comparison
        let (lower_bound_key, lower_inclusive) = match &lower {
            Bound::Inclusive(v) => {
                let mut key = key_prefix.clone();
                key = encode_field_value(key, v, range_descending)?;
                (Some(key), true)
            }
            Bound::Exclusive(v) => {
                let mut key = key_prefix.clone();
                key = encode_field_value(key, v, range_descending)?;
                (Some(key), false)
            }
            Bound::Unbounded => (None, true),
        };

        let (upper_bound_key, upper_inclusive) = match &upper {
            Bound::Inclusive(v) => {
                let mut key = key_prefix.clone();
                key = encode_field_value(key, v, range_descending)?;
                (Some(key), true)
            }
            Bound::Exclusive(v) => {
                let mut key = key_prefix.clone();
                key = encode_field_value(key, v, range_descending)?;
                (Some(key), false)
            }
            Bound::Unbounded => (None, true),
        };

        // Build start and end keys for the KV iterator
        let mut opts = IterOptions::default()
            .with_prefix(key_prefix.clone())
            .with_reverse(reverse);

        // Use the bound keys as iterator bounds (the iterator will do initial filtering)
        if let Some(ref start) = lower_bound_key {
            opts = opts.with_start(start.clone());
        }
        if let Some(ref end) = upper_bound_key {
            // Add 0xFF for inclusive upper bound to include all keys with the prefix
            let mut end_key = end.clone();
            if upper_inclusive {
                end_key.push(0xFF);
            }
            opts = opts.with_end(end_key);
        }

        let inner = txn.iterator(opts).await?;

        Ok(Self {
            inner,
            index_prefix_len,
            desc: desc.clone(),
            is_unique,
            lower_bound_key,
            upper_bound_key,
            lower_inclusive,
            upper_inclusive,
            exhausted: false,
            reverse,
        })
    }

    /// Extract document ID and field values from an index key.
    fn extract_entry(&self, key: &[u8], value: &[u8]) -> Result<IndexEntry> {
        // Decode field values and get remaining bytes (doc_id suffix)
        let (values, doc_id_bytes) = self.decode_field_values(key)?;

        // Determine if this is a NULL entry (doc_id in key suffix)
        let has_nil = values.iter().any(|v| v.is_nil());

        let doc_id = if self.is_unique && !has_nil && !value.is_empty() {
            // Unique index with non-NULL value: doc_id is in value
            String::from_utf8(value.to_vec())
                .map_err(|e| crate::corekv::Error::Other(format!("invalid doc_id: {}", e)))?
        } else {
            // Simple index or unique with NULL: doc_id is in key suffix
            self.extract_doc_id_from_key(doc_id_bytes)?
        };

        Ok(IndexEntry::new(doc_id, values))
    }

    /// Decode field values from an index key, returning values and remaining bytes.
    ///
    /// The remaining bytes after decoding all field values contain the doc_id suffix.
    fn decode_field_values<'a>(&self, key: &'a [u8]) -> Result<(Vec<NormalValue>, &'a [u8])> {
        let mut buf = &key[self.index_prefix_len..];
        let mut values = Vec::with_capacity(self.desc.fields.len());

        for field_desc in &self.desc.fields {
            if buf.is_empty() {
                break;
            }
            // Use blob kind as default - the encoded type marker determines the actual type.
            let kind = FieldKind::blob();
            let (rest, value) = decode_field_value(buf, field_desc.descending, &kind)?;
            values.push(value);
            buf = rest;
        }

        // Validate that we decoded the expected number of fields
        if values.len() < self.desc.fields.len() {
            return Err(crate::corekv::Error::Other(format!(
                "incomplete field values in index key: expected {} fields, decoded {} \
                 (key_len={}, remaining_buf_len={})",
                self.desc.fields.len(),
                values.len(),
                key.len(),
                buf.len()
            )));
        }

        // Return values and remaining bytes (the doc_id suffix)
        Ok((values, buf))
    }

    /// Extract doc_id from key suffix (the remaining bytes after field values).
    fn extract_doc_id_from_key(&self, doc_id_bytes: &[u8]) -> Result<String> {
        if doc_id_bytes.is_empty() {
            return Err(crate::corekv::Error::Other(
                "index key missing doc_id suffix".to_string(),
            ));
        }

        String::from_utf8(doc_id_bytes.to_vec())
            .map_err(|e| crate::corekv::Error::Other(format!("invalid doc_id: {}", e)))
    }

    /// Check if a key is within the range bounds using byte comparison.
    ///
    /// Key format: `[index_prefix][encoded_value][doc_id]`
    /// Bound format: `[index_prefix][encoded_value]` (no doc_id)
    ///
    /// The algorithm compares the key's prefix bytes against the bound. If the key
    /// starts with exactly the bound bytes, then the encoded value equals the bound
    /// value. For exclusive bounds, such keys must be excluded.
    fn is_key_within_bounds(&self, key: &[u8]) -> bool {
        // Check lower bound
        if let Some(ref lower) = self.lower_bound_key {
            let cmp_len = lower.len().min(key.len());
            let key_prefix = &key[..cmp_len];
            let lower_prefix = &lower[..cmp_len];

            if self.lower_inclusive {
                if key_prefix < lower_prefix {
                    return false;
                }
            } else {
                // Exclusive: encoded value must be strictly greater than bound
                if key_prefix < lower_prefix {
                    return false;
                }
                // If key starts with the bound exactly, the encoded value equals the bound
                if key_prefix == lower_prefix
                    && key.len() >= lower.len()
                    && &key[..lower.len()] == lower.as_slice()
                {
                    return false;
                }
            }
        }

        // Check upper bound
        if let Some(ref upper) = self.upper_bound_key {
            let cmp_len = upper.len().min(key.len());
            let key_prefix = &key[..cmp_len];
            let upper_prefix = &upper[..cmp_len];

            if self.upper_inclusive {
                if key_prefix > upper_prefix {
                    return false;
                }
            } else {
                // Exclusive: encoded value must be strictly less than bound
                if key_prefix > upper_prefix {
                    return false;
                }
                // If key starts with the bound exactly, the encoded value equals the bound
                if key_prefix == upper_prefix
                    && key.len() >= upper.len()
                    && &key[..upper.len()] >= upper.as_slice()
                {
                    return false;
                }
            }
        }

        true
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl IndexIterator for RangeIterator {
    async fn next(&mut self) -> Result<Option<IndexEntry>> {
        if self.exhausted {
            return Ok(None);
        }

        loop {
            match self.inner.next().await? {
                Some(kv) => {
                    // Check if key is within bounds using byte comparison
                    let has_bounds =
                        self.lower_bound_key.is_some() || self.upper_bound_key.is_some();
                    if has_bounds && !self.is_key_within_bounds(&kv.key) {
                        // Check if we've passed the boundary
                        if let Some(ref upper) = self.upper_bound_key {
                            if !self.reverse && kv.key.as_slice() >= upper.as_slice() {
                                trace!(
                                    index_id = self.desc.id,
                                    index_name = %self.desc.name,
                                    key_len = kv.key.len(),
                                    "range iterator exhausted: passed upper bound"
                                );
                                self.exhausted = true;
                                return Ok(None);
                            }
                        }
                        if let Some(ref lower) = self.lower_bound_key {
                            if self.reverse && kv.key.as_slice() < lower.as_slice() {
                                trace!(
                                    index_id = self.desc.id,
                                    index_name = %self.desc.name,
                                    key_len = kv.key.len(),
                                    "range iterator exhausted: passed lower bound (reverse)"
                                );
                                self.exhausted = true;
                                return Ok(None);
                            }
                        }
                        trace!(
                            index_id = self.desc.id,
                            index_name = %self.desc.name,
                            key_len = kv.key.len(),
                            "skipping entry outside bounds"
                        );
                        continue;
                    }

                    let entry = self.extract_entry(&kv.key, &kv.value)?;
                    return Ok(Some(entry));
                }
                None => {
                    self.exhausted = true;
                    return Ok(None);
                }
            }
        }
    }

    async fn close(&mut self) -> Result<()> {
        self.inner.close().await?;
        self.exhausted = true;
        Ok(())
    }

    async fn reset(&mut self) -> Result<()> {
        self.inner.reset().await?;
        self.exhausted = false;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::MemoryStore;
    use crate::corekv::Store;
    use crate::index::{CollectionIndex, SimpleIndex};
    use schema::IndexedFieldDescription;

    fn test_index_description() -> IndexDescription {
        IndexDescription {
            id: 1,
            name: "test_index".to_string(),
            unique: false,
            fields: vec![IndexedFieldDescription {
                name: "age".to_string(),
                descending: false,
            }],
        }
    }

    fn composite_index_description() -> IndexDescription {
        IndexDescription {
            id: 2,
            name: "composite_index".to_string(),
            unique: false,
            fields: vec![
                IndexedFieldDescription {
                    name: "category".to_string(),
                    descending: false,
                },
                IndexedFieldDescription {
                    name: "score".to_string(),
                    descending: false,
                },
            ],
        }
    }

    #[tokio::test]
    async fn test_range_scan_all() {
        let store = MemoryStore::new();
        let mut txn = store.new_txn(false).await.unwrap();

        let desc = test_index_description();
        let index = SimpleIndex::new(1, desc.clone());

        index
            .save(&mut txn, "doc1", &[NormalValue::Int(10)])
            .await
            .unwrap();
        index
            .save(&mut txn, "doc2", &[NormalValue::Int(20)])
            .await
            .unwrap();
        index
            .save(&mut txn, "doc3", &[NormalValue::Int(30)])
            .await
            .unwrap();
        txn.commit().await.unwrap();

        let txn = store.new_txn(true).await.unwrap();
        let mut iter = RangeIterator::new_scan(txn.as_ref(), 1, &desc, false, false)
            .await
            .unwrap();

        let entries = iter.collect_all().await.unwrap();
        assert_eq!(entries.len(), 3);
    }

    #[tokio::test]
    async fn test_range_scan_reverse() {
        let store = MemoryStore::new();
        let mut txn = store.new_txn(false).await.unwrap();

        let desc = test_index_description();
        let index = SimpleIndex::new(1, desc.clone());

        index
            .save(&mut txn, "doc1", &[NormalValue::Int(10)])
            .await
            .unwrap();
        index
            .save(&mut txn, "doc2", &[NormalValue::Int(20)])
            .await
            .unwrap();
        index
            .save(&mut txn, "doc3", &[NormalValue::Int(30)])
            .await
            .unwrap();
        txn.commit().await.unwrap();

        let txn = store.new_txn(true).await.unwrap();
        let mut iter = RangeIterator::new_scan(txn.as_ref(), 1, &desc, false, true)
            .await
            .unwrap();

        let entries = iter.collect_all().await.unwrap();
        assert_eq!(entries.len(), 3);

        // Should be in reverse order
        assert_eq!(entries[0].values[0], NormalValue::Int(30));
        assert_eq!(entries[2].values[0], NormalValue::Int(10));
    }

    #[tokio::test]
    async fn test_range_prefix_scan() {
        let store = MemoryStore::new();
        let mut txn = store.new_txn(false).await.unwrap();

        let desc = composite_index_description();
        let index = SimpleIndex::new(1, desc.clone());

        // Save documents with different categories
        index
            .save(
                &mut txn,
                "doc1",
                &[NormalValue::String("A".to_string()), NormalValue::Int(100)],
            )
            .await
            .unwrap();
        index
            .save(
                &mut txn,
                "doc2",
                &[NormalValue::String("A".to_string()), NormalValue::Int(200)],
            )
            .await
            .unwrap();
        index
            .save(
                &mut txn,
                "doc3",
                &[NormalValue::String("B".to_string()), NormalValue::Int(150)],
            )
            .await
            .unwrap();
        txn.commit().await.unwrap();

        // Scan only category "A"
        let txn = store.new_txn(true).await.unwrap();
        let mut iter = RangeIterator::new_prefix(
            txn.as_ref(),
            1,
            &desc,
            false,
            &[NormalValue::String("A".to_string())],
            false,
        )
        .await
        .unwrap();

        let entries = iter.collect_all().await.unwrap();
        assert_eq!(entries.len(), 2);
    }

    #[tokio::test]
    async fn test_range_bounded() {
        let store = MemoryStore::new();
        let mut txn = store.new_txn(false).await.unwrap();

        let desc = test_index_description();
        let index = SimpleIndex::new(1, desc.clone());

        for i in 1..=10 {
            index
                .save(&mut txn, &format!("doc{}", i), &[NormalValue::Int(i * 10)])
                .await
                .unwrap();
        }
        txn.commit().await.unwrap();

        // Range: 30 < x <= 70
        let txn = store.new_txn(true).await.unwrap();
        let mut iter = RangeIterator::new_range(
            txn.as_ref(),
            1,
            &desc,
            false,
            &[],
            Bound::gt(NormalValue::Int(30)),
            Bound::le(NormalValue::Int(70)),
            false,
        )
        .await
        .unwrap();

        let entries = iter.collect_all().await.unwrap();
        // Should include 40, 50, 60, 70
        assert_eq!(entries.len(), 4);
    }
}
