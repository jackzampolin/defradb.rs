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
use crate::corekv::{IterOptions, Iterator, MaybeSend, Reader, Result};
use crate::field_value::{decode_field_value, encode_field_value};
use crate::keys::doc_id_index::decode_doc_short_id;
use crate::keys::{IndexDataStoreKey, SEPARATOR};

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

    /// Apply a cursor seek to position the iterator for cursor-based pagination.
    ///
    /// Positions the underlying KV iterator at `seek_key`, then sets the bound
    /// so that the existing `is_key_within_bounds` filter enforces inclusivity:
    ///
    /// - Forward inclusive (`!reversed`, `inclusive`): lower bound = seek_key inclusive
    /// - Forward exclusive (`!reversed`, `!inclusive`): lower bound = seek_key exclusive
    /// - Reverse inclusive (`reversed`, `inclusive`): upper bound = seek_key inclusive
    /// - Reverse exclusive (`reversed`, `!inclusive`): upper bound = seek_key exclusive
    ///
    /// For reverse seeks, the actual storage seek uses `seek_key` appended with `0xFF` so
    /// that entries whose full key starts with `seek_key` (e.g. `[seek_key][doc short ID]`)
    /// are included in the initial seek position.
    ///
    /// `reversed` must match `self.reverse`. The planner sets `IndexScanType::PrefixScan.reverse`
    /// (or `RangeScan.reverse`) to the cursor direction before the KV iterator is created by
    /// `scan_prefix` / `scan_range`. By the time this method runs, the inner KV iterator's
    /// direction is already baked in and cannot be changed. `reversed` here is used only to
    /// select which bound slot (lower vs upper) to fill.
    pub async fn apply_cursor_seek(
        &mut self,
        seek_key: Vec<u8>,
        inclusive: bool,
        reversed: bool,
    ) -> Result<()> {
        // The planner must have already set the scan direction correctly before the
        // KV iterator was created. Asserts help catch regressions during development.
        debug_assert_eq!(
            self.reverse, reversed,
            "cursor reversed must match scan reverse (set_cursor_seek must mutate scan_type.reverse before iterator creation)"
        );
        if reversed {
            // For reverse iteration, seek_key is an upper bound.
            // Set the upper bound so is_key_within_bounds enforces inclusivity.
            self.upper_bound_key = Some(seek_key.clone());
            self.upper_inclusive = inclusive;
            // When seeking in reverse, we want to land at or before the last key that
            // starts with seek_key. Since keys have the form [seek_key][doc short ID],
            // appending 0xFF (max byte) ensures the storage seek finds those keys.
            let mut seek_bytes = seek_key;
            seek_bytes.push(0xFF);
            self.inner.seek(&seek_bytes).await?;
        } else {
            // For forward iteration, seek_key is a lower bound.
            // Set the lower bound so is_key_within_bounds enforces inclusivity.
            self.lower_bound_key = Some(seek_key.clone());
            self.lower_inclusive = inclusive;
            self.inner.seek(&seek_key).await?;
        }
        Ok(())
    }

    /// Extract the doc short ID and field values from an index key.
    fn extract_entry(&self, key: &[u8], value: &[u8]) -> Result<IndexEntry> {
        // Decode field values and get remaining bytes (doc short ID suffix)
        let (values, doc_suffix) = self.decode_field_values(key)?;

        // Determine if this is a NULL entry (doc short ID in key suffix)
        let has_nil = values.iter().any(|v| v.is_nil());

        let doc_short_id = if self.is_unique && !has_nil && !value.is_empty() {
            // Unique index with non-NULL value: doc short ID is in the value
            decode_doc_short_id(value)?
        } else {
            // Simple index or unique with NULL: doc short ID is in the key suffix
            Self::extract_doc_short_id_from_suffix(doc_suffix)?
        };

        Ok(IndexEntry::new(doc_short_id, values))
    }

    /// Decode field values from an index key, returning values and remaining bytes.
    ///
    /// The remaining bytes after decoding all field values contain the doc short ID suffix.
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

        // Return values and remaining bytes (the doc short ID suffix)
        Ok((values, buf))
    }

    /// Extract the doc short ID from the key suffix (the remaining bytes
    /// after the field values): `/[uvarint doc short ID]`.
    fn extract_doc_short_id_from_suffix(suffix: &[u8]) -> Result<u64> {
        if suffix.len() < 2 || suffix[0] != SEPARATOR {
            return Err(crate::corekv::Error::Other(
                "index key missing doc short ID suffix".to_string(),
            ));
        }
        decode_doc_short_id(&suffix[1..])
    }

    /// Check if a key is within the range bounds using byte comparison.
    ///
    /// Key format: `[index_prefix][encoded_value][doc short ID suffix]`
    /// Bound format: `[index_prefix][encoded_value]` (no doc suffix)
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

    async fn seek(&mut self, key: &[u8]) -> Result<bool> {
        if self.exhausted {
            return Ok(false);
        }
        self.inner.seek(key).await
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
            auto_generated: false,
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
            auto_generated: false,
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
            .save(&mut txn, 1, &[NormalValue::Int(10)])
            .await
            .unwrap();
        index
            .save(&mut txn, 2, &[NormalValue::Int(20)])
            .await
            .unwrap();
        index
            .save(&mut txn, 3, &[NormalValue::Int(30)])
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
            .save(&mut txn, 1, &[NormalValue::Int(10)])
            .await
            .unwrap();
        index
            .save(&mut txn, 2, &[NormalValue::Int(20)])
            .await
            .unwrap();
        index
            .save(&mut txn, 3, &[NormalValue::Int(30)])
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
                1,
                &[NormalValue::String("A".to_string()), NormalValue::Int(100)],
            )
            .await
            .unwrap();
        index
            .save(
                &mut txn,
                2,
                &[NormalValue::String("A".to_string()), NormalValue::Int(200)],
            )
            .await
            .unwrap();
        index
            .save(
                &mut txn,
                3,
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
                .save(&mut txn, i as u64, &[NormalValue::Int(i * 10)])
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

    /// Build an encoded seek key for an integer value on the test index (collection=1, index=1).
    fn build_age_seek_key(age: i64) -> Vec<u8> {
        let key_prefix = IndexDataStoreKey::index_prefix(1, 1);
        encode_field_value(key_prefix, &NormalValue::Int(age), false).unwrap()
    }

    #[tokio::test]
    async fn test_cursor_seek_forward_exclusive_skips_boundary() {
        // Setup: age index with doc 1 = 20, doc 2 = 30, doc 3 = 40.
        // Forward exclusive seek at age=30 → should return only doc 3 (age=40).
        let store = MemoryStore::new();
        let mut txn = store.new_txn(false).await.unwrap();

        let desc = test_index_description();
        let index = SimpleIndex::new(1, desc.clone());

        index
            .save(&mut txn, 1, &[NormalValue::Int(20)])
            .await
            .unwrap();
        index
            .save(&mut txn, 2, &[NormalValue::Int(30)])
            .await
            .unwrap();
        index
            .save(&mut txn, 3, &[NormalValue::Int(40)])
            .await
            .unwrap();
        txn.commit().await.unwrap();

        let txn = store.new_txn(true).await.unwrap();
        let mut iter = RangeIterator::new_scan(txn.as_ref(), 1, &desc, false, false)
            .await
            .unwrap();

        // Seek to age=30 exclusive (forward): iterator should land at doc 3 (age=40).
        let seek_key = build_age_seek_key(30);
        iter.apply_cursor_seek(seek_key, false, false)
            .await
            .unwrap();

        let entries = iter.collect_all().await.unwrap();
        let doc_ids: Vec<u64> = entries.iter().map(|e| e.doc_short_id).collect();

        assert!(!doc_ids.contains(&1), "doc 1 should be before the cursor");
        assert!(
            !doc_ids.contains(&2),
            "doc 2 is the exclusive boundary and must be skipped"
        );
        assert!(
            doc_ids.contains(&3),
            "doc 3 must be included (after boundary)"
        );
        assert_eq!(entries.len(), 1);
    }

    #[tokio::test]
    async fn test_cursor_seek_backward_inclusive_starts_at_boundary() {
        // Setup: age index with doc 1 = 20, doc 2 = 30, doc 3 = 40.
        // Backward inclusive seek at age=30 → should return doc 2 then doc 1.
        let store = MemoryStore::new();
        let mut txn = store.new_txn(false).await.unwrap();

        let desc = test_index_description();
        let index = SimpleIndex::new(1, desc.clone());

        index
            .save(&mut txn, 1, &[NormalValue::Int(20)])
            .await
            .unwrap();
        index
            .save(&mut txn, 2, &[NormalValue::Int(30)])
            .await
            .unwrap();
        index
            .save(&mut txn, 3, &[NormalValue::Int(40)])
            .await
            .unwrap();
        txn.commit().await.unwrap();

        let txn = store.new_txn(true).await.unwrap();
        let mut iter = RangeIterator::new_scan(txn.as_ref(), 1, &desc, false, true)
            .await
            .unwrap();

        // Seek to age=30 inclusive (reverse): iterator should include docs 2 and 1.
        // The seek_key encodes age=30; for a reverse scan the storage seek lands at or before it.
        let seek_key = build_age_seek_key(30);
        iter.apply_cursor_seek(seek_key, true, true).await.unwrap();

        let entries = iter.collect_all().await.unwrap();
        let doc_ids: Vec<u64> = entries.iter().map(|e| e.doc_short_id).collect();

        assert!(
            !doc_ids.contains(&3),
            "doc 3 is after the cursor and must be excluded"
        );
        assert!(
            doc_ids.contains(&2),
            "doc 2 is the inclusive boundary and must be included"
        );
        assert!(
            doc_ids.contains(&1),
            "doc 1 must be included (before boundary in reverse)"
        );
        assert_eq!(entries.len(), 2);
    }

    #[tokio::test]
    async fn test_cursor_seek_reversed_param_controls_upper_bound() {
        // Verify that the `reversed` parameter controls which bound slot is used,
        // overriding `self.reverse`. This is the P1.1 regression guard: the
        // `reversed` field in CursorSeek must be propagated explicitly rather than
        // silently relying on `self.reverse` already being set correctly.
        //
        // Scenario: build a reverse iterator (self.reverse=true), then call
        // apply_cursor_seek with reversed=true at age=30 inclusive.
        // The upper_bound_key should be set (reverse path), not lower_bound_key.
        // The existing test_cursor_seek_backward_inclusive_starts_at_boundary already
        // validates the behavioral outcome; this test checks the bound slot directly.
        let store = MemoryStore::new();
        let mut txn = store.new_txn(false).await.unwrap();

        let desc = test_index_description();
        let index = SimpleIndex::new(1, desc.clone());

        index
            .save(&mut txn, 1, &[NormalValue::Int(20)])
            .await
            .unwrap();
        index
            .save(&mut txn, 2, &[NormalValue::Int(30)])
            .await
            .unwrap();
        index
            .save(&mut txn, 3, &[NormalValue::Int(40)])
            .await
            .unwrap();
        txn.commit().await.unwrap();

        // Build a reverse iterator to match the reversed=true param.
        let txn = store.new_txn(true).await.unwrap();
        let mut iter = RangeIterator::new_scan(txn.as_ref(), 1, &desc, false, true)
            .await
            .unwrap();

        let seek_key = build_age_seek_key(30);
        iter.apply_cursor_seek(seek_key.clone(), true, true)
            .await
            .unwrap();

        // The reversed=true path must set upper_bound_key, not lower_bound_key.
        assert!(
            iter.upper_bound_key.is_some(),
            "reversed=true must set upper_bound_key"
        );
        assert!(
            iter.lower_bound_key.is_none(),
            "reversed=true must NOT set lower_bound_key"
        );
        assert_eq!(iter.upper_bound_key.as_deref(), Some(seek_key.as_slice()));
        assert!(
            iter.upper_inclusive,
            "inclusive=true must set upper_inclusive"
        );

        // Behavioral check: docs 2 and 1 returned (inclusive at 30, exclude doc 3 > 30).
        let entries = iter.collect_all().await.unwrap();
        let doc_ids: Vec<u64> = entries.iter().map(|e| e.doc_short_id).collect();
        assert!(!doc_ids.contains(&3), "doc 3 is above the cursor boundary");
        assert!(doc_ids.contains(&2), "doc 2 is at the boundary (inclusive)");
        assert!(doc_ids.contains(&1), "doc 1 is before boundary");
        assert_eq!(entries.len(), 2);
    }
}
