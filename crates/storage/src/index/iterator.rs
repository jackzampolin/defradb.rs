//! Index iterator traits and types for scanning index entries
//!
//! This module provides the core abstractions for iterating over index entries:
//! - `IndexEntry`: A single index entry containing document ID and field values
//! - `IndexIterator`: Trait for iterating over index entries
//! - Bound types for configuring range queries

use async_trait::async_trait;
use document::NormalValue;

use crate::corekv::{MaybeSend, Result};

/// A single entry from an index scan.
///
/// Contains the document ID and the indexed field values for that document.
/// The values are in the same order as the index field definitions.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexEntry {
    /// The document ID this entry points to
    pub doc_id: String,
    /// The indexed field values in field order
    pub values: Vec<NormalValue>,
}

impl IndexEntry {
    /// Create a new index entry
    pub fn new(doc_id: impl Into<String>, values: Vec<NormalValue>) -> Self {
        Self {
            doc_id: doc_id.into(),
            values,
        }
    }

    /// Create an index entry with a single value
    pub fn single(doc_id: impl Into<String>, value: NormalValue) -> Self {
        Self {
            doc_id: doc_id.into(),
            values: vec![value],
        }
    }
}

/// Bound for range queries on an index field.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Bound {
    /// Include the bound value in the range (>=, <=)
    Inclusive(NormalValue),
    /// Exclude the bound value from the range (>, <)
    Exclusive(NormalValue),
    /// No bound (unbounded in this direction)
    Unbounded,
}

impl Bound {
    /// Create an inclusive bound (>=)
    pub fn ge(value: NormalValue) -> Self {
        Bound::Inclusive(value)
    }

    /// Create an exclusive bound (>)
    pub fn gt(value: NormalValue) -> Self {
        Bound::Exclusive(value)
    }

    /// Create an inclusive upper bound (<=)
    pub fn le(value: NormalValue) -> Self {
        Bound::Inclusive(value)
    }

    /// Create an exclusive upper bound (<)
    pub fn lt(value: NormalValue) -> Self {
        Bound::Exclusive(value)
    }

    /// Check if this bound is unbounded
    pub fn is_unbounded(&self) -> bool {
        matches!(self, Bound::Unbounded)
    }

    /// Get the value if this is a bounded value
    pub fn value(&self) -> Option<&NormalValue> {
        match self {
            Bound::Inclusive(v) | Bound::Exclusive(v) => Some(v),
            Bound::Unbounded => None,
        }
    }
}

/// Trait for iterating over index entries.
///
/// IndexIterator provides an async interface for scanning index entries.
/// Implementations handle the specific iteration strategy (exact match, prefix, range).
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait IndexIterator: MaybeSend {
    /// Advance to the next index entry.
    ///
    /// Returns:
    /// - `Ok(Some(IndexEntry))` if there is a next entry
    /// - `Ok(None)` if iteration is complete
    /// - `Err(Error)` if an error occurred
    async fn next(&mut self) -> Result<Option<IndexEntry>>;

    /// Close the iterator and release resources.
    async fn close(&mut self) -> Result<()>;

    /// Reset the iterator to the beginning.
    async fn reset(&mut self) -> Result<()>;

    /// Seek the iterator to the given raw storage key.
    ///
    /// Positions the iterator at the first entry at or after `key` (forward)
    /// or at or before `key` (reverse). Returns `true` if a valid position
    /// was found, `false` if no entries remain after seeking.
    ///
    /// Used by cursor pagination to skip directly into an index without
    /// scanning from the beginning. Default implementation returns `Ok(false)`.
    async fn seek(&mut self, _key: &[u8]) -> Result<bool> {
        Ok(false)
    }

    /// Collect all remaining entries into a Vec.
    ///
    /// Consumes the iterator. Use with caution for large result sets.
    async fn collect_all(&mut self) -> Result<Vec<IndexEntry>> {
        let mut results = Vec::new();
        while let Some(entry) = self.next().await? {
            results.push(entry);
        }
        Ok(results)
    }

    /// Count the remaining entries without collecting them.
    async fn count(&mut self) -> Result<usize> {
        let mut count = 0;
        while (self.next().await?).is_some() {
            count += 1;
        }
        Ok(count)
    }
}

/// Implement IndexIterator for Box<dyn IndexIterator>
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl IndexIterator for Box<dyn IndexIterator> {
    async fn next(&mut self) -> Result<Option<IndexEntry>> {
        (**self).next().await
    }

    async fn close(&mut self) -> Result<()> {
        (**self).close().await
    }

    async fn reset(&mut self) -> Result<()> {
        (**self).reset().await
    }

    async fn seek(&mut self, key: &[u8]) -> Result<bool> {
        (**self).seek(key).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_entry_new() {
        let entry = IndexEntry::new(
            "doc1",
            vec![
                NormalValue::String("alice".to_string()),
                NormalValue::Int(25),
            ],
        );
        assert_eq!(entry.doc_id, "doc1");
        assert_eq!(entry.values.len(), 2);
    }

    #[test]
    fn test_index_entry_single() {
        let entry = IndexEntry::single("doc1", NormalValue::String("alice".to_string()));
        assert_eq!(entry.doc_id, "doc1");
        assert_eq!(entry.values.len(), 1);
    }

    #[test]
    fn test_bound_inclusive() {
        let bound = Bound::ge(NormalValue::Int(10));
        assert!(!bound.is_unbounded());
        assert_eq!(bound.value(), Some(&NormalValue::Int(10)));
    }

    #[test]
    fn test_bound_exclusive() {
        let bound = Bound::gt(NormalValue::Int(10));
        assert!(!bound.is_unbounded());
        assert_eq!(bound.value(), Some(&NormalValue::Int(10)));
    }

    #[test]
    fn test_bound_unbounded() {
        let bound = Bound::Unbounded;
        assert!(bound.is_unbounded());
        assert_eq!(bound.value(), None);
    }
}
