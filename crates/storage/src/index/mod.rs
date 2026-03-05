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
mod fulltext;
mod in_iterator;
mod index_type;
mod iterator;
mod matcher;
mod range_iterator;
mod simple;
mod traits;
mod unique;

#[cfg(test)]
mod tests;

pub use eq_iterator::ExactMatchIterator;
pub use fulltext::{parse_language, FullTextIndex};
pub use in_iterator::InIterator;
pub use index_type::IndexType;
pub use iterator::{Bound, IndexEntry, IndexIterator};
pub use matcher::{
    EqMatcher, GtMatcher, InMatcher, IndexMatcher, LikeMatcher, LtMatcher, NeMatcher, NinMatcher,
    NlikeMatcher,
};
pub use range_iterator::RangeIterator;
pub use simple::SimpleIndex;
pub use traits::CollectionIndex;
pub use unique::UniqueIndex;

use crate::corekv::Result;

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
