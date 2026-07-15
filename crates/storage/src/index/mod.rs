//! Secondary index implementations for DefraDB
//!
//! This module provides the `CollectionIndex` trait and implementations
//! for managing secondary indexes on collections.
//!
//! # Index Types
//!
//! - `SimpleIndex`: Non-unique index that appends the doc short ID to the key
//! - `UniqueIndex`: Unique index that stores the encoded doc short ID as the
//!   value, enforcing uniqueness on the indexed field(s)
//!
//! # Key Structure
//!
//! Index keys are structured as:
//! ```text
//! /[CollectionShortID]/[IndexID]/[EncodedFieldValue1][EncodedFieldValue2]...(/[DocShortID])
//! ```
//!
//! For SimpleIndex, the doc short ID is appended to the key (empty value).
//! For UniqueIndex, the encoded doc short ID is stored as the value; a key
//! collision is a uniqueness violation. Entries with a nil field fall back
//! to the short-ID-in-key layout so multiple NULLs can coexist.
//!
//! Iterators yield node-local doc short IDs; callers resolve them to public
//! DocIDs through the systemstore mapping at the db layer.
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

/// Validate that a doc short ID is valid for use in index entries.
///
/// Short IDs start at 1; 0 marks "unset" and never identifies a document.
pub(crate) fn validate_doc_short_id(doc_short_id: u64, index_name: &str) -> Result<()> {
    if doc_short_id == 0 {
        return Err(crate::corekv::Error::Other(format!(
            "index '{}': doc short ID cannot be 0",
            index_name
        )));
    }
    Ok(())
}
