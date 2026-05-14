//! Shared cursor-seek helper for index scan fetchers.

use query::planner::index_selection::CursorSeek;
use storage::index::RangeIterator;

/// Apply a cursor seek to a `RangeIterator` when `cursor_seek` is `Some`.
///
/// This is the single source of truth used by all three fetcher impls
/// (`DbDocFetcher`, `LensedDocFetcher`, `LensedAutoCommitFetcher`).
pub(crate) async fn apply_cursor_seek_to_iterator(
    iter: &mut RangeIterator,
    cursor_seek: &Option<CursorSeek>,
) -> Result<(), query::error::QueryError> {
    if let Some(ref seek) = cursor_seek {
        iter.apply_cursor_seek(seek.seek_key.clone(), seek.inclusive)
            .await
            .map_err(|e| {
                query::error::QueryError::execution(format!("cursor seek error: {}", e))
            })?;
    }
    Ok(())
}
