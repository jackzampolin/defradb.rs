//! Shared cursor-seek helper for index scan fetchers.

use datastore::NamespaceView;
use query::planner::index_selection::CursorSeek;
use storage::index::RangeIterator;
use storage::keys::{doc_id_index::encode_doc_short_id, SEPARATOR};

/// Apply a cursor seek to a `RangeIterator` when `cursor_seek` is `Some`.
///
/// This is the single source of truth used by all three fetcher impls
/// (`DbDocFetcher`, `LensedDocFetcher`, `LensedAutoCommitFetcher`).
pub(crate) async fn apply_cursor_seek_to_iterator(
    iter: &mut RangeIterator,
    cursor_seek: &Option<CursorSeek>,
    systemstore: &NamespaceView,
    collection_short_id: u32,
) -> Result<(), query::error::QueryError> {
    if let Some(ref seek) = cursor_seek {
        let seek_key = resolve_cursor_seek_key(seek, systemstore, collection_short_id).await?;
        iter.apply_cursor_seek(seek_key, seek.inclusive, seek.reversed)
            .await
            .map_err(|e| {
                query::error::QueryError::execution(format!("cursor seek error: {}", e))
            })?;
    }
    Ok(())
}

async fn resolve_cursor_seek_key(
    seek: &CursorSeek,
    systemstore: &NamespaceView,
    collection_short_id: u32,
) -> Result<Vec<u8>, query::error::QueryError> {
    let mut key = seek.seek_key.clone();
    let Some(doc_id) = seek.boundary_doc_id.as_deref() else {
        return Ok(key);
    };

    let doc_short_id =
        crate::docid::map::get_doc_short_id(systemstore, collection_short_id, doc_id)
            .await
            .map_err(|e| {
                query::error::QueryError::execution(format!("doc ID resolution error: {e}"))
            })?
            .ok_or_else(query::error::QueryError::cursor_invalid)?;

    key.push(SEPARATOR);
    key.extend_from_slice(&encode_doc_short_id(doc_short_id));
    Ok(key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datastore::SharedTxn;
    use storage::backends::MemoryStore;
    use storage::corekv::Store;
    use storage::namespace::Namespace;

    #[tokio::test]
    async fn cursor_boundary_uses_doc_short_id_suffix() {
        let store = MemoryStore::new();
        let txn = store.new_txn(false).await.unwrap();
        let systemstore = NamespaceView::new(SharedTxn::new(txn), Namespace::Systemstore);
        crate::docid::map::set_doc_id_mapping(&systemstore, 7, 42, "bae-boundary")
            .await
            .unwrap();

        let seek = CursorSeek {
            seek_key: vec![1, 2, 3],
            boundary_doc_id: Some("bae-boundary".to_string()),
            inclusive: false,
            reversed: false,
            expected_index_name: "idx".to_string(),
            fetch_limit: None,
        };

        let key = resolve_cursor_seek_key(&seek, &systemstore, 7)
            .await
            .unwrap();
        let mut expected = vec![1, 2, 3, SEPARATOR];
        expected.extend_from_slice(&encode_doc_short_id(42));
        assert_eq!(key, expected);
    }
}
