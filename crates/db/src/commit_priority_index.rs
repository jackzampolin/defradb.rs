use std::collections::HashSet;
use std::str::FromStr;

use cid::Cid;
use defra_core::block::Block;
use storage::corekv::{IterOptions, Key, Store};
use storage::keys::HeadstorePriorityKey;

use crate::{Error, Result, DB};

const COMMIT_PRIORITY_INDEX_MARKER_KEY: &[u8] = b"/meta/commit-priority-index-complete";

impl<S: Store> DB<S> {
    pub(crate) async fn maybe_backfill_commit_priority_index(&self) -> Result<()> {
        let read_txn = self.new_txn(true).await?;
        let already_indexed = {
            let headstore = read_txn.headstore()?;
            headstore
                .get(COMMIT_PRIORITY_INDEX_MARKER_KEY)
                .await
                .map_err(Error::Storage)?;
            headstore
                .get(COMMIT_PRIORITY_INDEX_MARKER_KEY)
                .await
                .map_err(Error::Storage)?
                .is_some()
        };
        let _ = read_txn.discard();

        if already_indexed {
            return Ok(());
        }

        self.backfill_commit_priority_index().await
    }

    /// Rebuild the `(doc_id, priority) -> cid` commit index from the current head chains.
    pub async fn backfill_commit_priority_index(&self) -> Result<()> {
        let write_txn = self.new_txn(false).await?;

        let indexed_count = {
            let headstore = write_txn.headstore()?;
            let blockstore = write_txn.blockstore()?;

            let mut head_iter = headstore
                .iterator(IterOptions::new().with_prefix(b"/d/".to_vec()))
                .await
                .map_err(Error::Storage)?;

            let mut root_heads = Vec::new();
            let mut seen_root_heads = HashSet::new();
            while let Some(pair) = head_iter.next().await.map_err(Error::Storage)? {
                let key_str = String::from_utf8_lossy(&pair.key);
                let Some(cid_str) = key_str.rsplit('/').next() else {
                    continue;
                };
                let Ok(cid) = Cid::from_str(cid_str) else {
                    continue;
                };
                if seen_root_heads.insert(cid) {
                    root_heads.push(cid);
                }
            }
            head_iter.close().await.map_err(Error::Storage)?;

            let mut stack = root_heads;
            let mut visited = HashSet::new();
            let mut indexed_count = 0u64;

            while let Some(cid) = stack.pop() {
                if !visited.insert(cid) {
                    continue;
                }

                let Some(block_bytes) = blockstore
                    .get(&cid.to_bytes())
                    .await
                    .map_err(Error::Storage)?
                else {
                    continue;
                };
                let Ok(block) = Block::from_dag_cbor(&block_bytes) else {
                    continue;
                };

                if let Some(doc_id_bytes) = block.delta.doc_id() {
                    let doc_id = String::from_utf8_lossy(doc_id_bytes).to_string();
                    let key = HeadstorePriorityKey::new(&doc_id, block.delta.priority(), cid);
                    headstore
                        .set(&key.bytes(), &[])
                        .await
                        .map_err(Error::Storage)?;
                    indexed_count += 1;
                }

                if let Some(heads) = &block.heads {
                    for parent in heads {
                        stack.push(*parent);
                    }
                }
            }

            indexed_count
        };

        {
            let headstore = write_txn.headstore()?;
            headstore
                .set(COMMIT_PRIORITY_INDEX_MARKER_KEY, b"1")
                .await
                .map_err(Error::Storage)?;
        }

        write_txn.commit().await?;
        tracing::debug!(indexed_count, "Backfilled commit priority index");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use document::{Document, NormalValue};
    use query::fetcher::CommitsQueryOptions;
    use storage::backends::MemoryStore;
    use storage::corekv::{IterOptions, Store};

    use super::*;
    use crate::block_builder::write_document_blocks;
    use crate::commits_fetcher::CommitsFetcher;
    use crate::txn::DbTxn;

    async fn count_priority_entries<S: Store>(txn: &DbTxn<S>) -> usize {
        let headstore = txn.headstore().unwrap();
        let mut iter = headstore
            .iterator(IterOptions::new().with_prefix(b"/p/".to_vec()))
            .await
            .unwrap();
        let mut count = 0usize;
        while iter.next().await.unwrap().is_some() {
            count += 1;
        }
        iter.close().await.unwrap();
        count
    }

    async fn has_priority_index_marker<S: Store>(txn: &DbTxn<S>) -> bool {
        txn.headstore()
            .unwrap()
            .get(COMMIT_PRIORITY_INDEX_MARKER_KEY)
            .await
            .unwrap()
            .is_some()
    }

    #[tokio::test]
    async fn test_backfill_commit_priority_index_rebuilds_field_and_composite_history() {
        let store = Arc::new(MemoryStore::new());
        let seed_db = DB::from_arc(store.clone()).unwrap();

        let doc_id = {
            let write_txn = seed_db.new_txn(false).await.unwrap();
            let doc_id = {
                let blockstore = write_txn.blockstore().unwrap();
                let headstore = write_txn.headstore().unwrap();

                let mut doc = Document::new();
                doc.generate_and_set_doc_id().unwrap();
                doc.set("name", NormalValue::String("Alice".to_string()));
                doc.set("age", NormalValue::Int(30));

                let doc_id = doc.id().unwrap().to_string();
                write_document_blocks(
                    &blockstore,
                    &headstore,
                    &doc,
                    "schema-v1",
                    None,
                    None,
                    None,
                )
                .await
                .unwrap();

                doc.set("age", NormalValue::Int(31));
                let age_only = std::iter::once("age".to_string()).collect();
                write_document_blocks(
                    &blockstore,
                    &headstore,
                    &doc,
                    "schema-v1",
                    Some(&age_only),
                    None,
                    None,
                )
                .await
                .unwrap();

                doc.set("name", NormalValue::String("Alicia".to_string()));
                let name_only = std::iter::once("name".to_string()).collect();
                write_document_blocks(
                    &blockstore,
                    &headstore,
                    &doc,
                    "schema-v1",
                    Some(&name_only),
                    None,
                    None,
                )
                .await
                .unwrap();

                doc_id
            };
            write_txn.commit().await.unwrap();
            doc_id
        };

        let delete_txn = seed_db.new_txn(false).await.unwrap();
        {
            let headstore = delete_txn.headstore().unwrap();
            let mut iter = headstore
                .iterator(IterOptions::new().with_prefix(b"/p/".to_vec()))
                .await
                .unwrap();
            let mut keys = Vec::new();
            while let Some(pair) = iter.next().await.unwrap() {
                keys.push(pair.key);
            }
            iter.close().await.unwrap();
            for key in keys {
                headstore.delete(&key).await.unwrap();
            }
        }
        delete_txn.commit().await.unwrap();

        let verify_txn = seed_db.new_txn(true).await.unwrap();
        assert_eq!(count_priority_entries(&verify_txn).await, 0);
        assert!(!has_priority_index_marker(&verify_txn).await);
        let _ = verify_txn.discard();

        let reopened = DB::open_from_arc(store.clone()).await.unwrap();

        let read_txn = reopened.new_txn(true).await.unwrap();
        assert_eq!(count_priority_entries(&read_txn).await, 7);
        assert!(has_priority_index_marker(&read_txn).await);

        let fetcher = CommitsFetcher::new(Arc::new(async_lock::Mutex::new(Some(read_txn))));
        let composite_commits = fetcher
            .fetch_commits(&crate::commits_fetcher::CommitsQueryOptions {
                doc_id: Some(doc_id.clone()),
                cid: None,
                depth: None,
                height_start: Some(1),
                height_end: Some(4),
                field_name: Some("_C".to_string()),
            })
            .await
            .unwrap();
        let mut composite_heights: Vec<_> = composite_commits
            .iter()
            .map(|commit| {
                commit
                    .get("height")
                    .and_then(|value| value.as_int())
                    .unwrap()
            })
            .collect();
        composite_heights.sort_unstable();
        assert_eq!(composite_heights, vec![1, 2, 3]);

        let age_commit = fetcher
            .fetch_commits(&crate::commits_fetcher::CommitsQueryOptions {
                doc_id: Some(doc_id),
                cid: None,
                depth: None,
                height_start: Some(2),
                height_end: Some(3),
                field_name: Some("age".to_string()),
            })
            .await
            .unwrap();
        assert_eq!(age_commit.len(), 1);
        assert_eq!(
            age_commit[0].get("height").and_then(|value| value.as_int()),
            Some(2)
        );
        assert_eq!(
            age_commit[0]
                .get("fieldName")
                .and_then(|value| value.as_str()),
            Some("age")
        );
    }

    #[tokio::test]
    async fn test_backfill_commit_priority_index_repairs_partial_index_without_marker() {
        let store = Arc::new(MemoryStore::new());
        let seed_db = DB::from_arc(store.clone()).unwrap();

        let partial_count = {
            let write_txn = seed_db.new_txn(false).await.unwrap();
            {
                let blockstore = write_txn.blockstore().unwrap();
                let headstore = write_txn.headstore().unwrap();

                let mut doc = Document::new();
                doc.generate_and_set_doc_id().unwrap();
                doc.set("name", NormalValue::String("Alice".to_string()));
                doc.set("age", NormalValue::Int(30));

                write_document_blocks(
                    &blockstore,
                    &headstore,
                    &doc,
                    "schema-v1",
                    None,
                    None,
                    None,
                )
                .await
                .unwrap();

                doc.set("age", NormalValue::Int(31));
                let age_only = std::iter::once("age".to_string()).collect();
                write_document_blocks(
                    &blockstore,
                    &headstore,
                    &doc,
                    "schema-v1",
                    Some(&age_only),
                    None,
                    None,
                )
                .await
                .unwrap();

                let mut iter = headstore
                    .iterator(IterOptions::new().with_prefix(b"/p/".to_vec()))
                    .await
                    .unwrap();
                let mut seen_first = false;
                while let Some(pair) = iter.next().await.unwrap() {
                    if !seen_first {
                        headstore.delete(&pair.key).await.unwrap();
                        seen_first = true;
                    }
                }
                iter.close().await.unwrap();
            }
            write_txn.commit().await.unwrap();

            let verify_txn = seed_db.new_txn(true).await.unwrap();
            let count = count_priority_entries(&verify_txn).await;
            assert!(
                count > 0,
                "test should leave a partially populated /p/ index"
            );
            assert!(!has_priority_index_marker(&verify_txn).await);
            let _ = verify_txn.discard();
            count
        };

        let reopened = DB::open_from_arc(store.clone()).await.unwrap();
        let read_txn = reopened.new_txn(true).await.unwrap();
        let rebuilt_count = count_priority_entries(&read_txn).await;
        assert!(rebuilt_count > partial_count);
        assert!(has_priority_index_marker(&read_txn).await);
        let _ = read_txn.discard();
    }

    #[test]
    fn test_commits_query_options_default_height_range() {
        let options = CommitsQueryOptions::default();
        assert!(options.height_start.is_none());
        assert!(options.height_end.is_none());
    }
}
