use std::collections::HashSet;

use cid::Cid;
use defra_core::block::Block;
use storage::corekv::{IterOptions, Key, Store};
use storage::keys::{HeadstoreDocKey, HeadstorePriorityKey};

use crate::{Error, Result, DB};

pub const COMMIT_PRIORITY_INDEX_MARKER_KEY: &[u8] = b"/meta/commit-priority-index-complete";

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

            // Deltas no longer carry docIDs (Go #4838): the doc short ID is
            // recovered from the head key ("/d/{short_id uvarint}/...") and
            // inherited by every ancestor reached through that head.
            let mut root_heads: Vec<(Cid, u64)> = Vec::new();
            let mut seen_root_heads = HashSet::new();
            while let Some(pair) = head_iter.next().await.map_err(Error::Storage)? {
                let Some(head_key) = HeadstoreDocKey::parse(&pair.key) else {
                    continue;
                };
                if seen_root_heads.insert((head_key.cid, head_key.doc_short_id)) {
                    root_heads.push((head_key.cid, head_key.doc_short_id));
                }
            }
            head_iter.close().await.map_err(Error::Storage)?;

            let mut stack = root_heads;
            let mut visited = HashSet::new();
            let mut indexed_count = 0u64;

            while let Some((cid, doc_short_id)) = stack.pop() {
                if !visited.insert((cid, doc_short_id)) {
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

                let key = HeadstorePriorityKey::new(doc_short_id, block.delta.priority(), cid);
                headstore
                    .set(&key.bytes(), &[])
                    .await
                    .map_err(Error::Storage)?;
                indexed_count += 1;

                if let Some(heads) = &block.heads {
                    for parent in heads {
                        stack.push((*parent, doc_short_id));
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
