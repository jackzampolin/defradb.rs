//! Document head provider implementation for P2P DocSync.
//!
//! This module provides a database-backed implementation of the `DocumentHeadProvider`
//! trait, allowing the P2P layer to query document heads for DocSync responses.

use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use cid::Cid;
use defra_core::{Block, CrdtDelta};
use storage::corekv::{IterOptions, Store};
use storage::keys::headstore::{HeadstoreDocKey, HeadstorePriorityKey};

use p2p::sync::DocumentHeadProvider;

use crate::collection::require_persisted_collection_short_id;
use crate::database::DB;

/// Database-backed document head provider.
///
/// This implementation queries the headstore for composite head CIDs
/// to respond to DocSync requests from peers.
pub struct DbHeadProvider<S: Store> {
    db: Arc<DB<S>>,
}

impl<S: Store> DbHeadProvider<S> {
    /// Create a new DbHeadProvider with the given database.
    pub fn new(db: Arc<DB<S>>) -> Self {
        Self { db }
    }
}

#[async_trait]
impl<S: Store + 'static> DocumentHeadProvider for DbHeadProvider<S> {
    async fn get_document_heads(&self, doc_id: &str) -> p2p::error::Result<Vec<Cid>> {
        // Create a read-only transaction to access the headstore
        let txn = self.db.new_txn(true).await.map_err(|e| {
            p2p::error::Error::HeadProvider(format!("failed to create transaction: {}", e))
        })?;

        let headstore = txn.headstore().map_err(|e| {
            p2p::error::Error::HeadProvider(format!("failed to get headstore: {}", e))
        })?;
        let systemstore = txn.systemstore().map_err(|e| {
            p2p::error::Error::HeadProvider(format!("failed to get systemstore: {}", e))
        })?;

        let Some(doc_ref) = crate::docid::map::get_doc_ref(&systemstore, doc_id)
            .await
            .map_err(|e| {
                p2p::error::Error::HeadProvider(format!("doc-ID mapping lookup failed: {}", e))
            })?
        else {
            return Ok(Vec::new());
        };
        let doc_short_id = doc_ref.doc_short_id;

        // Query composite heads with prefix /d/{doc_short_id}/C/
        let prefix = HeadstoreDocKey::field_prefix(doc_short_id, "C");
        let prefix_len = prefix.len();
        let opts = IterOptions::new().with_prefix(prefix);

        let mut iter = headstore.iterator(opts).await.map_err(|e| {
            p2p::error::Error::HeadProvider(format!("failed to iterate headstore: {}", e))
        })?;

        let mut cids = Vec::new();

        while let Some(pair) = iter.next().await.map_err(|e| {
            p2p::error::Error::HeadProvider(format!("headstore iteration error: {}", e))
        })? {
            let cid_str = String::from_utf8_lossy(&pair.key[prefix_len..]);
            if let Ok(cid) = Cid::from_str(&cid_str) {
                cids.push(cid);
            }
        }

        iter.close().await.map_err(|e| {
            p2p::error::Error::HeadProvider(format!("headstore close error: {}", e))
        })?;

        if cids.is_empty() {
            let blockstore = txn.blockstore().map_err(|e| {
                p2p::error::Error::HeadProvider(format!("failed to get blockstore: {}", e))
            })?;
            let mut priority_iter = headstore
                .iterator(
                    IterOptions::new()
                        .with_prefix(HeadstorePriorityKey::document_prefix(doc_short_id)),
                )
                .await
                .map_err(|e| {
                    p2p::error::Error::HeadProvider(format!(
                        "failed to iterate priority index: {}",
                        e
                    ))
                })?;

            let cid_offset = HeadstorePriorityKey::cid_offset(doc_short_id);
            let mut max_priority: Option<u64> = None;

            while let Some(pair) = priority_iter.next().await.map_err(|e| {
                p2p::error::Error::HeadProvider(format!("priority index iteration error: {}", e))
            })? {
                let cid_bytes = match pair.key.get(cid_offset..) {
                    Some(bytes) => bytes,
                    None => continue,
                };
                let Ok(cid) = Cid::try_from(cid_bytes) else {
                    continue;
                };

                let Some(block_bytes) = blockstore.get(&cid.to_bytes()).await.map_err(|e| {
                    p2p::error::Error::HeadProvider(format!(
                        "failed to read blockstore for priority index CID: {}",
                        e
                    ))
                })?
                else {
                    continue;
                };
                let Ok(block) = Block::from_dag_cbor(&block_bytes) else {
                    continue;
                };

                if !matches!(block.delta, CrdtDelta::Composite(_)) {
                    continue;
                }

                let priority = block.delta.priority();
                match max_priority {
                    Some(current) if priority < current => {}
                    Some(current) if priority == current => cids.push(cid),
                    _ => {
                        max_priority = Some(priority);
                        cids.clear();
                        cids.push(cid);
                    }
                }
            }

            priority_iter.close().await.map_err(|e| {
                p2p::error::Error::HeadProvider(format!("priority index close error: {}", e))
            })?;
        }

        if cids.is_empty() {
            // Last-resort fallback (headstore wiped): walk the block-ownership
            // index for composites owned by this document and pick the highest
            // priority. Deltas no longer carry a docID, so ownership is the
            // only recoverable link.
            let blockstore = txn.blockstore().map_err(|e| {
                p2p::error::Error::HeadProvider(format!("failed to get blockstore: {}", e))
            })?;
            let mut owner_iter = systemstore
                .iterator(IterOptions::new().with_prefix(b"/d/b/".to_vec()))
                .await
                .map_err(|e| {
                    p2p::error::Error::HeadProvider(format!(
                        "failed to iterate block ownership index: {}",
                        e
                    ))
                })?;

            let owned_suffix = format!("/{}", doc_id);
            let mut owned_cids = Vec::new();
            while let Some(pair) = owner_iter.next().await.map_err(|e| {
                p2p::error::Error::HeadProvider(format!(
                    "block ownership index iteration error: {}",
                    e
                ))
            })? {
                let key_str = String::from_utf8_lossy(&pair.key);
                if let Some(cid_str) = key_str
                    .strip_prefix("/d/b/")
                    .and_then(|rest| rest.strip_suffix(&owned_suffix))
                {
                    if let Ok(cid) = Cid::from_str(cid_str) {
                        owned_cids.push(cid);
                    }
                }
            }
            owner_iter.close().await.map_err(|e| {
                p2p::error::Error::HeadProvider(format!("block ownership index close error: {}", e))
            })?;

            let mut max_priority: Option<u64> = None;
            for cid in owned_cids {
                let Some(block_bytes) = blockstore.get(&cid.to_bytes()).await.map_err(|e| {
                    p2p::error::Error::HeadProvider(format!(
                        "failed to read blockstore for owned CID: {}",
                        e
                    ))
                })?
                else {
                    continue;
                };
                let Ok(block) = Block::from_dag_cbor(&block_bytes) else {
                    continue;
                };
                if !matches!(block.delta, CrdtDelta::Composite(_)) {
                    continue;
                }
                let priority = block.delta.priority();
                match max_priority {
                    Some(current) if priority < current => {}
                    Some(current) if priority == current => cids.push(cid),
                    _ => {
                        max_priority = Some(priority);
                        cids.clear();
                        cids.push(cid);
                    }
                }
            }
        }

        Ok(cids)
    }

    async fn get_collection_heads(&self, collection_id: &str) -> p2p::error::Result<Vec<Cid>> {
        let txn = self.db.new_txn(true).await.map_err(|e| {
            p2p::error::Error::HeadProvider(format!("failed to create transaction: {}", e))
        })?;

        let systemstore = txn.systemstore().map_err(|e| {
            p2p::error::Error::HeadProvider(format!("failed to get systemstore: {}", e))
        })?;
        let headstore = txn.headstore().map_err(|e| {
            p2p::error::Error::HeadProvider(format!("failed to get headstore: {}", e))
        })?;

        let short_id = require_persisted_collection_short_id(&systemstore, collection_id)
            .await
            .map_err(|e| {
                p2p::error::Error::HeadProvider(format!("failed to load short id: {}", e))
            })?;
        let found = crate::block::heads::live_collection_heads(&headstore, short_id)
            .await
            .map_err(|e| {
                p2p::error::Error::HeadProvider(format!("failed to read collection heads: {}", e))
            })?;

        Ok(found.live)
    }
}
