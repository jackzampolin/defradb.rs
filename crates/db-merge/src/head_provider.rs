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
use storage::keys::headstore::{HeadstoreColKey, HeadstoreDocKey, HeadstorePriorityKey};

use p2p::sync::DocumentHeadProvider;

use db::collection::require_persisted_collection_short_id;
use db::database::DB;

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

        let Some(doc_ref) = db::doc_id_map::get_doc_ref(&systemstore, doc_id)
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
        let prefix = HeadstoreColKey::collection_prefix(short_id);
        let opts = IterOptions::new().with_prefix(prefix);

        let mut iter = headstore.iterator(opts).await.map_err(|e| {
            p2p::error::Error::HeadProvider(format!("failed to iterate headstore: {}", e))
        })?;

        let mut cids = Vec::new();

        while let Some(pair) = iter.next().await.map_err(|e| {
            p2p::error::Error::HeadProvider(format!("headstore iteration error: {}", e))
        })? {
            // Parse CID from key: /c/{short_id}/{cid}
            let key_str = String::from_utf8_lossy(&pair.key);
            let parts: Vec<&str> = key_str.split('/').collect();
            if parts.len() < 4 {
                continue;
            }

            if let Ok(cid) = Cid::from_str(parts[3]) {
                cids.push(cid);
            }
        }

        iter.close().await.map_err(|e| {
            p2p::error::Error::HeadProvider(format!("headstore close error: {}", e))
        })?;

        Ok(cids)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use db::AutoCommitMutator;
    use db::DB;
    use defra_core::{block::generate_cid_from_bytes, Block, CompositeDeltaPayload, CrdtDelta};
    use document::Document;
    use query::mutator::DocMutator;
    use schema::{CollectionVersion, FieldDescription, FieldKind};
    use storage::backends::MemoryStore;
    use storage::corekv::Key;

    use super::*;

    #[tokio::test]
    async fn batch_create_docs_expose_composite_heads() {
        let store = Arc::new(MemoryStore::new());
        let db = Arc::new(DB::from_arc(store).unwrap());
        db.create_collection(CollectionVersion::new(
            "Transcript",
            "v1",
            "col-transcript",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "body", FieldKind::string()),
                FieldDescription::new("3", "idx", FieldKind::int()),
            ],
        ))
        .await
        .unwrap();

        let mutator = AutoCommitMutator::new(db.clone());
        let docs = vec![
            make_transcript("first", 1),
            make_transcript("second", 2),
            make_transcript("third", 3),
        ];
        let results = mutator.create_many("Transcript", docs).await.unwrap();
        assert_eq!(results.len(), 3);

        let provider = DbHeadProvider::new(db);
        for result in results {
            let doc_id = result.doc_id.to_string();
            let heads = provider.get_document_heads(&doc_id).await.unwrap();
            assert!(
                !heads.is_empty(),
                "expected composite heads for batch-created doc {}",
                doc_id
            );
        }
    }

    #[tokio::test]
    async fn falls_back_to_priority_index_when_composite_head_entry_is_missing() {
        let store = Arc::new(MemoryStore::new());
        let db = Arc::new(DB::from_arc(store).unwrap());
        db.create_collection(CollectionVersion::new(
            "Transcript",
            "v1",
            "col-transcript",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "body", FieldKind::string()),
                FieldDescription::new("3", "idx", FieldKind::int()),
            ],
        ))
        .await
        .unwrap();

        let mutator = AutoCommitMutator::new(db.clone());
        let result = mutator
            .create_many("Transcript", vec![make_transcript("first", 1)])
            .await
            .unwrap()
            .pop()
            .unwrap();
        let doc_id = result.doc_id.to_string();
        let commit_cid = result.commit_cid.expect("commit cid");
        let doc_short_id = doc_short_id_for(&db, &doc_id).await;

        let txn = db.new_txn(false).await.unwrap();
        txn.headstore()
            .unwrap()
            .delete(&HeadstoreDocKey::new(doc_short_id, "C", commit_cid).bytes())
            .await
            .unwrap();
        txn.commit().await.unwrap();

        let provider = DbHeadProvider::new(db);
        let heads = provider.get_document_heads(&doc_id).await.unwrap();
        assert_eq!(heads, vec![commit_cid]);
    }

    #[tokio::test]
    async fn falls_back_to_blockstore_scan_when_head_indexes_are_missing() {
        let store = Arc::new(MemoryStore::new());
        let db = Arc::new(DB::from_arc(store).unwrap());
        db.create_collection(CollectionVersion::new(
            "Transcript",
            "v1",
            "col-transcript",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "body", FieldKind::string()),
                FieldDescription::new("3", "idx", FieldKind::int()),
            ],
        ))
        .await
        .unwrap();

        let mutator = AutoCommitMutator::new(db.clone());
        let result = mutator
            .create_many("Transcript", vec![make_transcript("first", 1)])
            .await
            .unwrap()
            .pop()
            .unwrap();
        let doc_id = result.doc_id.to_string();
        let commit_cid = result.commit_cid.expect("commit cid");
        let commit_block = result.commit_block.expect("commit block");
        let priority = Block::from_dag_cbor(&commit_block)
            .expect("decode commit block")
            .delta
            .priority();
        let doc_short_id = doc_short_id_for(&db, &doc_id).await;

        let txn = db.new_txn(false).await.unwrap();
        txn.headstore()
            .unwrap()
            .delete(&HeadstoreDocKey::new(doc_short_id, "C", commit_cid).bytes())
            .await
            .unwrap();
        txn.headstore()
            .unwrap()
            .delete(&HeadstorePriorityKey::new(doc_short_id, priority, commit_cid).bytes())
            .await
            .unwrap();
        txn.commit().await.unwrap();

        let provider = DbHeadProvider::new(db);
        let heads = provider.get_document_heads(&doc_id).await.unwrap();
        assert_eq!(heads, vec![commit_cid]);
    }

    #[tokio::test]
    async fn falls_back_to_ownership_index_when_head_indexes_are_missing() {
        let store = Arc::new(MemoryStore::new());
        let db = Arc::new(DB::from_arc(store).unwrap());

        let block = Block::new_with_options(
            CrdtDelta::Composite(CompositeDeltaPayload {
                schema_version_id: "schema-v1".to_string(),
                priority: 7,
                status: 1,
            }),
            vec![],
            vec![],
            None,
            None,
        );
        let block_bytes = block.to_dag_cbor().unwrap();
        let cid = generate_cid_from_bytes(&block_bytes).unwrap();
        let doc_id = db_blocks::derive_doc_id(&cid);

        let txn = db.new_txn(false).await.unwrap();
        txn.blockstore()
            .unwrap()
            .set(&cid.to_bytes(), &block_bytes)
            .await
            .unwrap();
        {
            let systemstore = txn.systemstore().unwrap();
            let short_id = db.next_doc_short_id().await.unwrap();
            db::doc_id_map::set_doc_id_mapping(&systemstore, 1, short_id, &doc_id)
                .await
                .unwrap();
            db::doc_id_map::set_block_doc_id_mapping(&systemstore, &cid.to_string(), &doc_id)
                .await
                .unwrap();
        }
        txn.commit().await.unwrap();

        let provider = DbHeadProvider::new(db);
        let heads = provider.get_document_heads(&doc_id).await.unwrap();
        assert_eq!(heads, vec![cid]);
    }

    async fn doc_short_id_for(db: &Arc<DB<MemoryStore>>, doc_id: &str) -> u64 {
        let txn = db.new_txn(true).await.unwrap();
        let systemstore = txn.systemstore().unwrap();
        db::doc_id_map::get_doc_ref(&systemstore, doc_id)
            .await
            .unwrap()
            .expect("doc mapping")
            .doc_short_id
    }

    fn make_transcript(body: &str, idx: i64) -> Document {
        let mut doc = Document::new();
        doc.set("body", body.to_string());
        doc.set("idx", idx);
        doc
    }
}
