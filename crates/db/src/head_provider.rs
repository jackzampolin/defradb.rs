//! Document head provider implementation for P2P DocSync.
//!
//! This module provides a database-backed implementation of the `DocumentHeadProvider`
//! trait, allowing the P2P layer to query document heads for DocSync responses.

use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use cid::Cid;
use defra_core::{Block, CrdtDelta};
use document::DocID;
use storage::corekv::{IterOptions, Store};
use storage::keys::headstore::{HeadstoreColKey, HeadstoreDocKey, HeadstorePriorityKey};

use p2p::sync::DocumentHeadProvider;

use crate::collection::collection_short_id;
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
    async fn get_document_heads(&self, doc_id: &str) -> Result<Vec<Cid>, String> {
        let binary_doc_id = DocID::from_string(doc_id)
            .ok()
            .map(|parsed| parsed.to_bytes());

        // Create a read-only transaction to access the headstore
        let txn = self
            .db
            .new_txn(true)
            .await
            .map_err(|e| format!("failed to create transaction: {}", e))?;

        let headstore = txn
            .headstore()
            .map_err(|e| format!("failed to get headstore: {}", e))?;

        // Query composite heads with prefix /d/{doc_id}/C/
        let prefix = HeadstoreDocKey::field_prefix(doc_id, "C");
        let opts = IterOptions::new().with_prefix(prefix);

        let mut iter = headstore
            .iterator(opts)
            .await
            .map_err(|e| format!("failed to iterate headstore: {}", e))?;

        let mut cids = Vec::new();

        while let Some(pair) = iter
            .next()
            .await
            .map_err(|e| format!("headstore iteration error: {}", e))?
        {
            // Parse CID from key: /d/{doc_id}/C/{cid}
            let key_str = String::from_utf8_lossy(&pair.key);
            let parts: Vec<&str> = key_str.split('/').collect();
            if parts.len() < 5 {
                continue;
            }

            if let Ok(cid) = Cid::from_str(parts[4]) {
                cids.push(cid);
            }
        }

        iter.close()
            .await
            .map_err(|e| format!("headstore close error: {}", e))?;

        if cids.is_empty() {
            let blockstore = txn
                .blockstore()
                .map_err(|e| format!("failed to get blockstore: {}", e))?;
            let mut priority_iter = headstore
                .iterator(
                    IterOptions::new().with_prefix(HeadstorePriorityKey::document_prefix(doc_id)),
                )
                .await
                .map_err(|e| format!("failed to iterate priority index: {}", e))?;

            let cid_offset = HeadstorePriorityKey::cid_offset(doc_id);
            let mut max_priority: Option<u64> = None;

            while let Some(pair) = priority_iter
                .next()
                .await
                .map_err(|e| format!("priority index iteration error: {}", e))?
            {
                let cid_bytes = match pair.key.get(cid_offset..) {
                    Some(bytes) => bytes,
                    None => continue,
                };
                let Ok(cid) = Cid::try_from(cid_bytes) else {
                    continue;
                };

                let Some(block_bytes) = blockstore.get(&cid.to_bytes()).await.map_err(|e| {
                    format!("failed to read blockstore for priority index CID: {}", e)
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

            priority_iter
                .close()
                .await
                .map_err(|e| format!("priority index close error: {}", e))?;
        }

        if cids.is_empty() {
            let blockstore = txn
                .blockstore()
                .map_err(|e| format!("failed to get blockstore: {}", e))?;
            let mut block_iter = blockstore
                .iterator(IterOptions::new())
                .await
                .map_err(|e| format!("failed to iterate blockstore: {}", e))?;

            let mut max_priority: Option<u64> = None;
            while let Some(pair) = block_iter
                .next()
                .await
                .map_err(|e| format!("blockstore iteration error: {}", e))?
            {
                let Ok(block) = Block::from_dag_cbor(&pair.value) else {
                    continue;
                };
                if !matches!(block.delta, CrdtDelta::Composite(_)) {
                    continue;
                }
                let matches_string_doc_id = block.delta.doc_id() == Some(doc_id.as_bytes());
                let matches_binary_doc_id = binary_doc_id
                    .as_ref()
                    .is_some_and(|encoded| block.delta.doc_id() == Some(encoded.as_slice()));
                if !matches_string_doc_id && !matches_binary_doc_id {
                    continue;
                }

                let Ok(cid) = Cid::try_from(pair.key.as_slice()) else {
                    continue;
                };
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

            block_iter
                .close()
                .await
                .map_err(|e| format!("blockstore close error: {}", e))?;
        }

        Ok(cids)
    }

    async fn get_collection_heads(&self, collection_id: &str) -> Result<Vec<Cid>, String> {
        let txn = self
            .db
            .new_txn(true)
            .await
            .map_err(|e| format!("failed to create transaction: {}", e))?;

        let headstore = txn
            .headstore()
            .map_err(|e| format!("failed to get headstore: {}", e))?;

        // Derive short ID from collection_id string and query prefix /c/{short_id}/
        let short_id = collection_short_id(collection_id);
        let prefix = HeadstoreColKey::collection_prefix(short_id);
        let opts = IterOptions::new().with_prefix(prefix);

        let mut iter = headstore
            .iterator(opts)
            .await
            .map_err(|e| format!("failed to iterate headstore: {}", e))?;

        let mut cids = Vec::new();

        while let Some(pair) = iter
            .next()
            .await
            .map_err(|e| format!("headstore iteration error: {}", e))?
        {
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

        iter.close()
            .await
            .map_err(|e| format!("headstore close error: {}", e))?;

        Ok(cids)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::AutoCommitMutator;
    use crate::DB;
    use defra_core::{block::generate_cid_from_bytes, Block, CompositeDeltaPayload, CrdtDelta};
    use document::{DocID, Document};
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

        let txn = db.new_txn(false).await.unwrap();
        txn.headstore()
            .unwrap()
            .delete(&HeadstoreDocKey::new(&doc_id, "C", commit_cid).bytes())
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

        let txn = db.new_txn(false).await.unwrap();
        txn.headstore()
            .unwrap()
            .delete(&HeadstoreDocKey::new(&doc_id, "C", commit_cid).bytes())
            .await
            .unwrap();
        txn.headstore()
            .unwrap()
            .delete(&HeadstorePriorityKey::new(&doc_id, priority, commit_cid).bytes())
            .await
            .unwrap();
        txn.commit().await.unwrap();

        let provider = DbHeadProvider::new(db);
        let heads = provider.get_document_heads(&doc_id).await.unwrap();
        assert_eq!(heads, vec![commit_cid]);
    }

    #[tokio::test]
    async fn falls_back_to_blockstore_scan_for_binary_doc_id_encoding() {
        let store = Arc::new(MemoryStore::new());
        let db = Arc::new(DB::from_arc(store).unwrap());

        let mut doc = make_transcript("first", 1);
        doc.generate_and_set_doc_id().unwrap();
        let doc_id = doc.id().unwrap().to_string();

        let block = Block::new_with_options(
            CrdtDelta::Composite(CompositeDeltaPayload {
                doc_id: DocID::from_string(&doc_id).unwrap().to_bytes(),
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

        let txn = db.new_txn(false).await.unwrap();
        txn.blockstore()
            .unwrap()
            .set(&cid.to_bytes(), &block_bytes)
            .await
            .unwrap();
        txn.commit().await.unwrap();

        let provider = DbHeadProvider::new(db);
        let heads = provider.get_document_heads(&doc_id).await.unwrap();
        assert_eq!(heads, vec![cid]);
    }

    fn make_transcript(body: &str, idx: i64) -> Document {
        let mut doc = Document::new();
        doc.set("body", body.to_string());
        doc.set("idx", idx);
        doc
    }
}
