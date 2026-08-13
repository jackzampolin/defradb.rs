use std::collections::HashSet;
use std::sync::Arc;

use blockstore::{Blockstore, DefraBlockstore};
use cid::Cid;
use defra_core::browser_sync::{BrowserSyncBlock, BrowserSyncDocument, MAX_SYNC_PAYLOAD_BYTES};
use defra_core::merge::{BlockMetadata, MergeHandler, MergeOutcome};
use storage::corekv::{IterOptions, Store};

use crate::merge_handler::DbMergeHandler;
use crate::push_docs_common::{load_latest_composite_heads, load_push_dag_blocks};

mod validation;

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BrowserSyncError {
    #[error("invalid sync document: {0}")]
    Invalid(String),
    #[error("sync document is too large: {0}")]
    TooLarge(String),
    #[error("sync storage failed: {0}")]
    Storage(String),
    #[error("sync merge failed: {0}")]
    Merge(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrowserSyncDocumentRef {
    pub doc_id: String,
    pub collection_id: String,
    doc_short_id: u64,
}

pub struct ValidatedBrowserSyncDocument {
    doc_id: String,
    collection_id: String,
    roots: Vec<Cid>,
    blocks: Vec<(Cid, Vec<u8>)>,
    verified_genesis_creator: Option<String>,
}

impl ValidatedBrowserSyncDocument {
    pub fn doc_id(&self) -> &str {
        &self.doc_id
    }

    pub fn collection_id(&self) -> &str {
        &self.collection_id
    }

    /// The DID cryptographically verified from the genesis block's signature,
    /// if the genesis block is signed. This is the only identity that may be
    /// registered as the document's ACP owner.
    pub fn verified_genesis_creator(&self) -> Option<&str> {
        self.verified_genesis_creator.as_deref()
    }
}

pub struct BrowserSyncEngine<S: Store + 'static> {
    db: Arc<db::DB<S>>,
    blockstore: Arc<DefraBlockstore<S>>,
    merge_handler: Arc<DbMergeHandler<S, DefraBlockstore<S>>>,
}

impl<S: Store + 'static> BrowserSyncEngine<S> {
    pub fn new(db: Arc<db::DB<S>>) -> Self {
        let blockstore = Arc::new(DefraBlockstore::new(db.store().clone(), true));
        let merge_handler = Arc::new(DbMergeHandler::new(db.clone(), blockstore.clone()));
        if let Some(kms) = db.kms() {
            merge_handler.set_kms(kms);
        }
        Self {
            db,
            blockstore,
            merge_handler,
        }
    }

    pub fn database(&self) -> &Arc<db::DB<S>> {
        &self.db
    }

    pub fn merge_handler(&self) -> &Arc<DbMergeHandler<S, DefraBlockstore<S>>> {
        &self.merge_handler
    }

    pub async fn document_refs(&self) -> Result<Vec<BrowserSyncDocumentRef>, BrowserSyncError> {
        let txn = self
            .db
            .new_txn(true)
            .await
            .map_err(|error| BrowserSyncError::Storage(error.to_string()))?;
        let datastore = txn
            .datastore()
            .map_err(|error| BrowserSyncError::Storage(error.to_string()))?;
        let systemstore = txn
            .systemstore()
            .map_err(|error| BrowserSyncError::Storage(error.to_string()))?;
        let mut refs = Vec::new();

        for name in self
            .db
            .list_collections()
            .map_err(|error| BrowserSyncError::Storage(error.to_string()))?
        {
            let Some(collection) = self
                .db
                .get_collection(&name)
                .map_err(|error| BrowserSyncError::Storage(error.to_string()))?
            else {
                continue;
            };
            let prefix = format!("/d/{}/", collection.collection_id()).into_bytes();
            let prefix_len = prefix.len();
            let mut iterator = datastore
                .iterator(IterOptions::new().with_prefix(prefix).with_keys_only(true))
                .await
                .map_err(|error| BrowserSyncError::Storage(error.to_string()))?;

            while let Some(pair) = iterator
                .next()
                .await
                .map_err(|error| BrowserSyncError::Storage(error.to_string()))?
            {
                let Ok(doc_short_id) =
                    storage::keys::doc_id_index::decode_doc_short_id(&pair.key[prefix_len..])
                else {
                    continue;
                };
                let Some(doc_id) = db::doc_id_map::get_doc_id(&systemstore, doc_short_id)
                    .await
                    .map_err(|error| BrowserSyncError::Storage(error.to_string()))?
                else {
                    continue;
                };
                refs.push(BrowserSyncDocumentRef {
                    doc_id,
                    collection_id: collection.collection_id().to_string(),
                    doc_short_id,
                });
            }
            iterator
                .close()
                .await
                .map_err(|error| BrowserSyncError::Storage(error.to_string()))?;
        }

        refs.sort_by(|left, right| left.doc_id.cmp(&right.doc_id));
        refs.dedup_by(|left, right| left.doc_id == right.doc_id);
        Ok(refs)
    }

    pub async fn document_ref(
        &self,
        doc_id: &str,
    ) -> Result<Option<BrowserSyncDocumentRef>, BrowserSyncError> {
        let txn = self
            .db
            .new_txn(true)
            .await
            .map_err(|error| BrowserSyncError::Storage(error.to_string()))?;
        let systemstore = txn
            .systemstore()
            .map_err(|error| BrowserSyncError::Storage(error.to_string()))?;
        let Some(doc_ref) = db::doc_id_map::get_doc_ref(&systemstore, doc_id)
            .await
            .map_err(|error| BrowserSyncError::Storage(error.to_string()))?
        else {
            return Ok(None);
        };
        let canonical_doc_id = db::doc_id_map::get_doc_id(&systemstore, doc_ref.doc_short_id)
            .await
            .map_err(|error| BrowserSyncError::Storage(error.to_string()))?
            .ok_or_else(|| {
                BrowserSyncError::Storage(format!("document {doc_id} has no canonical ID mapping"))
            })?;

        for name in self
            .db
            .list_collections()
            .map_err(|error| BrowserSyncError::Storage(error.to_string()))?
        {
            let Some(collection) = self
                .db
                .get_collection(&name)
                .map_err(|error| BrowserSyncError::Storage(error.to_string()))?
            else {
                continue;
            };
            if collection.resolved_root_id() == doc_ref.collection_short_id {
                return Ok(Some(BrowserSyncDocumentRef {
                    doc_id: canonical_doc_id,
                    collection_id: collection.collection_id().to_string(),
                    doc_short_id: doc_ref.doc_short_id,
                }));
            }
        }

        Ok(None)
    }

    pub async fn load_document(
        &self,
        document_ref: &BrowserSyncDocumentRef,
    ) -> Result<Option<BrowserSyncDocument>, BrowserSyncError> {
        let txn = self
            .db
            .new_txn(true)
            .await
            .map_err(|error| BrowserSyncError::Storage(error.to_string()))?;
        let headstore = txn
            .headstore()
            .map_err(|error| BrowserSyncError::Storage(error.to_string()))?;
        let blockstore = txn
            .blockstore()
            .map_err(|error| BrowserSyncError::Storage(error.to_string()))?;
        let encstore = txn
            .encstore()
            .map_err(|error| BrowserSyncError::Storage(error.to_string()))?;
        let heads =
            load_latest_composite_heads(&headstore, &blockstore, document_ref.doc_short_id).await;
        if heads.is_empty() {
            return Ok(None);
        }

        let mut seen = HashSet::new();
        let mut blocks = Vec::new();
        let mut total_bytes = 0usize;
        for (root, root_data) in &heads {
            for (cid, data) in
                load_push_dag_blocks(&blockstore, &encstore, *root, root_data.clone()).await
            {
                if !seen.insert(cid) {
                    continue;
                }
                total_bytes = total_bytes.saturating_add(data.len());
                if total_bytes > MAX_SYNC_PAYLOAD_BYTES {
                    return Err(BrowserSyncError::TooLarge(format!(
                        "document {} exceeds {} bytes",
                        document_ref.doc_id, MAX_SYNC_PAYLOAD_BYTES
                    )));
                }
                blocks.push(BrowserSyncBlock {
                    cid: cid.to_string(),
                    data: hex::encode(data),
                });
            }
        }

        let document = BrowserSyncDocument {
            doc_id: document_ref.doc_id.clone(),
            collection_id: document_ref.collection_id.clone(),
            roots: heads.into_iter().map(|(cid, _)| cid.to_string()).collect(),
            blocks,
        };
        self.validate_document(&document)?;
        Ok(Some(document))
    }

    pub async fn apply_document(
        &self,
        document: &BrowserSyncDocument,
        creator: &str,
    ) -> Result<(), BrowserSyncError> {
        let document = self.validate_document(document)?;
        self.apply_validated_document(document, creator).await
    }

    pub async fn apply_validated_document(
        &self,
        document: ValidatedBrowserSyncDocument,
        creator: &str,
    ) -> Result<(), BrowserSyncError> {
        let block_refs: Vec<_> = document
            .blocks
            .iter()
            .map(|(cid, data)| (cid, data.as_slice()))
            .collect();
        self.blockstore
            .put_many(&block_refs)
            .await
            .map_err(|error| BrowserSyncError::Storage(error.to_string()))?;

        for root in &document.roots {
            let data = document
                .blocks
                .iter()
                .find_map(|(cid, data)| (cid == root).then_some(data.as_slice()))
                .expect("validated roots are present in blocks");
            match self
                .merge_handler
                .handle_block(
                    root,
                    data,
                    BlockMetadata::normal(
                        &document.doc_id,
                        &document.collection_id,
                        creator,
                        None,
                        false,
                    ),
                )
                .await
                .map_err(|error| BrowserSyncError::Merge(error.to_string()))?
            {
                MergeOutcome::Merged | MergeOutcome::Skipped { terminal: true, .. } => {}
                MergeOutcome::Skipped { reason, .. } | MergeOutcome::Rejected { reason } => {
                    return Err(BrowserSyncError::Merge(reason));
                }
                _ => return Err(BrowserSyncError::Merge("unsupported merge outcome".into())),
            }
        }

        let cids: Vec<_> = document.blocks.iter().map(|(cid, _)| *cid).collect();
        self.blockstore
            .mark_batch_as_merged(&cids)
            .await
            .map_err(|error| BrowserSyncError::Storage(error.to_string()))
    }
}

#[cfg(test)]
mod tests;
