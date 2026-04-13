use async_lock::Mutex as TokioMutex;
use async_trait::async_trait;
use cid::Cid;
use document::{DocID, Document};
use events::{Message, Update};
use query::mutator::{
    CreateResult, DeleteResult, DocMutator, MutationBatchController, UpdateResult,
};
use std::collections::HashSet;
use std::sync::Arc;
use storage::corekv::Store;
use tracing::warn;

use crate::block_builder::{write_collection_block, write_delete_block, write_document_blocks};
use crate::collection_loader::{get_collection_with_index_manager, get_collection_with_lazy_load};
use crate::database::DB;
use crate::txn::DbTxn;
use defra_core::encryption::{get_doc_encryption, get_encryption_config, store_doc_encryption};
use defra_core::signing::get_signing_config;

struct PendingUpdateEvent {
    collection_id: String,
    branchable: bool,
    doc_id: String,
    cid: Cid,
}

pub struct BatchMutator<S: Store> {
    db: Arc<DB<S>>,
    txn: Arc<TokioMutex<Option<DbTxn<S>>>>,
    pending_events: TokioMutex<Vec<PendingUpdateEvent>>,
}

impl<S: Store> BatchMutator<S> {
    pub fn new(db: Arc<DB<S>>, txn: Arc<TokioMutex<Option<DbTxn<S>>>>) -> Self {
        Self {
            db,
            txn,
            pending_events: TokioMutex::new(Vec::new()),
        }
    }

    async fn block_and_head_stores(
        &self,
    ) -> query::error::Result<(datastore::NamespaceView, datastore::NamespaceView)> {
        let txn = self.txn.lock().await;
        let txn = txn.as_ref().ok_or_else(|| {
            query::error::QueryError::execution("mutation batch transaction is no longer active")
        })?;
        let blockstore = txn.blockstore().map_err(|e| {
            query::error::QueryError::execution(format!("failed to get blockstore: {}", e))
        })?;
        let headstore = txn.headstore().map_err(|e| {
            query::error::QueryError::execution(format!("failed to get headstore: {}", e))
        })?;
        Ok((blockstore, headstore))
    }

    async fn take_txn(&self) -> query::error::Result<DbTxn<S>> {
        self.txn.lock().await.take().ok_or_else(|| {
            query::error::QueryError::execution("mutation batch transaction is no longer active")
        })
    }

    async fn queue_update_event(
        &self,
        collection_id: String,
        branchable: bool,
        doc_id: String,
        cid: Cid,
    ) {
        self.pending_events.lock().await.push(PendingUpdateEvent {
            collection_id,
            branchable,
            doc_id,
            cid,
        });
    }

    fn emit_update_event(&self, event: PendingUpdateEvent) {
        if let Some(bus) = self.db.event_bus() {
            let update = Update::new(
                event.doc_id,
                event.cid,
                event.collection_id.clone(),
                vec![],
                false,
                false,
            );
            bus.publish(Message::update(update));

            if event.branchable {
                let collection_update = Update::new(
                    String::new(),
                    event.cid,
                    event.collection_id,
                    vec![],
                    false,
                    false,
                );
                bus.publish(Message::update(collection_update));
            }
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S: Store + 'static> MutationBatchController for BatchMutator<S> {
    async fn commit(&self) -> query::error::Result<()> {
        let txn = self.take_txn().await?;

        if let Err(e) = txn.commit().await {
            self.pending_events.lock().await.clear();
            return Err(query::error::QueryError::execution(format!(
                "commit error: {}",
                e
            )));
        }

        let pending_events = std::mem::take(&mut *self.pending_events.lock().await);
        for event in pending_events {
            self.emit_update_event(event);
        }

        Ok(())
    }

    async fn rollback(&self) -> query::error::Result<()> {
        let txn = self.take_txn().await?;
        self.pending_events.lock().await.clear();

        txn.discard()
            .map_err(|e| query::error::QueryError::execution(format!("discard error: {}", e)))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S: Store + 'static> DocMutator for BatchMutator<S> {
    #[allow(clippy::type_complexity)]
    async fn create(
        &self,
        collection_name: &str,
        mut doc: Document,
    ) -> query::error::Result<CreateResult> {
        let (collection, datastore, index_manager) =
            get_collection_with_index_manager(&self.txn, collection_name).await?;
        let embedding_config = self.db.options().embedding_config();

        crate::embedding::set_embedding(
            &collection.schema().vector_embeddings,
            &mut doc,
            true,
            None,
            &embedding_config,
        )
        .await
        .map_err(|e| query::error::QueryError::execution(format!("embedding error: {}", e)))?;

        let id_was_generated = doc.id().is_none();
        if id_was_generated {
            doc.generate_and_set_doc_id().map_err(|e| {
                query::error::QueryError::execution(format!("failed to generate DocID: {}", e))
            })?;
        }

        let doc_id = doc.id().cloned().ok_or_else(|| {
            query::error::QueryError::execution("document should have ID after generation")
        })?;

        self.db
            .validate_downsample_write(&datastore, collection.schema(), &doc, None)
            .await
            .map_err(|e| query::error::QueryError::execution(e.to_string()))?;

        collection
            .create_with_indexes(&datastore, &doc, &index_manager, id_was_generated)
            .await
            .map_err(|e| {
                let msg = e.to_string();
                if msg.contains("can not index a doc's field(s) that violates unique index") {
                    query::error::QueryError::execution(
                        "can not index a doc's field(s) that violates unique index.".to_string(),
                    )
                } else {
                    query::error::QueryError::execution(format!("create error: {}", e))
                }
            })?;

        let short_id = collection.resolved_root_id();
        let schema_version_id = collection.version_id();
        let enc_config = get_encryption_config();
        let sign_config = get_signing_config();

        let commit_result: Option<(Cid, Vec<u8>, Option<(Cid, Vec<u8>)>)> = {
            let (blockstore, headstore) = self.block_and_head_stores().await?;

            match write_document_blocks(
                &blockstore,
                &headstore,
                &doc,
                schema_version_id,
                None,
                enc_config.as_ref(),
                sign_config.as_ref(),
            )
            .await
            {
                Ok(block_result) => {
                    if let Some(ref config) = enc_config {
                        store_doc_encryption(&doc_id.to_string(), config.clone());
                    }

                    let mut col_block_data: Option<(Cid, Vec<u8>)> = None;
                    if collection.schema().is_branchable {
                        match write_collection_block(
                            &blockstore,
                            &headstore,
                            short_id,
                            schema_version_id,
                            block_result.cid,
                            sign_config.as_ref(),
                        )
                        .await
                        {
                            Ok((col_cid, col_bytes)) => {
                                col_block_data = Some((col_cid, col_bytes));
                            }
                            Err(e) => {
                                warn!(
                                    collection = %collection_name,
                                    error = %e,
                                    "Failed to write collection block for branchable create"
                                );
                            }
                        }
                    }

                    Some((block_result.cid, block_result.block, col_block_data))
                }
                Err(e) => {
                    warn!(
                        collection = %collection_name,
                        error = %e,
                        "Failed to write document blocks - commits queries may not work"
                    );
                    None
                }
            }
        };

        let cid = commit_result
            .as_ref()
            .map(|(cid, _, _)| *cid)
            .unwrap_or_default();
        self.queue_update_event(
            collection.collection_id().to_string(),
            collection.schema().is_branchable,
            doc_id.to_string(),
            cid,
        )
        .await;

        match commit_result {
            Some((cid, block, col_data)) => {
                let mut result = CreateResult::with_commit(doc_id, doc, cid, block);
                if let Some((col_cid, col_bytes)) = col_data {
                    result.broadcast_cid = Some(col_cid);
                    result.broadcast_block = Some(col_bytes);
                }
                Ok(result)
            }
            None => Ok(CreateResult::new(doc_id, doc)),
        }
    }

    async fn update(
        &self,
        collection_name: &str,
        mut doc: Document,
        mut modified_fields: HashSet<String>,
    ) -> query::error::Result<UpdateResult> {
        let (collection, datastore, index_manager) =
            get_collection_with_index_manager(&self.txn, collection_name).await?;
        let embedding_config = self.db.options().embedding_config();

        let generated = crate::embedding::set_embedding(
            &collection.schema().vector_embeddings,
            &mut doc,
            false,
            Some(&modified_fields),
            &embedding_config,
        )
        .await
        .map_err(|e| query::error::QueryError::execution(format!("embedding error: {}", e)))?;

        for field in generated {
            modified_fields.insert(field);
        }

        self.db
            .validate_downsample_write(
                &datastore,
                collection.schema(),
                &doc,
                Some(&modified_fields),
            )
            .await
            .map_err(|e| query::error::QueryError::execution(e.to_string()))?;

        collection
            .update_with_indexes(&datastore, &doc, &index_manager)
            .await
            .map_err(|e| match e {
                crate::error::Error::DocumentNotFound(id) => {
                    query::error::QueryError::document_not_found(id)
                }
                other => {
                    let msg = other.to_string();
                    if msg.contains("can not index a doc's field(s) that violates unique index") {
                        query::error::QueryError::execution(
                            "can not index a doc's field(s) that violates unique index."
                                .to_string(),
                        )
                    } else {
                        query::error::QueryError::execution(format!("update error: {}", other))
                    }
                }
            })?;

        let short_id = collection.resolved_root_id();
        let schema_version_id = collection.version_id();
        let enc_config = get_encryption_config()
            .or_else(|| doc.id().and_then(|id| get_doc_encryption(&id.to_string())));
        let sign_config = get_signing_config();

        let commit_result: Option<(Cid, Vec<u8>, Option<(Cid, Vec<u8>)>)> = {
            let (blockstore, headstore) = self.block_and_head_stores().await?;

            match write_document_blocks(
                &blockstore,
                &headstore,
                &doc,
                schema_version_id,
                Some(&modified_fields),
                enc_config.as_ref(),
                sign_config.as_ref(),
            )
            .await
            {
                Ok(block_result) => {
                    let mut col_block_data: Option<(Cid, Vec<u8>)> = None;
                    if collection.schema().is_branchable {
                        match write_collection_block(
                            &blockstore,
                            &headstore,
                            short_id,
                            schema_version_id,
                            block_result.cid,
                            sign_config.as_ref(),
                        )
                        .await
                        {
                            Ok((col_cid, col_bytes)) => {
                                col_block_data = Some((col_cid, col_bytes));
                            }
                            Err(e) => {
                                warn!(
                                    collection = %collection_name,
                                    error = %e,
                                    "Failed to write collection block for branchable update"
                                );
                            }
                        }
                    }

                    Some((block_result.cid, block_result.block, col_block_data))
                }
                Err(e) => {
                    warn!(
                        collection = %collection_name,
                        error = %e,
                        "Failed to write document blocks - commits queries may not work"
                    );
                    None
                }
            }
        };

        if let Some(doc_id) = doc.id() {
            let cid = commit_result
                .as_ref()
                .map(|(cid, _, _)| *cid)
                .unwrap_or_default();
            self.queue_update_event(
                collection.collection_id().to_string(),
                collection.schema().is_branchable,
                doc_id.to_string(),
                cid,
            )
            .await;
        }

        let fields_modified = doc.values().len();
        match commit_result {
            Some((cid, block, col_data)) => {
                let mut result = UpdateResult::with_commit(doc, fields_modified, cid, block);
                if let Some((col_cid, col_bytes)) = col_data {
                    result.broadcast_cid = Some(col_cid);
                    result.broadcast_block = Some(col_bytes);
                }
                Ok(result)
            }
            None => Ok(UpdateResult::new(doc, fields_modified)),
        }
    }

    async fn delete(
        &self,
        collection_name: &str,
        doc_id: &DocID,
    ) -> query::error::Result<DeleteResult> {
        let (collection, datastore, index_manager) =
            get_collection_with_index_manager(&self.txn, collection_name).await?;

        let existed = collection
            .delete_with_indexes(&datastore, doc_id, &index_manager)
            .await
            .map_err(|e| query::error::QueryError::execution(format!("delete error: {}", e)))?;

        let short_id = collection.resolved_root_id();
        let schema_version_id = collection.version_id();
        let sign_config = get_signing_config();

        let commit_result: Option<(Cid, Vec<u8>)> = {
            let (blockstore, headstore) = self.block_and_head_stores().await?;

            match write_delete_block(
                &blockstore,
                &headstore,
                &doc_id.to_string(),
                schema_version_id,
                sign_config.as_ref(),
            )
            .await
            {
                Ok(block_result) => {
                    let composite_cid = block_result.cid;

                    if collection.schema().is_branchable {
                        if let Err(e) = write_collection_block(
                            &blockstore,
                            &headstore,
                            short_id,
                            schema_version_id,
                            composite_cid,
                            sign_config.as_ref(),
                        )
                        .await
                        .map(|_| ())
                        {
                            warn!(
                                collection = %collection_name,
                                error = %e,
                                "Failed to write collection block for branchable delete"
                            );
                        }
                    }

                    Some((composite_cid, block_result.block))
                }
                Err(e) => {
                    warn!(
                        collection = %collection_name,
                        error = %e,
                        "Failed to write delete block - commits queries may not work"
                    );
                    None
                }
            }
        };

        let cid = commit_result
            .as_ref()
            .map(|(cid, _)| *cid)
            .unwrap_or_default();
        self.queue_update_event(
            collection.collection_id().to_string(),
            collection.schema().is_branchable,
            doc_id.to_string(),
            cid,
        )
        .await;

        match commit_result {
            Some((cid, block)) => Ok(DeleteResult::with_commit(
                doc_id.clone(),
                existed,
                cid,
                block,
            )),
            None => Ok(DeleteResult::new(doc_id.clone(), existed)),
        }
    }

    async fn exists(&self, collection_name: &str, doc_id: &DocID) -> query::error::Result<bool> {
        let (collection, datastore) =
            get_collection_with_lazy_load(&self.txn, collection_name).await?;

        collection
            .exists_with_datastore(&datastore, doc_id)
            .await
            .map_err(|e| query::error::QueryError::execution(format!("exists error: {}", e)))
    }

    async fn get_for_update(
        &self,
        collection_name: &str,
        doc_id: &DocID,
    ) -> query::error::Result<Option<Document>> {
        let (collection, datastore) =
            get_collection_with_lazy_load(&self.txn, collection_name).await?;

        collection
            .get_with_datastore(&datastore, doc_id)
            .await
            .map_err(|e| {
                query::error::QueryError::execution(format!("get_for_update error: {}", e))
            })
    }
}
