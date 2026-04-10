use super::*;

use crate::collection::Collection;
use crate::database::DB;
use crate::index_manager::IndexManager;
use crate::txn::DbTxn;
use datastore::NamespaceView;
use defra_core::encryption::{store_doc_encryption, EncryptionConfig};
use defra_core::signing::SigningConfig;
use std::collections::HashSet;

pub(super) type DocumentCommitResult = (Cid, Vec<u8>, Option<(Cid, Vec<u8>)>);
pub(super) type DeleteCommitResult = (Cid, Vec<u8>);

pub(super) fn map_create_error(error: crate::error::Error) -> query::error::QueryError {
    let msg = error.to_string();
    if msg.contains("can not index a doc's field(s) that violates unique index") {
        query::error::QueryError::execution(
            "can not index a doc's field(s) that violates unique index.".to_string(),
        )
    } else {
        query::error::QueryError::execution(format!("create error: {}", error))
    }
}

pub(super) fn map_update_error(error: crate::error::Error) -> query::error::QueryError {
    match error {
        crate::error::Error::DocumentNotFound(id) => {
            query::error::QueryError::document_not_found(id)
        }
        other => {
            let msg = other.to_string();
            if msg.contains("can not index a doc's field(s) that violates unique index") {
                query::error::QueryError::execution(
                    "can not index a doc's field(s) that violates unique index.".to_string(),
                )
            } else {
                query::error::QueryError::execution(format!("update error: {}", other))
            }
        }
    }
}

pub(super) fn map_delete_error(error: crate::error::Error) -> query::error::QueryError {
    query::error::QueryError::execution(format!("delete error: {}", error))
}

pub(super) async fn commit_txn<S: Store>(
    txn: DbTxn<S>,
    collection_name: &str,
    operation: &str,
) -> query::error::Result<()> {
    if let Err(error) = txn.commit().await {
        warn!(
            collection = %collection_name,
            error = %error,
            "Failed to commit transaction after {}",
            operation
        );
        return Err(query::error::QueryError::execution(format!(
            "commit error: {}",
            error
        )));
    }

    Ok(())
}

pub(super) fn discard_txn<S: Store>(txn: DbTxn<S>, collection_name: &str, operation: &str) {
    if let Err(error) = txn.discard() {
        warn!(
            collection = %collection_name,
            error = %error,
            "Failed to discard transaction after {} error",
            operation
        );
    }
}

pub(super) fn blockstore_for_txn<S: Store>(txn: &DbTxn<S>) -> query::error::Result<NamespaceView> {
    txn.blockstore().map_err(|e| {
        query::error::QueryError::execution(format!("failed to get blockstore: {}", e))
    })
}

pub(super) fn headstore_for_txn<S: Store>(txn: &DbTxn<S>) -> query::error::Result<NamespaceView> {
    txn.headstore()
        .map_err(|e| query::error::QueryError::execution(format!("failed to get headstore: {}", e)))
}

pub(super) async fn write_document_commit_result(
    collection_name: &str,
    operation: &str,
    collection: &Collection,
    blockstore: &NamespaceView,
    headstore: &NamespaceView,
    doc: &Document,
    modified_fields: Option<&HashSet<String>>,
    enc_config: Option<&EncryptionConfig>,
    sign_config: Option<&SigningConfig>,
    should_store_doc_encryption: bool,
) -> Option<DocumentCommitResult> {
    match write_document_blocks(
        blockstore,
        headstore,
        doc,
        collection.version_id(),
        modified_fields,
        enc_config,
        sign_config,
    )
    .await
    {
        Ok(block_result) => {
            if should_store_doc_encryption {
                if let (Some(config), Some(doc_id)) = (enc_config, doc.id()) {
                    store_doc_encryption(&doc_id.to_string(), config.clone());
                }
            }

            let mut col_block_data: Option<(Cid, Vec<u8>)> = None;
            if collection.schema().is_branchable {
                let short_id = collection_short_id(collection.collection_id());
                match write_collection_block(
                    blockstore,
                    headstore,
                    short_id,
                    collection.version_id(),
                    block_result.cid,
                    sign_config,
                )
                .await
                {
                    Ok((col_cid, col_bytes)) => {
                        col_block_data = Some((col_cid, col_bytes));
                    }
                    Err(error) => {
                        warn!(
                            collection = %collection_name,
                            error = %error,
                            "Failed to write collection block for branchable {}",
                            operation
                        );
                    }
                }
            }

            Some((block_result.cid, block_result.block, col_block_data))
        }
        Err(error) => {
            warn!(
                collection = %collection_name,
                error = %error,
                "Failed to write document blocks - commits queries may not work"
            );
            None
        }
    }
}

pub(super) async fn write_delete_commit_result(
    collection_name: &str,
    operation: &str,
    collection: &Collection,
    blockstore: &NamespaceView,
    headstore: &NamespaceView,
    doc_id: &DocID,
    sign_config: Option<&SigningConfig>,
) -> Option<DeleteCommitResult> {
    match write_delete_block(
        blockstore,
        headstore,
        &doc_id.to_string(),
        collection.version_id(),
        sign_config,
    )
    .await
    {
        Ok(block_result) => {
            let composite_cid = block_result.cid;

            if collection.schema().is_branchable {
                if let Err(error) = write_collection_block(
                    blockstore,
                    headstore,
                    collection_short_id(collection.collection_id()),
                    collection.version_id(),
                    composite_cid,
                    sign_config,
                )
                .await
                .map(|_| ())
                {
                    warn!(
                        collection = %collection_name,
                        error = %error,
                        "Failed to write collection block for branchable {}",
                        operation
                    );
                }
            }

            Some((composite_cid, block_result.block))
        }
        Err(error) => {
            warn!(
                collection = %collection_name,
                error = %error,
                "Failed to write delete block - commits queries may not work"
            );
            None
        }
    }
}

pub(super) fn create_result_from_commit(
    doc_id: DocID,
    doc: Document,
    commit_result: Option<DocumentCommitResult>,
) -> CreateResult {
    match commit_result {
        Some((cid, block, col_data)) => {
            let mut result = CreateResult::with_commit(doc_id, doc, cid, block);
            if let Some((col_cid, col_bytes)) = col_data {
                result.broadcast_cid = Some(col_cid);
                result.broadcast_block = Some(col_bytes);
            }
            result
        }
        None => CreateResult::new(doc_id, doc),
    }
}

pub(super) fn update_result_from_commit(
    doc: Document,
    fields_modified: usize,
    commit_result: Option<DocumentCommitResult>,
) -> UpdateResult {
    match commit_result {
        Some((cid, block, col_data)) => {
            let mut result = UpdateResult::with_commit(doc, fields_modified, cid, block);
            if let Some((col_cid, col_bytes)) = col_data {
                result.broadcast_cid = Some(col_cid);
                result.broadcast_block = Some(col_bytes);
            }
            result
        }
        None => UpdateResult::new(doc, fields_modified),
    }
}

pub(super) fn delete_result_from_commit(
    doc_id: DocID,
    existed: bool,
    commit_result: Option<DeleteCommitResult>,
) -> DeleteResult {
    match commit_result {
        Some((cid, block)) => DeleteResult::with_commit(doc_id, existed, cid, block),
        None => DeleteResult::new(doc_id, existed),
    }
}

impl<S: Store + 'static> AutoCommitMutator<S> {
    /// Get collection from DB cache or return a not-found error.
    pub(super) fn get_collection_or_err(
        &self,
        collection_name: &str,
    ) -> query::error::Result<Collection> {
        self.db
            .get_collection(collection_name)
            .map_err(|e| query::error::QueryError::execution(format!("db error: {}", e)))?
            .ok_or_else(|| query::error::QueryError::collection_not_found(collection_name))
    }

    pub(super) async fn new_write_txn(&self) -> query::error::Result<DbTxn<S>> {
        Self::new_write_txn_for_db(&self.db).await
    }

    pub(super) async fn new_write_txn_for_db(db: &DB<S>) -> query::error::Result<DbTxn<S>> {
        db.new_txn(false).await.map_err(|e| {
            query::error::QueryError::execution(format!("failed to create txn: {}", e))
        })
    }

    pub(super) fn datastore_for_collection(
        &self,
        txn: &DbTxn<S>,
        collection_name: &str,
    ) -> query::error::Result<NamespaceView> {
        txn.datastore().map_err(|e| {
            query::error::QueryError::execution(format!(
                "failed to get datastore for collection '{}': {}",
                collection_name, e
            ))
        })
    }

    pub(super) fn index_manager_for_collection(
        &self,
        collection: &Collection,
        collection_name: &str,
    ) -> query::error::Result<IndexManager> {
        let short_id = collection_short_id(collection.collection_id());
        IndexManager::from_collection(short_id, collection.schema()).map_err(|e| {
            query::error::QueryError::execution(format!(
                "failed to create index manager for collection '{}': {}",
                collection_name, e
            ))
        })
    }

    /// Emit update events for subscriptions.
    ///
    /// For branchable collections, emits a second event for the collection-level DAG.
    pub(super) fn emit_update_events(&self, collection: &Collection, doc_id_str: &str, cid: Cid) {
        if let Some(bus) = self.db.event_bus() {
            let update = Update::new(
                doc_id_str.to_string(),
                cid,
                collection.collection_id().to_string(),
                vec![],
                false, // is_retry
                false, // is_relay (local mutation)
            );
            bus.publish(Message::update(update));

            if collection.schema().is_branchable {
                let col_update = Update::new(
                    String::new(), // empty doc_id → keyed by collection_id
                    cid,
                    collection.collection_id().to_string(),
                    vec![],
                    false,
                    false,
                );
                bus.publish(Message::update(col_update));
            }
        }
    }
}
