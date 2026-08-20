//! Auto-committing document mutator for non-transactional mutations.
//!
//! This mutator wraps a database and automatically creates and commits
//! a write transaction for each mutation operation. This enables mutations
//! without explicit transaction management while still providing proper
//! transactional semantics per operation.

pub mod batch;
mod create;
mod delete;
pub(crate) mod helpers;
mod read;
mod update;

use async_trait::async_trait;
use cid::Cid;
use document::{DocID, Document};
use events::{Message, Update};
use query::mutator::{CreateResult, DeleteResult, DocMutator, MutationBatch, UpdateResult};
use std::sync::{Arc, OnceLock};
use storage::corekv::Store;
use tracing::warn;

use crate::block_builder::{write_collection_block, write_delete_block, write_document_blocks};
use crate::collection::Collection;
use crate::database::DB;
use crate::index_manager::IndexManager;
use crate::read::lensed::fetcher::LensedDocFetcher;

/// Captured per-mutation commit data: the document block (cid + bytes) plus,
/// for branchable collections, the collection block (cid + bytes).
pub(super) type CommitArtifacts = (Cid, Vec<u8>, Option<(Cid, Vec<u8>)>);
use defra_core::encryption::get_encryption_config;
use defra_core::signing::get_signing_config;

pub use batch::BatchMutator;

/// Document mutator that auto-commits transactions for each operation.
///
/// This is useful for mutations that don't need explicit transaction control.
/// Each operation creates a new write transaction, performs the mutation,
/// and commits (or discards on error).
///
/// # Transaction Semantics
///
/// Each mutation is atomic: it either succeeds entirely or fails without
/// partial changes. However, multiple mutations are NOT atomic with respect
/// to each other - if you need multiple operations to be atomic, use
/// `DbDocMutator` with explicit transaction management instead.
pub struct AutoCommitMutator<S: Store> {
    db: Arc<DB<S>>,
    document_acp: OnceLock<Arc<dyn acp::DocumentACP>>,
}

impl<S: Store> AutoCommitMutator<S> {
    /// Create a new auto-committing mutator wrapping the given database.
    pub fn new(db: Arc<DB<S>>) -> Self {
        Self {
            db,
            document_acp: OnceLock::new(),
        }
    }

    pub fn set_document_acp(&self, acp: Arc<dyn acp::DocumentACP>) {
        let _ = self.document_acp.set(acp);
    }

    async fn register_created_doc_with_acp(
        &self,
        collection: &Collection,
        doc_id: &str,
    ) -> query::error::Result<()> {
        let Some(policy) = collection.schema().policy.as_ref() else {
            return Ok(());
        };
        let Some(creator_did) = defra_core::signing::get_broadcast_creator_did() else {
            return Ok(());
        };
        let Some(acp) = self.document_acp.get() else {
            return Ok(());
        };
        let creator = identity::Did::new(&creator_did).map_err(|error| {
            query::error::QueryError::execution(format!("invalid mutation creator DID: {error}"))
        })?;

        if !acp
            .is_doc_registered(&policy.id, &policy.resource_name, doc_id)
            .await
            .map_err(|error| {
                query::error::QueryError::execution(format!(
                    "failed to check ACP registration before publishing update: {error}"
                ))
            })?
        {
            acp.register_doc_object(&creator, &policy.id, &policy.resource_name, doc_id)
                .await
                .map_err(|error| {
                    query::error::QueryError::execution(format!(
                        "failed to register document with ACP before publishing update: {error}"
                    ))
                })?;
        }

        Ok(())
    }

    pub async fn new_batch_components(
        &self,
    ) -> query::error::Result<(Arc<BatchMutator<S>>, Arc<LensedDocFetcher<S>>)> {
        let txn = self.db.new_txn(false).await.map_err(|e| {
            query::error::QueryError::execution(format!("failed to create txn: {}", e))
        })?;
        let fetcher = Arc::new(LensedDocFetcher::new(
            self.db.clone(),
            txn,
            self.db.lens_store.clone(),
            false,
        ));
        let batch = Arc::new(BatchMutator::new(self.db.clone(), fetcher.shared_txn()));
        Ok((batch, fetcher))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S: Store + 'static> DocMutator for AutoCommitMutator<S> {
    fn set_document_acp(&self, acp: Arc<dyn acp::DocumentACP>) {
        AutoCommitMutator::set_document_acp(self, acp);
    }

    async fn begin_batch(&self) -> query::error::Result<Option<MutationBatch>> {
        let (batch, fetcher) = self.new_batch_components().await?;
        let mutator: Arc<dyn DocMutator> = batch.clone();
        let controller: Arc<dyn query::mutator::MutationBatchController> = batch;
        Ok(Some(MutationBatch::new(mutator, fetcher, controller)))
    }

    async fn create(
        &self,
        collection_name: &str,
        doc: Document,
    ) -> query::error::Result<CreateResult> {
        self.create_impl(collection_name, doc).await
    }

    async fn create_many(
        &self,
        collection_name: &str,
        docs: Vec<Document>,
    ) -> query::error::Result<Vec<CreateResult>> {
        self.create_many_impl(collection_name, docs).await
    }

    async fn update(
        &self,
        collection_name: &str,
        doc: Document,
        modified_fields: std::collections::HashSet<String>,
    ) -> query::error::Result<UpdateResult> {
        self.update_impl(collection_name, None, doc, modified_fields)
            .await
    }

    async fn update_if_unchanged(
        &self,
        collection_name: &str,
        expected: Document,
        doc: Document,
        modified_fields: std::collections::HashSet<String>,
    ) -> query::error::Result<UpdateResult> {
        self.update_impl(collection_name, Some(expected), doc, modified_fields)
            .await
    }

    async fn delete(
        &self,
        collection_name: &str,
        doc_id: &DocID,
    ) -> query::error::Result<DeleteResult> {
        self.delete_impl(collection_name, doc_id).await
    }

    async fn exists(&self, collection_name: &str, doc_id: &DocID) -> query::error::Result<bool> {
        self.exists_impl(collection_name, doc_id).await
    }

    async fn get_for_update(
        &self,
        collection_name: &str,
        doc_id: &DocID,
    ) -> query::error::Result<Option<Document>> {
        self.get_for_update_impl(collection_name, doc_id).await
    }
}
