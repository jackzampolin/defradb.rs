//! Schema and migration mutations inside a transaction.

use super::*;

impl<S: Store + 'static> DbTransactionRegistry<S> {
    /// Set a migration within an existing transaction.
    ///
    /// This registers a lens migration configuration within the specified transaction.
    /// The migration will only be visible after the transaction is committed.
    ///
    /// # Arguments
    ///
    /// * `txn_id` - The transaction ID from `begin_txn`
    /// * `config` - The lens configuration
    ///
    /// # Returns
    ///
    /// The transform ID that was registered.
    pub async fn set_migration_in_txn(
        &self,
        txn_id: &str,
        config: lens::LensConfig,
        identity: Option<&identity::Did>,
    ) -> Result<lens::TransformId> {
        self.db
            .check_node_access(identity, acp::nac::NodePermission::MigrationSet)
            .await?;
        let ctx = self
            .get_ctx(txn_id)?
            .ok_or_else(|| Error::TransactionNotFound(txn_id.to_string()))?;
        let action_lock = ctx.action_lock();
        let _action_guard = action_lock.lock().await;

        // Get the shared transaction from the fetcher
        let shared_txn = ctx.fetcher_shared_txn();
        let mut txn_guard = shared_txn.lock().await;
        let txn = txn_guard.as_mut().ok_or(Error::TxnNotActive)?;
        let txn_lens_store = ctx.lens_store();

        let outcome = self
            .db
            .set_migration_in_txn_with_store(txn, txn_lens_store.clone(), config.clone())
            .await?;
        let transform_id = outcome.transform_id.clone();
        let updated_destination = outcome.updated_destination.clone();
        ctx.invalidate_migration_cache().await;

        let db = self.db.clone();
        let transform_id_for_commit = transform_id.clone();
        let destination_version_id = updated_destination.version_id.clone();
        txn.on_success_async(Box::new(move || {
            let db = db.clone();
            let config = config.clone();
            let transform_id = transform_id_for_commit.clone();
            let updated_destination = updated_destination.clone();
            let destination_version_id = destination_version_id.clone();
            Box::pin(async move {
                db.bump_migration_generation();
                if let Err(error) = db
                    .lens_store()
                    .add_with_id(transform_id.clone(), config)
                    .await
                {
                    tracing::warn!(
                        transform_id = %transform_id,
                        error = %error,
                        "failed to promote committed transaction migration lens"
                    );
                }

                if !updated_destination.name.is_empty() {
                    if let Ok(mut cache) = db.collections.write() {
                        if let Some(cached) = cache.get(&updated_destination.name) {
                            if cached.schema().version_id == destination_version_id {
                                cache.insert(
                                    updated_destination.name.clone(),
                                    Collection::new(updated_destination.clone()),
                                );
                            }
                        }
                    }

                    if let Err(error) = db
                        .maybe_reindex_after_migration(
                            &updated_destination.name,
                            &updated_destination.version_id,
                        )
                        .await
                    {
                        tracing::warn!(
                            collection = %updated_destination.name,
                            version_id = %updated_destination.version_id,
                            error = %error,
                            "failed to reindex committed transaction migration"
                        );
                    }
                }
            })
        }))?;

        Ok(transform_id)
    }

    /// Add a schema within an existing transaction.
    ///
    /// Parses the SDL and creates collections within the transaction.
    /// The collections are only visible after the transaction is committed,
    /// but can be used by queries within the same transaction.
    pub async fn add_schema_in_txn(
        &self,
        txn_id: &str,
        sdl: &str,
    ) -> Result<Vec<schema::CollectionVersion>> {
        self.add_schema_in_txn_with_acp(txn_id, sdl, None, None)
            .await
    }

    /// Add a schema within an existing transaction, optionally registering
    /// branchable collection ACP objects before the storage commit.
    pub async fn add_schema_in_txn_with_acp(
        &self,
        txn_id: &str,
        sdl: &str,
        document_acp: Option<Arc<dyn acp::DocumentACP>>,
        creator: Option<identity::Did>,
    ) -> Result<Vec<schema::CollectionVersion>> {
        let ctx = self
            .get_ctx(txn_id)?
            .ok_or_else(|| Error::TransactionNotFound(txn_id.to_string()))?;
        self.db
            .check_node_access(None, acp::nac::NodePermission::CollectionPatch)
            .await?;
        let action_lock = ctx.action_lock();
        let _action_guard = action_lock.lock().await;

        let shared_txn = ctx.fetcher_shared_txn();
        let mut txn_guard = shared_txn.lock().await;
        let txn = txn_guard.as_mut().ok_or(Error::TxnNotActive)?;

        let known_types: std::collections::HashSet<String> = self
            .db
            .list_collections()
            .map_err(|e| Error::Other(format!("failed to list collections: {}", e)))?
            .into_iter()
            .collect();

        let collections = query::parse_sdl_with_known_types(sdl, known_types)
            .map_err(|e| Error::Other(format!("failed to parse SDL: {}", e)))?;

        schema::definition_validation::validate_new_collections(&collections)
            .map_err(|e| Error::Other(format!("failed to validate schema: {}", e)))?;
        let existing = self.db.get_all_collection_versions().await?;
        schema::definition_validation::validate_new_collections_with_existing(
            &collections,
            &existing,
        )
        .map_err(|e| Error::Other(format!("failed to validate schema: {}", e)))?;

        let mut finalized = Vec::new();
        for collection in collections {
            let schema = self.db.create_collection_with_txn(txn, collection).await?;
            finalized.push(schema);
        }

        if let (Some(document_acp), Some(creator)) = (document_acp, creator) {
            txn.stage_collection_acp_registration(document_acp, creator, finalized.clone());
        }

        // Register on_success callback to update the process-wide cache after commit
        let db = self.db.clone();
        let schemas_for_cache = finalized.clone();
        txn.on_success(Box::new(move || {
            for schema in &schemas_for_cache {
                let _ = db.unforbid_collection_id(&schema.collection_id);
            }
            if let Ok(mut cache) = db.collections.write() {
                for schema in &schemas_for_cache {
                    cache.insert(schema.name.clone(), Collection::new(schema.clone()));
                }
            }
        }))?;

        Ok(finalized)
    }
}
