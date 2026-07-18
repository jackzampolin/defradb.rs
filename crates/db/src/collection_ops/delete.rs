use super::*;

const TRUNCATE_CHUNK_SIZE: usize = 1000;

impl<S: Store> crate::database::DB<S> {
    /// Delete a collection within an existing transaction.
    ///
    /// This removes:
    /// 1. The collection schema from `/collection/id/{version_id}`
    /// 2. The name mapping from `/collection/name/{name}`
    /// 3. The version index entry from `/collection/version/{collection_id}/{version_id}`
    ///
    /// Note: This does NOT delete documents. Use `truncate_collection` first if needed.
    #[instrument(skip(self, txn), fields(collection = %name), name = "db.delete_collection")]
    pub(crate) async fn delete_collection_with_txn(
        &self,
        txn: &mut DbTxn<S>,
        name: &str,
    ) -> Result<()> {
        // Get the collection to find its version_id and collection_id
        let collection = txn
            .get_collection(name)
            .await?
            .ok_or_else(|| Error::CollectionNotFound(name.to_string()))?;

        let version_id = collection.version_id().to_string();
        let collection_id = collection.collection_id().to_string();

        let systemstore = txn.systemstore()?;

        // 1. Delete the full schema from /collection/id/{version_id}
        let collection_key = CollectionKey::new(&version_id);
        systemstore
            .delete(&collection_key.bytes())
            .await
            .map_err(Error::Storage)?;

        // 2. Delete the name mapping from /collection/name/{name}
        let name_key = CollectionNameKey::new(name);
        systemstore
            .delete(&name_key.bytes())
            .await
            .map_err(Error::Storage)?;

        // 3. Delete the version index entry from /collection/version/{collection_id}/{version_id}
        let version_index_key = CollectionVersionKey::new(&collection_id, &version_id);
        systemstore
            .delete(&version_index_key.bytes())
            .await
            .map_err(Error::Storage)?;

        // Remove from transaction's cache
        txn.uncache_collection(name);

        tracing::info!(
            collection_name = %name,
            version_id = %version_id,
            collection_id = %collection_id,
            "Deleted collection"
        );

        Ok(())
    }

    /// Delete a collection.
    ///
    /// This creates a new transaction, calls `delete_collection_with_txn`, commits,
    /// and updates the process-wide cache.
    ///
    /// Note: This does NOT delete documents. Use `truncate_collection` first if needed.
    #[instrument(skip(self), fields(collection = %name), name = "db.delete_collection_auto")]
    pub async fn delete_collection(&self, name: &str) -> Result<()> {
        self.check_node_access(None, acp::nac::NodePermission::CollectionPatch)
            .await?;
        let mut txn = self.new_txn(false).await?;

        match self.delete_collection_with_txn(&mut txn, name).await {
            Ok(()) => {
                txn.commit().await?;

                // Update the process-wide cache after successful commit
                let mut cache = self.collections.write().map_err(|e| {
                    tracing::error!(error = ?e, collection_name = %name, "Collection cache lock poisoned after delete");
                    Error::CacheUpdateFailedAfterCommit(name.to_string())
                })?;
                cache.remove(name);

                Ok(())
            }
            Err(e) => {
                if let Err(discard_err) = txn.discard() {
                    tracing::warn!(
                        error = %discard_err,
                        original_error = %e,
                        "Transaction discard failed after delete_collection error"
                    );
                }
                Err(e)
            }
        }
    }

    /// Delete one or more collections by name in a single call (Go #4688 parity).
    ///
    /// - If `active_only` is true, only the active head version of each named
    ///   collection is removed; earlier versions are kept intact.
    /// - If `active_only` is false (Go's default), every version of each named
    ///   collection is removed.
    ///
    /// Names are deduplicated. Name resolution happens up-front: if any name
    /// is unknown, no deletion is performed. Validation (no documents, no
    /// orphan child versions outside the batch) is delegated to
    /// `delete_collection_versions_batch`.
    pub async fn delete_collections(&self, names: Vec<String>, active_only: bool) -> Result<()> {
        self.check_node_access(None, acp::nac::NodePermission::CollectionPatch)
            .await?;
        if names.is_empty() {
            return Err(Error::InvalidPatch(
                "collection name required: pass at least one name to delete".into(),
            ));
        }

        let mut seen_names = std::collections::HashSet::new();
        let unique_names: Vec<String> = names
            .into_iter()
            .filter(|n| !n.is_empty() && seen_names.insert(n.clone()))
            .collect();

        if unique_names.is_empty() {
            return Err(Error::InvalidPatch(
                "collection name required: every supplied name was empty".into(),
            ));
        }

        let mut version_ids: Vec<String> = Vec::new();
        let mut seen_versions = std::collections::HashSet::new();

        if active_only {
            for name in &unique_names {
                let col = self
                    .get_collection(name)?
                    .ok_or_else(|| Error::CollectionNotFound(name.clone()))?;
                let vid = col.version_id().to_string();
                if seen_versions.insert(vid.clone()) {
                    version_ids.push(vid);
                }
            }
        } else {
            let all_versions = self.get_all_collection_versions().await?;
            for name in &unique_names {
                let col = self
                    .get_collection(name)?
                    .ok_or_else(|| Error::CollectionNotFound(name.clone()))?;
                let collection_id = col.collection_id().to_string();
                for v in &all_versions {
                    if v.collection_id == collection_id
                        && seen_versions.insert(v.version_id.clone())
                    {
                        version_ids.push(v.version_id.clone());
                    }
                }
            }
        }

        self.delete_collection_versions_batch(version_ids).await
    }

    /// Truncate a collection: delete all documents, heads, blocks, and index entries
    /// while preserving the collection schema.
    ///
    /// Processes deletes in chunks to avoid building a massive uncommitted write set.
    #[instrument(skip(self), fields(collection = %name), name = "db.truncate_collection")]
    pub async fn truncate_collection(
        &self,
        name: &str,
        identity: Option<&identity::Did>,
    ) -> Result<()> {
        self.check_node_access(identity, acp::nac::NodePermission::CollectionTruncate)
            .await?;
        let collection = self
            .get_collection(name)?
            .ok_or_else(|| Error::CollectionNotFound(name.to_string()))?;

        let collection_id = collection.collection_id().to_string();
        let short_id = collection.resolved_root_id();

        let action_execution = self
            .register_action(&collection_id, crate::Action::TRUNCATE)
            .await?;

        let result: Result<usize> = async {
            let doc_ids = self.collect_doc_short_ids(&collection).await?;

            for chunk in doc_ids.chunks(TRUNCATE_CHUNK_SIZE) {
                self.truncate_chunk(&collection, chunk).await?;
            }

            self.truncate_collection_metadata(&collection_id, short_id)
                .await?;

            Ok(doc_ids.len())
        }
        .await;

        let doc_count = match result {
            Ok(doc_count) => doc_count,
            Err(error) => {
                if let Err(action_error) =
                    self.fail_action(action_execution, &error.to_string()).await
                {
                    tracing::error!(
                        error = %action_error,
                        collection_id = %collection_id,
                        "Failed to record truncate action error"
                    );
                }
                return Err(error);
            }
        };

        self.complete_action(action_execution).await?;

        tracing::info!(
            collection_id = %collection_id,
            short_id = short_id,
            doc_count,
            "Truncated collection"
        );

        Ok(())
    }

    /// Collect all doc short IDs for a collection using a read-only transaction.
    async fn collect_doc_short_ids(&self, collection: &Collection) -> Result<Vec<u64>> {
        use storage::keys::doc_id_index::decode_doc_short_id;

        let txn = self.new_txn(true).await?;
        let doc_ids;
        {
            let datastore = txn.datastore()?;
            let doc_prefix = collection.collection_key_prefix();
            let prefix_len = doc_prefix.len();
            let opts = IterOptions::new().with_prefix(doc_prefix);
            let mut iter = datastore.iterator(opts).await.map_err(Error::Storage)?;
            let mut ids = Vec::new();
            while let Some(pair) = iter.next().await.map_err(Error::Storage)? {
                if let Ok(doc_short_id) = decode_doc_short_id(&pair.key[prefix_len..]) {
                    ids.push(doc_short_id);
                }
            }
            iter.close().await.map_err(Error::Storage)?;
            doc_ids = ids;
        }
        let _ = txn.discard();
        Ok(doc_ids)
    }

    /// Delete data for a chunk of doc short IDs within a single write transaction.
    async fn truncate_chunk(&self, collection: &Collection, doc_short_ids: &[u64]) -> Result<()> {
        use storage::keys::{HeadstoreDocKey, HeadstorePriorityKey};

        let txn = self.new_txn(false).await?;
        let datastore = txn.datastore()?;
        let headstore = txn.headstore()?;
        let blockstore = txn.blockstore()?;
        let systemstore = txn.systemstore()?;

        let result: Result<()> = async {
            for &doc_short_id in doc_short_ids {
                datastore
                    .delete(&collection.doc_key(doc_short_id))
                    .await
                    .map_err(Error::Storage)?;
                datastore
                    .delete(&collection.deleted_key(doc_short_id))
                    .await
                    .map_err(Error::Storage)?;
                datastore
                    .delete(&collection.version_key(doc_short_id))
                    .await
                    .map_err(Error::Storage)?;

                let head_prefix = HeadstoreDocKey::document_prefix(doc_short_id);
                let mut block_cids = Vec::new();
                {
                    let opts = IterOptions::new().with_prefix(head_prefix.clone());
                    let mut iter = headstore.iterator(opts).await.map_err(Error::Storage)?;
                    while let Some(pair) = iter.next().await.map_err(Error::Storage)? {
                        if let Some(cid_str) = extract_last_path_segment_str(&pair.key) {
                            if let Ok(cid) = cid::Cid::try_from(cid_str.as_str()) {
                                block_cids.push(cid);
                            }
                        }
                    }
                    iter.close().await.map_err(Error::Storage)?;
                }
                delete_prefix(&headstore, head_prefix).await?;
                delete_prefix(
                    &headstore,
                    HeadstorePriorityKey::document_prefix(doc_short_id),
                )
                .await?;

                let doc_id = crate::doc_id_map::get_doc_id(&systemstore, doc_short_id)
                    .await?
                    .ok_or_else(|| {
                        Error::InvalidDocument(format!(
                            "document short ID {doc_short_id} has no canonical DocID"
                        ))
                    })?;
                crate::block_cleanup::delete_owned_dag(
                    &blockstore,
                    &systemstore,
                    &block_cids,
                    &doc_id,
                )
                .await?;

                // Clear the identity mappings so recreating identical content
                // does not trip the create duplicate check.
                crate::doc_id_map::delete_doc_id_mappings(&systemstore, doc_short_id).await?;
            }
            Ok(())
        }
        .await;

        // Drop namespace views before commit/discard (they hold Arc refs to txn internals)
        drop(datastore);
        drop(headstore);
        drop(blockstore);
        drop(systemstore);

        match result {
            Ok(()) => {
                txn.commit().await?;
                Ok(())
            }
            Err(e) => {
                if let Err(discard_err) = txn.discard() {
                    tracing::warn!(
                        error = %discard_err,
                        original_error = %e,
                        "Transaction discard failed during truncate chunk"
                    );
                }
                Err(e)
            }
        }
    }

    /// Delete collection-level metadata: index entries and collection heads.
    async fn truncate_collection_metadata(&self, collection_id: &str, short_id: u32) -> Result<()> {
        use storage::keys::{HeadstoreColKey, IndexDataStoreKey};

        let txn = self.new_txn(false).await?;
        let datastore = txn.datastore()?;
        let headstore = txn.headstore()?;
        let blockstore = txn.blockstore()?;
        let systemstore = txn.systemstore()?;

        let result: Result<()> = async {
            // Delete index entries
            let idx_prefix = IndexDataStoreKey::collection_prefix(short_id);
            delete_prefix(&datastore, idx_prefix).await?;

            // Collect block CIDs from collection heads, then delete
            let col_head_prefix = HeadstoreColKey::collection_prefix(short_id);
            let mut block_cids = Vec::new();
            {
                let opts = IterOptions::new().with_prefix(col_head_prefix.clone());
                let mut iter = headstore.iterator(opts).await.map_err(Error::Storage)?;
                while let Some(pair) = iter.next().await.map_err(Error::Storage)? {
                    if let Some(cid_str) = extract_last_path_segment_str(&pair.key) {
                        if let Ok(cid) = cid::Cid::try_from(cid_str.as_str()) {
                            block_cids.push(cid);
                        }
                    }
                }
                iter.close().await.map_err(Error::Storage)?;
            }
            delete_prefix(&headstore, col_head_prefix).await?;

            crate::block_cleanup::delete_owned_dag(&blockstore, &systemstore, &block_cids, "")
                .await?;

            // Delete the top-level doc/del/version prefixes
            let doc_prefix = format!("/d/{}/", collection_id).into_bytes();
            delete_prefix(&datastore, doc_prefix).await?;
            let del_prefix = format!("/del/{}/", collection_id).into_bytes();
            delete_prefix(&datastore, del_prefix).await?;
            let version_prefix = format!("/v/{}/", collection_id).into_bytes();
            delete_prefix(&datastore, version_prefix).await?;

            Ok(())
        }
        .await;

        drop(datastore);
        drop(headstore);
        drop(blockstore);
        drop(systemstore);

        match result {
            Ok(()) => {
                txn.commit().await?;
                Ok(())
            }
            Err(e) => {
                if let Err(discard_err) = txn.discard() {
                    tracing::warn!(
                        error = %discard_err,
                        original_error = %e,
                        "Transaction discard failed during truncate metadata"
                    );
                }
                Err(e)
            }
        }
    }
}
