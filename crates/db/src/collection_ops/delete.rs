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
    pub async fn delete_collection_with_txn(&self, txn: &mut DbTxn<S>, name: &str) -> Result<()> {
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

    /// Truncate a collection: delete all documents, heads, blocks, and index entries
    /// while preserving the collection schema.
    ///
    /// Processes deletes in chunks to avoid building a massive uncommitted write set.
    #[instrument(skip(self), fields(collection = %name), name = "db.truncate_collection")]
    pub async fn truncate_collection(&self, name: &str) -> Result<()> {
        let collection = self
            .get_collection(name)?
            .ok_or_else(|| Error::CollectionNotFound(name.to_string()))?;

        let collection_id = collection.collection_id().to_string();
        let short_id = collection.resolved_root_id();

        // Phase 1: Collect all doc_ids in a read-only txn
        let doc_ids = self.collect_doc_ids(&collection_id).await?;

        // Phase 2: Delete document data in chunks
        for chunk in doc_ids.chunks(TRUNCATE_CHUNK_SIZE) {
            self.truncate_chunk(&collection_id, chunk).await?;
        }

        // Phase 3: Delete collection-level metadata (indexes, collection heads)
        self.truncate_collection_metadata(&collection_id, short_id)
            .await?;

        tracing::info!(
            collection_id = %collection_id,
            short_id = short_id,
            doc_count = doc_ids.len(),
            "Truncated collection"
        );

        Ok(())
    }

    /// Collect all doc_ids for a collection using a read-only transaction.
    async fn collect_doc_ids(&self, collection_id: &str) -> Result<Vec<String>> {
        let txn = self.new_txn(true).await?;
        let doc_ids;
        {
            let datastore = txn.datastore()?;
            let doc_prefix = format!("/d/{}/", collection_id).into_bytes();
            let opts = IterOptions::new().with_prefix(doc_prefix);
            let mut iter = datastore.iterator(opts).await.map_err(Error::Storage)?;
            let mut ids = Vec::new();
            while let Some(pair) = iter.next().await.map_err(Error::Storage)? {
                if pair.key.ends_with(b"/v") {
                    continue;
                }
                if let Some(pos) = pair.key.iter().rposition(|&b| b == b'/') {
                    let doc_id = String::from_utf8_lossy(&pair.key[pos + 1..]).to_string();
                    if !doc_id.is_empty() {
                        ids.push(doc_id);
                    }
                }
            }
            iter.close().await.map_err(Error::Storage)?;
            doc_ids = ids;
        }
        let _ = txn.discard();
        Ok(doc_ids)
    }

    /// Delete data for a chunk of doc_ids within a single write transaction.
    async fn truncate_chunk(&self, collection_id: &str, doc_ids: &[String]) -> Result<()> {
        use storage::keys::HeadstoreDocKey;

        let txn = self.new_txn(false).await?;
        let datastore = txn.datastore()?;
        let headstore = txn.headstore()?;
        let blockstore = txn.blockstore()?;

        let result: Result<()> = async {
            for doc_id in doc_ids {
                let doc_key_prefix = format!("/d/{}/{}", collection_id, doc_id).into_bytes();
                delete_prefix(&datastore, doc_key_prefix).await?;

                let del_key_prefix = format!("/del/{}/{}", collection_id, doc_id).into_bytes();
                delete_prefix(&datastore, del_key_prefix).await?;

                let head_prefix = HeadstoreDocKey::document_prefix(doc_id);
                let mut block_cids = Vec::new();
                {
                    let opts = IterOptions::new().with_prefix(head_prefix.clone());
                    let mut iter = headstore.iterator(opts).await.map_err(Error::Storage)?;
                    while let Some(pair) = iter.next().await.map_err(Error::Storage)? {
                        if let Some(cid_str) = extract_last_path_segment_str(&pair.key) {
                            if let Ok(cid) = cid::Cid::try_from(cid_str.as_str()) {
                                block_cids.push(cid.to_bytes());
                            }
                        }
                    }
                    iter.close().await.map_err(Error::Storage)?;
                }
                delete_prefix(&headstore, head_prefix).await?;

                for cid_bytes in &block_cids {
                    let _ = blockstore.delete(cid_bytes).await;
                }
            }
            Ok(())
        }
        .await;

        // Drop namespace views before commit/discard (they hold Arc refs to txn internals)
        drop(datastore);
        drop(headstore);
        drop(blockstore);

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
                            block_cids.push(cid.to_bytes());
                        }
                    }
                }
                iter.close().await.map_err(Error::Storage)?;
            }
            delete_prefix(&headstore, col_head_prefix).await?;

            for cid_bytes in &block_cids {
                let _ = blockstore.delete(cid_bytes).await;
            }

            // Delete the top-level doc/del prefixes (version keys, etc.)
            let doc_prefix = format!("/d/{}/", collection_id).into_bytes();
            delete_prefix(&datastore, doc_prefix).await?;
            let del_prefix = format!("/del/{}/", collection_id).into_bytes();
            delete_prefix(&datastore, del_prefix).await?;

            Ok(())
        }
        .await;

        drop(datastore);
        drop(headstore);
        drop(blockstore);

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
