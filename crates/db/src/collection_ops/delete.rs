use super::*;

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
    /// This resets the collection to an empty state as if it were just created.
    #[instrument(skip(self), fields(collection = %name), name = "db.truncate_collection")]
    pub async fn truncate_collection(&self, name: &str) -> Result<()> {
        let collection = self
            .get_collection(name)?
            .ok_or_else(|| Error::CollectionNotFound(name.to_string()))?;

        let collection_id = collection.collection_id().to_string();
        // Hash-based short ID used by index manager and collection head keys
        let short_id = crate::collection::collection_short_id(&collection_id);

        let mut txn = self.new_txn(false).await?;
        match self
            .truncate_collection_inner(&mut txn, &collection_id, short_id)
            .await
        {
            Ok(()) => {
                txn.commit().await?;
                Ok(())
            }
            Err(e) => {
                if let Err(discard_err) = txn.discard() {
                    tracing::warn!(
                        error = %discard_err,
                        original_error = %e,
                        "Transaction discard failed after truncate_collection error"
                    );
                }
                Err(e)
            }
        }
    }

    /// Inner truncation logic within an existing transaction.
    async fn truncate_collection_inner(
        &self,
        txn: &mut DbTxn<S>,
        collection_id: &str,
        short_id: u32,
    ) -> Result<()> {
        use storage::keys::{HeadstoreColKey, HeadstoreDocKey, IndexDataStoreKey};

        let datastore = txn.datastore()?;
        let headstore = txn.headstore()?;
        let blockstore = txn.blockstore()?;

        // Document data key prefix: /d/<collection_id>/
        let doc_prefix = format!("/d/{}/", collection_id).into_bytes();
        // Deletion marker prefix: /del/<collection_id>/
        let del_prefix = format!("/del/{}/", collection_id).into_bytes();

        // 1. Collect doc_ids from document data keys (/d/<collection_id>/<doc_id>)
        let mut doc_ids: Vec<String> = Vec::new();
        {
            let opts = IterOptions::new().with_prefix(doc_prefix.clone());
            let mut iter = datastore.iterator(opts).await.map_err(Error::Storage)?;
            while let Some(pair) = iter.next().await.map_err(Error::Storage)? {
                // Skip version keys (end with /v)
                if pair.key.ends_with(b"/v") {
                    continue;
                }
                // Key format: /d/<collection_id>/<doc_id>
                if let Some(pos) = pair.key.iter().rposition(|&b| b == b'/') {
                    let doc_id = String::from_utf8_lossy(&pair.key[pos + 1..]).to_string();
                    if !doc_id.is_empty() {
                        doc_ids.push(doc_id);
                    }
                }
            }
            iter.close().await.map_err(Error::Storage)?;
        }

        // 2. Delete all document data from datastore
        delete_prefix(&datastore, doc_prefix).await?;

        // 3. Delete all deletion markers from datastore
        delete_prefix(&datastore, del_prefix).await?;

        // 4. Delete index entries from datastore (uses hash-based short_id)
        let idx_prefix = IndexDataStoreKey::collection_prefix(short_id);
        delete_prefix(&datastore, idx_prefix).await?;

        // 5. Delete document head entries from headstore + collect block CIDs
        let mut block_cids: Vec<Vec<u8>> = Vec::new();
        for doc_id in &doc_ids {
            let head_prefix = HeadstoreDocKey::document_prefix(doc_id);
            let opts = IterOptions::new().with_prefix(head_prefix);
            let mut iter = headstore.iterator(opts).await.map_err(Error::Storage)?;
            while let Some(pair) = iter.next().await.map_err(Error::Storage)? {
                // Extract CID string from key: /d/{doc_id}/{field_id}/{CID_string}
                if let Some(cid_str) = extract_last_path_segment_str(&pair.key) {
                    if let Ok(cid) = cid::Cid::try_from(cid_str.as_str()) {
                        block_cids.push(cid.to_bytes());
                    }
                }
            }
            iter.close().await.map_err(Error::Storage)?;

            // Now delete all head entries for this doc
            let head_prefix = HeadstoreDocKey::document_prefix(doc_id);
            delete_prefix(&headstore, head_prefix).await?;
        }

        // 6. Delete collection-level head entries from headstore (uses hash-based short_id)
        let col_head_prefix = HeadstoreColKey::collection_prefix(short_id);
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

        // 7. Delete blocks from blockstore
        for cid_bytes in &block_cids {
            let _ = blockstore.delete(cid_bytes).await;
        }

        tracing::info!(
            collection_id = %collection_id,
            short_id = short_id,
            doc_count = doc_ids.len(),
            block_count = block_cids.len(),
            "Truncated collection"
        );

        Ok(())
    }
}
