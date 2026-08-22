use super::*;

impl<S: Store> crate::database::DB<S> {
    /// Set the active collection version.
    ///
    /// This activates the collection with the given version ID and deactivates
    /// any other versions of the same collection.
    ///
    /// # Arguments
    ///
    /// * `version_id` - The version ID of the collection to activate
    ///
    /// # Errors
    ///
    /// - `CollectionVersionNotFound` if no collection with the given version ID exists
    #[instrument(skip(self), fields(version_id = %version_id), name = "db.set_active_version")]
    pub async fn set_active_collection_version(&self, version_id: &str) -> Result<()> {
        self.check_node_access(None, acp::nac::NodePermission::CollectionPatch)
            .await?;
        if version_id.is_empty() {
            return Err(Error::CollectionVersionIDEmpty);
        }

        // Load the target collection from persistent store by version_id
        let txn = self.new_txn(false).await?;

        // Extract the target schema and perform all systemstore operations in a block
        // so the systemstore reference is dropped before calling txn.commit()
        let (target_schema, name, _collection_guards) = {
            let systemstore = txn.systemstore()?;

            let collection_key = CollectionKey::new(version_id);
            let target_bytes = systemstore
                .get(&collection_key.bytes())
                .await
                .map_err(Error::Storage)?
                .ok_or(Error::CollectionVersionNotFound(version_id.to_string()))?;

            let mut target_schema: CollectionVersion = serde_json::from_slice(&target_bytes)
                .map_err(|e| {
                    Error::collection_schema_json(
                        format!(
                            "failed to deserialize schema for version_id '{}'",
                            version_id
                        ),
                        e,
                    )
                })?;
            crate::collection::populate_collection_root_id(&systemstore, &mut target_schema)
                .await?;

            let name = target_schema.name.clone();
            let collection_id = target_schema.collection_id.clone();
            let collection_guards = self
                .collection_write_guards(std::iter::once(collection_id.clone()))
                .await?;

            // Update target to be active
            target_schema.is_active = true;

            // Store the updated target schema
            let target_data = serde_json::to_vec(&target_schema).map_err(|e| {
                Error::collection_schema_json(
                    format!("failed to serialize schema for version_id '{}'", version_id),
                    e,
                )
            })?;
            systemstore
                .set(&collection_key.bytes(), &target_data)
                .await
                .map_err(Error::Storage)?;

            // Update the name pointer to point to this version
            let name_key = CollectionNameKey::new(&name);
            systemstore
                .set(&name_key.bytes(), version_id.as_bytes())
                .await
                .map_err(Error::Storage)?;

            // Find and deactivate other versions with the same collection_id
            let version_prefix = CollectionVersionKey::collection_prefix(&collection_id);
            let opts = IterOptions::new().with_prefix(version_prefix);
            let mut iter = systemstore.iterator(opts).await.map_err(Error::Storage)?;

            while let Some(pair) = iter.next().await.map_err(Error::Storage)? {
                // Extract version_id from key: /collection/version/{collection_id}/{version_id}
                let key_str = String::from_utf8_lossy(&pair.key);
                if let Some(other_version_id) = key_str.rsplit('/').next() {
                    if other_version_id != version_id {
                        // Load, deactivate, and store the other version
                        let other_key = CollectionKey::new(other_version_id);
                        if let Some(other_bytes) = systemstore
                            .get(&other_key.bytes())
                            .await
                            .map_err(Error::Storage)?
                        {
                            if let Ok(mut other_schema) =
                                serde_json::from_slice::<CollectionVersion>(&other_bytes)
                            {
                                if other_schema.is_active {
                                    other_schema.is_active = false;
                                    if let Ok(other_data) = serde_json::to_vec(&other_schema) {
                                        let _ =
                                            systemstore.set(&other_key.bytes(), &other_data).await;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            iter.close().await.map_err(Error::Storage)?;

            (target_schema, name, collection_guards)
        };

        txn.commit().await?;

        // Update the process-wide cache (scoped to drop lock before reindex)
        {
            let mut cache = self.collections.write().map_err(|e| {
                tracing::error!(
                    error = ?e,
                    version_id = %version_id,
                    "Collection cache lock poisoned during set_active_collection_version"
                );
                Error::CacheUpdateFailedAfterCommit(name.clone())
            })?;
            cache.insert(name.clone(), Collection::new(target_schema));
        }

        tracing::info!(
            collection_name = %name,
            version_id = %version_id,
            "Set active collection version"
        );

        // After switching the active version, rebuild indexes with migrated values
        // if the new version's history chain contains any migrations.
        self.reindex_collection_with_migrations(&name).await?;

        Ok(())
    }

    /// Get a collection by version ID from the cache.
    ///
    /// This searches the in-memory cache for a collection with the given version ID.
    /// It only returns active collections that are in the cache.
    pub fn get_collection_by_version_id(&self, version_id: &str) -> Result<Option<Collection>> {
        let cache = self.collections.read().map_err(|e| {
            tracing::error!(
                error = ?e,
                version_id = %version_id,
                "Collection cache lock poisoned during get_collection_by_version_id"
            );
            Error::LockPoisoned(
                "collection cache lock poisoned during get_collection_by_version_id".into(),
            )
        })?;
        Ok(cache
            .values()
            .find(|c| c.version_id() == version_id)
            .cloned())
    }

    /// Get a collection by version ID, searching both cache and KV store.
    pub async fn get_collection_by_version_id_full(
        &self,
        version_id: &str,
    ) -> Result<Option<Collection>> {
        // Check cache first
        if let Some(c) = self.get_collection_by_version_id(version_id)? {
            return Ok(Some(c));
        }
        // Search all stored versions (including inactive)
        let all_versions = self.get_all_collection_versions().await?;
        Ok(all_versions
            .into_iter()
            .find(|v| v.version_id == version_id)
            .map(Collection::new))
    }

    /// Get all collection versions from storage (active and inactive).
    ///
    /// This scans `/collection/id/` prefix to load ALL versions, matching
    /// Go's behavior of loading all versions for cross-collection validation.
    pub async fn get_all_collection_versions(&self) -> Result<Vec<CollectionVersion>> {
        let txn = self.new_txn(true).await?;
        let mut versions = Vec::new();
        let prefix = CollectionKey::collection_prefix();

        {
            let systemstore = txn.systemstore()?;
            let opts = IterOptions::new().with_prefix(prefix);
            let mut iter = systemstore.iterator(opts).await.map_err(Error::Storage)?;

            while let Some(pair) = iter.next().await.map_err(Error::Storage)? {
                match serde_json::from_slice::<CollectionVersion>(&pair.value) {
                    Ok(mut col) => {
                        crate::collection::populate_collection_root_id(&systemstore, &mut col)
                            .await?;
                        versions.push(col);
                    }
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            "Failed to deserialize collection version during scan"
                        );
                    }
                }
            }

            iter.close().await.map_err(Error::Storage)?;
        }

        let _ = txn.discard();
        Ok(versions)
    }

    /// Delete a specific collection version by version_id.
    ///
    /// This performs true deletion (not deactivation): removes the version from
    /// the KV store, version index, name pointer (if active), blockstore blocks,
    /// and in-memory cache.
    ///
    /// # Validation
    ///
    /// - The version must exist
    /// - No non-deleted child versions may reference this as PreviousVersion
    ///   (unless those children are also in `also_deleting` batch)
    /// - The collection must have no documents
    pub async fn delete_collection_version(
        &self,
        version_id: &str,
        also_deleting: &[String],
    ) -> Result<()> {
        self.check_node_access(None, acp::nac::NodePermission::CollectionPatch)
            .await?;
        // Load the version from KV store
        let txn = self.new_txn(true).await?;
        let target_schema = {
            let systemstore = txn.systemstore()?;
            let collection_key = CollectionKey::new(version_id);
            let target_bytes = systemstore
                .get(&collection_key.bytes())
                .await
                .map_err(Error::Storage)?
                .ok_or(Error::CollectionVersionNotFound(version_id.to_string()))?;
            let mut schema =
                serde_json::from_slice::<CollectionVersion>(&target_bytes).map_err(|e| {
                    Error::collection_schema_json(
                        format!("failed to deserialize version '{}'", version_id),
                        e,
                    )
                })?;

            crate::collection::populate_collection_root_id(&systemstore, &mut schema).await?;

            schema
        };
        let _ = txn.discard();

        let collection_id = target_schema.collection_id.clone();
        let name = target_schema.name.clone();

        // Validate: no child versions reference this (excluding batch siblings)
        let all_versions = self.get_all_collection_versions().await?;
        for other in &all_versions {
            if let Some(ref prev) = other.previous_version {
                if prev.source_collection_id == version_id
                    && !also_deleting.contains(&other.version_id)
                {
                    return Err(Error::InvalidPatch(
                        "cannot delete a version that is used by a newer version, first delete the new version".to_string(),
                    ));
                }
            }
        }
        let mut deleting_versions: std::collections::HashSet<&str> =
            also_deleting.iter().map(String::as_str).collect();
        deleting_versions.insert(version_id);
        let is_deleting_last_local_version = all_versions.iter().all(|version| {
            version.collection_id != collection_id
                || deleting_versions.contains(version.version_id.as_str())
        });

        // Validate: no documents exist
        if target_schema.is_active {
            let has_data = self.collection_has_data(&target_schema).await?;
            if has_data {
                return Err(Error::InvalidPatch(
                    "cannot delete a collection that has documents, first delete the documents and then delete the version".to_string(),
                ));
            }
        }

        // Perform the actual deletion
        let txn = self.new_txn(false).await?;
        {
            let systemstore = txn.systemstore()?;

            // 1. Delete /collection/id/{version_id}
            let collection_key = CollectionKey::new(version_id);
            systemstore
                .delete(&collection_key.bytes())
                .await
                .map_err(Error::Storage)?;

            // 2. Delete /collection/version/{collection_id}/{version_id}
            let version_index_key = CollectionVersionKey::new(&collection_id, version_id);
            systemstore
                .delete(&version_index_key.bytes())
                .await
                .map_err(Error::Storage)?;

            // 3. If version was active, delete /collection/name/{name}
            if target_schema.is_active {
                let name_key = CollectionNameKey::new(&name);
                systemstore
                    .delete(&name_key.bytes())
                    .await
                    .map_err(Error::Storage)?;
            }
        }

        // 4. Delete IPLD blocks from blockstore (collection CID + field CIDs)
        {
            let blockstore = txn.blockstore()?;
            // Delete the version CID block itself
            if let Ok(cid) = cid::Cid::try_from(version_id) {
                let _ = blockstore.delete(&cid.to_bytes()).await;
            }
            // Delete field CID blocks
            for field in &target_schema.fields {
                if !field.id.is_empty() {
                    if let Ok(cid) = cid::Cid::try_from(field.id.as_str()) {
                        let _ = blockstore.delete(&cid.to_bytes()).await;
                    }
                }
            }
        }

        txn.commit().await?;

        // 5. Remove from in-memory cache if active
        if target_schema.is_active {
            if let Ok(mut cache) = self.collections.write() {
                cache.remove(&name);
            }
        }
        if is_deleting_last_local_version {
            self.forbid_collection_id(&collection_id)?;
        }

        tracing::info!(
            version_id = %version_id,
            collection_name = %name,
            "Deleted collection version"
        );

        Ok(())
    }

    /// Delete multiple collection versions in a single batch.
    ///
    /// Versions are sorted topologically (children before parents based on
    /// PreviousVersion) and deleted in that order. When checking for child
    /// references, other versions in the batch are excluded from validation.
    pub async fn delete_collection_versions_batch(&self, version_ids: Vec<String>) -> Result<()> {
        self.check_node_access(None, acp::nac::NodePermission::CollectionPatch)
            .await?;
        if version_ids.is_empty() {
            return Ok(());
        }

        // Sort topologically: children before parents
        // A child has a PreviousVersion pointing to a parent in the batch
        let all_versions = self.get_all_collection_versions().await?;
        let version_map: std::collections::HashMap<&str, &CollectionVersion> = all_versions
            .iter()
            .map(|v| (v.version_id.as_str(), v))
            .collect();
        let deleting_versions: std::collections::HashSet<&str> =
            version_ids.iter().map(String::as_str).collect();

        for version_id in &version_ids {
            if !version_map.contains_key(version_id.as_str()) {
                return Err(Error::CollectionVersionNotFound(version_id.clone()));
            }
        }

        let _collection_guards = self
            .collection_write_guards(version_ids.iter().filter_map(|version_id| {
                version_map
                    .get(version_id.as_str())
                    .map(|version| version.collection_id.clone())
            }))
            .await?;

        for version in &all_versions {
            if deleting_versions.contains(version.version_id.as_str()) {
                if version.is_active && self.collection_has_data(version).await? {
                    return Err(Error::InvalidPatch(
                        "cannot delete a collection that has documents, first delete the documents and then delete the version".to_string(),
                    ));
                }
                continue;
            }

            if version.previous_version.as_ref().is_some_and(|previous| {
                deleting_versions.contains(previous.source_collection_id.as_str())
            }) {
                return Err(Error::InvalidPatch(
                    "cannot delete a version that is used by a newer version, first delete the new version".to_string(),
                ));
            }
        }

        let removed_collection_ids: std::collections::HashSet<&str> = all_versions
            .iter()
            .filter(|version| {
                deleting_versions.contains(version.version_id.as_str())
                    && all_versions.iter().all(|other| {
                        other.collection_id != version.collection_id
                            || deleting_versions.contains(other.version_id.as_str())
                    })
            })
            .map(|version| version.collection_id.as_str())
            .collect();

        if all_versions.iter().any(|version| {
            !deleting_versions.contains(version.version_id.as_str())
                && version.fields.iter().any(|field| {
                    field
                        .kind
                        .relation_collection_id()
                        .is_some_and(|id| removed_collection_ids.contains(id))
                })
        }) {
            return Err(Error::InvalidPatch(
                "cannot remove a collection while another field references it".to_string(),
            ));
        }

        let mut sorted = Vec::with_capacity(version_ids.len());
        let mut remaining: std::collections::HashSet<String> =
            version_ids.iter().cloned().collect();

        // Simple topological sort: repeatedly find versions whose children
        // (within the batch) have already been added to sorted
        let max_iterations = version_ids.len() * version_ids.len();
        let mut iterations = 0;
        while !remaining.is_empty() {
            iterations += 1;
            if iterations > max_iterations {
                // Break cycles by just adding remaining in any order
                for id in &remaining {
                    sorted.push(id.clone());
                }
                break;
            }

            let mut added_any = false;
            let remaining_snapshot: Vec<String> = remaining.iter().cloned().collect();
            for id in &remaining_snapshot {
                // Check if any remaining version has this as its parent
                let has_remaining_child = remaining.iter().any(|other_id| {
                    if other_id == id {
                        return false;
                    }
                    version_map
                        .get(other_id.as_str())
                        .and_then(|v| v.previous_version.as_ref())
                        .map(|prev| prev.source_collection_id == *id)
                        .unwrap_or(false)
                });

                if !has_remaining_child {
                    sorted.push(id.clone());
                    remaining.remove(id);
                    added_any = true;
                }
            }

            if !added_any {
                // All remaining have circular deps, add them all
                for id in &remaining {
                    sorted.push(id.clone());
                }
                break;
            }
        }

        // Delete each version in topological order
        for version_id in &sorted {
            self.delete_collection_version(version_id, &version_ids)
                .await?;
        }

        Ok(())
    }

    /// Check if a collection has any documents in the datastore.
    pub(crate) async fn collection_has_data(
        &self,
        version: &schema::CollectionVersion,
    ) -> Result<bool> {
        if !version.is_materialized {
            return Ok(false);
        }

        let txn = self.new_txn(true).await?;
        let has_data = {
            let datastore = txn.datastore()?;

            let prefix = if version.query.is_some() {
                // View: check view cache prefix using short ID
                storage::keys::datastore::ViewCacheKey::collection_prefix(version.root_id)
            } else {
                // Regular collection: check document prefix
                format!("/d/{}/", version.collection_id).as_bytes().to_vec()
            };

            let opts = IterOptions::new().with_prefix(prefix);
            let mut iter = datastore.iterator(opts).await.map_err(Error::Storage)?;
            let has_any = iter.next().await.map_err(Error::Storage)?.is_some();
            iter.close().await.map_err(Error::Storage)?;
            has_any
        };
        let _ = txn.discard();
        Ok(has_data)
    }
}
