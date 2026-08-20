use super::*;
use lens::TransformStore;
use storage::keys::systemstore::LensConfigKey;

impl<S: Store> crate::database::DB<S> {
    /// Create and store a new schema version from a validated patched schema.
    ///
    /// Handles default CRDTs for new fields, cross-collection validation,
    /// unique index creation, CID generation, placeholder/pending migration
    /// linking, IsActive handling, transaction writes, and cache updates.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn store_new_version(
        &self,
        mut new_schema: CollectionVersion,
        old_schema: &CollectionVersion,
        old_version_id: &str,
        actual_name: &str,
        collection_id: &str,
        collection_name: &str,
        is_active_explicitly_set: bool,
        migration: Option<lens::LensConfig>,
    ) -> Result<CollectionVersion> {
        // Go compatibility: default new fields with CType::None to CType::LwwRegister.
        // Go's patchCollection does this in collection_define.go for new fields that
        // don't have an explicit CRDT type. This must happen before CID generation.
        {
            let old_field_names: std::collections::HashSet<&str> =
                old_schema.fields.iter().map(|f| f.name.as_str()).collect();
            for field in &mut new_schema.fields {
                if !old_field_names.contains(field.name.as_str())
                    && !field.kind.is_relation()
                    && field.crdt_type == schema::CType::None
                {
                    field.crdt_type = schema::CType::LwwRegister;
                }
            }
        }

        // Run Go-compatible cross-collection validators (before schema validate() which
        // uses different error messages). These validators cover duplicate fields,
        // CRDT/kind compatibility, and all Go-specific patch constraints.
        let all_existing = self.get_all_collection_versions().await?;
        let new_collections: Vec<CollectionVersion> = all_existing
            .iter()
            .filter(|c| c.version_id != old_version_id)
            .cloned()
            .chain(std::iter::once(new_schema.clone()))
            .collect();
        schema::definition_validation::validate_collection_changes(&all_existing, &new_collections)
            .map_err(Error::InvalidPatch)?;

        // Also run schema-level validation for checks not covered by definition validators
        // (e.g., relation field requires relation_name, policy format validation)
        new_schema.validate()?;

        // Block unsafe policy transitions (protected→open, resource name change).
        // These transitions can silently expose previously protected documents.
        crate::collection::acp::block_unsafe_policy_transition(
            actual_name,
            old_schema.policy.as_ref(),
            new_schema.policy.as_ref(),
            false,
        )?;

        // Auto-create missing FK indexes for primary relations added via patch.
        // This runs AFTER validation (which rejects index mutations on existing schemas)
        // but BEFORE CID generation (since indexes are part of the schema content).
        {
            // Go uses sequential IDs starting from the next available for this collection
            let schema_max_index_id = new_schema
                .indexes
                .iter()
                .map(|idx| idx.id)
                .max()
                .unwrap_or(0);
            let mut next_index_id = schema_max_index_id;

            let mut indexes_to_add = Vec::new();
            for field in &new_schema.fields {
                if !field.kind.is_relation() || field.kind.is_array() {
                    continue;
                }
                let rel_name = match field.relation_name.as_ref() {
                    Some(n) => n,
                    None => continue,
                };
                let other_col_id = match field.kind.relation_collection_id() {
                    Some(id) => id,
                    None => continue,
                };
                let other_col = if other_col_id == new_schema.name
                    || other_col_id == new_schema.collection_id
                {
                    Some(&new_schema)
                } else {
                    all_existing.iter().find(|c| {
                        (c.name == other_col_id || c.collection_id == other_col_id) && c.is_active
                    })
                };

                let other_field = other_col
                    .and_then(|col| col.field_by_relation(rel_name, &new_schema.name, &field.name));
                if field.is_primary {
                    // Go's getOneToOneIndexRequestsForPatch only creates unique
                    // indexes for one-to-one relations added via patch. One-to-many
                    // indexes are NOT auto-created during patching (only during
                    // initial schema creation via finalizeRelations).
                    //
                    // When the other side doesn't exist yet (multi-collection patch
                    // where collections are patched sequentially), skip index creation
                    // here. The missing index will be created when the other collection
                    // is patched (see cross-collection index check below).
                    let is_one_to_one = other_field.map(|f| !f.kind.is_array()).unwrap_or(false);
                    if is_one_to_one {
                        match new_schema.ensure_one_to_one_unique_index(&field.name, &mut || {
                            next_index_id += 1;
                            next_index_id
                        }) {
                            Ok(Some(index)) => indexes_to_add.push(index),
                            Ok(None) => {}
                            Err(e) => return Err(Error::InvalidPatch(e.to_string())),
                        }
                    }
                }
            }
            for index in indexes_to_add {
                new_schema.indexes.push(index);
            }
        }

        // Read current heads from schema_heads (emulates Go's persistent headstore).
        // For branching patches (v1→v2 then v1→v3), the headstore tracks the latest
        // CID after v2, so v3 gets heads=[v2_cid] and priority=3, matching Go.
        let (collection_heads, collection_priority) = {
            let heads_map = self
                .schema_heads
                .read()
                .map_err(|_| Error::LockPoisoned("schema_heads lock poisoned".into()))?;
            match heads_map.get(actual_name) {
                Some((heads, h)) => (heads.clone(), *h + 1),
                None => {
                    // Fallback: compute from version chain (for databases loaded from storage)
                    let versions_map: std::collections::HashMap<&str, &CollectionVersion> =
                        all_existing
                            .iter()
                            .map(|v| (v.version_id.as_str(), v))
                            .collect();
                    let mut depth = 0u64;
                    let mut current_id = old_schema.version_id.as_str();
                    while let Some(v) = versions_map.get(current_id) {
                        match &v.previous_version {
                            Some(prev) => {
                                depth += 1;
                                current_id = prev.source_collection_id.as_str();
                            }
                            None => break,
                        }
                    }
                    let version_depth = depth + 1;
                    let old_cid = cid::Cid::try_from(old_schema.version_id.as_str()).ok();
                    (old_cid.into_iter().collect(), version_depth + 1)
                }
            }
        };

        // Build collection name → collection_id map for resolving FieldKind::Named
        let collection_id_map: std::collections::HashMap<String, String> = all_existing
            .iter()
            .filter(|c| c.is_active)
            .map(|c| (c.name.clone(), c.collection_id.clone()))
            .collect();

        // Generate new version_id from schema content with headstore heads and priority
        let (new_version_id, query_select, query_transform) =
            Self::generate_patch_version_id_with_heads(
                &mut new_schema,
                old_schema,
                collection_priority,
                &collection_heads,
                &collection_id_map,
            );

        // Update new schema with version info
        new_schema.version_id = new_version_id.clone();

        // Check if a placeholder version exists with this ID (from pre-registered migration).
        // When set_migration is called before patch_collection, it creates a placeholder
        // with previous_version.transform set. We need to copy that transform to preserve
        // the migration link.
        let placeholder_transform = {
            let read_txn = self.new_txn(true).await?;
            let systemstore = read_txn.systemstore()?;
            let placeholder_key = CollectionKey::new(&new_version_id);
            match systemstore
                .get(&placeholder_key.bytes())
                .await
                .map_err(Error::Storage)?
            {
                Some(data) => {
                    let placeholder: CollectionVersion =
                        serde_json::from_slice(&data).map_err(|e| {
                            Error::collection_schema_json(
                                "failed to deserialize placeholder version",
                                e,
                            )
                        })?;
                    tracing::debug!(
                        new_version_id = %new_version_id,
                        is_placeholder = placeholder.is_placeholder,
                        has_previous_version = placeholder.previous_version.is_some(),
                        transform = ?placeholder.previous_version.as_ref().and_then(|pv| pv.transform.as_ref()),
                        "patch_collection: found existing version"
                    );
                    if placeholder.is_placeholder {
                        // Found a placeholder - extract its transform
                        placeholder.previous_version.and_then(|pv| pv.transform)
                    } else {
                        None
                    }
                }
                None => None,
            }
        };

        // Use placeholder transform if available, otherwise None
        new_schema.previous_version = Some(CollectionSource {
            source_collection_id: old_version_id.to_string(),
            transform: placeholder_transform.clone(),
        });

        if placeholder_transform.is_some() {
            tracing::debug!(
                new_version = %new_version_id,
                transform_id = ?placeholder_transform,
                "Linked pre-registered migration from placeholder to new schema version"
            );
        }

        // Also check for pending migrations targeting this new version (in-memory fallback)
        {
            let pending = self.pending_migrations.read().map_err(|e| {
                tracing::error!(error = ?e, "Pending migrations lock poisoned");
                Error::LockPoisoned(
                    "pending migrations lock poisoned during patch_collection".into(),
                )
            })?;
            if let Some((_source_id, transform_id)) = pending.get(&new_version_id) {
                if let Some(ref mut prev) = new_schema.previous_version {
                    // Only override if we didn't already get a transform from the placeholder
                    if prev.transform.is_none() {
                        prev.transform = Some(transform_id.clone());
                        tracing::debug!(
                            new_version = %new_version_id,
                            transform_id = %transform_id,
                            "Linked pending migration to new schema version"
                        );
                    }
                }
            }
        }

        // Go compatibility: respect explicit IsActive=false in the patch, otherwise default to true.
        // When IsActive was explicitly set to false in the patch, preserve it.
        // When the new version is inactive, keep the old version active.
        if !is_active_explicitly_set {
            new_schema.is_active = true;
        }

        let committed_migration = if let Some(mut config) = migration {
            config.source_schema_version_id = old_version_id.to_string();
            config.destination_schema_version_id = new_version_id.clone();

            let txn_lens_store = crate::txn::lenses::TxnLensStore::new(self.lens_store.clone())?;
            let transform_id = txn_lens_store.add(config.clone()).await?;
            new_schema.previous_version = Some(CollectionSource {
                source_collection_id: old_version_id.to_string(),
                transform: Some(transform_id.to_string()),
            });
            Some((transform_id, config))
        } else {
            None
        };

        // Create old schema copy for storage. If new schema is active, mark old as inactive.
        // If new schema is inactive (explicit IsActive=false), old version stays active.
        let mut old_schema_inactive = old_schema.clone();
        if new_schema.is_active {
            old_schema_inactive.is_active = false;
        }

        tracing::info!(
            collection = %collection_name,
            old_version = %old_version_id,
            new_version = %new_version_id,
            field_count = new_schema.fields.len(),
            "Creating new schema version"
        );

        // Begin transaction to store all version data
        let txn = self.new_txn(false).await?;

        // Prepare serialized data before getting systemstore reference
        let old_version_key = CollectionKey::new(old_version_id);
        let old_version_data = serde_json::to_vec(&old_schema_inactive).map_err(|e| {
            Error::collection_schema_json(
                format!(
                    "failed to serialize old schema version '{}'",
                    old_version_id
                ),
                e,
            )
        })?;
        let new_version_key = CollectionKey::new(&new_version_id);
        let new_version_data = serde_json::to_vec(&new_schema).map_err(|e| {
            Error::collection_schema_json(
                format!(
                    "failed to serialize new schema version '{}'",
                    new_version_id
                ),
                e,
            )
        })?;
        let name_key = CollectionNameKey::new(actual_name);
        let version_index_key = CollectionVersionKey::new(collection_id, &new_version_id);
        let old_version_index_key = CollectionVersionKey::new(collection_id, old_version_id);

        // Perform all writes in a scoped block so systemstore reference is dropped
        {
            let systemstore = txn.systemstore()?;

            // 1. Store old version at /collection/id/{old_version_id} with is_active = false
            systemstore
                .set(&old_version_key.bytes(), &old_version_data)
                .await
                .map_err(Error::Storage)?;

            // 2. Store new version at /collection/id/{new_version_id}
            systemstore
                .set(&new_version_key.bytes(), &new_version_data)
                .await
                .map_err(Error::Storage)?;

            // 3. Update /collection/name/{name} - only point to new version if it's active.
            // If new version is inactive, keep name pointing to old version (which stays active).
            if new_schema.is_active {
                systemstore
                    .set(&name_key.bytes(), new_version_id.as_bytes())
                    .await
                    .map_err(Error::Storage)?;
            }

            // 4. Add version index at /collection/version/{collection_id}/{new_version_id}
            systemstore
                .set(&version_index_key.bytes(), b"1")
                .await
                .map_err(Error::Storage)?;

            // 5. Also ensure old version is in the version index (may already exist)
            systemstore
                .set(&old_version_index_key.bytes(), b"1")
                .await
                .map_err(Error::Storage)?;

            if let Some((transform_id, config)) = &committed_migration {
                let lens_key = LensConfigKey::new(transform_id.to_string());
                let lens_data = serde_json::to_vec(config)
                    .map_err(|e| Error::lens_config_json("failed to serialize lens config", e))?;
                systemstore
                    .set(&lens_key.bytes(), &lens_data)
                    .await
                    .map_err(Error::Storage)?;
            }
        } // systemstore reference dropped here

        // Store field and collection definition blocks in blockstore for Bitswap sync.
        // This mirrors create_collection_with_txn's block storage but only for NEW fields
        // and uses the patch-specific heads/priority.
        {
            let blockstore = txn.blockstore()?;

            // Identify new fields (same logic as generate_patch_version_id_with_heads)
            let old_field_names: std::collections::HashSet<&str> = old_schema
                .fields
                .iter()
                .filter(|f| !f.id.is_empty())
                .map(|f| f.name.as_str())
                .collect();

            let mut new_field_indices: Vec<usize> = new_schema
                .fields
                .iter()
                .enumerate()
                .filter(|(_, f)| {
                    let is_new = !old_field_names.contains(f.name.as_str());
                    let is_secondary_relation = f.relation_name.is_some() && !f.is_primary;
                    is_new && !is_secondary_relation
                })
                .map(|(i, _)| i)
                .collect();
            new_field_indices.sort_by(|&a, &b| {
                let fa = &new_schema.fields[a];
                let fb = &new_schema.fields[b];
                if fa.name == "_docID" {
                    std::cmp::Ordering::Less
                } else if fb.name == "_docID" {
                    std::cmp::Ordering::Greater
                } else {
                    fa.name.cmp(&fb.name)
                }
            });

            // Generate and store field blocks for new fields
            let mut field_cids = Vec::new();
            for &idx in &new_field_indices {
                let field = &new_schema.fields[idx];
                match schema::generate_field_block_with_priority_and_heads(field, 1, &[]) {
                    Ok(block_with_cid) => {
                        blockstore
                            .set(&block_with_cid.cid.to_bytes(), &block_with_cid.bytes)
                            .await
                            .map_err(Error::Storage)?;
                        field_cids.push(block_with_cid.cid);
                    }
                    Err(e) => {
                        tracing::warn!(
                            field_name = %field.name,
                            error = %e,
                            "Failed to generate field block for patch"
                        );
                    }
                }
            }

            // Generate and store the collection definition block
            let name_changed = new_schema.name != old_schema.name;
            let col_name = if name_changed {
                Some(new_schema.name.as_str())
            } else {
                None
            };
            match schema::generate_collection_block_full_with_query(
                col_name,
                &field_cids,
                collection_priority,
                &collection_heads,
                query_select.as_deref(),
                query_transform.as_ref(),
            ) {
                Ok(block_with_cid) => {
                    blockstore
                        .set(&block_with_cid.cid.to_bytes(), &block_with_cid.bytes)
                        .await
                        .map_err(Error::Storage)?;
                }
                Err(e) => {
                    tracing::warn!(
                        collection_name = %actual_name,
                        error = %e,
                        "Failed to generate collection block for patch"
                    );
                }
            }
        }

        txn.commit().await?;

        if let Some((transform_id, config)) = committed_migration {
            self.bump_migration_generation();
            if let Err(error) = self
                .lens_store()
                .add_with_id(transform_id.clone(), config)
                .await
            {
                tracing::warn!(
                    transform_id = %transform_id,
                    error = %error,
                    "failed to promote collection patch migration lens"
                );
            }
        }

        // Publish the new head only after all durable patch writes succeed.
        if let Ok(new_cid) = cid::Cid::try_from(new_version_id.as_str()) {
            if let Ok(mut heads) = self.schema_heads.write() {
                heads.insert(
                    actual_name.to_string(),
                    (vec![new_cid], collection_priority),
                );
            }
        }

        // Clean up any pending migration that was linked to this version
        {
            let mut pending = self.pending_migrations.write().map_err(|e| {
                tracing::error!(error = ?e, "Pending migrations lock poisoned during cleanup");
                Error::CacheUpdateFailedAfterCommit(collection_name.to_string())
            })?;
            pending.remove(&new_version_id);
        }

        // Update cache based on which version is active
        {
            let mut cache = self.collections.write().map_err(|e| {
                tracing::error!(
                    error = ?e,
                    collection_name = %collection_name,
                    "Collection cache lock poisoned during patch_collection update"
                );
                Error::CacheUpdateFailedAfterCommit(collection_name.to_string())
            })?;
            if new_schema.is_active {
                // New version is active - cache it under the actual collection name
                // (not collection_name, which might be a version_id for branching patches)
                cache.insert(actual_name.to_string(), Collection::new(new_schema.clone()));
            }
            // If new version is inactive, old version stays in cache (already there)
        }

        // Cross-collection one-to-one index creation.
        // When collections are patched sequentially (e.g., Author then Book), the
        // primary side (Author) may not get its unique index during its own patch
        // because the other side (Book) didn't have the relation field yet. Now that
        // this collection is stored, check if any OTHER collection has a primary
        // non-array relation that targets this collection and now forms a one-to-one,
        // and add the missing unique index.
        if new_schema.is_active {
            self.create_cross_collection_one_to_one_indexes(&new_schema)
                .await?;
        }

        // After switching active versions, reindex if the new version's history has migrations
        if new_schema.is_active {
            if let Err(e) = self.maybe_reindex_on_version_switch(actual_name).await {
                tracing::warn!(
                    error = %e,
                    collection = %actual_name,
                    "Failed to reindex after version switch"
                );
            }
        }

        Ok(new_schema)
    }

    /// Check other active collections for primary relation fields that target
    /// `just_patched` and now form one-to-one relations needing unique indexes.
    async fn create_cross_collection_one_to_one_indexes(
        &self,
        just_patched: &CollectionVersion,
    ) -> Result<()> {
        // Collect candidates from the cache: other collections with primary non-array
        // relation fields pointing at just_patched.
        let candidates: Vec<(String, CollectionVersion)> = {
            let cache = self
                .collections
                .read()
                .map_err(|_| Error::LockPoisoned("collection cache lock poisoned".into()))?;
            cache
                .iter()
                .filter(|(name, _)| name.as_str() != just_patched.name)
                .map(|(name, col)| (name.clone(), col.schema().clone()))
                .collect()
        };

        for (coll_name, other_schema) in &candidates {
            let mut needs_update = false;
            let mut updated_schema = other_schema.clone();

            let max_index_id = updated_schema
                .indexes
                .iter()
                .map(|idx| idx.id)
                .max()
                .unwrap_or(0);
            let mut next_id = max_index_id;

            for field in &other_schema.fields {
                if !field.kind.is_relation() || field.kind.is_array() || !field.is_primary {
                    continue;
                }
                let rel_name = match field.relation_name.as_ref() {
                    Some(n) => n,
                    None => continue,
                };
                let target_col_id = match field.kind.relation_collection_id() {
                    Some(id) => id,
                    None => continue,
                };
                // Only interested in relations targeting the just-patched collection
                if target_col_id != just_patched.name && target_col_id != just_patched.collection_id
                {
                    continue;
                }
                // Find the matching field on just_patched
                let matching_field =
                    just_patched.field_by_relation(rel_name, &other_schema.name, &field.name);
                let is_one_to_one = matching_field.map(|f| !f.kind.is_array()).unwrap_or(false);
                if !is_one_to_one {
                    continue;
                }
                // Check if unique index already exists
                match updated_schema.ensure_one_to_one_unique_index(&field.name, &mut || {
                    next_id += 1;
                    next_id
                }) {
                    Ok(Some(index)) => {
                        updated_schema.indexes.push(index);
                        needs_update = true;
                    }
                    Ok(None) => {}
                    Err(e) => return Err(Error::InvalidPatch(e.to_string())),
                }
            }

            if needs_update {
                // Store updated schema with new indexes
                let txn = self.new_txn(false).await?;
                let key = CollectionKey::new(&updated_schema.version_id);
                let data = serde_json::to_vec(&updated_schema).map_err(|e| {
                    Error::collection_schema_json(
                        format!(
                            "failed to serialize updated schema '{}'",
                            updated_schema.version_id
                        ),
                        e,
                    )
                })?;
                {
                    let systemstore = txn.systemstore()?;
                    systemstore
                        .set(&key.bytes(), &data)
                        .await
                        .map_err(Error::Storage)?;
                }
                txn.commit().await?;

                // Update cache
                let mut cache = self
                    .collections
                    .write()
                    .map_err(|_| Error::LockPoisoned("collection cache lock poisoned".into()))?;
                cache.insert(coll_name.clone(), Collection::new(updated_schema));
            }
        }

        Ok(())
    }
}
