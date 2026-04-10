use super::*;

impl<S: Store> crate::database::DB<S> {
    /// Get the next collection short ID from the sequence key.
    pub(crate) async fn next_collection_short_id(systemstore: &NamespaceView) -> Result<u32> {
        let seq_key = CollectionIDSequenceKey;
        let key_bytes = seq_key.bytes();
        let current: u32 = match systemstore.get(&key_bytes).await.map_err(Error::Storage)? {
            Some(bytes) => {
                if bytes.len() == 4 {
                    u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
                } else {
                    0
                }
            }
            None => 0,
        };
        let next = current + 1;
        systemstore
            .set(&key_bytes, &next.to_be_bytes())
            .await
            .map_err(Error::Storage)?;
        Ok(next)
    }

    /// Create a collection within an existing transaction.
    ///
    /// This method validates the collection schema, assigns a unique short ID,
    /// stores the schema in the systemstore, and stores field/collection blocks
    /// in the blockstore for P2P sync.
    ///
    /// # Arguments
    ///
    /// * `txn` - The transaction to use
    /// * `schema` - The collection schema (will be validated and potentially modified)
    ///
    /// # Returns
    ///
    /// The finalized schema with assigned short ID and field IDs.
    ///
    /// # Errors
    ///
    /// - `InvalidCollectionName` if the collection name is invalid
    /// - `CollectionAlreadyExists` if a collection with this name already exists
    #[instrument(skip(self, txn, schema), fields(collection = %schema.name), name = "db.create_collection")]
    pub async fn create_collection_with_txn(
        &self,
        txn: &mut DbTxn<S>,
        mut schema: CollectionVersion,
    ) -> Result<CollectionVersion> {
        // Validate collection name
        let collection_name = CollectionName::new(&schema.name)?;

        // Validate schema (includes policy validation for path traversal prevention)
        schema.validate()?;
        let name = collection_name.as_str().to_string();

        // For views, regenerate version_id to include query_select in the CID
        // (matching Go's saveBlocks which includes querySelect in the delta).
        // This must happen before any systemstore writes that use version_id.
        if schema.query.is_some() {
            let (qs_bytes, qt_cid) = if let Some(ref qs) = schema.query {
                let select_cbor = serde_cbor::to_vec(&qs.query).unwrap_or_default();
                let transform_cid = qs
                    .transform
                    .as_ref()
                    .and_then(|t| cid::Cid::try_from(t.as_str()).ok());
                (Some(select_cbor), transform_cid)
            } else {
                (None, None)
            };

            // Regenerate field CIDs to compute the correct version_id
            let mut sorted: Vec<&schema::FieldDescription> =
                schema.fields.iter().filter(|f| !f.id.is_empty()).collect();
            sorted.sort_by(|a, b| {
                if a.name == "_docID" {
                    std::cmp::Ordering::Less
                } else if b.name == "_docID" {
                    std::cmp::Ordering::Greater
                } else {
                    a.name.cmp(&b.name)
                }
            });
            let mut fld_cids = Vec::new();
            for field in &sorted {
                if let Ok(cid) = schema::generate_field_cid_with_priority(field, 1) {
                    fld_cids.push(cid);
                }
            }

            if let Ok(new_cid) = schema::generate_collection_cid_full_with_query(
                Some(&schema.name),
                &fld_cids,
                1,
                &[],
                qs_bytes.as_deref(),
                qt_cid.as_ref(),
            ) {
                let new_version_id = new_cid.to_string();
                let old_version_id = schema.version_id.clone();
                schema.version_id = new_version_id.clone();
                if schema.collection_id == old_version_id {
                    schema.collection_id = new_version_id;
                }
            }
        }

        let version_id = &schema.version_id.clone();
        let collection_id = &schema.collection_id.clone();

        // Check if collection exists in txn cache or store
        if txn.get_collection(&name).await?.is_some() {
            return Err(Error::CollectionAlreadyExists(name));
        }

        let systemstore = txn.systemstore()?;

        // Assign sequential short ID (matches Go's monotonic counter)
        let short_id = Self::next_collection_short_id(&systemstore).await?;
        schema.root_id = short_id;

        // Re-assign index IDs from the persistent sequence so they start at 1.
        // The SDL parser assigns placeholder IDs based on field_id_counter, but
        // Go assigns them via IndexManager.next_index_id() which uses a per-collection
        // sequence key. We replicate that here so IDs match Go exactly.
        if !schema.indexes.is_empty() {
            let seq_key = IndexIDSequenceKey::new(format!("{}", short_id));
            let key_bytes = seq_key.bytes();
            let mut current: u32 =
                match systemstore.get(&key_bytes).await.map_err(Error::Storage)? {
                    Some(bytes) => {
                        if bytes.len() == 4 {
                            u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
                        } else {
                            0
                        }
                    }
                    None => 0,
                };
            for idx in &mut schema.indexes {
                current += 1;
                idx.id = current;
            }
            systemstore
                .set(&key_bytes, &current.to_be_bytes())
                .await
                .map_err(Error::Storage)?;
        }

        // Store short ID mapping at /collection/shortID/{collection_id}
        let short_id_key = CollectionID::new(collection_id.as_str());
        systemstore
            .set(&short_id_key.bytes(), short_id.to_string().as_bytes())
            .await
            .map_err(Error::Storage)?;

        // 1. Store full schema at /collection/id/{version_id}
        let collection_key = CollectionKey::new(version_id.as_str());
        let data = serde_json::to_vec(&schema).map_err(|e| {
            Error::Serialization(format!(
                "failed to serialize schema for collection '{}': {}",
                name, e
            ))
        })?;
        systemstore
            .set(&collection_key.bytes(), &data)
            .await
            .map_err(Error::Storage)?;

        // Store field and collection definition blocks in blockstore for Bitswap sync.
        // Go stores these blocks so peers can fetch them via Bitswap during collection version sync.
        let blockstore = txn.blockstore()?;

        // Store each field definition block
        // IMPORTANT: Go uses priority=1 for ALL fields during AddSchema (not incrementing).
        // This was verified by comparing actual Go AddSchema output with manual CID generation.
        // Only fields with non-empty FieldID are stored (secondary relations are excluded).
        // Fields must be sorted: _docID first, then alphabetically by name (matches Go).
        let mut sorted_fields: Vec<&schema::FieldDescription> =
            schema.fields.iter().filter(|f| !f.id.is_empty()).collect();
        sorted_fields.sort_by(|a, b| {
            if a.name == "_docID" {
                std::cmp::Ordering::Less
            } else if b.name == "_docID" {
                std::cmp::Ordering::Greater
            } else {
                a.name.cmp(&b.name)
            }
        });

        let mut field_cids = Vec::with_capacity(sorted_fields.len());
        for field in &sorted_fields {
            // Generate field block with priority=1 (matches Go)
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
                        "Failed to generate field block CID"
                    );
                }
            }
        }

        // Store the collection definition block.
        // For views, include query_select and query_transform so the stored block's
        // CID matches the version_id (which was computed with these fields).
        let (qs_bytes_for_block, qt_cid_for_block) = if let Some(ref qs) = schema.query {
            let select_cbor = serde_cbor::to_vec(&qs.query).unwrap_or_default();
            let transform_cid = qs
                .transform
                .as_ref()
                .and_then(|t| cid::Cid::try_from(t.as_str()).ok());
            (Some(select_cbor), transform_cid)
        } else {
            (None, None)
        };
        match schema::generate_collection_block_full_with_query(
            Some(&schema.name),
            &field_cids,
            1,   // priority=1 for new collections
            &[], // no heads for new collections
            qs_bytes_for_block.as_deref(),
            qt_cid_for_block.as_ref(),
        ) {
            Ok(block_with_cid) => {
                blockstore
                    .set(&block_with_cid.cid.to_bytes(), &block_with_cid.bytes)
                    .await
                    .map_err(Error::Storage)?;
            }
            Err(e) => {
                tracing::warn!(
                    collection_name = %name,
                    error = %e,
                    "Failed to generate collection block"
                );
            }
        }

        // 2. Store name -> version_id mapping at /collection/name/{name}
        let name_key = CollectionNameKey::new(&name);
        systemstore
            .set(&name_key.bytes(), version_id.as_bytes())
            .await
            .map_err(Error::Storage)?;

        // 3. Store version index at /collection/version/{collection_id}/{version_id}
        let version_index_key = CollectionVersionKey::new(collection_id.as_str(), version_id);
        systemstore
            .set(&version_index_key.bytes(), b"1")
            .await
            .map_err(Error::Storage)?;

        // Update schema_heads: new collection starts at height=1
        if let Ok(cid) = cid::Cid::try_from(version_id.as_str()) {
            if let Ok(mut heads) = self.schema_heads.write() {
                heads.insert(name.clone(), (vec![cid], 1));
            }
        }

        // Add to transaction's cache
        txn.cache_collection(Collection::new(schema.clone()));

        tracing::info!(
            collection_name = %name,
            version_id = %version_id,
            collection_id = %collection_id,
            field_count = schema.fields.len(),
            "Created collection"
        );

        Ok(schema)
    }

    /// Create a new collection.
    ///
    /// This creates a new transaction, calls `create_collection_with_txn`, commits,
    /// and updates the process-wide cache.
    #[instrument(skip(self, schema), fields(collection = %schema.name), name = "db.create_collection_auto")]
    pub async fn create_collection(&self, schema: CollectionVersion) -> Result<()> {
        let name = schema.name.clone();
        let mut txn = self.new_txn(false).await?;

        let finalized_schema = self.create_collection_with_txn(&mut txn, schema).await?;

        txn.commit().await?;

        // Update the process-wide cache after successful commit
        let mut cache = self.collections.write().map_err(|e| {
            tracing::error!(error = ?e, collection_name = %name, "Collection cache lock poisoned after create");
            Error::CacheUpdateFailedAfterCommit(name.clone())
        })?;
        cache.insert(name, Collection::new(finalized_schema));

        Ok(())
    }

    /// Create multiple collections atomically in a single transaction.
    ///
    /// This is useful for creating related collections (e.g., with relations between them)
    /// where all must succeed or none should be created.
    pub async fn create_collections_atomic(
        &self,
        schemas: Vec<CollectionVersion>,
    ) -> Result<Vec<CollectionVersion>> {
        // Track old collection_id -> new collection_id mappings.
        // When views are created, create_collection_with_txn regenerates CIDs to include
        // query source data. Sibling schemas' relation fields may reference the old CIDs
        // and need to be updated afterward (mirrors Go's substituteSecondaryRelationFieldKinds).
        let old_ids: Vec<(String, String)> = schemas
            .iter()
            .map(|s| (s.name.clone(), s.collection_id.clone()))
            .collect();

        let mut txn = self.new_txn(false).await?;
        let mut finalized_schemas = Vec::with_capacity(schemas.len());

        for schema in schemas {
            let finalized = self.create_collection_with_txn(&mut txn, schema).await?;
            finalized_schemas.push(finalized);
        }

        // Build old_collection_id -> new_collection_id mapping
        let mut id_remap: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();
        for (name, old_id) in &old_ids {
            if let Some(finalized) = finalized_schemas.iter().find(|s| &s.name == name) {
                if *old_id != finalized.collection_id {
                    id_remap.insert(old_id.clone(), finalized.collection_id.clone());
                }
            }
        }

        // Also map by name -> new collection_id for Named field kinds
        let mut name_to_id: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();
        for schema in &finalized_schemas {
            name_to_id.insert(schema.name.clone(), schema.collection_id.clone());
        }

        // Update relation field kinds that reference old CIDs and re-save affected schemas
        if !id_remap.is_empty() {
            let systemstore = txn.systemstore()?;
            for schema in &mut finalized_schemas {
                let mut changed = false;
                for field in &mut schema.fields {
                    match &field.kind {
                        schema::FieldKind::Relation {
                            collection_id,
                            is_array,
                        } => {
                            if let Some(new_id) = id_remap.get(collection_id) {
                                field.kind = schema::FieldKind::Relation {
                                    collection_id: new_id.clone(),
                                    is_array: *is_array,
                                };
                                changed = true;
                            }
                        }
                        schema::FieldKind::Named { name, is_array } => {
                            if let Some(new_id) = name_to_id.get(name) {
                                field.kind = schema::FieldKind::Relation {
                                    collection_id: new_id.clone(),
                                    is_array: *is_array,
                                };
                                changed = true;
                            }
                        }
                        _ => {}
                    }
                }
                if changed {
                    let collection_key = CollectionKey::new(&schema.version_id);
                    let data = serde_json::to_vec(&schema).map_err(|e| {
                        Error::Serialization(format!(
                            "failed to re-serialize schema for '{}': {}",
                            schema.name, e
                        ))
                    })?;
                    systemstore
                        .set(&collection_key.bytes(), &data)
                        .await
                        .map_err(Error::Storage)?;
                }
            }
        }

        txn.commit().await?;

        // Update the process-wide cache after successful commit
        let mut cache = self.collections.write().map_err(|e| {
            tracing::error!(error = ?e, "Collection cache lock poisoned after atomic create");
            Error::CacheUpdateFailedAfterCommit("atomic collections".to_string())
        })?;

        for schema in &finalized_schemas {
            cache.insert(schema.name.clone(), Collection::new(schema.clone()));
        }

        Ok(finalized_schemas)
    }
}
