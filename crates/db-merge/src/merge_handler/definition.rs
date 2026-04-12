use super::*;

impl<S: Store, B: blockstore::Blockstore + Send + Sync> DbMergeHandler<S, B> {
    /// Process a CollectionDefinition delta - register synced collection schema in systemstore.
    ///
    /// When a peer receives collection definition blocks via Bitswap sync, this method
    /// reconstructs the `CollectionVersion` from the definition deltas and stores it
    /// in systemstore so `set_active_collection_version` can find and activate it.
    pub(crate) async fn process_collection_definition_delta(
        &self,
        cid: &Cid,
        block: &Block,
        payload: &CollectionDefinitionDeltaPayload,
        _metadata: &BlockMetadata<'_>,
    ) -> Result<MergeOutcome, MergeError> {
        // The version_id is the CID of this collection definition block
        let version_id = cid.to_string();

        // For patched versions, payload.name is None (name didn't change).
        // Resolve name and collection_id from the previous version via block.heads.
        let (collection_name, collection_id, prev_fields) = match &payload.name {
            Some(name) => {
                // Initial version: name is explicit, collection_id = version_id
                (name.clone(), version_id.clone(), Vec::new())
            }
            None => {
                // Patched version: look up previous version from heads
                let prev_version = self.resolve_previous_collection_version(block).await?;
                match prev_version {
                    Some(prev) => {
                        let name = prev.name.clone();
                        let col_id = prev.collection_id.clone();
                        let fields = prev.fields.clone();
                        (name, col_id, fields)
                    }
                    None => {
                        tracing::debug!(cid = %cid, "CollectionDefinition has no name and no resolvable previous version - skipping");
                        return Ok(MergeOutcome::terminal_skip(
                            "collection definition has no name and no previous version",
                        ));
                    }
                }
            }
        };

        tracing::info!(
            cid = %cid,
            collection_name = %collection_name,
            version_id = %version_id,
            "Processing collection definition delta"
        );

        // Load and decode linked field definition blocks (new fields for this version)
        let mut new_fields = Vec::new();
        if let Some(links) = &block.links {
            for link in links.iter() {
                let field_cid = &link.link;
                let field_bytes = self
                    .blockstore
                    .get(field_cid)
                    .await
                    .map_err(|e| MergeError::Storage(format!("Failed to load field block: {}", e)))?
                    .ok_or_else(|| {
                        MergeError::Storage(format!("Field block not found: {}", field_cid))
                    })?;

                let field_block = Block::from_dag_cbor(&field_bytes).map_err(|e| {
                    MergeError::BlockDecode(format!("Failed to decode field block: {}", e))
                })?;

                if let CrdtDelta::FieldDefinition(field_payload) = &field_block.delta {
                    let field_desc = self
                        .field_definition_to_description(field_payload, &field_cid.to_string())?;
                    new_fields.push(field_desc);
                } else {
                    tracing::warn!(
                        field_cid = %field_cid,
                        "Linked block is not a FieldDefinition - skipping"
                    );
                }
            }
        }

        // Merge previous version's fields with new fields from this delta.
        // For initial versions, prev_fields is empty so fields = new_fields.
        // For patched versions, combine existing fields + newly added fields.
        let mut fields = prev_fields;
        let existing_names: HashSet<String> = fields.iter().map(|f| f.name.clone()).collect();
        for field in new_fields {
            if !existing_names.contains(&field.name) {
                fields.push(field);
            }
        }

        // Ensure _docID is first in the fields list (Go expects this ordering)
        if let Some(docid_pos) = fields.iter().position(|f| f.name == "_docID") {
            if docid_pos > 0 {
                let docid_field = fields.remove(docid_pos);
                fields.insert(0, docid_field);
            }
        }

        // Build the CollectionVersion
        // Synced collections come in as inactive (user must activate manually via SetActiveCollectionVersion)
        // and materialized (matching Go's behavior)
        let mut schema =
            CollectionVersion::new(&collection_name, &version_id, &collection_id, fields);
        schema.is_active = false;

        // For patched versions, set previous_version to point to the head (previous version CID)
        if let Some(heads) = &block.heads {
            if let Some(head_cid) = heads.first() {
                schema.previous_version = Some(CollectionSource::new(head_cid.to_string()));
            }
        }

        // Views (collections with a query_select) are non-materialized and carry query metadata.
        // Regular collections are materialized.
        if let Some(ref query_bytes) = payload.query_select {
            schema.is_materialized = false;
            if let Ok(query_value) = serde_cbor::from_slice::<serde_json::Value>(query_bytes) {
                let mut source = QuerySource::new(query_value);
                if let Some(ref transform_cid) = payload.query_transform {
                    source.transform = Some(transform_cid.to_string());
                }
                schema.query = Some(source);
            } else {
                tracing::warn!(
                    cid = %cid,
                    "Failed to decode query_select CBOR bytes for view collection"
                );
            }
        } else {
            schema.is_materialized = true;
        }

        // Store in systemstore
        let txn = self.db.new_txn(false).await.map_err(MergeError::Database)?;
        {
            let systemstore = txn.systemstore().map_err(MergeError::Database)?;
            schema.root_id =
                db::collection::ensure_persisted_collection_short_id(&systemstore, &collection_id)
                    .await
                    .map_err(MergeError::Database)?;

            // 1. Store full schema at /collection/id/{version_id}
            let collection_key = CollectionKey::new(&version_id);
            let data = serde_json::to_vec(&schema).map_err(|e| {
                MergeError::Storage(format!("Failed to serialize collection schema: {}", e))
            })?;
            systemstore
                .set(&collection_key.bytes(), &data)
                .await
                .map_err(|e| MergeError::Storage(format!("Failed to store collection: {}", e)))?;

            // 2. Store version index at /collection/version/{collection_id}/{version_id}
            let version_key = CollectionVersionKey::new(&collection_id, &version_id);
            systemstore
                .set(&version_key.bytes(), b"1")
                .await
                .map_err(|e| {
                    MergeError::Storage(format!("Failed to store version index: {}", e))
                })?;
        }
        txn.commit().await.map_err(MergeError::Database)?;

        // Add to runtime cache so it's visible via list_collections/get_collection.
        // Synced collections are inactive but still need to be in the cache for
        // GetCollections with GetInactive=true to find them.
        self.db
            .add_collection_to_cache(schema.clone())
            .map_err(MergeError::Database)?;

        tracing::debug!(
            collection_name = %collection_name,
            version_id = %version_id,
            is_active = schema.is_active,
            is_materialized = schema.is_materialized,
            "Stored synced collection schema in cache"
        );

        tracing::info!(
            collection_name = %collection_name,
            version_id = %version_id,
            field_count = schema.fields.len(),
            "Registered synced collection schema in systemstore and cache (inactive, requires manual activation)"
        );

        Ok(MergeOutcome::Merged)
    }

    /// Convert a FieldDefinitionDeltaPayload to a FieldDescription.
    pub(crate) fn field_definition_to_description(
        &self,
        payload: &FieldDefinitionDeltaPayload,
        field_id: &str,
    ) -> Result<FieldDescription, MergeError> {
        let name = payload
            .name
            .clone()
            .unwrap_or_else(|| format!("field_{}", field_id));

        // Determine the FieldKind from the payload
        let kind = if let Some(collection_id) = &payload.collection_id {
            // Relation field
            FieldKind::Relation {
                collection_id: collection_id.clone(),
                is_array: false, // Default; actual value would need additional info
            }
        } else if let Some(relative_id) = payload.relative_id {
            // Self-referencing field
            FieldKind::SelfRef {
                relative_id: relative_id.to_string(),
                is_array: false,
            }
        } else if let Some(scalar_kind_u8) = payload.scalar_kind {
            // Scalar field - convert u8 to ScalarKind
            let scalar_kind = match scalar_kind_u8 {
                0 => ScalarKind::None,
                1 => ScalarKind::DocID,
                2 => ScalarKind::Bool,
                4 => ScalarKind::Int,
                6 => ScalarKind::Float64,
                8 => ScalarKind::Float32,
                10 => ScalarKind::DateTime,
                11 => ScalarKind::String,
                13 => ScalarKind::Blob,
                14 => ScalarKind::Json,
                _ => ScalarKind::None,
            };
            FieldKind::Scalar(scalar_kind)
        } else {
            // Default to None scalar
            FieldKind::Scalar(ScalarKind::None)
        };

        // Determine CRDT type
        let crdt_type = payload.crdt.map(CType::from_u8).unwrap_or_default();

        Ok(FieldDescription::new(field_id.to_string(), name, kind).with_crdt_type(crdt_type))
    }
}
