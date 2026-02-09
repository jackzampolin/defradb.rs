//! Lens migration operations for schema versioning.
//!
//! This module handles registration and execution of lens migrations
//! between schema versions. Migrations allow documents stored with
//! older schema versions to be transformed when fetched.

use std::collections::HashMap;
use std::sync::Arc;

use lens::{
    build_targeted_history, CollectionHistoryLink, Lens, LensConfig, LensDoc, TransformId,
    TransformStore, DOC_ID_FIELD,
};
use schema::{CollectionSource, CollectionVersion, FieldKind, ScalarKind, ORPHAN_COLLECTION_ID};
use storage::corekv::{Key, Store};
use storage::keys::systemstore::{CollectionKey, CollectionVersionKey};
use tracing::instrument;

use crate::collection::{collection_short_id, Collection};
use crate::error::{Error, Result};
use crate::index_manager::IndexManager;
use crate::schema_loader::get_collections_by_collection_id;
use crate::txn::DbTxn;
use crate::DB;

impl<S: Store> DB<S> {
    /// Get a reference to the lens transform store.
    ///
    /// The lens store manages schema migration transforms that can be applied
    /// when documents are fetched from older schema versions.
    pub fn lens_store(&self) -> &Arc<dyn TransformStore> {
        &self.lens_store
    }

    /// Set a migration between two schema versions.
    ///
    /// This registers a lens transform that will be applied to documents
    /// when migrating from the source schema version to the destination.
    ///
    /// # Arguments
    ///
    /// * `config` - The lens configuration containing source/destination versions and transform
    ///
    /// # Returns
    ///
    /// The transform ID that was registered.
    #[instrument(skip(self, config), fields(
        source = %config.source_schema_version_id,
        dest = %config.destination_schema_version_id
    ))]
    pub async fn set_migration(&self, config: LensConfig) -> Result<TransformId> {
        let dest_version_id = config.destination_schema_version_id.clone();
        let source_version_id = config.source_schema_version_id.clone();

        let txn = self.new_txn(false).await?;

        // Look up source and destination versions, creating placeholders if needed
        // (matches Go's setMigration in internal/db/lens.go)
        let (source_col, mut dst_col) = {
            let systemstore = txn.systemstore()?;

            // Look up source version
            let src_key = CollectionKey::new(&source_version_id);
            let src_data = systemstore
                .get(&src_key.bytes())
                .await
                .map_err(Error::Storage)?;
            let source_col: CollectionVersion = match src_data {
                Some(data) => serde_json::from_slice(&data).map_err(|e| {
                    Error::Serialization(format!(
                        "failed to deserialize source schema '{}': {}",
                        source_version_id, e
                    ))
                })?,
                None => {
                    // Source doesn't exist — create a placeholder
                    let placeholder = create_orphan_placeholder(&source_version_id, "", "");
                    let data = serde_json::to_vec(&placeholder).map_err(|e| {
                        Error::Serialization(format!(
                            "failed to serialize source placeholder '{}': {}",
                            source_version_id, e
                        ))
                    })?;
                    systemstore
                        .set(&src_key.bytes(), &data)
                        .await
                        .map_err(Error::Storage)?;
                    placeholder
                }
            };

            // Look up destination version
            let dst_key = CollectionKey::new(&dest_version_id);
            let dst_data = systemstore
                .get(&dst_key.bytes())
                .await
                .map_err(Error::Storage)?;
            let dst_col: CollectionVersion = match dst_data {
                Some(data) => serde_json::from_slice(&data).map_err(|e| {
                    Error::Serialization(format!(
                        "failed to deserialize destination schema '{}': {}",
                        dest_version_id, e
                    ))
                })?,
                None => {
                    // Destination doesn't exist — create a placeholder
                    let placeholder = create_placeholder_with_source(
                        &dest_version_id,
                        &source_col.name,
                        &source_col.collection_id,
                    );
                    // Store destination placeholder (same as source placeholder above)
                    let data = serde_json::to_vec(&placeholder).map_err(|e| {
                        Error::Serialization(format!(
                            "failed to serialize destination placeholder '{}': {}",
                            dest_version_id, e
                        ))
                    })?;
                    systemstore
                        .set(&dst_key.bytes(), &data)
                        .await
                        .map_err(Error::Storage)?;
                    placeholder
                }
            };

            (source_col, dst_col)
        };

        // Validate version adjacency
        if let Some(ref prev) = dst_col.previous_version {
            if prev.source_collection_id != source_col.version_id {
                return Err(Error::InvalidPatch(format!(
                    "cannot migrate between non-adjacent collection versions. \
                     Destination '{}' already has previous version '{}', but migration source is '{}'",
                    dest_version_id, prev.source_collection_id, source_version_id
                )));
            }
        }

        // Register the transform in the lens store
        let transform_id = self
            .lens_store
            .add(config)
            .await
            .map_err(|e| Error::Lens(e.to_string()))?;

        // Set the destination's previous_version with source and transform
        dst_col.previous_version = Some(CollectionSource {
            source_collection_id: source_col.version_id.clone(),
            transform: Some(transform_id.to_string()),
        });

        tracing::debug!(
            dest_version_id = %dest_version_id,
            source_version_id = %source_version_id,
            is_placeholder = dst_col.is_placeholder,
            transform_id = %transform_id,
            "set_migration: storing destination version with transform"
        );

        // Save the destination version
        let collection_name = dst_col.name.clone();
        let dst_key = CollectionKey::new(&dest_version_id);
        let dst_data = serde_json::to_vec(&dst_col).map_err(|e| {
            Error::Serialization(format!(
                "failed to serialize destination schema '{}': {}",
                dest_version_id, e
            ))
        })?;

        {
            let systemstore = txn.systemstore()?;
            systemstore
                .set(&dst_key.bytes(), &dst_data)
                .await
                .map_err(Error::Storage)?;

            // Write CollectionVersionKey entries so get_collection_version_ids() can
            // find these versions via prefix scan on /collection/version/{collection_id}/
            if !source_col.collection_id.is_empty() {
                let src_version_key =
                    CollectionVersionKey::new(&source_col.collection_id, &source_version_id);
                systemstore
                    .set(&src_version_key.bytes(), b"1")
                    .await
                    .map_err(Error::Storage)?;
            }
            if !dst_col.collection_id.is_empty() {
                let dst_version_key =
                    CollectionVersionKey::new(&dst_col.collection_id, &dest_version_id);
                systemstore
                    .set(&dst_version_key.bytes(), b"1")
                    .await
                    .map_err(Error::Storage)?;
            }
        }
        txn.commit().await?;

        // Update in-memory cache if this is the active collection
        if !collection_name.is_empty() {
            let mut cache = self.collections.write().map_err(|e| {
                tracing::error!(error = ?e, "Collection cache lock poisoned during set_migration");
                Error::LockPoisoned("collection cache lock poisoned during set_migration".into())
            })?;

            if let Some(cached) = cache.get(&collection_name) {
                if cached.schema().version_id == dest_version_id {
                    cache.insert(collection_name.clone(), Collection::new(dst_col));
                }
            }
        }

        // Rebuild secondary indexes if the destination version is the active collection
        // and has indexes (matches Go's behavior of reindexing after migration registration)
        if !collection_name.is_empty() {
            if let Err(e) = self
                .maybe_reindex_after_migration(&collection_name, &dest_version_id)
                .await
            {
                tracing::warn!(
                    error = %e,
                    collection = %collection_name,
                    "Failed to reindex after migration"
                );
            }
        }

        Ok(transform_id)
    }

    /// Set a migration within an existing transaction context.
    ///
    /// This performs the same operations as `set_migration` but uses the provided
    /// transaction instead of creating a new one. The caller is responsible for
    /// committing or rolling back the transaction.
    ///
    /// This is used for transaction-aware migration configuration via the FFI.
    #[instrument(skip(self, txn, config), fields(
        source = %config.source_schema_version_id,
        dest = %config.destination_schema_version_id
    ))]
    pub async fn set_migration_in_txn(
        &self,
        txn: &DbTxn<S>,
        config: LensConfig,
    ) -> Result<TransformId> {
        let dest_version_id = config.destination_schema_version_id.clone();
        let source_version_id = config.source_schema_version_id.clone();

        // Look up source and destination versions, creating placeholders if needed
        let (source_col, mut dst_col) = {
            let systemstore = txn.systemstore()?;

            // Look up source version
            let src_key = CollectionKey::new(&source_version_id);
            let src_data = systemstore
                .get(&src_key.bytes())
                .await
                .map_err(Error::Storage)?;
            let source_col: CollectionVersion = match src_data {
                Some(data) => serde_json::from_slice(&data).map_err(|e| {
                    Error::Serialization(format!(
                        "failed to deserialize source schema '{}': {}",
                        source_version_id, e
                    ))
                })?,
                None => {
                    // Source doesn't exist — create a placeholder
                    let placeholder = create_orphan_placeholder(&source_version_id, "", "");
                    let data = serde_json::to_vec(&placeholder).map_err(|e| {
                        Error::Serialization(format!(
                            "failed to serialize source placeholder '{}': {}",
                            source_version_id, e
                        ))
                    })?;
                    systemstore
                        .set(&src_key.bytes(), &data)
                        .await
                        .map_err(Error::Storage)?;
                    placeholder
                }
            };

            // Look up destination version
            let dst_key = CollectionKey::new(&dest_version_id);
            let dst_data = systemstore
                .get(&dst_key.bytes())
                .await
                .map_err(Error::Storage)?;
            let dst_col: CollectionVersion = match dst_data {
                Some(data) => serde_json::from_slice(&data).map_err(|e| {
                    Error::Serialization(format!(
                        "failed to deserialize destination schema '{}': {}",
                        dest_version_id, e
                    ))
                })?,
                None => {
                    // Destination doesn't exist — create a placeholder
                    let placeholder = create_placeholder_with_source(
                        &dest_version_id,
                        &source_col.name,
                        &source_col.collection_id,
                    );
                    let data = serde_json::to_vec(&placeholder).map_err(|e| {
                        Error::Serialization(format!(
                            "failed to serialize destination placeholder '{}': {}",
                            dest_version_id, e
                        ))
                    })?;
                    systemstore
                        .set(&dst_key.bytes(), &data)
                        .await
                        .map_err(Error::Storage)?;
                    placeholder
                }
            };

            (source_col, dst_col)
        };

        // Validate version adjacency
        if let Some(ref prev) = dst_col.previous_version {
            if prev.source_collection_id != source_col.version_id {
                return Err(Error::InvalidPatch(format!(
                    "cannot migrate between non-adjacent collection versions. \
                     Destination '{}' already has previous version '{}', but migration source is '{}'",
                    dest_version_id, prev.source_collection_id, source_version_id
                )));
            }
        }

        // Register the transform in the lens store
        let transform_id = self
            .lens_store
            .add(config)
            .await
            .map_err(|e| Error::Lens(e.to_string()))?;

        // Set the destination's previous_version with source and transform
        dst_col.previous_version = Some(CollectionSource {
            source_collection_id: source_col.version_id.clone(),
            transform: Some(transform_id.to_string()),
        });

        tracing::debug!(
            dest_version_id = %dest_version_id,
            source_version_id = %source_version_id,
            is_placeholder = dst_col.is_placeholder,
            transform_id = %transform_id,
            "set_migration_in_txn: storing destination version with transform"
        );

        // Save the destination version
        let collection_name = dst_col.name.clone();
        let dst_key = CollectionKey::new(&dest_version_id);
        let dst_data = serde_json::to_vec(&dst_col).map_err(|e| {
            Error::Serialization(format!(
                "failed to serialize destination schema '{}': {}",
                dest_version_id, e
            ))
        })?;

        {
            let systemstore = txn.systemstore()?;
            systemstore
                .set(&dst_key.bytes(), &dst_data)
                .await
                .map_err(Error::Storage)?;

            // Write CollectionVersionKey entries
            if !source_col.collection_id.is_empty() {
                let src_version_key =
                    CollectionVersionKey::new(&source_col.collection_id, &source_version_id);
                systemstore
                    .set(&src_version_key.bytes(), b"1")
                    .await
                    .map_err(Error::Storage)?;
            }
            if !dst_col.collection_id.is_empty() {
                let dst_version_key =
                    CollectionVersionKey::new(&dst_col.collection_id, &dest_version_id);
                systemstore
                    .set(&dst_version_key.bytes(), b"1")
                    .await
                    .map_err(Error::Storage)?;
            }
        }

        // NOTE: We don't commit here - caller is responsible for transaction lifecycle

        // Update in-memory cache if this is the active collection
        if !collection_name.is_empty() {
            let mut cache = self.collections.write().map_err(|e| {
                tracing::error!(error = ?e, "Collection cache lock poisoned during set_migration_in_txn");
                Error::LockPoisoned(
                    "collection cache lock poisoned during set_migration_in_txn".into(),
                )
            })?;

            if let Some(cached) = cache.get(&collection_name) {
                if cached.schema().version_id == dest_version_id {
                    cache.insert(collection_name.clone(), Collection::new(dst_col));
                }
            }
        }

        Ok(transform_id)
    }

    /// Rebuild secondary indexes for a collection after a migration is registered.
    ///
    /// Checks if the destination version is in the active version's history chain
    /// (not just an exact match). This handles cases where migrations are registered
    /// for ancestor versions that affect the active version's data.
    #[instrument(skip(self), fields(collection = %collection_name, dest_version = %dest_version_id))]
    pub(crate) async fn maybe_reindex_after_migration(
        &self,
        collection_name: &str,
        dest_version_id: &str,
    ) -> Result<()> {
        let collection = match self.get_collection(collection_name)? {
            Some(c) => c,
            None => {
                return Ok(());
            }
        };

        if collection.get_indexes().is_empty() {
            return Ok(());
        }

        // Check if dest_version_id is in the active version's history chain
        let collection_id = collection.collection_id().to_string();
        let target_version_id = collection.version_id().to_string();

        let read_txn = self.new_txn(true).await?;
        let systemstore = read_txn.systemstore()?;
        let versions = get_collections_by_collection_id(&systemstore, &collection_id).await?;
        let _ = read_txn.discard();

        let history = crate::lens_utils::build_collection_history(&versions, &target_version_id);
        let in_history = history
            .as_ref()
            .is_some_and(|h| h.contains_key(dest_version_id));

        if !in_history {
            return Ok(());
        }

        self.reindex_collection_with_migrations(collection_name)
            .await
    }

    /// Reindex a collection after the active version changes (e.g., via patch).
    ///
    /// If the new active version's history contains any migrations, rebuild
    /// all secondary indexes with lens-migrated document values.
    pub(crate) async fn maybe_reindex_on_version_switch(
        &self,
        collection_name: &str,
    ) -> Result<()> {
        let collection = match self.get_collection(collection_name)? {
            Some(c) => c,
            None => return Ok(()),
        };

        if collection.get_indexes().is_empty() {
            return Ok(());
        }

        let collection_id = collection.collection_id().to_string();
        let target_version_id = collection.version_id().to_string();

        let read_txn = self.new_txn(true).await?;
        let systemstore = read_txn.systemstore()?;
        let versions = get_collections_by_collection_id(&systemstore, &collection_id).await?;
        let _ = read_txn.discard();

        let history = crate::lens_utils::build_collection_history(&versions, &target_version_id);
        let has_migrations = history
            .as_ref()
            .is_some_and(|h| h.values().any(|link| link.transform.is_some()));

        if !has_migrations {
            return Ok(());
        }

        self.reindex_collection_with_migrations(collection_name)
            .await
    }

    /// Rebuild secondary indexes for a collection using lens-migrated documents.
    ///
    /// Fetches all documents, applies lens migration to any that need it,
    /// drops existing index entries, and rebuilds them with migrated values.
    pub async fn reindex_collection_with_migrations(&self, collection_name: &str) -> Result<()> {
        let collection = match self.get_collection(collection_name)? {
            Some(c) => c,
            None => return Ok(()),
        };

        let collection_id = collection.collection_id().to_string();
        let target_version_id = collection.version_id().to_string();
        let short_id = collection_short_id(&collection_id);

        // Load all versions and build migration history
        let read_txn = self.new_txn(true).await?;
        let systemstore = read_txn.systemstore()?;
        let versions = get_collections_by_collection_id(&systemstore, &collection_id).await?;
        let _ = read_txn.discard();

        let history = {
            let mut full_history: HashMap<String, CollectionHistoryLink> = HashMap::new();
            for version in &versions {
                let mut link =
                    CollectionHistoryLink::new(&version.version_id, &version.collection_id);
                if let Some(ref prev) = version.previous_version {
                    link = link.with_previous(&prev.source_collection_id);
                    if let Some(ref transform_id) = prev.transform {
                        link = link.with_transform(transform_id);
                    }
                }
                full_history.insert(version.version_id.clone(), link);
            }

            // Build next links
            let reverse_links: Vec<(String, String)> = full_history
                .values()
                .flat_map(|link| {
                    link.previous
                        .iter()
                        .map(|prev_id| (prev_id.clone(), link.version_id.clone()))
                        .collect::<Vec<_>>()
                })
                .collect();
            for (parent_id, child_id) in reverse_links {
                if let Some(parent_link) = full_history.get_mut(&parent_id) {
                    if !parent_link.next.contains(&child_id) {
                        parent_link.next.push(child_id);
                    }
                }
            }

            match build_targeted_history(&full_history, &target_version_id) {
                Some(h) => h,
                None => return Ok(()),
            }
        };

        let has_migrations = history.values().any(|link| link.transform.is_some());
        if !has_migrations {
            return Ok(());
        }

        // Create a write transaction for the reindex
        let write_txn = self.new_txn(false).await?;

        // Scope the datastore borrow so it's dropped before commit
        {
            let datastore = write_txn.datastore()?;

            // Fetch all documents (raw, with their stored schema versions)
            let raw_docs = collection.get_all_with_datastore(&datastore).await?;

            // Apply lens migration to each document that needs it
            let mut migrated_docs = Vec::with_capacity(raw_docs.len());
            for doc in raw_docs {
                let doc_version = doc
                    .schema_version_id()
                    .unwrap_or(&target_version_id)
                    .to_string();

                if doc_version == target_version_id {
                    migrated_docs.push(doc);
                    continue;
                }

                // Convert to LensDoc
                if let Ok(map) = doc.to_map() {
                    let mut lens_doc = LensDoc::new();
                    for (key, value) in map {
                        lens_doc.insert(key, value);
                    }

                    let mut lens =
                        Lens::new(self.lens_store.clone(), &target_version_id, history.clone());

                    if let Ok(()) = lens.put(&doc_version, lens_doc).await {
                        if let Some(Ok(migrated_lens_doc)) = lens.next().await {
                            let mut migrated = document::Document::new();
                            if let Some(id) = doc.id() {
                                migrated.set_id(id.clone());
                            }
                            for (field_name, value) in migrated_lens_doc {
                                if field_name != DOC_ID_FIELD {
                                    let native_value = json_to_native_value(
                                        &value,
                                        &field_name,
                                        collection.schema(),
                                    );
                                    migrated.set(&field_name, native_value);
                                }
                            }
                            migrated.set_schema_version_id(&target_version_id);
                            migrated_docs.push(migrated);
                            continue;
                        }
                    }
                }

                // If migration fails for a doc, keep original
                migrated_docs.push(doc);
            }

            // Rebuild indexes: drop all entries, re-index from migrated documents
            let index_manager = IndexManager::from_collection(short_id, collection.schema())
                .map_err(|e| Error::Other(format!("failed to create index manager: {}", e)))?;

            for index_desc in collection.get_indexes() {
                // Drop existing entries
                if let Some(index) = index_manager.get_index(&index_desc.name) {
                    index
                        .remove_all(&mut datastore.clone())
                        .await
                        .map_err(Error::Storage)?;
                }

                // Bulk re-index with migrated documents
                index_manager
                    .bulk_index(
                        &datastore,
                        &index_desc.name,
                        &migrated_docs,
                        collection.schema(),
                    )
                    .await?;
            }

            tracing::debug!(
                collection = %collection_name,
                doc_count = migrated_docs.len(),
                index_count = collection.get_indexes().len(),
                "Rebuilt indexes after migration"
            );
        } // datastore reference dropped here

        write_txn.commit().await?;

        Ok(())
    }

    /// Check if a migration exists between two schema versions.
    pub fn has_migration(&self, transform_id: &TransformId) -> bool {
        self.lens_store.has_transform(transform_id)
    }
}

/// Create an orphan placeholder collection version.
///
/// Used when a migration references a version that doesn't exist yet.
fn create_orphan_placeholder(
    version_id: &str,
    name: &str,
    collection_id: &str,
) -> CollectionVersion {
    let mut placeholder = CollectionVersion {
        version_id: version_id.to_string(),
        collection_id: if collection_id.is_empty() {
            ORPHAN_COLLECTION_ID.to_string()
        } else {
            collection_id.to_string()
        },
        name: name.to_string(),
        is_materialized: true,
        is_placeholder: true,
        ..CollectionVersion::new("", "", "", Vec::new())
    };
    placeholder.is_active = false;
    placeholder
}

/// Create a placeholder with source collection info.
fn create_placeholder_with_source(
    version_id: &str,
    source_name: &str,
    source_collection_id: &str,
) -> CollectionVersion {
    let mut placeholder = CollectionVersion {
        name: source_name.to_string(),
        version_id: version_id.to_string(),
        collection_id: source_collection_id.to_string(),
        is_materialized: true,
        is_placeholder: true,
        ..CollectionVersion::new("", "", "", Vec::new())
    };
    placeholder.is_active = false;
    placeholder
}

/// Convert a JSON value to a native NormalValue based on the field's schema type.
///
/// When documents are migrated through lens transforms, they come back as JSON values.
/// This function converts them to the appropriate native type (Int, Float, String, etc.)
/// based on the field's declared type in the schema.
pub fn json_to_native_value(
    value: &serde_json::Value,
    field_name: &str,
    schema: &CollectionVersion,
) -> document::NormalValue {
    // Handle null values
    if value.is_null() {
        return document::NormalValue::Null;
    }

    // Find the field definition in the schema
    let field_kind = schema
        .fields
        .iter()
        .find(|f| f.name == field_name)
        .map(|f| &f.kind);

    if let Some(FieldKind::Scalar(scalar)) = field_kind {
        match scalar {
            ScalarKind::Int => {
                if let Some(n) = value.as_i64() {
                    return document::NormalValue::Int(n);
                }
            }
            ScalarKind::Float64 => {
                if let Some(n) = value.as_f64() {
                    return document::NormalValue::Float64(n);
                }
            }
            ScalarKind::Float32 => {
                if let Some(n) = value.as_f64() {
                    return document::NormalValue::Float32(n as f32);
                }
            }
            ScalarKind::Bool => {
                if let Some(b) = value.as_bool() {
                    return document::NormalValue::Bool(b);
                }
            }
            ScalarKind::String | ScalarKind::DocID => {
                if let Some(s) = value.as_str() {
                    return document::NormalValue::String(s.to_string());
                }
            }
            ScalarKind::Blob => {
                // Blobs may be base64 encoded strings in JSON
                if let Some(s) = value.as_str() {
                    return document::NormalValue::Bytes(s.as_bytes().to_vec());
                }
            }
            ScalarKind::DateTime => {
                // DateTime as string - keep as string for now, the document layer handles parsing
                if let Some(s) = value.as_str() {
                    return document::NormalValue::String(s.to_string());
                }
            }
            ScalarKind::Json | ScalarKind::None => {
                // Keep as JSON
            }
        }
    }

    // Fallback: keep as JSON (this preserves the original behavior for unknown types)
    document::NormalValue::Json(value.clone())
}
