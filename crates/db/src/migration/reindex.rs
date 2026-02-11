//! Reindexing after migration registration.

use std::collections::HashMap;

use lens::{build_targeted_history, CollectionHistoryLink, Lens, LensDoc, DOC_ID_FIELD};
use storage::corekv::Store;
use tracing::instrument;

use super::helpers::json_to_native_value;
use crate::collection::collection_short_id;
use crate::error::{Error, Result};
use crate::index_manager::IndexManager;
use crate::schema_loader::get_collections_by_collection_id;
use crate::DB;

impl<S: Store> DB<S> {
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

        let write_txn = self.new_txn(false).await?;

        {
            let datastore = write_txn.datastore()?;

            let raw_docs = collection.get_all_with_datastore(&datastore).await?;

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

                migrated_docs.push(doc);
            }

            let index_manager = IndexManager::from_collection(short_id, collection.schema())
                .map_err(|e| Error::Other(format!("failed to create index manager: {}", e)))?;

            for index_desc in collection.get_indexes() {
                if let Some(index) = index_manager.get_index(&index_desc.name) {
                    index
                        .remove_all(&mut datastore.clone())
                        .await
                        .map_err(Error::Storage)?;
                }

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
        }

        write_txn.commit().await?;

        Ok(())
    }
}
