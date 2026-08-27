//! Datastore materialization and reindexing after schema migration.

use lens::Lens;
use storage::corekv::Store;
use tracing::{instrument, warn};

use super::helpers::{cache_document_version, cache_migrated_document, lens_doc_to_document};
use crate::definition::loader::get_collections_by_collection_id;
use crate::error::{Error, Result};
use crate::index::IndexManager;
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

        let history =
            crate::definition::lens::build_collection_history(&versions, &target_version_id);
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
    /// Version switches can preserve index definitions while changing the active
    /// schema. Rebuild entries for the active schema, applying lens migrations
    /// where the history contains transforms.
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

        self.reindex_collection_with_migrations(collection_name)
            .await
    }

    /// Rebuild secondary indexes for a collection using lens-migrated documents.
    ///
    /// Documents that cross a registered transform are also written back to the
    /// datastore so the rebuilt indexes and cached values cannot diverge.
    pub async fn reindex_collection_with_migrations(&self, collection_name: &str) -> Result<()> {
        if self.get_collection(collection_name)?.is_none() {
            return Ok(());
        }
        self.materialize_collection_inner(collection_name, false)
            .await
            .map(|_| ())
    }

    /// Eagerly advance every known-version document in a collection to its active version.
    ///
    /// This writes only the datastore blob, version key, and secondary indexes. It does not
    /// create CRDT commits, update heads, emit mutation events, or gossip data to peers.
    ///
    /// Returns the number of documents whose stored version was advanced.
    pub async fn materialize_collection(&self, collection_name: &str) -> Result<usize> {
        self.check_node_access(None, acp::nac::NodePermission::CollectionPatch)
            .await?;
        self.materialize_collection_inner(collection_name, true)
            .await
    }

    async fn materialize_collection_inner(
        &self,
        collection_name: &str,
        materialize_identity_paths: bool,
    ) -> Result<usize> {
        let collection = self
            .get_collection(collection_name)?
            .ok_or_else(|| Error::CollectionNotFound(collection_name.to_string()))?;
        let collection_id = collection.collection_id().to_string();
        let target_version_id = collection.version_id().to_string();
        let short_id = collection.resolved_root_id();

        let read_txn = self.new_txn(true).await?;
        let systemstore = read_txn.systemstore()?;
        let versions = get_collections_by_collection_id(&systemstore, &collection_id).await?;
        let _ = read_txn.discard();

        let history =
            crate::definition::lens::build_collection_history(&versions, &target_version_id)
                .ok_or_else(|| {
                    Error::Lens(format!(
                        "failed to build migration history for collection '{}'",
                        collection_name
                    ))
                })?;

        let write_txn = self.new_txn(false).await?;
        let mut materialized_count = 0usize;

        {
            let datastore = write_txn.datastore()?;
            let txn_systemstore = write_txn.systemstore()?;

            let raw_docs = collection
                .get_all_with_datastore_short_ids(&datastore, &txn_systemstore, false)
                .await?;

            let mut migrated_docs = Vec::with_capacity(raw_docs.len());
            for (doc_short_id, doc, _) in raw_docs {
                let Some(doc_version) = doc.schema_version_id().map(str::to_string) else {
                    if materialize_identity_paths {
                        let mut restamped = doc.clone();
                        restamped.set_schema_version_id(&target_version_id);
                        if cache_document_version(&datastore, &txn_systemstore, &collection, &doc)
                            .await?
                        {
                            materialized_count += 1;
                        }
                        migrated_docs.push((doc_short_id, restamped));
                    } else {
                        migrated_docs.push((doc_short_id, doc));
                    }
                    continue;
                };

                if doc_version == target_version_id {
                    migrated_docs.push((doc_short_id, doc));
                    continue;
                }

                if !history.contains_key(&doc_version) {
                    warn!(
                        doc_id = ?doc.id(),
                        doc_version = %doc_version,
                        target_version = %target_version_id,
                        "Skipping materialization for a document version unknown locally"
                    );
                    migrated_docs.push((doc_short_id, doc));
                    continue;
                }

                let path_has_transform = crate::definition::lens::migration_path_has_transform(
                    &history,
                    &doc_version,
                    &target_version_id,
                );
                if !path_has_transform {
                    if materialize_identity_paths {
                        let cached =
                            cache_document_version(&datastore, &txn_systemstore, &collection, &doc)
                                .await?;
                        let mut restamped = doc;
                        restamped.set_schema_version_id(&target_version_id);
                        if cached {
                            materialized_count += 1;
                        }
                        migrated_docs.push((doc_short_id, restamped));
                    } else {
                        migrated_docs.push((doc_short_id, doc));
                    }
                    continue;
                }

                let lens_doc = crate::definition::lens::doc_to_lens_doc(&doc).ok_or_else(|| {
                    Error::Lens(format!(
                        "failed to convert document {:?} for materialization",
                        doc.id()
                    ))
                })?;
                let mut lens =
                    Lens::new(self.lens_store.clone(), &target_version_id, history.clone());
                lens.put(&doc_version, lens_doc)
                    .await
                    .map_err(|e| Error::Lens(e.to_string()))?;
                let migrated_lens_doc = lens
                    .next()
                    .await
                    .ok_or_else(|| {
                        Error::Lens(format!(
                            "lens produced no document while materializing {:?}",
                            doc.id()
                        ))
                    })?
                    .map_err(|e| Error::Lens(e.to_string()))?;
                let migrated = lens_doc_to_document(migrated_lens_doc, &doc, &collection);
                if cache_migrated_document(&datastore, &txn_systemstore, &collection, &migrated)
                    .await?
                {
                    materialized_count += 1;
                }
                migrated_docs.push((doc_short_id, migrated));
            }

            if !collection.write_indexes().is_empty() {
                let index_manager = IndexManager::from_indexes(
                    short_id,
                    collection.schema(),
                    collection.write_indexes(),
                )
                .map_err(|e| Error::Other(format!("failed to create index manager: {}", e)))?;

                for index_desc in collection.write_indexes() {
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
            }

            tracing::debug!(
                collection = %collection_name,
                doc_count = migrated_docs.len(),
                materialized_count,
                index_count = collection.get_indexes().len(),
                materialize_identity_paths,
                "Materialized collection datastore and rebuilt indexes"
            );
        }

        write_txn.commit().await?;

        Ok(materialized_count)
    }
}
