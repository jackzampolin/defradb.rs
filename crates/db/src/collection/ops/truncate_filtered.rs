use std::collections::HashSet;
use std::sync::Arc;

use storage::keys::{
    DataStoreKey, DatastoreSE, HeadstoreDocKey, HeadstorePriorityKey, InstanceType,
    PrimaryDataStoreKey,
};

use super::*;

const CHUNK_SIZE: usize = 1000;

impl<S: Store> crate::database::DB<S> {
    /// Permanently remove documents matching `filter` while preserving the collection.
    #[instrument(skip(self, filter), fields(collection = %name), name = "db.truncate_collection_filtered")]
    pub async fn truncate_collection_with_filter(
        self: &Arc<Self>,
        name: &str,
        filter: query::Filter,
        identity: Option<&identity::Did>,
    ) -> Result<()>
    where
        S: 'static,
    {
        self.check_node_access(identity, acp::nac::NodePermission::CollectionTruncate)
            .await?;
        filter.validate_depth()?;

        let collection = self
            .get_collection(name)?
            .ok_or_else(|| Error::CollectionNotFound(name.to_string()))?;
        if collection.schema().is_branchable {
            return Err(Error::FilteredTruncateBranchableCollection);
        }

        let collection_id = collection.collection_id().to_string();
        let short_id = collection.resolved_root_id();
        let _collection_guards = self
            .collection_write_guards(std::iter::once(collection_id.clone()))
            .await?;
        let action_execution = self
            .register_action(&collection_id, crate::Action::TRUNCATE)
            .await?;

        let fetcher = crate::LensedAutoCommitFetcher::new_without_write_back(self.clone());
        let provider = crate::DbCollectionProvider::new_arc(self.clone());
        let runner = query::QueryRunner::with_provider(fetcher, provider)
            .with_lens_store(self.lens_store().clone());

        let result: Result<usize> = async {
            let mut doc_count = 0;
            loop {
                let doc_ids = runner
                    .matching_doc_ids(name, filter.clone(), CHUNK_SIZE, true)
                    .await?;
                if doc_ids.is_empty() {
                    break;
                }
                doc_count += self.truncate_filtered_chunk(&collection, &doc_ids).await?;
            }
            Ok(doc_count)
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
                        "Failed to record filtered truncate action error"
                    );
                }
                return Err(error);
            }
        };

        self.complete_action(action_execution).await?;
        tracing::info!(
            collection_id = %collection_id,
            short_id,
            doc_count,
            "Truncated matching collection documents"
        );
        Ok(())
    }

    async fn truncate_filtered_chunk(
        &self,
        collection: &Collection,
        doc_ids: &[String],
    ) -> Result<usize> {
        let txn = self.new_txn(false).await?;
        let datastore = txn.datastore()?;
        let headstore = txn.headstore()?;
        let blockstore = txn.blockstore()?;
        let systemstore = txn.systemstore()?;

        let result: Result<usize> = async {
            let index_manager = crate::IndexManager::from_indexes(
                collection.resolved_root_id(),
                collection.schema(),
                collection.write_indexes(),
            )?;
            let mut seen = HashSet::new();
            let mut targets = Vec::with_capacity(doc_ids.len());
            let mut aliases_to_delete = HashSet::new();

            for doc_id in doc_ids {
                let parsed = document::DocID::from_string(doc_id)?;
                let (doc_short_id, canonical_doc_id) = collection
                    .resolve_doc_identity(&systemstore, &parsed)
                    .await?
                    .ok_or_else(|| Error::DocumentNotFound(doc_id.clone()))?;
                if !seen.insert(doc_short_id) {
                    continue;
                }

                let mut aliases =
                    crate::docid::map::get_doc_id_aliases(&systemstore, doc_short_id).await?;
                if !aliases
                    .iter()
                    .any(|alias| alias == &canonical_doc_id.to_string())
                {
                    aliases.push(canonical_doc_id.to_string());
                }
                aliases_to_delete.extend(aliases.iter().cloned());
                targets.push((doc_short_id, canonical_doc_id, aliases));
            }

            let se_prefix = DatastoreSE::collection_prefix(collection.collection_id());
            let mut se_iter = datastore
                .iterator(IterOptions::new().with_prefix(se_prefix))
                .await
                .map_err(Error::Storage)?;
            let mut se_keys = Vec::new();
            while let Some(pair) = se_iter.next().await.map_err(Error::Storage)? {
                if extract_last_path_segment_str(&pair.key)
                    .is_some_and(|doc_id| aliases_to_delete.contains(&doc_id))
                {
                    se_keys.push(pair.key.to_vec());
                }
            }
            se_iter.close().await.map_err(Error::Storage)?;
            for key in se_keys {
                datastore.delete(&key).await.map_err(Error::Storage)?;
            }

            for (doc_short_id, canonical_doc_id, aliases) in &targets {
                if let Some((doc, _)) = collection
                    .get_with_datastore_include_deleted(
                        &datastore,
                        *doc_short_id,
                        canonical_doc_id,
                        true,
                    )
                    .await?
                {
                    index_manager
                        .on_document_delete(&datastore, &doc, *doc_short_id, collection.schema())
                        .await?;
                }

                datastore
                    .delete(&collection.doc_key(*doc_short_id))
                    .await
                    .map_err(Error::Storage)?;
                datastore
                    .delete(&collection.deleted_key(*doc_short_id))
                    .await
                    .map_err(Error::Storage)?;
                datastore
                    .delete(&collection.version_key(*doc_short_id))
                    .await
                    .map_err(Error::Storage)?;
                datastore
                    .delete(
                        &PrimaryDataStoreKey::new(collection.resolved_root_id(), *doc_short_id)
                            .bytes(),
                    )
                    .await
                    .map_err(Error::Storage)?;
                for instance_type in [
                    InstanceType::Value,
                    InstanceType::Priority,
                    InstanceType::Deleted,
                ] {
                    delete_prefix(
                        &datastore,
                        DataStoreKey::document_prefix(
                            collection.resolved_root_id(),
                            instance_type,
                            *doc_short_id,
                        ),
                    )
                    .await?;
                }

                let head_prefix = HeadstoreDocKey::document_prefix(*doc_short_id);
                let mut block_cids = Vec::new();
                let mut head_iter = headstore
                    .iterator(IterOptions::new().with_prefix(head_prefix.clone()))
                    .await
                    .map_err(Error::Storage)?;
                while let Some(pair) = head_iter.next().await.map_err(Error::Storage)? {
                    if let Some(cid_str) = extract_last_path_segment_str(&pair.key) {
                        if let Ok(cid) = cid::Cid::try_from(cid_str.as_str()) {
                            block_cids.push(cid);
                        }
                    }
                }
                head_iter.close().await.map_err(Error::Storage)?;
                delete_prefix(&headstore, head_prefix).await?;
                delete_prefix(
                    &headstore,
                    HeadstorePriorityKey::document_prefix(*doc_short_id),
                )
                .await?;

                crate::block::cleanup::delete_owned_dag_for_owners(
                    &blockstore,
                    &systemstore,
                    &block_cids,
                    aliases,
                )
                .await?;
                crate::docid::map::delete_doc_id_mappings(&systemstore, *doc_short_id).await?;
            }

            Ok(targets.len())
        }
        .await;

        drop(datastore);
        drop(headstore);
        drop(blockstore);
        drop(systemstore);

        match result {
            Ok(count) => {
                txn.commit().await?;
                Ok(count)
            }
            Err(error) => {
                if let Err(discard_err) = txn.discard() {
                    tracing::warn!(
                        error = %discard_err,
                        original_error = %error,
                        "Transaction discard failed during filtered truncate"
                    );
                }
                Err(error)
            }
        }
    }
}
