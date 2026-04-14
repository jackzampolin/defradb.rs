//! Migration registration methods on DB.

use lens::{LensConfig, TransformId};
use schema::CollectionSource;
use storage::corekv::{Key, Store};
use storage::keys::systemstore::{CollectionKey, CollectionVersionKey, LensConfigKey};
use tracing::instrument;

use super::helpers::{create_orphan_placeholder, create_placeholder_with_source};
use crate::collection::Collection;
use crate::error::{Error, Result};
use crate::txn::DbTxn;
use crate::DB;

pub(crate) struct SetMigrationInTxnOutcome {
    pub(crate) transform_id: TransformId,
    pub(crate) updated_destination: schema::CollectionVersion,
}

impl<S: Store> DB<S> {
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
        let config_for_persistence = config.clone();

        let txn = self.new_txn(false).await?;

        let (source_col, mut dst_col) = {
            let systemstore = txn.systemstore()?;

            let src_key = CollectionKey::new(&source_version_id);
            let src_data = systemstore
                .get(&src_key.bytes())
                .await
                .map_err(Error::Storage)?;
            let mut source_col: schema::CollectionVersion = match src_data {
                Some(data) => serde_json::from_slice(&data).map_err(|e| {
                    Error::collection_schema_json(
                        format!(
                            "failed to deserialize source schema '{}'",
                            source_version_id
                        ),
                        e,
                    )
                })?,
                None => {
                    let mut placeholder = create_orphan_placeholder(&source_version_id, "", "");
                    placeholder.root_id = crate::collection::ensure_persisted_collection_short_id(
                        &systemstore,
                        &placeholder.collection_id,
                    )
                    .await?;
                    let data = serde_json::to_vec(&placeholder).map_err(|e| {
                        Error::collection_schema_json(
                            format!(
                                "failed to serialize source placeholder '{}'",
                                source_version_id
                            ),
                            e,
                        )
                    })?;
                    systemstore
                        .set(&src_key.bytes(), &data)
                        .await
                        .map_err(Error::Storage)?;
                    placeholder
                }
            };

            let dst_key = CollectionKey::new(&dest_version_id);
            let dst_data = systemstore
                .get(&dst_key.bytes())
                .await
                .map_err(Error::Storage)?;
            let mut dst_col: schema::CollectionVersion = match dst_data {
                Some(data) => serde_json::from_slice(&data).map_err(|e| {
                    Error::collection_schema_json(
                        format!(
                            "failed to deserialize destination schema '{}'",
                            dest_version_id
                        ),
                        e,
                    )
                })?,
                None => {
                    let mut placeholder = create_placeholder_with_source(
                        &dest_version_id,
                        &source_col.name,
                        &source_col.collection_id,
                    );
                    placeholder.root_id = crate::collection::ensure_persisted_collection_short_id(
                        &systemstore,
                        &placeholder.collection_id,
                    )
                    .await?;
                    let data = serde_json::to_vec(&placeholder).map_err(|e| {
                        Error::collection_schema_json(
                            format!(
                                "failed to serialize destination placeholder '{}'",
                                dest_version_id
                            ),
                            e,
                        )
                    })?;
                    systemstore
                        .set(&dst_key.bytes(), &data)
                        .await
                        .map_err(Error::Storage)?;
                    placeholder
                }
            };

            if !source_col.collection_id.is_empty() {
                source_col.root_id = crate::collection::ensure_persisted_collection_short_id(
                    &systemstore,
                    &source_col.collection_id,
                )
                .await?;
            }
            if !dst_col.collection_id.is_empty() {
                dst_col.root_id = crate::collection::ensure_persisted_collection_short_id(
                    &systemstore,
                    &dst_col.collection_id,
                )
                .await?;
            }

            (source_col, dst_col)
        };

        if let Some(ref prev) = dst_col.previous_version {
            if prev.source_collection_id != source_col.version_id {
                return Err(Error::InvalidPatch(format!(
                    "cannot migrate between non-adjacent collection versions. \
                     Destination '{}' already has previous version '{}', but migration source is '{}'",
                    dest_version_id, prev.source_collection_id, source_version_id
                )));
            }
        }

        let transform_id = self
            .lens_store
            .add(config)
            .await
            .map_err(|e| Error::Lens(e.to_string()))?;

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

        let collection_name = dst_col.name.clone();
        let dst_key = CollectionKey::new(&dest_version_id);
        let dst_data = serde_json::to_vec(&dst_col).map_err(|e| {
            Error::collection_schema_json(
                format!(
                    "failed to serialize destination schema '{}'",
                    dest_version_id
                ),
                e,
            )
        })?;

        {
            let systemstore = txn.systemstore()?;
            systemstore
                .set(&dst_key.bytes(), &dst_data)
                .await
                .map_err(Error::Storage)?;

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

            let lens_key = LensConfigKey::new(transform_id.to_string());
            let lens_data = serde_json::to_vec(&config_for_persistence)
                .map_err(|e| Error::lens_config_json("failed to serialize lens config", e))?;
            systemstore
                .set(&lens_key.bytes(), &lens_data)
                .await
                .map_err(Error::Storage)?;
        }
        txn.commit().await?;

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
        self.set_migration_in_txn_with_store(txn, self.lens_store.clone(), config)
            .await
            .map(|outcome| outcome.transform_id)
    }

    #[instrument(skip(self, txn, lens_store, config), fields(
        source = %config.source_schema_version_id,
        dest = %config.destination_schema_version_id
    ))]
    pub(crate) async fn set_migration_in_txn_with_store(
        &self,
        txn: &DbTxn<S>,
        lens_store: std::sync::Arc<dyn lens::TransformStore>,
        config: LensConfig,
    ) -> Result<SetMigrationInTxnOutcome> {
        let dest_version_id = config.destination_schema_version_id.clone();
        let source_version_id = config.source_schema_version_id.clone();
        let config_for_persistence = config.clone();

        let (source_col, mut dst_col) = {
            let systemstore = txn.systemstore()?;

            let src_key = CollectionKey::new(&source_version_id);
            let src_data = systemstore
                .get(&src_key.bytes())
                .await
                .map_err(Error::Storage)?;
            let mut source_col: schema::CollectionVersion = match src_data {
                Some(data) => serde_json::from_slice(&data).map_err(|e| {
                    Error::collection_schema_json(
                        format!(
                            "failed to deserialize source schema '{}'",
                            source_version_id
                        ),
                        e,
                    )
                })?,
                None => {
                    let mut placeholder = create_orphan_placeholder(&source_version_id, "", "");
                    placeholder.root_id = crate::collection::ensure_persisted_collection_short_id(
                        &systemstore,
                        &placeholder.collection_id,
                    )
                    .await?;
                    let data = serde_json::to_vec(&placeholder).map_err(|e| {
                        Error::collection_schema_json(
                            format!(
                                "failed to serialize source placeholder '{}'",
                                source_version_id
                            ),
                            e,
                        )
                    })?;
                    systemstore
                        .set(&src_key.bytes(), &data)
                        .await
                        .map_err(Error::Storage)?;
                    placeholder
                }
            };

            let dst_key = CollectionKey::new(&dest_version_id);
            let dst_data = systemstore
                .get(&dst_key.bytes())
                .await
                .map_err(Error::Storage)?;
            let mut dst_col: schema::CollectionVersion = match dst_data {
                Some(data) => serde_json::from_slice(&data).map_err(|e| {
                    Error::collection_schema_json(
                        format!(
                            "failed to deserialize destination schema '{}'",
                            dest_version_id
                        ),
                        e,
                    )
                })?,
                None => {
                    let mut placeholder = create_placeholder_with_source(
                        &dest_version_id,
                        &source_col.name,
                        &source_col.collection_id,
                    );
                    placeholder.root_id = crate::collection::ensure_persisted_collection_short_id(
                        &systemstore,
                        &placeholder.collection_id,
                    )
                    .await?;
                    let data = serde_json::to_vec(&placeholder).map_err(|e| {
                        Error::collection_schema_json(
                            format!(
                                "failed to serialize destination placeholder '{}'",
                                dest_version_id
                            ),
                            e,
                        )
                    })?;
                    systemstore
                        .set(&dst_key.bytes(), &data)
                        .await
                        .map_err(Error::Storage)?;
                    placeholder
                }
            };

            if !source_col.collection_id.is_empty() {
                source_col.root_id = crate::collection::ensure_persisted_collection_short_id(
                    &systemstore,
                    &source_col.collection_id,
                )
                .await?;
            }
            if !dst_col.collection_id.is_empty() {
                dst_col.root_id = crate::collection::ensure_persisted_collection_short_id(
                    &systemstore,
                    &dst_col.collection_id,
                )
                .await?;
            }

            (source_col, dst_col)
        };

        if let Some(ref prev) = dst_col.previous_version {
            if prev.source_collection_id != source_col.version_id {
                return Err(Error::InvalidPatch(format!(
                    "cannot migrate between non-adjacent collection versions. \
                     Destination '{}' already has previous version '{}', but migration source is '{}'",
                    dest_version_id, prev.source_collection_id, source_version_id
                )));
            }
        }

        let transform_id = lens_store.add(config).await.map_err(Error::from)?;

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

        let dst_key = CollectionKey::new(&dest_version_id);
        let dst_data = serde_json::to_vec(&dst_col).map_err(|e| {
            Error::collection_schema_json(
                format!(
                    "failed to serialize destination schema '{}'",
                    dest_version_id
                ),
                e,
            )
        })?;

        {
            let systemstore = txn.systemstore()?;
            systemstore
                .set(&dst_key.bytes(), &dst_data)
                .await
                .map_err(Error::Storage)?;

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

            let lens_key = LensConfigKey::new(transform_id.to_string());
            let lens_data = serde_json::to_vec(&config_for_persistence)
                .map_err(|e| Error::lens_config_json("failed to serialize lens config", e))?;
            systemstore
                .set(&lens_key.bytes(), &lens_data)
                .await
                .map_err(Error::Storage)?;
        }

        Ok(SetMigrationInTxnOutcome {
            transform_id,
            updated_destination: dst_col,
        })
    }
}
