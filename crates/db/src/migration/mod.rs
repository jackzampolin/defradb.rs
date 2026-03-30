//! Lens migration operations for schema versioning.
//!
//! This module handles registration and execution of lens migrations
//! between schema versions. Migrations allow documents stored with
//! older schema versions to be transformed when fetched.

pub(crate) mod helpers;
mod reindex;
mod set_migration;

use std::sync::Arc;

use lens::{LensConfig, TransformId, TransformStore};
use storage::corekv::{IterOptions, Key, Store};
use storage::keys::systemstore::LensConfigKey;

use crate::error::{Error, Result};
use crate::DB;

impl<S: Store> DB<S> {
    /// Get a reference to the lens transform store.
    ///
    /// The lens store manages schema migration transforms that can be applied
    /// when documents are fetched from older schema versions.
    pub fn lens_store(&self) -> &Arc<dyn TransformStore> {
        &self.lens_store
    }

    /// Check if a migration exists between two schema versions.
    pub fn has_migration(&self, transform_id: &TransformId) -> bool {
        self.lens_store.has_transform(transform_id)
    }

    /// Add a lens, building IPLD blocks and storing them in the blockstore for P2P sync.
    ///
    /// This matches Go's lens store behavior: when a lens is added, the WASM bytes
    /// are serialized into IPLD blocks (LensConfigBlock → LensModuleBlock → LensWasmBlock)
    /// and stored in the blockstore. The root CID of this DAG becomes the transform ID,
    /// which is a valid IPLD CID that peers can fetch during version sync.
    pub async fn add_lens(&self, config: LensConfig) -> Result<TransformId> {
        let first_lens = config
            .lens()
            .ok_or_else(|| Error::Lens("lens config has no modules".into()))?;

        let wasm_bytes = if let Some(ref bytes) = first_lens.module {
            bytes.clone()
        } else if let Some(ref path) = first_lens.path {
            tokio::fs::read(path)
                .await
                .map_err(|e| Error::Lens(format!("failed to read WASM from {}: {}", path, e)))?
        } else {
            return Err(Error::Lens("lens module has neither path nor bytes".into()));
        };

        let arguments: Vec<(String, String)> = first_lens
            .arguments
            .as_ref()
            .and_then(|v| v.as_object())
            .map(|obj| {
                obj.iter()
                    .map(|(k, v)| (k.clone(), v.as_str().unwrap_or_default().to_string()))
                    .collect()
            })
            .unwrap_or_default();

        let (config_cid, blocks) =
            defra_core::build_lens_ipld_blocks(&wasm_bytes, first_lens.inverse, &arguments)
                .map_err(|e| Error::Lens(format!("failed to build lens IPLD blocks: {}", e)))?;

        let transform_id = TransformId::new(config_cid.to_string());

        let txn = self.new_txn(false).await?;
        {
            let blockstore = txn.blockstore()?;
            for (cid, data) in &blocks {
                blockstore
                    .set(&cid.to_bytes(), data)
                    .await
                    .map_err(Error::Storage)?;
            }
        }
        {
            let systemstore = txn.systemstore()?;
            let lens_key = LensConfigKey::new(transform_id.to_string());
            let lens_data = serde_json::to_vec(&config).map_err(|e| {
                Error::Serialization(format!("failed to serialize lens config: {}", e))
            })?;
            systemstore
                .set(&lens_key.bytes(), &lens_data)
                .await
                .map_err(Error::Storage)?;
        }
        txn.commit().await?;

        self.lens_store
            .add_with_id(transform_id.clone(), config)
            .await
            .map_err(|e| Error::Lens(e.to_string()))?;

        tracing::info!(
            transform_id = %transform_id,
            blocks_stored = blocks.len(),
            "Lens added with IPLD blocks in blockstore"
        );

        Ok(transform_id)
    }

    /// Reload persisted lens configs from the systemstore into the lens store.
    ///
    /// Called during database open to restore transforms that were registered
    /// before the last shutdown. Matches Go's `getLensStore().Reload()` call
    /// in `db.initialize()`.
    pub async fn reload_lens_configs(&self) -> Result<()> {
        let txn = self.new_txn(true).await?;

        {
            let systemstore = txn.systemstore()?;
            let prefix = LensConfigKey::prefix();
            let opts = IterOptions::new().with_prefix(prefix);
            let mut iter = systemstore.iterator(opts).await.map_err(Error::Storage)?;

            let prefix_str = "/lens/config/";

            while let Some(pair) = iter.next().await.map_err(Error::Storage)? {
                let config: LensConfig = match serde_json::from_slice(&pair.value) {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            key = ?String::from_utf8_lossy(&pair.key),
                            "Skipping malformed lens config during reload"
                        );
                        continue;
                    }
                };

                let key_str = String::from_utf8_lossy(&pair.key);
                let transform_id = key_str.strip_prefix(prefix_str).map(TransformId::new);

                let result = if let Some(id) = transform_id {
                    self.lens_store.add_with_id(id, config).await
                } else {
                    self.lens_store.add(config).await.map(|_| ())
                };

                if let Err(e) = result {
                    tracing::warn!(
                        error = %e,
                        key = ?String::from_utf8_lossy(&pair.key),
                        "Failed to reload lens config"
                    );
                }
            }

            iter.close().await.map_err(Error::Storage)?;
        }

        let _ = txn.discard();
        Ok(())
    }
}
