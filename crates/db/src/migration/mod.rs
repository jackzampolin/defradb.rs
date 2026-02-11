//! Lens migration operations for schema versioning.
//!
//! This module handles registration and execution of lens migrations
//! between schema versions. Migrations allow documents stored with
//! older schema versions to be transformed when fetched.

pub(crate) mod helpers;
mod reindex;
mod set_migration;

pub use helpers::json_to_native_value;

use std::sync::Arc;

use lens::{LensConfig, TransformId, TransformStore};
use storage::corekv::{IterOptions, Store};
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

                if let Err(e) = self.lens_store.add(config).await {
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
