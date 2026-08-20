//! Lens transform registration and listing inside a transaction.

use super::*;

impl<S: Store + 'static> DbTransactionRegistry<S> {
    async fn compute_lens_transform_id(config: &LensConfig) -> Result<TransformId> {
        let first_lens = config
            .lens()
            .ok_or_else(|| Error::Lens("lens config has no modules".into()))?;

        let wasm_bytes = if let Some(ref bytes) = first_lens.module {
            bytes.clone()
        } else if let Some(ref path) = first_lens.path {
            #[cfg(not(target_arch = "wasm32"))]
            {
                let clean_path = path.strip_prefix("file://").unwrap_or(path);
                tokio::fs::read(clean_path)
                    .await
                    .map_err(|e| Error::Lens(format!("failed to read WASM from {}: {}", path, e)))?
            }
            #[cfg(target_arch = "wasm32")]
            {
                return Err(Error::Lens(format!(
                    "path-based lens modules not supported on wasm32 (path: {}); pass bytes instead",
                    path
                )));
            }
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

        let (config_cid, _) =
            defra_core::build_lens_ipld_blocks(&wasm_bytes, first_lens.inverse, &arguments)
                .map_err(|e| Error::Lens(format!("failed to build lens IPLD blocks: {}", e)))?;

        Ok(TransformId::new(config_cid.to_string()))
    }

    /// Add a standalone lens within a transaction.
    ///
    /// The lens is visible only within this transaction until commit. On commit it is
    /// persisted through the regular DB lens path so restart behavior stays consistent.
    pub async fn add_lens_in_txn(&self, txn_id: &str, config: LensConfig) -> Result<TransformId> {
        let ctx = self
            .get_ctx(txn_id)?
            .ok_or_else(|| Error::TransactionNotFound(txn_id.to_string()))?;
        self.db
            .check_node_access(None, acp::nac::NodePermission::LensCreate)
            .await?;
        let action_lock = ctx.action_lock();
        let _action_guard = action_lock.lock().await;

        let shared_txn = ctx.fetcher_shared_txn();
        let mut txn_guard = shared_txn.lock().await;
        let txn = txn_guard.as_mut().ok_or(Error::TxnNotActive)?;
        let txn_lens_store = ctx.lens_store();

        let transform_id = Self::compute_lens_transform_id(&config).await?;
        if txn_lens_store.has_transform(&transform_id) {
            return Ok(transform_id);
        }

        let db = self.db.clone();
        let config_for_commit = config.clone();
        let transform_id_for_log = transform_id.to_string();
        txn.on_success_async(Box::new(move || {
            let db = db.clone();
            Box::pin(async move {
                if let Err(error) = db.add_lens(config_for_commit).await {
                    tracing::warn!(
                        transform_id = %transform_id_for_log,
                        error = %error,
                        "failed to persist committed transaction lens"
                    );
                }
            })
        }))?;

        txn_lens_store
            .add_with_id(transform_id.clone(), config)
            .await
            .map_err(Error::from)?;

        Ok(transform_id)
    }

    /// List all lenses visible within a transaction.
    pub async fn list_lenses_in_txn(
        &self,
        txn_id: &str,
    ) -> Result<std::collections::HashMap<String, LensModule>> {
        let ctx = self
            .get_ctx(txn_id)?
            .ok_or_else(|| Error::TransactionNotFound(txn_id.to_string()))?;
        let action_lock = ctx.action_lock();
        let _action_guard = action_lock.lock().await;

        ctx.lens_store()
            .list()
            .await
            .map_err(|e| Error::Lens(e.to_string()))
    }
}
