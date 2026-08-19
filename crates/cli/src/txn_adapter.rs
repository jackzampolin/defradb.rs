//! Adapter to bridge DbTransactionRegistry to HTTP's TransactionOperations trait.
//!
//! The underlying `DbTransactionRegistry` methods hold a transaction lock guard
//! (`DbTxn<S>`) across await points. Because `DbTxn<S>` contains non-`Sync` types
//! (closures), the resulting futures are not `Send`. We bridge this by using
//! `spawn_blocking` + `Handle::block_on`, matching the pattern used by the FFI crate.

use std::sync::Arc;

use async_trait::async_trait;

use defra_http::router::TransactionOperations;
use identity::Did;
use storage::corekv::Store;

/// Adapter that implements TransactionOperations using a shared DbTransactionRegistry.
pub struct TxnRegistryAdapter<S: Store> {
    registry: Arc<db::DbTransactionRegistry<S>>,
    document_acp: Option<Arc<dyn acp::DocumentACP>>,
}

impl<S: Store + 'static> TxnRegistryAdapter<S> {
    /// Create an Arc-wrapped adapter backed by the shared transaction registry.
    pub fn new_arc(registry: Arc<db::DbTransactionRegistry<S>>) -> Arc<dyn TransactionOperations> {
        Arc::new(Self {
            registry,
            document_acp: None,
        })
    }

    /// Create an Arc-wrapped adapter with document ACP registration support.
    pub fn new_arc_with_acp(
        registry: Arc<db::DbTransactionRegistry<S>>,
        document_acp: Arc<dyn acp::DocumentACP>,
    ) -> Arc<dyn TransactionOperations> {
        Arc::new(Self {
            registry,
            document_acp: Some(document_acp),
        })
    }
}

#[async_trait]
impl<S: Store + 'static> TransactionOperations for TxnRegistryAdapter<S> {
    async fn set_migration_in_txn(&self, txn_id: &str, config: &str) -> Result<String, String> {
        let lens_config: lens::LensConfig = serde_json::from_str(config)
            .map_err(|e| format!("failed to parse lens config: {}", e))?;

        let registry = self.registry.clone();
        let txn_id = txn_id.to_string();
        let handle = tokio::runtime::Handle::current();

        tokio::task::spawn_blocking(move || {
            handle.block_on(async {
                registry
                    .set_migration_in_txn(&txn_id, lens_config, None)
                    .await
                    .map(|id| id.to_string())
                    .map_err(|e| format!("{}", e))
            })
        })
        .await
        .map_err(|e| format!("task join error: {}", e))?
    }

    async fn get_collections_in_txn(
        &self,
        txn_id: &str,
    ) -> Result<Vec<schema::CollectionVersion>, String> {
        let registry = self.registry.clone();
        let txn_id = txn_id.to_string();
        let handle = tokio::runtime::Handle::current();

        tokio::task::spawn_blocking(move || {
            handle.block_on(async {
                registry
                    .get_collections_in_txn(&txn_id)
                    .await
                    .map_err(|e| format!("{}", e))
            })
        })
        .await
        .map_err(|e| format!("task join error: {}", e))?
    }

    async fn add_schema_in_txn(
        &self,
        txn_id: &str,
        sdl: &str,
    ) -> Result<Vec<schema::CollectionVersion>, String> {
        let registry = self.registry.clone();
        let document_acp = self.document_acp.clone();
        // Fail closed: a present-but-malformed ambient identity must error, not
        // silently degrade a permissioned collection to public (unregistered).
        let creator = match defra_core::current_identity::get_effective_identity() {
            Some(raw) => {
                Some(Did::new(raw).map_err(|e| format!("malformed ambient identity: {}", e))?)
            }
            None => None,
        };
        let txn_id = txn_id.to_string();
        let sdl = sdl.to_string();
        let handle = tokio::runtime::Handle::current();

        tokio::task::spawn_blocking(move || {
            handle.block_on(async {
                registry
                    .add_schema_in_txn_with_acp(&txn_id, &sdl, document_acp, creator)
                    .await
                    .map_err(|e| format!("{}", e))
            })
        })
        .await
        .map_err(|e| format!("task join error: {}", e))?
    }
}
