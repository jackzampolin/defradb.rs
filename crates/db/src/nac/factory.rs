//! Factory functions for creating NAC managers.

use std::sync::Arc;

use acp::MemoryZanzibarStore;

#[cfg(not(target_arch = "wasm32"))]
use acp::PersistentZanzibarStore;
#[cfg(not(target_arch = "wasm32"))]
use storage::RedbStore;

use crate::error::{Error, Result};

use super::{NacConfig, NacManager};

/// Create an in-memory NAC manager for testing.
pub fn create_memory_nac_manager(config: NacConfig) -> NacManager<MemoryZanzibarStore> {
    let store = Arc::new(MemoryZanzibarStore::new());
    NacManager::new(store, config)
}

/// Create a persistent NAC manager.
///
/// The NAC data is stored in a separate directory (`local_node_acp/`) under the data path.
#[cfg(not(target_arch = "wasm32"))]
pub fn create_persistent_nac_manager(
    data_path: &std::path::Path,
) -> Result<NacManager<PersistentZanzibarStore<RedbStore>>> {
    let nac_path = data_path.join("local_node_acp");
    std::fs::create_dir_all(&nac_path)
        .map_err(|e| Error::Other(format!("failed to create NAC data directory: {}", e)))?;

    let db_path = nac_path.join("nac.db");
    let store = PersistentZanzibarStore::open(&db_path)
        .map_err(|e| Error::Acp(format!("failed to open NAC store: {}", e)))?;

    Ok(NacManager::new(
        Arc::new(store),
        NacConfig::default().with_data_path(nac_path.display().to_string()),
    ))
}
