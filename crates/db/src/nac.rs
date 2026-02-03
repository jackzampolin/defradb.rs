//! Node Access Control (NAC) state management.
//!
//! This module provides the NAC manager that wraps the `NodeACP` from the `acp` crate
//! and adds config-aware behavior for the database layer.
//!
//! # NAC Lifecycle
//!
//! 1. Node starts with NAC disabled (default) - all operations allowed
//! 2. Admin enables NAC with `--node-acp-enable` flag
//! 3. Owner identity is bootstrapped from keyring
//! 4. NAC enforces permissions for node operations
//!
//! # Configuration
//!
//! NAC is controlled by:
//! - `acp.node_enable` config option (default: false)
//! - Owner identity from the node's keyring

use std::sync::Arc;

use acp::nac::{NacStatus, NodeACP, NodePermission};
use acp::{MemoryZanzibarStore, ZanzibarStore};
use identity::Did;

#[cfg(not(target_arch = "wasm32"))]
use acp::PersistentZanzibarStore;
#[cfg(not(target_arch = "wasm32"))]
use storage::RedbStore;

use crate::error::{Error, Result};

/// NAC configuration options.
#[derive(Debug, Clone, Default)]
pub struct NacConfig {
    /// Whether NAC should be enabled.
    pub enabled: bool,

    /// Whether dev mode is enabled (allows purge operation).
    pub dev_mode: bool,

    /// Path to store NAC data (if using persistent storage).
    pub data_path: Option<String>,
}

impl NacConfig {
    /// Create a new NAC config.
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable NAC.
    pub fn with_enabled(mut self) -> Self {
        self.enabled = true;
        self
    }

    /// Enable dev mode (allows purge).
    pub fn with_dev_mode(mut self) -> Self {
        self.dev_mode = true;
        self
    }

    /// Set the data path for persistent storage.
    pub fn with_data_path(mut self, path: impl Into<String>) -> Self {
        self.data_path = Some(path.into());
        self
    }
}

/// NAC manager that provides config-aware NAC operations.
///
/// The manager wraps the `NodeACP` and provides:
/// - Initialization based on configuration
/// - Permission checking with config-aware fallbacks
/// - Admin management with authorization
pub struct NacManager<S: ZanzibarStore> {
    nac: NodeACP<S>,
    config: NacConfig,
}

impl<S: ZanzibarStore> NacManager<S> {
    /// Create a new NAC manager with the given store and config.
    pub fn new(store: Arc<S>, config: NacConfig) -> Self {
        Self {
            nac: NodeACP::new(store),
            config,
        }
    }

    /// Initialize NAC from config and possibly enable it.
    ///
    /// This should be called during node startup. If NAC is configured to be
    /// enabled and an owner identity is provided, NAC will be activated.
    pub async fn initialize(&self, owner_identity: Option<&Did>) -> Result<()> {
        // First, try to load existing NAC state
        self.nac
            .load()
            .await
            .map_err(|e| Error::Other(format!("failed to load NAC state: {}", e)))?;

        // If NAC is configured to be enabled and we have an owner identity
        if self.config.enabled {
            let current_status = self.nac.status().await;

            match current_status {
                NacStatus::NotConfigured => {
                    // NAC needs to be enabled for the first time
                    if let Some(owner) = owner_identity {
                        self.nac
                            .enable(owner)
                            .await
                            .map_err(|e| Error::Other(format!("failed to enable NAC: {}", e)))?;
                        tracing::info!(
                            target: "nac::startup",
                            owner = %owner,
                            "NAC enabled for the first time"
                        );
                    } else {
                        return Err(Error::Other(
                            "NAC is configured but no owner identity provided. \
                             Ensure keyring is configured with a node identity."
                                .into(),
                        ));
                    }
                }
                NacStatus::Enabled => {
                    tracing::info!(
                        target: "nac::startup",
                        "NAC already enabled from previous session"
                    );
                }
                NacStatus::DisabledTemporarily => {
                    // Re-enable NAC since config says it should be enabled
                    self.nac
                        .re_enable()
                        .await
                        .map_err(|e| Error::Other(format!("failed to re-enable NAC: {}", e)))?;
                    tracing::info!(
                        target: "nac::startup",
                        "NAC re-enabled from temporarily disabled state"
                    );
                }
            }
        }

        Ok(())
    }

    /// Get the current NAC status.
    pub async fn status(&self) -> NacStatus {
        self.nac.status().await
    }

    /// Get the owner identity.
    pub async fn owner(&self) -> Option<Did> {
        self.nac.owner().await
    }

    /// Check if NAC is effectively enabled.
    ///
    /// NAC is effectively enabled when:
    /// - Config says enabled AND
    /// - NAC status is Enabled
    pub async fn is_enabled(&self) -> bool {
        self.config.enabled && self.nac.status().await == NacStatus::Enabled
    }

    /// Check if an identity has a specific node permission.
    ///
    /// Returns `true` if:
    /// - NAC is not enabled (all operations allowed)
    /// - The identity has the required permission
    pub async fn check_permission(
        &self,
        identity: &Did,
        permission: NodePermission,
    ) -> Result<bool> {
        self.nac
            .check_permission(identity, permission)
            .await
            .map_err(|e| Error::Acp(format!("permission check failed: {}", e)))
    }

    /// Check if an identity is an admin.
    pub async fn is_admin(&self, identity: &Did) -> Result<bool> {
        self.nac
            .is_admin(identity)
            .await
            .map_err(|e| Error::Acp(format!("admin check failed: {}", e)))
    }

    /// Check if an identity is an admin based on stored relationships.
    ///
    /// Unlike `is_admin()`, this checks actual stored relationships regardless
    /// of NAC status. Used for operations like re-enable where we verify admin
    /// access even when NAC is temporarily disabled.
    pub async fn is_admin_persisted(&self, identity: &Did) -> Result<bool> {
        self.nac
            .is_admin_persisted(identity)
            .await
            .map_err(|e| Error::Acp(format!("admin check failed: {}", e)))
    }

    /// Check if an identity is the owner.
    pub async fn is_owner(&self, identity: &Did) -> bool {
        self.nac.is_owner(identity).await
    }

    /// Enable NAC with the given owner.
    ///
    /// This can only be called when NAC is not already enabled.
    pub async fn enable(&self, owner: &Did) -> Result<()> {
        self.nac
            .enable(owner)
            .await
            .map_err(|e| Error::Acp(format!("failed to enable NAC: {}", e)))
    }

    /// Temporarily disable NAC.
    ///
    /// The requestor must be an admin.
    pub async fn disable(&self, requestor: &Did) -> Result<()> {
        // Only admins can disable
        if !self.is_admin(requestor).await? {
            return Err(Error::Acp("only admins can disable NAC".into()));
        }

        self.nac
            .disable()
            .await
            .map_err(|e| Error::Acp(format!("failed to disable NAC: {}", e)))
    }

    /// Re-enable NAC after temporary disable.
    ///
    /// The requestor must be an admin. Uses persisted admin check since
    /// NAC is disabled (is_admin returns true for everyone when disabled).
    pub async fn re_enable(&self, requestor: &Did) -> Result<()> {
        // Use persisted check since is_admin returns true for everyone when disabled
        if !self.is_admin_persisted(requestor).await? {
            return Err(Error::Acp("only admins can re-enable NAC".into()));
        }

        self.nac
            .re_enable()
            .await
            .map_err(|e| Error::Acp(format!("failed to re-enable NAC: {}", e)))
    }

    /// Purge all NAC state (dev mode only).
    ///
    /// The requestor must be an admin and dev mode must be enabled.
    pub async fn purge(&self, requestor: &Did) -> Result<()> {
        if !self.config.dev_mode {
            return Err(Error::Acp("NAC purge is only allowed in dev mode".into()));
        }

        // Only admins can purge
        if !self.is_admin(requestor).await? {
            return Err(Error::Acp("only admins can purge NAC".into()));
        }

        self.nac
            .purge()
            .await
            .map_err(|e| Error::Acp(format!("failed to purge NAC: {}", e)))
    }

    /// Add an admin relationship.
    ///
    /// The requestor must be an admin.
    pub async fn add_admin(&self, requestor: &Did, target: &Did) -> Result<bool> {
        self.nac
            .add_admin(requestor, target)
            .await
            .map_err(|e| Error::Acp(format!("failed to add admin: {}", e)))
    }

    /// Remove an admin relationship.
    ///
    /// The requestor must be an admin. The owner cannot be removed.
    pub async fn remove_admin(&self, requestor: &Did, target: &Did) -> Result<bool> {
        self.nac
            .remove_admin(requestor, target)
            .await
            .map_err(|e| Error::Acp(format!("failed to remove admin: {}", e)))
    }

    /// Grant a specific permission to an identity.
    pub async fn add_permission_grant(
        &self,
        requestor: &Did,
        target: &Did,
        permission: NodePermission,
    ) -> Result<bool> {
        self.nac
            .add_permission_grant(requestor, target, permission)
            .await
            .map_err(|e| Error::Acp(format!("failed to grant permission: {}", e)))
    }

    /// Revoke a specific permission from an identity.
    pub async fn remove_permission_grant(
        &self,
        requestor: &Did,
        target: &Did,
        permission: NodePermission,
    ) -> Result<bool> {
        self.nac
            .remove_permission_grant(requestor, target, permission)
            .await
            .map_err(|e| Error::Acp(format!("failed to revoke permission: {}", e)))
    }

    /// Get the underlying NAC config.
    pub fn config(&self) -> &NacConfig {
        &self.config
    }
}

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

/// Information about NAC status for HTTP responses.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct NacInfo {
    /// Current NAC status
    pub status: String,

    /// Whether NAC is configured to be enabled
    pub configured_enabled: bool,

    /// Whether dev mode is enabled
    pub dev_mode: bool,

    /// Owner DID if NAC is enabled
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
}

impl<S: ZanzibarStore> NacManager<S> {
    /// Get NAC info for HTTP responses.
    pub async fn info(&self) -> NacInfo {
        let status = self.status().await;
        let owner = self.owner().await;

        NacInfo {
            status: status.to_string(),
            configured_enabled: self.config.enabled,
            dev_mode: self.config.dev_mode,
            owner: owner.map(|d| d.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_did() -> Did {
        Did::new("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK").unwrap()
    }

    fn test_did2() -> Did {
        Did::new("did:key:z6MkpTHR8VNsBxYAAWHut2Geadd9jSwuBV8xRoAnwWsdvktH").unwrap()
    }

    #[tokio::test]
    async fn test_nac_manager_disabled_by_default() {
        let manager = create_memory_nac_manager(NacConfig::default());

        assert!(!manager.is_enabled().await);
        assert_eq!(manager.status().await, NacStatus::NotConfigured);

        // All permissions should be allowed when disabled
        let identity = test_did();
        let allowed = manager
            .check_permission(&identity, NodePermission::DacBypass)
            .await
            .unwrap();
        assert!(allowed);
    }

    #[tokio::test]
    async fn test_nac_manager_enable() {
        let manager = create_memory_nac_manager(NacConfig::new().with_enabled());

        let owner = test_did();
        manager.initialize(Some(&owner)).await.unwrap();

        assert!(manager.is_enabled().await);
        assert_eq!(manager.status().await, NacStatus::Enabled);
        assert!(manager.is_owner(&owner).await);
    }

    #[tokio::test]
    async fn test_nac_manager_permission_check() {
        let manager = create_memory_nac_manager(NacConfig::new().with_enabled());

        let owner = test_did();
        let other = test_did2();
        manager.initialize(Some(&owner)).await.unwrap();

        // Owner has all permissions
        assert!(manager
            .check_permission(&owner, NodePermission::DacBypass)
            .await
            .unwrap());

        // Non-owner denied
        assert!(!manager
            .check_permission(&other, NodePermission::DacBypass)
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn test_nac_manager_admin() {
        let manager = create_memory_nac_manager(NacConfig::new().with_enabled());

        let owner = test_did();
        let admin = test_did2();
        manager.initialize(Some(&owner)).await.unwrap();

        // Add admin
        let added = manager.add_admin(&owner, &admin).await.unwrap();
        assert!(added);

        // Admin has all permissions
        assert!(manager
            .check_permission(&admin, NodePermission::DacBypass)
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn test_nac_manager_disable_reenable() {
        let manager = create_memory_nac_manager(NacConfig::new().with_enabled());

        let owner = test_did();
        let other = test_did2();
        manager.initialize(Some(&owner)).await.unwrap();

        // Disable
        manager.disable(&owner).await.unwrap();
        assert_eq!(manager.status().await, NacStatus::DisabledTemporarily);

        // All operations allowed when disabled
        assert!(manager
            .check_permission(&other, NodePermission::DacBypass)
            .await
            .unwrap());

        // Re-enable
        manager.re_enable(&owner).await.unwrap();
        assert_eq!(manager.status().await, NacStatus::Enabled);

        // Non-owner denied again
        assert!(!manager
            .check_permission(&other, NodePermission::DacBypass)
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn test_nac_manager_purge_requires_dev_mode() {
        let manager = create_memory_nac_manager(NacConfig::new().with_enabled());

        let owner = test_did();
        manager.initialize(Some(&owner)).await.unwrap();

        // Purge should fail without dev mode
        let result = manager.purge(&owner).await;
        assert!(result.is_err());

        // With dev mode
        let manager = create_memory_nac_manager(NacConfig::new().with_enabled().with_dev_mode());
        manager.initialize(Some(&owner)).await.unwrap();

        let result = manager.purge(&owner).await;
        assert!(result.is_ok());
        assert_eq!(manager.status().await, NacStatus::NotConfigured);
    }

    #[tokio::test]
    async fn test_nac_manager_info() {
        let manager = create_memory_nac_manager(NacConfig::new().with_enabled().with_dev_mode());

        let owner = test_did();
        manager.initialize(Some(&owner)).await.unwrap();

        let info = manager.info().await;
        assert_eq!(info.status, "enabled");
        assert!(info.configured_enabled);
        assert!(info.dev_mode);
        assert!(info.owner.is_some());
    }
}
