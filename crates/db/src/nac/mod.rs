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

pub mod factory;
#[cfg(test)]
mod tests;
mod trait_impl;

pub use factory::create_memory_nac_manager;
#[cfg(not(target_arch = "wasm32"))]
pub use factory::create_persistent_nac_manager;

use std::sync::Arc;

use acp::nac::{NacStatus, NodeACP, NodePermission};
use acp::ZanzibarStore;
use async_trait::async_trait;
use identity::Did;

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

/// Trait for NAC manager operations, enabling dynamic dispatch over different store backends.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait NacManagerApi: defra_core::thread_bounds::MaybeSendSync {
    async fn initialize(&self, owner_identity: Option<&Did>) -> Result<()>;
    async fn status(&self) -> NacStatus;
    async fn owner(&self) -> Option<Did>;
    async fn is_enabled(&self) -> bool;
    async fn check_permission(&self, identity: &Did, permission: NodePermission) -> Result<bool>;
    async fn is_admin(&self, identity: &Did) -> Result<bool>;
    async fn is_admin_persisted(&self, identity: &Did) -> Result<bool>;
    async fn is_owner(&self, identity: &Did) -> bool;
    async fn enable(&self, owner: &Did) -> Result<()>;
    async fn disable(&self, requestor: &Did) -> Result<()>;
    async fn re_enable(&self, requestor: &Did) -> Result<()>;
    async fn purge(&self, requestor: &Did) -> Result<()>;
    async fn add_admin(&self, requestor: &Did, target: &Did) -> Result<bool>;
    async fn remove_admin(&self, requestor: &Did, target: &Did) -> Result<bool>;
    async fn add_permission_grant(
        &self,
        requestor: &Did,
        target: &Did,
        permission: NodePermission,
    ) -> Result<bool>;
    async fn remove_permission_grant(
        &self,
        requestor: &Did,
        target: &Did,
        permission: NodePermission,
    ) -> Result<bool>;
    async fn info(&self) -> NacInfo;
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
        self.nac
            .load()
            .await
            .map_err(|e| Error::Other(format!("failed to load NAC state: {}", e)))?;

        if self.config.enabled {
            let current_status = self.nac.status().await;

            match current_status {
                NacStatus::NotConfigured => {
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
                    self.nac
                        .re_enable()
                        .await
                        .map_err(|e| Error::Other(format!("failed to re-enable NAC: {}", e)))?;
                    tracing::info!(
                        target: "nac::startup",
                        "NAC re-enabled from temporarily disabled state"
                    );
                }
                _ => return Err(Error::Other("unexpected NacStatus variant".to_string())),
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
    pub async fn is_enabled(&self) -> bool {
        self.config.enabled && self.nac.status().await == NacStatus::Enabled
    }

    /// Check if an identity has a specific node permission.
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
    pub async fn enable(&self, owner: &Did) -> Result<()> {
        self.nac
            .enable(owner)
            .await
            .map_err(|e| Error::Acp(format!("failed to enable NAC: {}", e)))
    }

    /// Temporarily disable NAC.
    pub async fn disable(&self, requestor: &Did) -> Result<()> {
        if !self.is_admin(requestor).await? {
            return Err(Error::Acp("only admins can disable NAC".into()));
        }

        self.nac
            .disable()
            .await
            .map_err(|e| Error::Acp(format!("failed to disable NAC: {}", e)))
    }

    /// Re-enable NAC after temporary disable.
    pub async fn re_enable(&self, requestor: &Did) -> Result<()> {
        if !self.is_admin_persisted(requestor).await? {
            return Err(Error::Acp("only admins can re-enable NAC".into()));
        }

        self.nac
            .re_enable()
            .await
            .map_err(|e| Error::Acp(format!("failed to re-enable NAC: {}", e)))
    }

    /// Purge all NAC state (dev mode only).
    pub async fn purge(&self, requestor: &Did) -> Result<()> {
        if !self.config.dev_mode {
            return Err(Error::Acp("NAC purge is only allowed in dev mode".into()));
        }

        if !self.is_admin(requestor).await? {
            return Err(Error::Acp("only admins can purge NAC".into()));
        }

        self.nac
            .purge()
            .await
            .map_err(|e| Error::Acp(format!("failed to purge NAC: {}", e)))
    }

    /// Add an admin relationship.
    pub async fn add_admin(&self, requestor: &Did, target: &Did) -> Result<bool> {
        self.nac
            .add_admin(requestor, target)
            .await
            .map_err(|e| Error::Acp(format!("failed to add admin: {}", e)))
    }

    /// Remove an admin relationship.
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
