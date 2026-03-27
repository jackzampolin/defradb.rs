//! Node Access Control (NAC) implementation.
//!
//! NAC provides node-level access control using the Zanzibar permission model.
//!
//! ## Security Features
//!
//! - **Write blocking when disabled**: All relationship modifications are blocked
//!   when NAC is disabled to prevent privilege escalation attacks.

mod lifecycle;
mod operations;

use std::sync::Arc;
use storage::corekv::MaybeSendSync;

use async_lock::RwLock;
use async_trait::async_trait;
use identity::Did;

use crate::error::{Error, Result};
use crate::nac::permission::NodePermission;
use crate::nac::policy::{
    validate_node_policy, NODE_POLICY_ID, NODE_RESOURCE_NAME, OWNER_RELATION,
};
use zanzibar::{PermissionEngine, Subject, ZanzibarStore};

/// The fixed object ID for the node resource.
/// There's only one "node" instance, so we use a constant ID.
pub const NODE_OBJECT_ID: &str = "singleton";

/// Sentinel relation name used to persist "disabled temporarily" status.
/// Stored as a relationship in the Zanzibar store so it survives restarts.
const DISABLED_RELATION: &str = "_disabled";

/// NAC lifecycle state machine.
///
/// ```text
/// NotConfigured --[enable()]--> Enabled
/// Enabled --[disable()]--> DisabledTemporarily
/// DisabledTemporarily --[re_enable()]--> Enabled
/// Enabled --[purge()]--> NotConfigured
/// DisabledTemporarily --[purge()]--> NotConfigured
/// ```
///
/// - `NotConfigured`: NAC has never been enabled or has been purged.
///   All node operations are unrestricted.
/// - `Enabled`: NAC is active. All node operations require permission checks.
/// - `DisabledTemporarily`: NAC is paused. Permission checks are bypassed but
///   write operations (adding/removing admins) are blocked to prevent
///   privilege escalation. State is preserved for re-enable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NacStatus {
    NotConfigured,
    Enabled,
    DisabledTemporarily,
}

impl std::fmt::Display for NacStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotConfigured => write!(f, "not configured"),
            Self::Enabled => write!(f, "enabled"),
            Self::DisabledTemporarily => write!(f, "disabled temporarily"),
        }
    }
}

/// Node Access Control implementation.
///
/// Uses a local Zanzibar store to manage node-level permissions.
/// Unlike DAC, NAC is always local (no SourceHub option).
pub struct NodeACP<S: ZanzibarStore> {
    store: Arc<S>,
    engine: RwLock<PermissionEngine<S>>,
    status: RwLock<NacStatus>,
    owner: RwLock<Option<Did>>,
}

impl<S: ZanzibarStore> NodeACP<S> {
    /// Create a new NodeACP with the given store.
    ///
    /// Starts in NotConfigured status. Call `enable()` to activate.
    pub fn new(store: Arc<S>) -> Self {
        Self {
            store: store.clone(),
            engine: RwLock::new(PermissionEngine::new(store)),
            status: RwLock::new(NacStatus::NotConfigured),
            owner: RwLock::new(None),
        }
    }

    /// Load existing NAC state from the store.
    ///
    /// Should be called during startup to restore NAC state.
    pub async fn load(&self) -> Result<()> {
        if let Some(policy) = self.store.get_policy(NODE_POLICY_ID).await? {
            validate_node_policy(&policy).map_err(Error::InvalidPolicy)?;

            let mut engine = self.engine.write().await;
            engine.add_policy(&policy);

            let subjects = self
                .store
                .get_relation_subjects(
                    NODE_POLICY_ID,
                    NODE_RESOURCE_NAME,
                    NODE_OBJECT_ID,
                    OWNER_RELATION,
                )
                .await?;

            if let Some(Subject::Entity(owner_did)) = subjects.first() {
                *self.owner.write().await = Some(owner_did.clone());

                let disabled_subjects = self
                    .store
                    .get_relation_subjects(
                        NODE_POLICY_ID,
                        NODE_RESOURCE_NAME,
                        NODE_OBJECT_ID,
                        DISABLED_RELATION,
                    )
                    .await?;

                if disabled_subjects.is_empty() {
                    *self.status.write().await = NacStatus::Enabled;
                } else {
                    *self.status.write().await = NacStatus::DisabledTemporarily;
                }

                tracing::info!(
                    target: "nac::audit",
                    event = "nac_loaded",
                    owner = %owner_did,
                    disabled = !disabled_subjects.is_empty(),
                    "NAC state loaded from store"
                );
            }
        }

        Ok(())
    }

    pub async fn status(&self) -> NacStatus {
        *self.status.read().await
    }

    pub async fn owner(&self) -> Option<Did> {
        self.owner.read().await.clone()
    }
}

/// Trait for NAC operations accessible via HTTP.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait NodeAcpOperations: MaybeSendSync {
    async fn check_permission(&self, identity: &Did, permission: NodePermission) -> Result<bool>;
    async fn get_status(&self) -> NacStatus;
    async fn add_admin(&self, requestor: &Did, target: &Did) -> Result<bool>;
    async fn remove_admin(&self, requestor: &Did, target: &Did) -> Result<bool>;
    async fn owner(&self) -> Option<Did>;
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S: ZanzibarStore + 'static> NodeAcpOperations for NodeACP<S> {
    async fn check_permission(&self, identity: &Did, permission: NodePermission) -> Result<bool> {
        NodeACP::check_permission(self, identity, permission).await
    }

    async fn get_status(&self) -> NacStatus {
        self.status().await
    }

    async fn add_admin(&self, requestor: &Did, target: &Did) -> Result<bool> {
        NodeACP::add_admin(self, requestor, target).await
    }

    async fn remove_admin(&self, requestor: &Did, target: &Did) -> Result<bool> {
        NodeACP::remove_admin(self, requestor, target).await
    }

    async fn owner(&self) -> Option<Did> {
        NodeACP::owner(self).await
    }
}
