//! Adapter to bridge NacManager to HTTP's NodeAcpOperations trait.

use std::sync::Arc;

use async_trait::async_trait;

use acp::nac::{NacStatus, NodePermission};
use db::NacManagerApi;
use defra_http::router::{NacStatusInfo, NodeAcpOperations};
use identity::Did;

/// Adapter that implements NodeAcpOperations and NacChecker using NacManagerApi.
pub struct NacAdapter {
    nac: Arc<dyn NacManagerApi>,
}

impl NacAdapter {
    /// Create a new adapter wrapping the given NAC manager.
    pub fn new(nac: Arc<dyn NacManagerApi>) -> Self {
        Self { nac }
    }

    /// Create an Arc-wrapped adapter.
    pub fn new_arc(nac: Arc<dyn NacManagerApi>) -> Arc<dyn NodeAcpOperations> {
        Arc::new(Self::new(nac))
    }

    /// The underlying NAC manager. Used to build the KMS `NodeAcpRead` bridge.
    pub fn nac_manager(&self) -> Arc<dyn NacManagerApi> {
        self.nac.clone()
    }
}

#[async_trait]
impl NodeAcpOperations for NacAdapter {
    async fn check_permission(
        &self,
        identity: &Did,
        permission: NodePermission,
    ) -> Result<bool, String> {
        self.nac
            .check_permission(identity, permission)
            .await
            .map_err(|e| format!("{}", e))
    }

    async fn get_status(&self) -> NacStatus {
        self.nac.status().await
    }

    async fn owner(&self) -> Option<Did> {
        self.nac.owner().await
    }

    async fn is_admin(&self, identity: &Did) -> Result<bool, String> {
        self.nac
            .is_admin(identity)
            .await
            .map_err(|e| format!("{}", e))
    }

    async fn add_admin(&self, requestor: &Did, target: &Did) -> Result<bool, String> {
        self.nac
            .add_admin(requestor, target)
            .await
            .map_err(|e| format!("{}", e))
    }

    async fn remove_admin(&self, requestor: &Did, target: &Did) -> Result<bool, String> {
        self.nac
            .remove_admin(requestor, target)
            .await
            .map_err(|e| format!("{}", e))
    }

    async fn disable(&self, requestor: &Did) -> Result<(), String> {
        self.nac
            .disable(requestor)
            .await
            .map_err(|e| format!("{}", e))
    }

    async fn re_enable(&self, requestor: &Did) -> Result<(), String> {
        self.nac
            .re_enable(requestor)
            .await
            .map_err(|e| format!("{}", e))
    }

    async fn enable(&self, owner: &Did) -> Result<(), String> {
        self.nac.enable(owner).await.map_err(|e| format!("{}", e))
    }

    async fn add_relationship(
        &self,
        requestor: &Did,
        target: &Did,
        relation: &str,
    ) -> Result<bool, String> {
        if relation == "admin" {
            self.nac
                .add_admin(requestor, target)
                .await
                .map_err(|e| format!("{}", e))
        } else if let Some(perm) = NodePermission::parse(relation) {
            self.nac
                .add_permission_grant(requestor, target, perm)
                .await
                .map_err(|e| format!("{}", e))
        } else {
            Err("relation not in resource".to_string())
        }
    }

    async fn remove_relationship(
        &self,
        requestor: &Did,
        target: &Did,
        relation: &str,
    ) -> Result<bool, String> {
        if relation == "admin" {
            self.nac
                .remove_admin(requestor, target)
                .await
                .map_err(|e| format!("{}", e))
        } else if let Some(perm) = NodePermission::parse(relation) {
            self.nac
                .remove_permission_grant(requestor, target, perm)
                .await
                .map_err(|e| format!("{}", e))
        } else {
            Err("relation not in resource".to_string())
        }
    }

    async fn info(&self) -> NacStatusInfo {
        let info = self.nac.info().await;
        NacStatusInfo {
            status: info.status,
            configured_enabled: info.configured_enabled,
            dev_mode: info.dev_mode,
            owner: info.owner,
        }
    }
}

#[async_trait]
impl query::NacChecker for NacAdapter {
    async fn check_permission(&self, identity: &Did, permission: NodePermission) -> bool {
        self.nac
            .check_permission(identity, permission)
            .await
            .unwrap_or(false)
    }
}
