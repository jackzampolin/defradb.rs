//! Adapter to bridge NacManager to HTTP's NodeAcpOperations trait.

use std::sync::Arc;

use async_trait::async_trait;

use acp::nac::{NacStatus, NodePermission};
use db::NacManagerApi;
use defra_http::router::NodeAcpOperations;
use identity::Did;

/// Adapter that implements NodeAcpOperations using NacManagerApi.
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
}
