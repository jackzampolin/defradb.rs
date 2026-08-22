//! NacManagerApi trait implementation for NacManager.

use acp::nac::{NacStatus, NodePermission};
use acp::ZanzibarStore;
use async_trait::async_trait;
use identity::Did;

use crate::nac::error::Result;

use super::{NacInfo, NacManager, NacManagerApi};

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S: ZanzibarStore> NacManagerApi for NacManager<S> {
    async fn initialize(&self, owner_identity: Option<&Did>) -> Result<()> {
        NacManager::initialize(self, owner_identity).await
    }
    async fn status(&self) -> NacStatus {
        NacManager::status(self).await
    }
    async fn owner(&self) -> Option<Did> {
        NacManager::owner(self).await
    }
    async fn is_enabled(&self) -> bool {
        NacManager::is_enabled(self).await
    }
    async fn check_permission(&self, identity: &Did, permission: NodePermission) -> Result<bool> {
        NacManager::check_permission(self, identity, permission).await
    }
    async fn is_admin(&self, identity: &Did) -> Result<bool> {
        NacManager::is_admin(self, identity).await
    }
    async fn is_admin_persisted(&self, identity: &Did) -> Result<bool> {
        NacManager::is_admin_persisted(self, identity).await
    }
    async fn is_owner(&self, identity: &Did) -> bool {
        NacManager::is_owner(self, identity).await
    }
    async fn enable(&self, owner: &Did) -> Result<()> {
        NacManager::enable(self, owner).await
    }
    async fn disable(&self, requestor: &Did) -> Result<()> {
        NacManager::disable(self, requestor).await
    }
    async fn re_enable(&self, requestor: &Did) -> Result<()> {
        NacManager::re_enable(self, requestor).await
    }
    async fn purge(&self, requestor: &Did) -> Result<()> {
        NacManager::purge(self, requestor).await
    }
    async fn add_admin(&self, requestor: &Did, target: &Did) -> Result<bool> {
        NacManager::add_admin(self, requestor, target).await
    }
    async fn remove_admin(&self, requestor: &Did, target: &Did) -> Result<bool> {
        NacManager::remove_admin(self, requestor, target).await
    }
    async fn add_permission_grant(
        &self,
        requestor: &Did,
        target: &Did,
        permission: NodePermission,
    ) -> Result<bool> {
        NacManager::add_permission_grant(self, requestor, target, permission).await
    }
    async fn remove_permission_grant(
        &self,
        requestor: &Did,
        target: &Did,
        permission: NodePermission,
    ) -> Result<bool> {
        NacManager::remove_permission_grant(self, requestor, target, permission).await
    }
    async fn info(&self) -> NacInfo {
        NacManager::info(self).await
    }
}
