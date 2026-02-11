//! Mock NAC (Node Access Control) operations for testing.

use async_trait::async_trait;
use identity::Did;
use std::sync::{Arc, RwLock};

use crate::router::{NacStatus, NodeAcpOperations, NodePermission};

/// Mock NAC operations for testing NAC-protected handlers.
///
/// Configurable mock that allows controlling:
/// - NAC status (enabled, disabled, not configured)
/// - Owner identity
/// - Admin identities
/// - Permission grants
#[derive(Debug)]
pub struct MockNodeAcpOperations {
    status: Arc<RwLock<NacStatus>>,
    owner: Arc<RwLock<Option<Did>>>,
    admins: Arc<RwLock<Vec<Did>>>,
    /// Permission grants: (identity, permission) pairs
    grants: Arc<RwLock<Vec<(Did, NodePermission)>>>,
}

impl Clone for MockNodeAcpOperations {
    fn clone(&self) -> Self {
        Self {
            status: Arc::clone(&self.status),
            owner: Arc::clone(&self.owner),
            admins: Arc::clone(&self.admins),
            grants: Arc::clone(&self.grants),
        }
    }
}

impl Default for MockNodeAcpOperations {
    fn default() -> Self {
        Self::new()
    }
}

impl MockNodeAcpOperations {
    /// Create a new mock NAC with NAC not configured (permissive).
    pub fn new() -> Self {
        Self {
            status: Arc::new(RwLock::new(NacStatus::NotConfigured)),
            owner: Arc::new(RwLock::new(None)),
            admins: Arc::new(RwLock::new(vec![])),
            grants: Arc::new(RwLock::new(vec![])),
        }
    }

    /// Create with NAC enabled and the given owner.
    pub fn enabled_with_owner(owner: Did) -> Self {
        Self {
            status: Arc::new(RwLock::new(NacStatus::Enabled)),
            owner: Arc::new(RwLock::new(Some(owner))),
            admins: Arc::new(RwLock::new(vec![])),
            grants: Arc::new(RwLock::new(vec![])),
        }
    }

    /// Create with NAC disabled temporarily.
    pub fn disabled() -> Self {
        Self {
            status: Arc::new(RwLock::new(NacStatus::DisabledTemporarily)),
            owner: Arc::new(RwLock::new(None)),
            admins: Arc::new(RwLock::new(vec![])),
            grants: Arc::new(RwLock::new(vec![])),
        }
    }

    /// Add an admin identity.
    pub fn with_admin(self, admin: Did) -> Self {
        self.admins.write().unwrap().push(admin);
        self
    }

    /// Add a permission grant.
    pub fn with_grant(self, identity: Did, permission: NodePermission) -> Self {
        self.grants.write().unwrap().push((identity, permission));
        self
    }
}

#[async_trait]
impl NodeAcpOperations for MockNodeAcpOperations {
    async fn check_permission(
        &self,
        identity: &Did,
        permission: NodePermission,
    ) -> Result<bool, String> {
        let status = *self.status.read().unwrap();

        // If NAC is not enabled, allow all
        if status != NacStatus::Enabled {
            return Ok(true);
        }

        // Check if owner
        if let Some(owner) = self.owner.read().unwrap().as_ref() {
            if owner == identity {
                return Ok(true);
            }
        }

        // Check if admin (admins have all permissions)
        if self.admins.read().unwrap().contains(identity) {
            return Ok(true);
        }

        // Check specific permission grants
        let grants = self.grants.read().unwrap();
        for (grantee, perm) in grants.iter() {
            if grantee == identity && *perm == permission {
                return Ok(true);
            }
        }

        Ok(false)
    }

    async fn get_status(&self) -> NacStatus {
        *self.status.read().unwrap()
    }

    async fn owner(&self) -> Option<Did> {
        self.owner.read().unwrap().clone()
    }

    async fn is_admin(&self, identity: &Did) -> Result<bool, String> {
        let status = *self.status.read().unwrap();
        if status != NacStatus::Enabled {
            return Ok(true); // Everyone is admin when NAC is disabled
        }

        // Check if owner
        if let Some(owner) = self.owner.read().unwrap().as_ref() {
            if owner == identity {
                return Ok(true);
            }
        }

        // Check admins list
        Ok(self.admins.read().unwrap().contains(identity))
    }

    async fn add_admin(&self, _requestor: &Did, target: &Did) -> Result<bool, String> {
        let status = *self.status.read().unwrap();
        if status == NacStatus::DisabledTemporarily {
            return Err(
                "cannot modify relationships while NAC is disabled - re-enable NAC first".into(),
            );
        }

        let mut admins = self.admins.write().unwrap();
        if admins.contains(target) {
            return Ok(false);
        }
        admins.push(target.clone());
        Ok(true)
    }

    async fn remove_admin(&self, _requestor: &Did, target: &Did) -> Result<bool, String> {
        let status = *self.status.read().unwrap();
        if status == NacStatus::DisabledTemporarily {
            return Err(
                "cannot modify relationships while NAC is disabled - re-enable NAC first".into(),
            );
        }

        // Cannot remove owner
        if let Some(owner) = self.owner.read().unwrap().as_ref() {
            if owner == target {
                return Err("cannot remove owner's admin access".into());
            }
        }

        let mut admins = self.admins.write().unwrap();
        let initial_len = admins.len();
        admins.retain(|a| a != target);
        Ok(admins.len() < initial_len)
    }

    async fn disable(&self, _requestor: &Did) -> Result<(), String> {
        let mut status = self.status.write().unwrap();
        *status = NacStatus::DisabledTemporarily;
        Ok(())
    }

    async fn re_enable(&self, _requestor: &Did) -> Result<(), String> {
        let mut status = self.status.write().unwrap();
        *status = NacStatus::Enabled;
        Ok(())
    }
}

/// Mock NAC operations that always fails with a configurable error.
///
/// Use this to test error handling paths in handlers when NAC
/// permission checks fail with internal errors (not just permission denied).
#[derive(Debug, Clone)]
pub struct FailingMockNodeAcpOperations {
    error: String,
}

impl FailingMockNodeAcpOperations {
    /// Create a new failing mock with the given error message.
    pub fn new(error: impl Into<String>) -> Self {
        Self {
            error: error.into(),
        }
    }
}

#[async_trait]
impl NodeAcpOperations for FailingMockNodeAcpOperations {
    async fn check_permission(
        &self,
        _identity: &Did,
        _permission: NodePermission,
    ) -> Result<bool, String> {
        Err(self.error.clone())
    }

    async fn get_status(&self) -> NacStatus {
        NacStatus::Enabled
    }

    async fn owner(&self) -> Option<Did> {
        None
    }

    async fn is_admin(&self, _identity: &Did) -> Result<bool, String> {
        Err(self.error.clone())
    }

    async fn add_admin(&self, _requestor: &Did, _target: &Did) -> Result<bool, String> {
        Err(self.error.clone())
    }

    async fn remove_admin(&self, _requestor: &Did, _target: &Did) -> Result<bool, String> {
        Err(self.error.clone())
    }

    async fn disable(&self, _requestor: &Did) -> Result<(), String> {
        Err(self.error.clone())
    }

    async fn re_enable(&self, _requestor: &Did) -> Result<(), String> {
        Err(self.error.clone())
    }
}
