use identity::Did;

use super::node_acp::NacStatus;
use super::permission::NodePermission;

/// Determine whether DAC bypass should be enabled for an identity.
///
/// Returns `true` only when NAC is enabled *and* the identity has `DacBypass`
/// permission (admin/owner can read all documents regardless of DAC policies).
///
/// The `check_permission` closure abstracts over the different NAC traits
/// used by FFI (`NacManagerApi`) and HTTP (`NodeAcpOperations`).
pub async fn should_bypass_dac<'a, F, Fut>(
    status: NacStatus,
    did: Option<&'a Did>,
    check_permission: F,
) -> bool
where
    F: FnOnce(&'a Did, NodePermission) -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    if status != NacStatus::Enabled {
        return false;
    }
    let Some(did) = did else {
        return false;
    };
    check_permission(did, NodePermission::DacBypass).await
}
