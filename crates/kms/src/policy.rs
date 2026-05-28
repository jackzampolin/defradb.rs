//! Authorization gate for DEK release + supporting lookup traits.

use async_trait::async_trait;
use identity::Did;

use crate::error::Result;
use crate::types::{KeyScope, PolicyDecision};

/// Authorization gate consulted before releasing a DEK.
///
/// `DefraKms` calls `check_release` on the serving peer before ECIES-wrapping
/// a reply block, and (defense-in-depth) on the requesting peer before
/// caching a received DEK. `NacDacPolicy` is the M1 default; M4 adds
/// `SourceHubAttestedPolicy`.
#[async_trait]
pub trait AccessPolicy: Send + Sync {
    /// Per-key authorization check. Returns `Allow` to release the DEK,
    /// `Deny` to refuse.
    async fn check_release(&self, actor: Option<&Did>, scope: &KeyScope) -> Result<PolicyDecision>;

    /// Node-level authorization check for collection-scoped keys.
    /// Called when the scope is `KeyScope::Collection`.
    async fn check_node_release(
        &self,
        actor: Option<&Did>,
        scope: &KeyScope,
    ) -> Result<PolicyDecision>;
}

/// Minimal abstraction over the node ACP. Lives in `crates/kms/` so the
/// KMS doesn't take a direct dep on `crates/db/`'s NacManagerApi; the
/// embedded-node layer (Phase K) provides an adapter that wraps the real
/// NacManager.
#[async_trait]
pub trait NodeAcpRead: Send + Sync {
    /// Check whether `actor` holds the named node-level permission.
    /// Permission strings match NAC's internal names (e.g. `"read-document"`).
    async fn check_node_permission(&self, actor: &Did, permission: &str) -> acp::Result<bool>;
}

/// Resolves a document's collection metadata for ACP policy checks.
///
/// Mirrors Go's `internal/db/collection_retriever.go::RetrieveCollectionFromDocID`:
/// uses the headstore (already keyed by doc_id) to find the doc's first
/// head block, reads `schema_version_id` off the delta, resolves the
/// collection + its policy. Implementation lives in the embedded-node
/// layer (Phase K); kms keeps this trait abstract so unit tests can
/// inject fakes.
#[async_trait]
pub trait DocCollectionLookup: Send + Sync {
    /// Returns `None` if the doc is unknown locally.
    async fn collection_for_doc(&self, doc_id: &str) -> Result<Option<DocCollectionInfo>>;
}

/// Subset of collection metadata the KMS needs for ACP gating.
#[derive(Debug, Clone)]
pub struct DocCollectionInfo {
    /// Collection id (for tracing; not used in the policy check itself).
    pub collection_id: String,
    /// Policy id under which to check the DAC permission.
    pub policy_id: String,
    /// Resource name within the policy.
    pub resource_name: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    fn assert_object_safe<T: ?Sized + Send + Sync>() {}

    #[test]
    fn access_policy_is_object_safe() {
        assert_object_safe::<dyn AccessPolicy>();
    }
    #[test]
    fn doc_collection_lookup_is_object_safe() {
        assert_object_safe::<dyn DocCollectionLookup>();
    }
    #[test]
    fn node_acp_read_is_object_safe() {
        assert_object_safe::<dyn NodeAcpRead>();
    }
}
