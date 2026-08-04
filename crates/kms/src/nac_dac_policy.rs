//! NAC + DAC policy. Implements the dual-gate split from Go PR #4778.
//!
//! Document-scoped DEK release:
//!   - Collection lookup runs internally (node-level; no actor check at
//!     this step, mirroring how the serving peer resolves which policy
//!     to apply).
//!   - DAC permission check runs as the transport-authenticated peer DID.
//!
//! Collection-scoped DEK release:
//!   - NAC `read-document` check on the actor.
//!
//! `node_acp` is held in an `OnceLock` because NAC initializes after the
//! KMS service in `EmbeddedNode::build_with_store` (mirrors Go's
//! `func() NACInfo` getter pattern from PR #4778).

use async_trait::async_trait;
use identity::Did;
use std::sync::Arc;
use std::sync::OnceLock;

use crate::error::{Error, Result};
use crate::policy::{AccessPolicy, DocCollectionLookup, NodeAcpRead};
use crate::types::{KeyScope, PolicyDecision};

/// Default `AccessPolicy` impl for `DefraKms`. Combines DAC for
/// document-scoped keys with NAC for collection-scoped keys, both gated
/// on the transport-authenticated peer DID.
pub struct NacDacPolicy {
    doc_acp: Arc<dyn acp::DocumentACP>,
    doc_lookup: Arc<dyn DocCollectionLookup>,
    node_acp: OnceLock<Arc<dyn NodeAcpRead>>,
}

impl NacDacPolicy {
    /// Construct a policy. `node_acp` starts unset — call `set_node_acp`
    /// after NAC has initialized.
    pub fn new(
        doc_acp: Arc<dyn acp::DocumentACP>,
        doc_lookup: Arc<dyn DocCollectionLookup>,
    ) -> Self {
        Self {
            doc_acp,
            doc_lookup,
            node_acp: OnceLock::new(),
        }
    }

    /// Install the NAC reader. First call wins; subsequent calls are
    /// silently discarded (matches `OnceLock` semantics). Callers must not
    /// rely on swapping the NAC reader at runtime. Called by the embedded-
    /// node layer once NAC is ready.
    pub fn set_node_acp(&self, nac: Arc<dyn NodeAcpRead>) {
        let _ = self.node_acp.set(nac);
    }
}

/// Map an `acp::Error` to either `AccessDenied` (the backend actively
/// refused, e.g. permission denied / not owner / not manager / not
/// registered) or `Internal` (everything else — storage, JSON, cycle
/// detected, invalid policy, etc. — which are NOT policy denials).
fn classify_acp_error(e: acp::Error) -> Error {
    use acp::Error as A;
    match e {
        A::PermissionDenied(_)
        | A::DocumentNotRegistered(_)
        | A::NotOwner { .. }
        | A::NotManager { .. } => Error::AccessDenied {
            reason: e.to_string(),
        },
        other => Error::Internal(format!("acp backend: {other}")),
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl AccessPolicy for NacDacPolicy {
    async fn check_release(&self, actor: Option<&Did>, scope: &KeyScope) -> Result<PolicyDecision> {
        match scope {
            KeyScope::Document { doc_id, .. } => {
                // Collection lookup runs internally (no actor check at this
                // step; mirrors Go's "node-level" resolution).
                //
                // `None` means the document's collection has no DAC policy
                // attached (ACP not configured for it). With no policy there is
                // no per-document access gate, so the DEK is freely releasable —
                // matching the legacy decrypt path, which reads the key from the
                // Encryption block with no policy check at all.
                let Some(info) = self.doc_lookup.collection_for_doc(doc_id).await? else {
                    return Ok(PolicyDecision::Allow);
                };

                let actor_id: acp::Identity = actor.into();
                let checker = acp::read_access::DirectChecker {
                    acp: self.doc_acp.as_ref(),
                    identity: &actor_id,
                    node_did: None,
                };
                let allowed = acp::read_access::check_doc_read_access(
                    &checker,
                    &info.policy_id,
                    &info.resource_name,
                    &info.collection_id,
                    info.is_branchable,
                    doc_id,
                )
                .await
                .map_err(classify_acp_error)?;
                Ok(if allowed {
                    PolicyDecision::Allow
                } else {
                    PolicyDecision::Deny
                })
            }
            KeyScope::Collection { .. } => self.check_node_release(actor, scope).await,
        }
    }

    async fn check_node_release(
        &self,
        actor: Option<&Did>,
        _scope: &KeyScope,
    ) -> Result<PolicyDecision> {
        let Some(nac) = self.node_acp.get() else {
            // NAC not configured => allow. Mirrors Go's behavior when NAC
            // is in `NACNotConfigured` state.
            return Ok(PolicyDecision::Allow);
        };
        let Some(did) = actor else {
            // NAC is configured but the caller is anonymous. NAC grants are
            // keyed by DID; anonymous cannot satisfy a permission check.
            // (When NAC is NOT configured, anonymous is allowed at the
            //  `node_acp.get()` early-return above.)
            return Ok(PolicyDecision::Deny);
        };
        let allowed = nac
            .check_node_permission(did, "read-document")
            .await
            .map_err(classify_acp_error)?;
        Ok(if allowed {
            PolicyDecision::Allow
        } else {
            PolicyDecision::Deny
        })
    }
}

#[cfg(test)]
#[path = "nac_dac_policy_tests.rs"]
mod tests;
