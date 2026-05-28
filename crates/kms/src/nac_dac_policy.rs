//! NAC + DAC policy. Implements the dual-gate split from Go PR #4778.
//!
//! Document-scoped DEK release:
//!   - Collection lookup runs internally (node-level; no actor check at
//!     this step, mirroring how the serving peer resolves which policy
//!     to apply).
//!   - DAC permission check runs as the **actor** (the caller from the
//!     wire `identity` field).
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
/// on the actor (the caller DID from the wire request).
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

#[async_trait]
impl AccessPolicy for NacDacPolicy {
    async fn check_release(&self, actor: Option<&Did>, scope: &KeyScope) -> Result<PolicyDecision> {
        match scope {
            KeyScope::Document { doc_id, .. } => {
                // Collection lookup runs internally (no actor check at this
                // step; mirrors Go's "node-level" resolution).
                let info = self
                    .doc_lookup
                    .collection_for_doc(doc_id)
                    .await?
                    .ok_or_else(|| Error::Internal(format!("no collection for doc {doc_id}")))?;

                // DAC permission check runs as the actor.
                let actor_id: acp::Identity = actor.into();
                let allowed = self
                    .doc_acp
                    .check_doc_access(
                        &actor_id,
                        acp::DocumentPermission::Read,
                        &info.policy_id,
                        &info.resource_name,
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
mod tests {
    use super::*;
    use crate::policy::{DocCollectionInfo, DocCollectionLookup, NodeAcpRead};
    use crate::types::{KeyScope, PolicyDecision};
    use std::sync::Arc;

    struct FakeDac {
        allow: bool,
    }
    #[async_trait::async_trait]
    impl acp::DocumentACP for FakeDac {
        async fn register_doc_object(
            &self,
            _: &identity::Did,
            _: &str,
            _: &str,
            _: &str,
        ) -> acp::Result<()> {
            Ok(())
        }
        async fn is_doc_registered(&self, _: &str, _: &str, _: &str) -> acp::Result<bool> {
            Ok(true)
        }
        async fn check_doc_access(
            &self,
            _: &acp::Identity,
            _: acp::DocumentPermission,
            _: &str,
            _: &str,
            _: &str,
        ) -> acp::Result<bool> {
            Ok(self.allow)
        }
        async fn add_actor_relationship(
            &self,
            _: &identity::Did,
            _: &identity::Did,
            _: &str,
            _: &str,
            _: &str,
            _: &str,
            _: &[String],
        ) -> acp::Result<bool> {
            Ok(true)
        }
        async fn delete_actor_relationship(
            &self,
            _: &identity::Did,
            _: &identity::Did,
            _: &str,
            _: &str,
            _: &str,
            _: &str,
            _: &[String],
        ) -> acp::Result<bool> {
            Ok(true)
        }
        async fn unregister_doc_object(&self, _: &str, _: &str, _: &str) -> acp::Result<()> {
            Ok(())
        }
    }

    struct FakeNac {
        allow: bool,
    }
    #[async_trait::async_trait]
    impl NodeAcpRead for FakeNac {
        async fn check_node_permission(&self, _: &identity::Did, _: &str) -> acp::Result<bool> {
            Ok(self.allow)
        }
    }

    struct FakeLookup;
    #[async_trait::async_trait]
    impl DocCollectionLookup for FakeLookup {
        async fn collection_for_doc(&self, _: &str) -> crate::Result<Option<DocCollectionInfo>> {
            Ok(Some(DocCollectionInfo {
                collection_id: "col-1".into(),
                policy_id: "policy-1".into(),
                resource_name: "doc".into(),
            }))
        }
    }

    fn did(s: &str) -> identity::Did {
        s.parse().unwrap()
    }

    #[tokio::test]
    async fn doc_scope_allow_when_user_has_dac_grant() {
        let policy = NacDacPolicy::new(Arc::new(FakeDac { allow: true }), Arc::new(FakeLookup));
        policy.set_node_acp(Arc::new(FakeNac { allow: true }));
        let decision = policy
            .check_release(
                Some(&did("did:key:zalice")),
                &KeyScope::Document {
                    doc_id: "d1".into(),
                    field: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(decision, PolicyDecision::Allow);
    }

    #[tokio::test]
    async fn doc_scope_deny_when_user_lacks_grant() {
        let policy = NacDacPolicy::new(Arc::new(FakeDac { allow: false }), Arc::new(FakeLookup));
        policy.set_node_acp(Arc::new(FakeNac { allow: true }));
        let decision = policy
            .check_release(
                Some(&did("did:key:zalice")),
                &KeyScope::Document {
                    doc_id: "d1".into(),
                    field: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(decision, PolicyDecision::Deny);
    }

    #[tokio::test]
    async fn collection_scope_uses_nac() {
        let policy = NacDacPolicy::new(Arc::new(FakeDac { allow: false }), Arc::new(FakeLookup));
        policy.set_node_acp(Arc::new(FakeNac { allow: true }));
        let decision = policy
            .check_release(
                Some(&did("did:key:zalice")),
                &KeyScope::Collection {
                    collection_id: "c".into(),
                },
            )
            .await
            .unwrap();
        assert_eq!(decision, PolicyDecision::Allow);
    }

    #[tokio::test]
    async fn nac_not_configured_allows_collection() {
        let policy = NacDacPolicy::new(Arc::new(FakeDac { allow: false }), Arc::new(FakeLookup));
        // No set_node_acp call.
        let decision = policy
            .check_release(
                Some(&did("did:key:zalice")),
                &KeyScope::Collection {
                    collection_id: "c".into(),
                },
            )
            .await
            .unwrap();
        assert_eq!(decision, PolicyDecision::Allow);
    }

    #[tokio::test]
    async fn doc_scope_with_no_actor_uses_anonymous_dac() {
        // FakeDac::allow=false means the DAC denies regardless of identity,
        // so this exercises the Anonymous-conversion path on the None branch.
        let policy = NacDacPolicy::new(Arc::new(FakeDac { allow: false }), Arc::new(FakeLookup));
        // NAC unset.
        let decision = policy
            .check_release(
                None,
                &KeyScope::Document {
                    doc_id: "d1".into(),
                    field: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(decision, PolicyDecision::Deny);
    }

    #[tokio::test]
    async fn collection_scope_with_nac_set_and_no_actor_denies() {
        let policy = NacDacPolicy::new(Arc::new(FakeDac { allow: true }), Arc::new(FakeLookup));
        policy.set_node_acp(Arc::new(FakeNac { allow: true }));
        let decision = policy
            .check_release(
                None,
                &KeyScope::Collection {
                    collection_id: "c".into(),
                },
            )
            .await
            .unwrap();
        assert_eq!(decision, PolicyDecision::Deny);
    }

    #[tokio::test]
    async fn doc_scope_missing_collection_returns_internal_error() {
        struct EmptyLookup;
        #[async_trait::async_trait]
        impl crate::policy::DocCollectionLookup for EmptyLookup {
            async fn collection_for_doc(
                &self,
                _: &str,
            ) -> crate::Result<Option<crate::policy::DocCollectionInfo>> {
                Ok(None)
            }
        }

        let policy = NacDacPolicy::new(Arc::new(FakeDac { allow: true }), Arc::new(EmptyLookup));
        let result = policy
            .check_release(
                Some(&did("did:key:zalice")),
                &KeyScope::Document {
                    doc_id: "missing".into(),
                    field: None,
                },
            )
            .await;
        assert!(matches!(result, Err(crate::Error::Internal(_))));
    }
}
