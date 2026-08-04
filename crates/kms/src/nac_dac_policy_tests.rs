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

struct BranchableDac {
    doc_registered: bool,
    doc_granted: bool,
}
#[async_trait::async_trait]
impl acp::DocumentACP for BranchableDac {
    async fn register_doc_object(
        &self,
        _: &identity::Did,
        _: &str,
        _: &str,
        _: &str,
    ) -> acp::Result<()> {
        Ok(())
    }
    async fn is_doc_registered(&self, _: &str, _: &str, object_id: &str) -> acp::Result<bool> {
        Ok(object_id == "col-1" || (object_id == "d1" && self.doc_registered))
    }
    async fn check_doc_access(
        &self,
        _: &acp::Identity,
        _: acp::DocumentPermission,
        _: &str,
        _: &str,
        object_id: &str,
    ) -> acp::Result<bool> {
        Ok(object_id == "d1" && self.doc_granted)
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
            is_branchable: false,
        }))
    }
}

struct BranchableLookup;
#[async_trait::async_trait]
impl DocCollectionLookup for BranchableLookup {
    async fn collection_for_doc(&self, _: &str) -> crate::Result<Option<DocCollectionInfo>> {
        Ok(Some(DocCollectionInfo {
            collection_id: "col-1".into(),
            policy_id: "policy-1".into(),
            resource_name: "doc".into(),
            is_branchable: true,
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
async fn delegated_release_is_bound_to_the_document_collection() {
    let policy = NacDacPolicy::new(Arc::new(FakeDac { allow: true }), Arc::new(FakeLookup));
    let scope = KeyScope::Document {
        doc_id: "d1".into(),
        field: None,
    };
    let actor = did("did:key:zalice");

    assert_eq!(
        policy
            .check_delegated_release(&actor, &scope, "col-1")
            .await
            .unwrap(),
        PolicyDecision::Allow
    );
    assert_eq!(
        policy
            .check_delegated_release(&actor, &scope, "other-collection")
            .await
            .unwrap(),
        PolicyDecision::Deny
    );
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
async fn doc_scope_no_policy_collection_allows_release() {
    // A collection with no DAC policy attached resolves to `None`. With no
    // policy there is no per-document access gate, so release is allowed —
    // matching the legacy decrypt path (no policy check at all).
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

    let policy = NacDacPolicy::new(Arc::new(FakeDac { allow: false }), Arc::new(EmptyLookup));
    let result = policy
        .check_release(
            Some(&did("did:key:zalice")),
            &KeyScope::Document {
                doc_id: "no-policy".into(),
                field: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(result, PolicyDecision::Allow);
}

#[tokio::test]
async fn branchable_public_doc_denies_when_collection_denies() {
    let policy = NacDacPolicy::new(
        Arc::new(BranchableDac {
            doc_registered: false,
            doc_granted: false,
        }),
        Arc::new(BranchableLookup),
    );

    let result = policy
        .check_release(
            Some(&did("did:key:zalice")),
            &KeyScope::Document {
                doc_id: "d1".into(),
                field: None,
            },
        )
        .await
        .unwrap();

    assert_eq!(result, PolicyDecision::Deny);
}

#[tokio::test]
async fn branchable_explicit_doc_grant_allows_despite_collection_denial() {
    let policy = NacDacPolicy::new(
        Arc::new(BranchableDac {
            doc_registered: true,
            doc_granted: true,
        }),
        Arc::new(BranchableLookup),
    );

    let result = policy
        .check_release(
            Some(&did("did:key:zalice")),
            &KeyScope::Document {
                doc_id: "d1".into(),
                field: None,
            },
        )
        .await
        .unwrap();

    assert_eq!(result, PolicyDecision::Allow);
}
