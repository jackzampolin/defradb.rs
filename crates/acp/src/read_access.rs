//! Shared document/collection read-access rule.

use async_trait::async_trait;
use identity::Did;
use storage::corekv::MaybeSendSync;

use crate::{DocumentACP, DocumentPermission, Identity, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DocAccess {
    pub has_access: bool,
    pub explicit: bool,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait ObjectAccessChecker: MaybeSendSync {
    async fn object_access(
        &self,
        policy_id: &str,
        resource_name: &str,
        object_id: &str,
    ) -> Result<DocAccess>;
}

pub struct DirectChecker<'a> {
    pub acp: &'a dyn DocumentACP,
    pub identity: &'a Identity,
    pub node_did: Option<&'a Did>,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl ObjectAccessChecker for DirectChecker<'_> {
    async fn object_access(
        &self,
        policy_id: &str,
        resource_name: &str,
        object_id: &str,
    ) -> Result<DocAccess> {
        if defra_core::dac_bypass::get_dac_bypass() {
            return Ok(DocAccess {
                has_access: true,
                explicit: true,
            });
        }

        if let (Some(node), Identity::Authenticated(requester)) = (self.node_did, self.identity) {
            if node == requester {
                return Ok(DocAccess {
                    has_access: true,
                    explicit: true,
                });
            }
        }

        if !self
            .acp
            .is_doc_registered(policy_id, resource_name, object_id)
            .await?
        {
            return Ok(DocAccess {
                has_access: true,
                explicit: false,
            });
        }

        let has_access = self
            .acp
            .check_doc_access(
                self.identity,
                DocumentPermission::Read,
                policy_id,
                resource_name,
                object_id,
            )
            .await?;
        Ok(DocAccess {
            has_access,
            explicit: true,
        })
    }
}

/// Branchable read rule. `doc_id == ""` means a collection-level commit.
pub async fn check_doc_read_access(
    checker: &dyn ObjectAccessChecker,
    policy_id: &str,
    resource_name: &str,
    collection_id: &str,
    is_branchable: bool,
    doc_id: &str,
) -> Result<bool> {
    if !doc_id.is_empty() {
        let access = checker
            .object_access(policy_id, resource_name, doc_id)
            .await?;
        if access.explicit {
            return Ok(access.has_access);
        }
    }

    if is_branchable {
        let access = checker
            .object_access(policy_id, resource_name, collection_id)
            .await?;
        if !access.has_access {
            return Ok(false);
        }
    }

    Ok(true)
}
