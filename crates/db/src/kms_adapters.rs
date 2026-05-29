//! Adapters bridging db types into the KMS trait surface.
//!
//! Kept in `crates/db` (not `crates/kms`) so the KMS crate doesn't depend on
//! `db`. Shared by both the embedded node and the CLI node.

use async_trait::async_trait;
use std::sync::Arc;

use kms::{DocCollectionInfo, DocCollectionLookup, NodeAcpRead};

/// Resolves doc_id → collection via the DB headstore (Go's
/// `RetrieveCollectionFromDocID` port).
pub struct DbDocCollectionLookup<S: storage::corekv::Store + Send + Sync + 'static> {
    db: Arc<crate::DB<S>>,
}

impl<S: storage::corekv::Store + Send + Sync + 'static> DbDocCollectionLookup<S> {
    pub fn new(db: Arc<crate::DB<S>>) -> Self {
        Self { db }
    }
}

#[async_trait]
impl<S: storage::corekv::Store + Send + Sync + 'static> DocCollectionLookup
    for DbDocCollectionLookup<S>
{
    async fn collection_for_doc(&self, doc_id: &str) -> kms::Result<Option<DocCollectionInfo>> {
        match crate::resolve_collection_from_doc_id(&self.db, doc_id).await {
            Ok(Some(info)) => Ok(Some(DocCollectionInfo {
                collection_id: info.collection_id,
                policy_id: info.policy_id,
                resource_name: info.resource_name,
            })),
            Ok(None) => Ok(None),
            Err(e) => Err(kms::Error::Storage(e.to_string())),
        }
    }
}

/// Bridges the node NAC manager into the KMS `NodeAcpRead` trait.
pub struct DbNodeAcpRead {
    nac: Arc<dyn crate::NacManagerApi>,
}

impl DbNodeAcpRead {
    pub fn new(nac: Arc<dyn crate::NacManagerApi>) -> Self {
        Self { nac }
    }
}

#[async_trait]
impl NodeAcpRead for DbNodeAcpRead {
    async fn check_node_permission(
        &self,
        identity: &identity::Did,
        permission: &str,
    ) -> acp::Result<bool> {
        let perm = match permission {
            "read-document" => acp::nac::NodePermission::DocumentRead,
            other => {
                return Err(acp::Error::PermissionDenied(format!(
                    "unknown node permission {other}"
                )))
            }
        };
        self.nac
            .check_permission(identity, perm)
            .await
            .map_err(|e| acp::Error::Storage(e.to_string()))
    }
}
