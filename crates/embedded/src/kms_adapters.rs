//! Adapters bridging embedded-node types into the KMS trait surface.
//!
//! Kept out of `crates/kms` so the KMS crate doesn't depend on `db`.

use async_trait::async_trait;
use std::sync::Arc;

use kms::{DocCollectionInfo, DocCollectionLookup, NodeAcpRead};

/// Resolves doc_id → collection via the DB headstore (Go's
/// `RetrieveCollectionFromDocID` port).
pub struct EmbeddedDocCollectionLookup<S: storage::corekv::Store + Send + Sync + 'static> {
    db: Arc<db::DB<S>>,
}

impl<S: storage::corekv::Store + Send + Sync + 'static> EmbeddedDocCollectionLookup<S> {
    pub fn new(db: Arc<db::DB<S>>) -> Self {
        Self { db }
    }
}

#[async_trait]
impl<S: storage::corekv::Store + Send + Sync + 'static> DocCollectionLookup
    for EmbeddedDocCollectionLookup<S>
{
    async fn collection_for_doc(&self, doc_id: &str) -> kms::Result<Option<DocCollectionInfo>> {
        match db::resolve_collection_from_doc_id(&self.db, doc_id).await {
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
pub struct EmbeddedNodeAcpRead {
    nac: Arc<dyn db::NacManagerApi>,
}

impl EmbeddedNodeAcpRead {
    pub fn new(nac: Arc<dyn db::NacManagerApi>) -> Self {
        Self { nac }
    }
}

#[async_trait]
impl NodeAcpRead for EmbeddedNodeAcpRead {
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
