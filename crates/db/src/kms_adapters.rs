//! Adapters bridging db types into the KMS trait surface.
//!
//! Kept in `crates/db` (not `crates/kms`) so the KMS crate doesn't depend on
//! `db`. Shared by both the embedded node and the CLI node.

use async_trait::async_trait;
use std::sync::Arc;

use kms::{DocCollectionInfo, DocCollectionLookup, EncBlockStore, EncryptionCid, NodeAcpRead};

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

/// Durable `Encryption`-block store backing the KMS `BlockstoreKeyStore`.
///
/// Mirrors Go's `internal/kms/enc_store.go`: reads the encstore→blockstore
/// (the same order as the merge decrypt path) and writes new blocks to the
/// durable blockstore. This lets the KMS serve a DEK for ANY encrypted write,
/// including blocks written by the legacy auto-commit path.
pub struct DbEncBlockStore<S, B>
where
    S: storage::corekv::Store + Send + Sync + 'static,
    B: blockstore::Blockstore,
{
    db: Arc<crate::DB<S>>,
    blockstore: Arc<B>,
}

impl<S, B> DbEncBlockStore<S, B>
where
    S: storage::corekv::Store + Send + Sync + 'static,
    B: blockstore::Blockstore,
{
    pub fn new(db: Arc<crate::DB<S>>, blockstore: Arc<B>) -> Self {
        Self { db, blockstore }
    }
}

#[async_trait]
impl<S, B> EncBlockStore for DbEncBlockStore<S, B>
where
    S: storage::corekv::Store + Send + Sync + 'static,
    B: blockstore::Blockstore + 'static,
{
    async fn get_block(&self, cid: &EncryptionCid) -> kms::Result<Option<Vec<u8>>> {
        // Mirror the merge decrypt path: encstore first, then blockstore.
        let txn = self
            .db
            .new_txn(true)
            .await
            .map_err(|e| kms::Error::Storage(e.to_string()))?;
        let encstore = txn
            .encstore()
            .map_err(|e| kms::Error::Storage(e.to_string()))?;
        let from_encstore = encstore
            .get(&cid.to_bytes())
            .await
            .map_err(|e| kms::Error::Storage(e.to_string()))?;
        // Read txn carries no writes; dropping it releases the snapshot.
        drop(txn);
        if let Some(bytes) = from_encstore {
            return Ok(Some(bytes));
        }
        match self
            .blockstore
            .get(cid)
            .await
            .map_err(|e| kms::Error::Storage(e.to_string()))?
        {
            Some(bytes) => Ok(Some(bytes.to_vec())),
            None => Ok(None),
        }
    }

    async fn put_block(&self, cid: EncryptionCid, bytes: Vec<u8>) -> kms::Result<()> {
        self.blockstore
            .put(&cid, &bytes)
            .await
            .map_err(|e| kms::Error::Storage(e.to_string()))
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
