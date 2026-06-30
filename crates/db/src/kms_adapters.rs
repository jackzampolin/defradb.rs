//! Adapters bridging db types into the KMS trait surface.
//!
//! Kept in `crates/db` (not `crates/kms`) so the KMS crate doesn't depend on
//! `db`. Shared by both the embedded node and the CLI node.

use async_trait::async_trait;
use std::sync::{Arc, Weak};

use kms::{DocCollectionInfo, DocCollectionLookup, EncBlockStore, EncryptionCid, NodeAcpRead};

/// Resolves doc_id → collection via the DB headstore (Go's
/// `RetrieveCollectionFromDocID` port).
///
/// Holds a [`Weak`] back-reference to the DB: the DB owns the KMS (and the KMS
/// owns this adapter), so a strong `Arc<DB>` here would form a reference cycle
/// that keeps the DB — and its storage lock — alive forever (#976). Each call
/// upgrades the weak handle; if the DB is gone the node is shutting down, so we
/// report no collection.
pub struct DbDocCollectionLookup<S: storage::corekv::Store + Send + Sync + 'static> {
    db: Weak<crate::DB<S>>,
}

impl<S: storage::corekv::Store + Send + Sync + 'static> DbDocCollectionLookup<S> {
    pub fn new(db: Arc<crate::DB<S>>) -> Self {
        Self {
            db: Arc::downgrade(&db),
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S: storage::corekv::Store + Send + Sync + 'static> DocCollectionLookup
    for DbDocCollectionLookup<S>
{
    async fn collection_for_doc(&self, doc_id: &str) -> kms::Result<Option<DocCollectionInfo>> {
        let Some(db) = self.db.upgrade() else {
            // DB dropped → node shutting down → treat as "no collection".
            return Ok(None);
        };
        match crate::resolve_collection_from_doc_id(&db, doc_id).await {
            Ok(Some(info)) => Ok(Some(DocCollectionInfo {
                collection_id: info.collection_id,
                policy_id: info.policy_id,
                resource_name: info.resource_name,
                is_branchable: info.is_branchable,
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
///
/// Holds only [`Weak`] back-references (no `Arc<DB>`, no `Arc<blockstore>`):
/// both `Arc<DB>` and the blockstore's `Arc<Store>` keep the underlying store —
/// and thus its exclusive storage lock — alive forever via the
/// DB→KMS→`KeyStore`→adapter cycle, so the lock never releases on node close
/// and reopening the same data dir fails (#976). The owning `Arc`s live on the
/// node (the DB itself, and the shared KMS `DefraBlockstore` parked alongside
/// it), so they drop on node teardown and the lock is freed.
///
/// The blockstore is held as a `Weak` to the SAME shared instance the rest of
/// the node uses, preserving its in-process block cache. That cache matters for
/// correctness, not just speed: a fresh per-call blockstore misses a DEK that
/// was just written, forcing a redundant cross-peer fetch + `put` whose
/// independent write transaction conflicts with the in-flight merge txn
/// ("transaction conflict. Please retry") and aborts the merge.
pub struct DbEncBlockStore<S, B>
where
    S: storage::corekv::Store + Send + Sync + 'static,
    B: blockstore::Blockstore,
{
    db: Weak<crate::DB<S>>,
    blockstore: Weak<B>,
}

impl<S, B> DbEncBlockStore<S, B>
where
    S: storage::corekv::Store + Send + Sync + 'static,
    B: blockstore::Blockstore,
{
    /// `blockstore` must be the node's shared durable blockstore; the caller
    /// retains the owning `Arc` for the node's lifetime (see struct docs).
    pub fn new(db: Arc<crate::DB<S>>, blockstore: Arc<B>) -> Self {
        Self {
            db: Arc::downgrade(&db),
            blockstore: Arc::downgrade(&blockstore),
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S, B> EncBlockStore for DbEncBlockStore<S, B>
where
    S: storage::corekv::Store + Send + Sync + 'static,
    B: blockstore::Blockstore + 'static,
{
    async fn get_block(&self, cid: &EncryptionCid) -> kms::Result<Option<Vec<u8>>> {
        let Some(db) = self.db.upgrade() else {
            // DB dropped → node shutting down → nothing to serve.
            return Ok(None);
        };
        // Mirror the merge decrypt path: encstore first, then blockstore.
        let txn = db
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
        let Some(blockstore) = self.blockstore.upgrade() else {
            return Ok(None);
        };
        match blockstore
            .get(cid)
            .await
            .map_err(|e| kms::Error::Storage(e.to_string()))?
        {
            Some(bytes) => Ok(Some(bytes.to_vec())),
            None => Ok(None),
        }
    }

    async fn put_block(&self, cid: EncryptionCid, bytes: Vec<u8>) -> kms::Result<()> {
        let Some(blockstore) = self.blockstore.upgrade() else {
            return Err(kms::Error::Storage("blockstore gone".into()));
        };
        blockstore
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

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
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
