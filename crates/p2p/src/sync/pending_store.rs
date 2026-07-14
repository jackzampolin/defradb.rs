//! Durable storage for pending-DAG registrations (#1099).
//!
//! A hub success-acks a PushLog whose DAG has missing links on the strength of
//! its pending-DAG registration; the pusher then deletes its persisted retry
//! record. If the registration lives only in process memory, a crash between
//! the ack and Bitswap completion silently loses the document (modeled in
//! `proofs/tla/PendingDagRestart.tla`). Installing a store makes each
//! push-originated registration durable: it is written before the success
//! reply, deleted only when the root is successfully marked merged (never at
//! DagReady emission, TTL eviction, or clear), and re-driven at startup and
//! by the periodic resync sweep via `resync_persisted_pending_dags`.
//!
//! Pull-originated registrations (DocSync/BranchableSync) are not persisted —
//! no remote retry state is destroyed by their acks, so restart loss is
//! recoverable by re-issuing the pull.

use async_trait::async_trait;
use cid::Cid;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use storage::corekv::{IterOptions, Key, Store};
use storage::keys::systemstore::{P2PPendingDagKey, P2PQuarantinedDagKey};
use storage::stores::Systemstore;

use crate::error::{Error, Result};
use crate::ExplicitReplayAuthorization;

/// Verified explicit-replay claims carried by a persisted registration.
///
/// The capability signature was verified at admission time; persisting the
/// claim summary lets a restored registration merge with the same
/// authorization semantics it was admitted with.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedReplayAuthorization {
    pub source_peer_id: String,
    pub target_peer_id: String,
    pub collection_id: String,
    pub authorizer_did: String,
    pub expires_at: u64,
}

impl From<&ExplicitReplayAuthorization> for PersistedReplayAuthorization {
    fn from(auth: &ExplicitReplayAuthorization) -> Self {
        Self {
            source_peer_id: auth.source_peer_id.clone(),
            target_peer_id: auth.target_peer_id.clone(),
            collection_id: auth.collection_id.clone(),
            authorizer_did: auth.authorizer_did.clone(),
            expires_at: auth.expires_at,
        }
    }
}

impl From<PersistedReplayAuthorization> for ExplicitReplayAuthorization {
    fn from(auth: PersistedReplayAuthorization) -> Self {
        Self {
            source_peer_id: auth.source_peer_id,
            target_peer_id: auth.target_peer_id,
            collection_id: auth.collection_id,
            authorizer_did: auth.authorizer_did,
            expires_at: auth.expires_at,
        }
    }
}

/// Compact durable form of one pending-DAG registration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedPendingDag {
    pub doc_id: String,
    pub collection_id: String,
    pub creator: String,
    pub source_peer: Option<String>,
    pub is_explicit_replicator: bool,
    pub explicit_replay_authorization: Option<PersistedReplayAuthorization>,
}

impl PersistedPendingDag {
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        serde_cbor::to_vec(self).map_err(|e| Error::Storage(e.to_string()))
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        serde_cbor::from_slice(bytes).map_err(|e| Error::Storage(e.to_string()))
    }
}

/// A pending-DAG record whose merge failed deterministically (e.g. a unique-index
/// rejection): moved out of the live keyspace, retained for forensics and counted,
/// and never re-driven by the resync sweep.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedQuarantinedDag {
    pub record: PersistedPendingDag,
    pub reason: String,
    pub quarantined_at_unix_secs: u64,
}

impl PersistedQuarantinedDag {
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        serde_cbor::to_vec(self).map_err(|e| Error::Storage(e.to_string()))
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        serde_cbor::from_slice(bytes).map_err(|e| Error::Storage(e.to_string()))
    }

    /// Current wall-clock time as Unix seconds, for stamping `quarantined_at_unix_secs`.
    pub fn now_unix_secs() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }
}

/// Durable KV backing for push-originated pending-DAG registrations.
#[async_trait]
pub trait PendingDagStorage: Send + Sync {
    async fn put(&self, root_cid: &Cid, record: &PersistedPendingDag) -> Result<()>;
    async fn remove(&self, root_cid: &Cid) -> Result<()>;
    async fn load_all(&self) -> Result<Vec<(Cid, PersistedPendingDag)>>;

    /// Move a terminally-rejected root into the quarantine keyspace. The caller
    /// is responsible for deleting the live `/p2p/pending_dag/` record (write
    /// quarantine first, delete live second, so a crash mid-transition leaves
    /// a re-drivable live record rather than a silently lost one).
    async fn quarantine(&self, root_cid: &Cid, entry: &PersistedQuarantinedDag) -> Result<()>;
    async fn is_quarantined(&self, root_cid: &Cid) -> Result<bool>;
    async fn load_quarantined(&self) -> Result<Vec<(Cid, PersistedQuarantinedDag)>>;
    async fn remove_quarantined(&self, root_cid: &Cid) -> Result<()>;
}

/// `PendingDagStorage` over the systemstore keyspace (`/p2p/pending_dag/`).
pub struct PendingDagStore<S: Store> {
    systemstore: Systemstore<S>,
}

impl<S: Store> PendingDagStore<S> {
    pub fn new(store: Arc<S>) -> Self {
        Self {
            systemstore: Systemstore::new(store),
        }
    }
}

#[async_trait]
impl<S: Store + 'static> PendingDagStorage for PendingDagStore<S> {
    async fn put(&self, root_cid: &Cid, record: &PersistedPendingDag) -> Result<()> {
        let key = P2PPendingDagKey::new(root_cid.to_string());
        let value = record.to_bytes()?;
        let mut txn = self
            .systemstore
            .new_txn(false)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        txn.set(&key.bytes(), &value)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        txn.commit()
            .await
            .map_err(|e| Error::Storage(e.to_string()))
    }

    async fn remove(&self, root_cid: &Cid) -> Result<()> {
        let key = P2PPendingDagKey::new(root_cid.to_string());
        let mut txn = self
            .systemstore
            .new_txn(false)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        txn.delete(&key.bytes())
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        txn.commit()
            .await
            .map_err(|e| Error::Storage(e.to_string()))
    }

    async fn load_all(&self) -> Result<Vec<(Cid, PersistedPendingDag)>> {
        let txn = self
            .systemstore
            .new_txn(true)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        let opts = IterOptions::new().with_prefix(P2PPendingDagKey::p2p_pending_dag_prefix());
        let mut iter = txn
            .iterator(opts)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        let mut records = Vec::new();
        while let Some(pair) = iter
            .next()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?
        {
            let key_str = String::from_utf8_lossy(&pair.key);
            let Some(cid_str) = key_str.strip_prefix("/p2p/pending_dag/") else {
                continue;
            };
            let Ok(root_cid) = cid_str.parse::<Cid>() else {
                tracing::warn!(key = %key_str, "Skipping persisted pending DAG with invalid CID key");
                continue;
            };
            match PersistedPendingDag::from_bytes(&pair.value) {
                Ok(record) => records.push((root_cid, record)),
                Err(error) => {
                    tracing::warn!(
                        root_cid = %root_cid,
                        error = %error,
                        "Skipping undecodable persisted pending DAG record"
                    );
                }
            }
        }
        iter.close()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(records)
    }

    async fn quarantine(&self, root_cid: &Cid, entry: &PersistedQuarantinedDag) -> Result<()> {
        let key = P2PQuarantinedDagKey::new(root_cid.to_string());
        let value = entry.to_bytes()?;
        let mut txn = self
            .systemstore
            .new_txn(false)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        txn.set(&key.bytes(), &value)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        txn.commit()
            .await
            .map_err(|e| Error::Storage(e.to_string()))
    }

    async fn is_quarantined(&self, root_cid: &Cid) -> Result<bool> {
        let key = P2PQuarantinedDagKey::new(root_cid.to_string());
        let txn = self
            .systemstore
            .new_txn(true)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        let value = txn
            .get(&key.bytes())
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(value.is_some())
    }

    async fn load_quarantined(&self) -> Result<Vec<(Cid, PersistedQuarantinedDag)>> {
        let txn = self
            .systemstore
            .new_txn(true)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        let opts =
            IterOptions::new().with_prefix(P2PQuarantinedDagKey::p2p_quarantined_dag_prefix());
        let mut iter = txn
            .iterator(opts)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;

        let mut records = Vec::new();
        while let Some(pair) = iter
            .next()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?
        {
            let key_str = String::from_utf8_lossy(&pair.key);
            let Some(cid_str) = key_str.strip_prefix("/p2p/quarantined_dag/") else {
                continue;
            };
            let Ok(root_cid) = cid_str.parse::<Cid>() else {
                tracing::warn!(key = %key_str, "Skipping quarantined DAG with invalid CID key");
                continue;
            };
            match PersistedQuarantinedDag::from_bytes(&pair.value) {
                Ok(record) => records.push((root_cid, record)),
                Err(error) => {
                    tracing::warn!(
                        root_cid = %root_cid,
                        error = %error,
                        "Skipping undecodable quarantined DAG record"
                    );
                }
            }
        }
        iter.close()
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        Ok(records)
    }

    async fn remove_quarantined(&self, root_cid: &Cid) -> Result<()> {
        let key = P2PQuarantinedDagKey::new(root_cid.to_string());
        let mut txn = self
            .systemstore
            .new_txn(false)
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        txn.delete(&key.bytes())
            .await
            .map_err(|e| Error::Storage(e.to_string()))?;
        txn.commit()
            .await
            .map_err(|e| Error::Storage(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use multihash_codetable::{Code, MultihashDigest};
    use storage::backends::MemoryStore;

    fn record(doc: &str) -> PersistedPendingDag {
        PersistedPendingDag {
            doc_id: doc.to_string(),
            collection_id: "collection".to_string(),
            creator: "creator".to_string(),
            source_peer: Some("peer-1".to_string()),
            is_explicit_replicator: true,
            explicit_replay_authorization: Some(PersistedReplayAuthorization {
                source_peer_id: "peer-1".to_string(),
                target_peer_id: "peer-2".to_string(),
                collection_id: "collection".to_string(),
                authorizer_did: "did:key:z123".to_string(),
                expires_at: 42,
            }),
        }
    }

    fn cid(seed: &[u8]) -> Cid {
        Cid::new_v1(0x55, Code::Sha2_256.digest(seed))
    }

    #[tokio::test]
    async fn put_load_remove_roundtrip() {
        let store = PendingDagStore::new(Arc::new(MemoryStore::new()));
        let root_a = cid(b"a");
        let root_b = cid(b"b");

        store.put(&root_a, &record("doc-a")).await.unwrap();
        store.put(&root_b, &record("doc-b")).await.unwrap();

        let mut loaded = store.load_all().await.unwrap();
        loaded.sort_by(|(_, x), (_, y)| x.doc_id.cmp(&y.doc_id));
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].1, record("doc-a"));
        assert_eq!(loaded[0].0, root_a);

        store.remove(&root_a).await.unwrap();
        let remaining = store.load_all().await.unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].0, root_b);

        // Removing an absent record is a no-op, not an error.
        store.remove(&root_a).await.unwrap();
    }

    #[tokio::test]
    async fn put_overwrites_existing_record() {
        let store = PendingDagStore::new(Arc::new(MemoryStore::new()));
        let root = cid(b"a");
        store.put(&root, &record("doc-old")).await.unwrap();
        store.put(&root, &record("doc-new")).await.unwrap();

        let loaded = store.load_all().await.unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].1.doc_id, "doc-new");
    }

    fn quarantined(doc: &str, reason: &str) -> PersistedQuarantinedDag {
        PersistedQuarantinedDag {
            record: record(doc),
            reason: reason.to_string(),
            quarantined_at_unix_secs: 1_700_000_000,
        }
    }

    #[tokio::test]
    async fn quarantine_load_remove_roundtrip() {
        let store = PendingDagStore::new(Arc::new(MemoryStore::new()));
        let root = cid(b"a");

        assert!(!store.is_quarantined(&root).await.unwrap());

        let entry = quarantined("doc-a", "unique constraint violation");
        store.quarantine(&root, &entry).await.unwrap();

        assert!(store.is_quarantined(&root).await.unwrap());

        let loaded = store.load_quarantined().await.unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].0, root);
        assert_eq!(loaded[0].1, entry);

        store.remove_quarantined(&root).await.unwrap();
        assert!(!store.is_quarantined(&root).await.unwrap());
        assert!(store.load_quarantined().await.unwrap().is_empty());

        // Removing an absent record is a no-op, not an error.
        store.remove_quarantined(&root).await.unwrap();
    }

    /// The quarantine keyspace is deliberately outside `/p2p/pending_dag/`:
    /// the live resync sweep's prefix scan (`load_all`) must never observe a
    /// quarantined root, or it would re-drive a merge known to fail every time.
    #[tokio::test]
    async fn quarantined_root_is_absent_from_live_load_all() {
        let store = PendingDagStore::new(Arc::new(MemoryStore::new()));
        let live_root = cid(b"live");
        let quarantined_root = cid(b"quarantined");

        store.put(&live_root, &record("doc-live")).await.unwrap();
        store
            .quarantine(
                &quarantined_root,
                &quarantined("doc-quarantined", "unique constraint violation"),
            )
            .await
            .unwrap();

        let live = store.load_all().await.unwrap();
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].0, live_root);

        let quarantined_records = store.load_quarantined().await.unwrap();
        assert_eq!(quarantined_records.len(), 1);
        assert_eq!(quarantined_records[0].0, quarantined_root);
    }
}
