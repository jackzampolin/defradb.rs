//! Pending DAG registration and retry logic.

use std::time::Instant;

use cid::Cid;

use blockstore::Blockstore;

use crate::error::{Error, Result};
use crate::sync::manager::events::SyncEvent;
use crate::sync::manager::links::find_all_missing_links;
use crate::sync::manager::pending::{PendingDag, MAX_PENDING_DAGS, PENDING_DAG_TTL};

use super::SyncManager;

impl<B: Blockstore + 'static> SyncManager<B> {
    /// Get the pending DAGs count (for testing/monitoring).
    pub fn pending_dag_count(&self) -> usize {
        self.pending_dags.read().len()
    }

    /// Get CIDs of all pending DAGs.
    pub fn pending_dag_cids(&self) -> Vec<Cid> {
        self.pending_dags.read().keys().copied().collect()
    }

    /// Get missing CIDs for a pending DAG.
    pub fn pending_dag_missing(&self, root_cid: &Cid) -> Vec<Cid> {
        self.pending_dags
            .read()
            .get(root_cid)
            .map(|dag| dag.missing.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Get the source peer for a pending DAG (the peer that originally provided it).
    pub fn pending_dag_source_peer(&self, root_cid: &Cid) -> Option<String> {
        self.pending_dags
            .read()
            .get(root_cid)
            .and_then(|dag| dag.source_peer.clone())
    }

    /// Retry pending DAGs that were waiting on `cid`.
    ///
    /// This covers the explicit replay path where a composite can be registered
    /// as pending before its linked field blocks arrive via later PushLog
    /// requests rather than Bitswap.
    pub async fn retry_pending_dags_waiting_on(&self, cid: &Cid) -> Result<Vec<Cid>> {
        let waiting_roots: Vec<Cid> = {
            let pending = self.pending_dags.read();
            pending
                .iter()
                .filter_map(|(root_cid, dag)| dag.missing.contains(cid).then_some(*root_cid))
                .collect()
        };

        let mut completed = Vec::new();
        for root_cid in waiting_roots {
            if self.retry_pending_dag(&root_cid).await? {
                completed.push(root_cid);
            }
        }

        Ok(completed)
    }

    /// Insert a pending DAG entry, enforcing TTL eviction and capacity limits.
    ///
    /// Expired entries (older than `PENDING_DAG_TTL`) are removed before checking
    /// the capacity. If the map is still at `MAX_PENDING_DAGS` after eviction the
    /// new entry is silently dropped and `false` is returned so callers can log.
    fn insert_pending_dag(&self, root_cid: Cid, dag: PendingDag) -> bool {
        let mut pending = self.pending_dags.write();
        let now = Instant::now();

        // Evict expired entries.
        pending.retain(|_, v| now.duration_since(v.inserted_at) < PENDING_DAG_TTL);

        if pending.len() >= MAX_PENDING_DAGS {
            return false;
        }

        pending.insert(root_cid, dag);
        true
    }

    /// Register a pending DAG for DocSync.
    ///
    /// This is called when a DocSyncReply contains head CIDs that need to be
    /// fetched via Bitswap. Unlike PushLog-initiated syncs, DocSync doesn't
    /// have collection_id or creator in the message, so we use empty strings.
    /// The merge handler will extract the actual metadata from the block data.
    ///
    /// # Arguments
    ///
    /// * `root_cid` - The head CID to fetch
    /// * `doc_id` - Document ID from the DocSyncItem
    pub fn register_docsync_dag(&self, root_cid: Cid, doc_id: String, source_peer: String) {
        tracing::debug!(
            cid = %root_cid,
            doc_id = %doc_id,
            source_peer = %source_peer,
            "Registering DocSync pending DAG"
        );

        if !self.insert_pending_dag(
            root_cid,
            PendingDag {
                doc_id,
                // DocSync protocol doesn't include collection_id or creator.
                // The merge handler will extract these from the block data.
                collection_id: String::new(),
                creator: String::new(),
                missing: std::iter::once(root_cid).collect(),
                source_peer: Some(source_peer),
                is_explicit_replicator: false,
                explicit_replay_authorization: None,
                inserted_at: Instant::now(),
            },
        ) {
            tracing::warn!(
                cid = %root_cid,
                max = MAX_PENDING_DAGS,
                "Pending DAGs at capacity, dropping DocSync registration"
            );
        }
    }

    /// Register a pending DAG for branchable collection sync.
    ///
    /// Unlike `register_docsync_dag` which stores the document ID,
    /// this stores the collection ID so the merge handler can look up
    /// the local collection for cross-schema-version merges.
    pub fn register_branchable_dag(
        &self,
        root_cid: Cid,
        collection_id: String,
        source_peer: String,
    ) {
        tracing::debug!(
            cid = %root_cid,
            collection_id = %collection_id,
            "Registering branchable sync pending DAG"
        );

        if !self.insert_pending_dag(
            root_cid,
            PendingDag {
                doc_id: String::new(),
                collection_id,
                creator: String::new(),
                missing: std::iter::once(root_cid).collect(),
                source_peer: Some(source_peer),
                is_explicit_replicator: false,
                explicit_replay_authorization: None,
                inserted_at: Instant::now(),
            },
        ) {
            tracing::warn!(
                cid = %root_cid,
                max = MAX_PENDING_DAGS,
                "Pending DAGs at capacity, dropping branchable DAG registration"
            );
        }
    }

    /// Process a pending DAG after Bitswap blocks have been received.
    ///
    /// This is called when BitswapComplete is received, indicating all requested
    /// blocks have arrived. We re-check the DAG for any remaining missing links
    /// (recursively, at all depths) and process it if complete.
    pub async fn retry_pending_dag(&self, root_cid: &Cid) -> Result<bool> {
        // Get the pending DAG info
        let pending_info = {
            let pending = self.pending_dags.read();
            pending.get(root_cid).cloned()
        };

        let Some(info) = pending_info else {
            tracing::warn!(
                root_cid = %root_cid,
                "No pending DAG found for retry"
            );
            return Ok(false);
        };

        // Check TTL before retrying.
        if info.inserted_at.elapsed() >= PENDING_DAG_TTL {
            self.pending_dags.write().remove(root_cid);
            tracing::warn!(
                root_cid = %root_cid,
                "Pending DAG expired (TTL exceeded), dropping"
            );
            return Ok(false);
        }

        // Load the root block from blockstore
        let block_data = match self.blockstore.get(root_cid).await {
            Ok(Some(data)) => data,
            Ok(None) => {
                tracing::error!(
                    root_cid = %root_cid,
                    "Root block not found in blockstore during retry"
                );
                return Err(Error::BlockstoreError("Root block not found".to_string()));
            }
            Err(e) => {
                tracing::error!(
                    root_cid = %root_cid,
                    error = %e,
                    "Failed to load root block from blockstore"
                );
                return Err(Error::BlockstoreError(e.to_string()));
            }
        };

        // Recursively check ALL missing links at every depth of the DAG.
        // This is critical for multi-level DAGs like Collection → Composite → LWW
        // where a single-level check would declare the DAG "ready" prematurely.
        let missing = match find_all_missing_links(self.blockstore.as_ref(), &block_data).await {
            Ok(missing) => missing,
            Err(e) => {
                tracing::error!(
                    root_cid = %root_cid,
                    error = %e,
                    "Failed to re-check missing links for pending DAG"
                );
                return Err(e);
            }
        };

        tracing::debug!(
            root_cid = %root_cid,
            doc_id = %info.doc_id,
            missing_count = missing.len(),
            "Retrying pending DAG"
        );

        if !missing.is_empty() {
            tracing::debug!(
                root_cid = %root_cid,
                missing_count = missing.len(),
                "Still missing blocks for DAG"
            );
            // Update the pending info with new missing CIDs (preserve original inserted_at).
            self.pending_dags.write().insert(
                *root_cid,
                PendingDag {
                    missing: missing.into_iter().collect(),
                    ..info
                },
            );
            return Ok(false);
        }

        // DAG is complete at all depths - remove from pending and process
        self.pending_dags.write().remove(root_cid);
        tracing::info!(
            root_cid = %root_cid,
            doc_id = %info.doc_id,
            "DAG complete, emitting DagReady"
        );

        // Emit event that DAG is ready for merge
        let _ = self
            .event_tx
            .send(SyncEvent::DagReady {
                root_cid: *root_cid,
                doc_id: info.doc_id.clone(),
                collection_id: info.collection_id.clone(),
                creator: info.creator.clone(),
                sender_peer: info.source_peer.clone(),
                is_explicit_replicator: info.is_explicit_replicator,
                explicit_replay_authorization: info.explicit_replay_authorization.clone(),
            })
            .await;

        Ok(true)
    }
}
