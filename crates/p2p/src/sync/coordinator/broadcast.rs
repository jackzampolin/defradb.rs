//! Broadcasting local updates to the network.
//!
//! Replicator pushes are admitted into the bounded push backlog as compact
//! job specs; the fixed worker pool (`push_worker`) expands DAGs, signs, and
//! sends. Admission happens before any task is spawned or payload captured,
//! so outbound resident state stays bounded under sustained writes (#1099).

use std::sync::Arc;
use std::time::Duration;

use blockstore::Blockstore;
use bytes::Bytes;
use cid::Cid;
use serde_json::Value as JsonValue;

use super::push_worker::{report_observed_head, report_push_failure};
use super::SyncCoordinator;
use crate::error::Result;
use crate::message::{PushSEArtifactsRequest, SEArtifact};
use crate::sync::broadcaster::Broadcaster;
use crate::sync::push_backlog::{EnqueueOutcome, PushJobSpec};
use crate::sync::push_fanout_coalescer::PendingPush;
use crate::sync::BroadcastResult;
use crate::transport::{P2PTransport, PeerId};

pub(super) const MAX_RATE_LIMITED_PUSH_ATTEMPTS: usize = 10;

pub(super) fn rate_limited_push_delay(attempt: usize) -> Duration {
    #[cfg(test)]
    {
        match attempt {
            1 => Duration::from_millis(1),
            2 => Duration::from_millis(2),
            3 => Duration::from_millis(4),
            4 => Duration::from_millis(8),
            _ => Duration::from_millis(10),
        }
    }

    #[cfg(not(test))]
    {
        match attempt {
            1 => Duration::from_millis(25),
            2 => Duration::from_millis(50),
            3 => Duration::from_millis(100),
            4 => Duration::from_millis(200),
            5 => Duration::from_millis(400),
            _ => Duration::from_millis(500),
        }
    }
}

impl<B: Blockstore + 'static, T: P2PTransport> SyncCoordinator<B, T> {
    async fn list_replicators_for_push(&self) -> Option<Vec<crate::replicator::ReplicatorInfo>> {
        if self.runtime.shutdown.is_shutting_down() {
            tracing::debug!("Skipping replicator push because coordinator is shutting down");
            return None;
        }

        match self.runtime.transport.list_replicators().await {
            Ok(replicators) => Some(replicators),
            Err(e) => {
                if e.is_connection_like() {
                    tracing::debug!(
                        error = %e,
                        "Skipping replicator push because the transport is unavailable"
                    );
                } else {
                    tracing::warn!(error = %e, "Failed to get replicators for push");
                }
                None
            }
        }
    }

    /// Admit one replicator push into the bounded backlog. Overflow is an
    /// explicit outcome: it is counted, logged, and handed to the persisted
    /// retry ladder — never a silent drop and never another waiting task.
    async fn enqueue_replicator_push(&self, job: PushJobSpec) {
        let peer_id = job.peer_id.clone();
        let doc_id = job.doc_id.clone();
        let collection_id = job.collection_id.clone();
        let root_cid = job.root_cid;
        let head_priority = job.head_priority();
        let outcome = self.runtime.push_backlog.try_enqueue(job.clone());
        match outcome {
            EnqueueOutcome::Enqueued => {
                report_observed_head(&self.runtime.failure_tx, &job).await;
            }
            EnqueueOutcome::Coalesced | EnqueueOutcome::RetiredStale => {}
            EnqueueOutcome::RejectedItems | EnqueueOutcome::RejectedBytes => {
                tracing::warn!(
                    peer_id = %peer_id,
                    doc_id = %doc_id,
                    collection_id = %collection_id,
                    outcome = ?outcome,
                    "Outbound push backlog full; deferring push to persisted retry"
                );
                report_push_failure(
                    &self.runtime.failure_tx,
                    &peer_id,
                    doc_id,
                    collection_id,
                    Some(root_cid),
                    head_priority,
                )
                .await;
            }
            EnqueueOutcome::Closed => {
                tracing::debug!(
                    peer_id = %peer_id,
                    doc_id = %doc_id,
                    "Skipping replicator push because the backlog is closed"
                );
            }
        }
    }

    fn replicator_in_collection(
        rep: &crate::replicator::ReplicatorInfo,
        collection_id: &str,
    ) -> bool {
        rep.collections.is_empty() || rep.collections.iter().any(|id| id == collection_id)
    }

    fn peer_id_for_replicator(rep: &crate::replicator::ReplicatorInfo) -> Option<PeerId> {
        let peer_id_str = rep.peer_id_str();
        if peer_id_str.is_empty() {
            None
        } else {
            Some(PeerId::new(peer_id_str.to_string()))
        }
    }

    /// Broadcast a local update to the network.
    pub async fn broadcast_local_update(
        &self,
        cid: &Cid,
        block: &[u8],
        doc_id: &str,
        collection_id: &str,
    ) -> Result<BroadcastResult> {
        self.broadcast_local_update_with_creator(cid, block, doc_id, collection_id, None)
            .await
    }

    /// Broadcast a local update with an optional creator override.
    ///
    /// When `creator_override` is Some, the PushLog Creator field uses the
    /// given DID instead of this node's PeerId. This enables ACP owner
    /// registration on the receiving node during merge.
    pub async fn broadcast_local_update_with_creator(
        &self,
        cid: &Cid,
        block: &[u8],
        doc_id: &str,
        collection_id: &str,
        creator_override: Option<&str>,
    ) -> Result<BroadcastResult> {
        let creator = creator_override.unwrap_or(&self.access.local_peer_id);
        let broadcast =
            Broadcaster::<T>::create_broadcast(cid, block, doc_id, collection_id, creator);
        if doc_id.is_empty() {
            return self.runtime.broadcaster.broadcast_update(&broadcast).await;
        }
        let broadcaster = self.runtime.broadcaster.clone();
        self.runtime
            .broadcast_coalescer
            .run(broadcast, move |latest| async move {
                broadcaster
                    .broadcast_update(&latest)
                    .await
                    .map_err(|error| error.to_string())
            })
            .await
            .map_err(crate::error::Error::GossipSubPublish)
    }

    /// Push a full document DAG to replicator peers.
    pub async fn push_dag_to_replicators(
        &self,
        cid: &Cid,
        block: &[u8],
        doc_id: &str,
        collection_id: &str,
    ) {
        self.push_dag_to_replicators_with_creator(cid, block, doc_id, collection_id, None)
            .await
    }

    /// Push a full document DAG to replicators with optional creator override.
    pub async fn push_dag_to_replicators_with_creator(
        &self,
        cid: &Cid,
        block: &[u8],
        doc_id: &str,
        collection_id: &str,
        creator_override: Option<&str>,
    ) {
        let creator = creator_override.unwrap_or(&self.access.local_peer_id);
        self.coalesce_replicator_push(PendingPush {
            cid: *cid,
            block: Bytes::copy_from_slice(block),
            doc_id: doc_id.to_string(),
            collection_id: collection_id.to_string(),
            creator: creator.to_string(),
            expand_unfiltered_dag: true,
            document: None,
        })
        .await;
    }

    /// Push a single block to replicator peers (no DAG expansion).
    pub async fn push_to_replicators(
        &self,
        cid: &Cid,
        block: &[u8],
        doc_id: &str,
        collection_id: &str,
    ) {
        self.push_to_replicators_with_creator(cid, block, doc_id, collection_id, None)
            .await
    }

    /// Push a single block to replicators with optional creator override.
    pub async fn push_to_replicators_with_creator(
        &self,
        cid: &Cid,
        block: &[u8],
        doc_id: &str,
        collection_id: &str,
        creator_override: Option<&str>,
    ) {
        let creator = creator_override.unwrap_or(&self.access.local_peer_id);
        self.coalesce_replicator_push(PendingPush {
            cid: *cid,
            block: Bytes::copy_from_slice(block),
            doc_id: doc_id.to_string(),
            collection_id: collection_id.to_string(),
            creator: creator.to_string(),
            expand_unfiltered_dag: false,
            document: None,
        })
        .await;
    }

    /// Push a committed document update to replicators using document JSON to
    /// evaluate filtered peers. Filtered peers receive the full document DAG so
    /// merge completeness does not depend on generic Bitswap access.
    pub async fn push_document_to_replicators_with_creator(
        &self,
        cid: &Cid,
        block: &[u8],
        doc_id: &str,
        collection_id: &str,
        document: &JsonValue,
        creator_override: Option<&str>,
    ) {
        let creator = creator_override.unwrap_or(&self.access.local_peer_id);
        self.coalesce_replicator_push(PendingPush {
            cid: *cid,
            block: Bytes::copy_from_slice(block),
            doc_id: doc_id.to_string(),
            collection_id: collection_id.to_string(),
            creator: creator.to_string(),
            expand_unfiltered_dag: false,
            document: Some(document.clone()),
        })
        .await;
    }

    async fn coalesce_replicator_push(&self, push: PendingPush) {
        let coalescer = Arc::clone(&self.runtime.push_fanout_coalescer);
        coalescer
            .run(push, |latest| async move {
                self.dispatch_replicator_push(latest).await;
            })
            .await;
    }

    async fn dispatch_replicator_push(&self, push: PendingPush) {
        let Some(replicators) = self.list_replicators_for_push().await else {
            return;
        };
        if replicators.is_empty() {
            return;
        }
        tracing::debug!(
            cid = %push.cid,
            doc_id = %push.doc_id,
            collection_id = %push.collection_id,
            replicator_count = replicators.len(),
            "Queueing coalesced push to replicators"
        );
        let mut payload_guard = None;
        for rep in &replicators {
            if !Self::replicator_in_collection(rep, &push.collection_id) {
                continue;
            }
            let Some(peer_id) = Self::peer_id_for_replicator(rep) else {
                continue;
            };
            let expand_dag = if rep.is_filtered_for_collection(&push.collection_id) {
                let Some(document) = push.document.as_ref() else {
                    continue;
                };
                if !rep.matches_filter(
                    self.runtime.filter_matcher.as_ref(),
                    &push.collection_id,
                    document,
                ) {
                    continue;
                }
                true
            } else {
                push.expand_unfiltered_dag
            };
            let mut job = PushJobSpec::new(
                peer_id,
                push.doc_id.clone(),
                push.collection_id.clone(),
                push.creator.clone(),
                push.cid,
                push.block.clone(),
                expand_dag,
            );
            let payload = self.runtime.push_encode_cache.acquire(&job);
            payload_guard.get_or_insert_with(|| Arc::clone(&payload));
            job.encoded_payload = Some(payload);
            self.enqueue_replicator_push(job).await;
        }
    }

    /// Push searchable-encryption artifacts for a committed document to
    /// replicators of the collection. This mirrors Go's SE coordinator, which
    /// listens to committed update events independently of document access.
    pub async fn push_se_artifacts_to_replicators(
        &self,
        collection_id: &str,
        artifacts: Vec<SEArtifact>,
    ) {
        if artifacts.is_empty() {
            return;
        }

        let Some(replicators) = self.list_replicators_for_push().await else {
            return;
        };

        for rep in replicators {
            if !rep.collections.iter().any(|id| id == collection_id) {
                continue;
            }
            if rep.is_filtered_for_collection(collection_id) {
                continue;
            }

            let peer_id = PeerId::new(rep.id.clone());
            let request = PushSEArtifactsRequest::new(collection_id.to_string(), artifacts.clone());
            if let Err(error) = self
                .runtime
                .transport
                .send_se_artifacts(&peer_id, request)
                .await
            {
                tracing::warn!(
                    peer_id = %peer_id,
                    collection_id,
                    error = %error,
                    "Failed to push SE artifacts to replicator"
                );
                // Record a retry entry per (peer, doc) so the replicator retry
                // pass regenerates and re-pushes the SE artifacts once the peer
                // reconnects. Mirrors Go's independent `seRetryInfo`; the doc
                // block push failure is racy and may not fire when the SE push
                // does, so SE pushes must record their own retries.
                for doc_id in artifacts
                    .iter()
                    .map(|artifact| artifact.doc_id.clone())
                    .collect::<std::collections::HashSet<_>>()
                {
                    report_push_failure(
                        &self.runtime.failure_tx,
                        &peer_id,
                        doc_id,
                        collection_id.to_string(),
                        None,
                        0,
                    )
                    .await;
                }
            }
        }
    }

    /// Push SE artifacts with document-filter evaluation for filtered peers.
    pub async fn push_se_artifacts_to_replicators_for_document(
        &self,
        collection_id: &str,
        artifacts: Vec<SEArtifact>,
        document: &JsonValue,
    ) {
        if artifacts.is_empty() {
            return;
        }

        let Some(replicators) = self.list_replicators_for_push().await else {
            return;
        };

        for rep in replicators {
            if !rep.collections.iter().any(|id| id == collection_id) {
                continue;
            }
            if !rep.matches_filter(
                self.runtime.filter_matcher.as_ref(),
                collection_id,
                document,
            ) {
                continue;
            }

            let Some(peer_id) = Self::peer_id_for_replicator(&rep) else {
                continue;
            };
            let request = PushSEArtifactsRequest::new(collection_id.to_string(), artifacts.clone());
            if let Err(error) = self
                .runtime
                .transport
                .send_se_artifacts(&peer_id, request)
                .await
            {
                tracing::warn!(
                    peer_id = %peer_id,
                    collection_id,
                    error = %error,
                    "Failed to push SE artifacts to replicator"
                );
                for doc_id in artifacts
                    .iter()
                    .map(|artifact| artifact.doc_id.clone())
                    .collect::<std::collections::HashSet<_>>()
                {
                    report_push_failure(
                        &self.runtime.failure_tx,
                        &peer_id,
                        doc_id,
                        collection_id.to_string(),
                        None,
                        0,
                    )
                    .await;
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "broadcast_tests.rs"]
pub(super) mod tests;
