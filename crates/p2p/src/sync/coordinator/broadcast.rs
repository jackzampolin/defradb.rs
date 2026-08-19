//! Broadcasting local updates to the network.
//!
//! Replicator pushes are admitted into the bounded push backlog as compact
//! job specs; the fixed worker pool (`push_worker`) authorizes rooted CAR, signs, and
//! sends. Admission happens before any task is spawned or payload captured,
//! so outbound resident state stays bounded under sustained writes (#1099).

use std::sync::Arc;

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

impl<B: Blockstore + 'static, T: P2PTransport> SyncCoordinator<B, T> {
    async fn list_replicators_for_push(&self) -> Result<Vec<crate::replicator::ReplicatorInfo>> {
        if self.runtime.shutdown.is_shutting_down() {
            return Err(crate::error::Error::DurableHeadMarker(
                "coordinator is shutting down".to_string(),
            ));
        }

        self.runtime
            .transport
            .list_replicators()
            .await
            .map_err(|error| {
                crate::error::Error::DurableHeadMarker(format!(
                    "failed to enumerate durable replicators: {error}"
                ))
            })
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
            EnqueueOutcome::Enqueued => {}
            EnqueueOutcome::Coalesced | EnqueueOutcome::RetiredStale => {}
            EnqueueOutcome::RejectedItems | EnqueueOutcome::RejectedBytes => {
                tracing::warn!(
                    peer_id = %peer_id,
                    doc_id = %doc_id,
                    collection_id = %collection_id,
                    outcome = ?outcome,
                    "Outbound push backlog full; deferring push to persisted retry"
                );
                let _ = report_push_failure(
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
        let mut broadcast =
            Broadcaster::<T>::create_broadcast(cid, block, doc_id, collection_id, creator);
        // Iroh exposes only the immediate relay for received gossip. Carry
        // and authenticate the publisher so a directly connected receiver can
        // recover from the actual DAG owner; sparse receivers safely fall back
        // to their separately authenticated propagation hop.
        broadcast.source_peer_id = Some(self.access.local_peer_id.clone());
        let origin_bytes = broadcast.origin_signing_bytes()?;
        broadcast.origin_signature = Some(self.runtime.transport.sign(&origin_bytes)?);
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

    /// Announce one current head block to replicator peers. The receiver pulls
    /// missing linked blocks through the rooted CAR path.
    pub async fn push_to_replicators(
        &self,
        cid: &Cid,
        block: &[u8],
        doc_id: &str,
        collection_id: &str,
    ) -> Result<()> {
        self.push_to_replicators_with_creator(cid, block, doc_id, collection_id, None)
            .await
    }

    /// Announce one current head with an optional creator override.
    pub async fn push_to_replicators_with_creator(
        &self,
        cid: &Cid,
        block: &[u8],
        doc_id: &str,
        collection_id: &str,
        creator_override: Option<&str>,
    ) -> Result<()> {
        let creator = creator_override.unwrap_or(&self.access.local_peer_id);
        self.coalesce_replicator_push(PendingPush {
            cid: *cid,
            block: Bytes::copy_from_slice(block),
            doc_id: doc_id.to_string(),
            collection_id: collection_id.to_string(),
            creator: creator.to_string(),
            document: None,
        })
        .await
    }

    /// Push a committed document update to replicators using document JSON to
    /// evaluate filtered peers. Matching peers receive the same bounded head
    /// hint and pull missing dependencies through selective CAR.
    pub async fn push_document_to_replicators_with_creator(
        &self,
        cid: &Cid,
        block: &[u8],
        doc_id: &str,
        collection_id: &str,
        document: &JsonValue,
        creator_override: Option<&str>,
    ) -> Result<()> {
        let creator = creator_override.unwrap_or(&self.access.local_peer_id);
        self.coalesce_replicator_push(PendingPush {
            cid: *cid,
            block: Bytes::copy_from_slice(block),
            doc_id: doc_id.to_string(),
            collection_id: collection_id.to_string(),
            creator: creator.to_string(),
            document: Some(document.clone()),
        })
        .await
    }

    async fn coalesce_replicator_push(&self, push: PendingPush) -> Result<()> {
        // Register every peer/scope obligation before coalescing or queueing.
        // The marker is presence-only, so observing a newer head remains
        // idempotent while making a storage failure visible to the committed
        // write path instead of silently dropping delivery.
        for job in self.push_jobs(&push).await? {
            if !report_observed_head(&self.runtime.failure_tx, &job).await {
                self.runtime.push_backlog.record_head_hint_failure(
                    crate::sync::push_backlog::HeadHintFailureReason::Local,
                );
                tracing::error!(
                    peer_id = %job.peer_id,
                    collection_id = %job.collection_id,
                    doc_id = %job.doc_id,
                    "Committed head has no durable sender marker"
                );
                return Err(crate::error::Error::DurableHeadMarker(format!(
                    "failed to register peer {} scope {}/{}",
                    job.peer_id, job.collection_id, job.doc_id
                )));
            }
        }

        let coalescer = Arc::clone(&self.runtime.push_fanout_coalescer);
        coalescer
            .run(push, |latest| async move {
                self.dispatch_replicator_push(latest).await;
            })
            .await;
        Ok(())
    }

    async fn push_jobs(&self, push: &PendingPush) -> Result<Vec<PushJobSpec>> {
        let replicators = self.list_replicators_for_push().await?;
        let mut jobs = Vec::new();
        for rep in &replicators {
            if !Self::replicator_in_collection(rep, &push.collection_id) {
                continue;
            }
            let Some(peer_id) = Self::peer_id_for_replicator(rep) else {
                continue;
            };
            if rep.is_filtered_for_collection(&push.collection_id) {
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
            }
            jobs.push(PushJobSpec::new(
                peer_id,
                push.doc_id.clone(),
                push.collection_id.clone(),
                push.creator.clone(),
                push.cid,
                push.block.clone(),
            ));
        }
        Ok(jobs)
    }

    async fn dispatch_replicator_push(&self, push: PendingPush) {
        let jobs = match self.push_jobs(&push).await {
            Ok(jobs) => jobs,
            Err(error) => {
                tracing::warn!(%error, "Durably marked head deferred to retry sweep");
                return;
            }
        };
        tracing::debug!(
            cid = %push.cid,
            doc_id = %push.doc_id,
            collection_id = %push.collection_id,
            replicator_count = jobs.len(),
            "Queueing coalesced push to replicators"
        );
        for job in jobs {
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

        let replicators = match self.list_replicators_for_push().await {
            Ok(replicators) => replicators,
            Err(error) => {
                tracing::warn!(%error, "Failed to enumerate replicators for SE artifact push");
                return;
            }
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
                    let _ = report_push_failure(
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

        let replicators = match self.list_replicators_for_push().await {
            Ok(replicators) => replicators,
            Err(error) => {
                tracing::warn!(%error, "Failed to enumerate replicators for filtered SE artifact push");
                return;
            }
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
                    let _ = report_push_failure(
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
