//! Broadcasting local updates to the network.

use std::collections::HashSet;

use acp::ReplicatedDocActorRelationships;
use blockstore::Blockstore;
use bytes::Bytes;
use cid::Cid;

use super::SyncCoordinator;
use crate::error::Result;
use crate::message::PushLogRequest;
use crate::signing::sign_with_transport;
use crate::sync::broadcaster::Broadcaster;
use crate::sync::BroadcastResult;
use crate::transport::{P2PTransport, PeerId};

impl<B: Blockstore + 'static, T: P2PTransport> SyncCoordinator<B, T> {
    async fn list_replicators_for_push(&self) -> Option<Vec<crate::replicator::ReplicatorInfo>> {
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

    fn report_push_failure(
        failure_tx: &Option<tokio::sync::mpsc::Sender<super::PushFailure>>,
        peer_id: &PeerId,
        doc_id: String,
        collection_id: String,
    ) {
        if let Some(tx) = failure_tx {
            let _ = tx.try_send(super::PushFailure {
                peer_id: peer_id.to_string(),
                doc_id,
                collection_id,
            });
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
        self.broadcast_local_update_with_creator_and_relationships(
            cid,
            block,
            doc_id,
            collection_id,
            None,
            None,
        )
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
        self.broadcast_local_update_with_creator_and_relationships(
            cid,
            block,
            doc_id,
            collection_id,
            creator_override,
            None,
        )
        .await
    }

    /// Broadcast a local update with optional creator and ACP relationship snapshot.
    pub async fn broadcast_local_update_with_creator_and_relationships(
        &self,
        cid: &Cid,
        block: &[u8],
        doc_id: &str,
        collection_id: &str,
        creator_override: Option<&str>,
        acp_actor_relationships: Option<ReplicatedDocActorRelationships>,
    ) -> Result<BroadcastResult> {
        let creator = creator_override.unwrap_or(&self.access.local_peer_id);
        let broadcast = Broadcaster::<T>::create_broadcast(
            cid,
            block,
            doc_id,
            collection_id,
            creator,
            acp_actor_relationships,
        );
        self.runtime.broadcaster.broadcast_update(&broadcast).await
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
        let Some(replicators) = self.list_replicators_for_push().await else {
            return;
        };

        if replicators.is_empty() {
            return;
        }

        let dag_blocks = self
            .load_dag_blocks(*cid, Bytes::copy_from_slice(block))
            .await;

        tracing::debug!(
            cid = %cid,
            doc_id = %doc_id,
            collection_id = %collection_id,
            replicator_count = replicators.len(),
            dag_block_count = dag_blocks.len(),
            "Pushing DAG to replicators"
        );

        for rep in &replicators {
            if !rep.collections.is_empty() && !rep.collections.contains(&collection_id.to_string())
            {
                continue;
            }

            let peer_id_str = rep.peer_id_str().to_string();
            if peer_id_str.is_empty() {
                continue;
            }
            let peer_id = PeerId::new(peer_id_str);

            let mut requests: Vec<(Cid, PushLogRequest)> = Vec::new();

            for (block_cid, block_data) in &dag_blocks {
                let mut req = PushLogRequest::new(
                    doc_id.to_string(),
                    Bytes::from(block_cid.to_bytes()),
                    collection_id.to_string(),
                    creator.to_string(),
                    block_data.clone(),
                );
                if sign_with_transport(&self.runtime.transport, &mut req).is_ok() {
                    requests.push((*block_cid, req));
                }
            }

            // Spawn a task per peer, bounded by push_semaphore to prevent
            // resource exhaustion during document creation bursts.
            let transport = self.runtime.transport.clone();
            let failure_tx = self.runtime.failure_tx.clone();
            let doc_id_owned = doc_id.to_string();
            let collection_id_owned = collection_id.to_string();
            let semaphore = self.runtime.push_semaphore.clone();
            tokio::spawn(async move {
                let _permit = semaphore.acquire().await;
                let any_failed =
                    Self::send_ordered_pushlogs_via_transport(&transport, &peer_id, requests).await;
                if any_failed {
                    Self::report_push_failure(
                        &failure_tx,
                        &peer_id,
                        doc_id_owned,
                        collection_id_owned,
                    );
                }
            });
        }
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
        let Some(replicators) = self.list_replicators_for_push().await else {
            return;
        };

        for rep in &replicators {
            if !rep.collections.is_empty() && !rep.collections.contains(&collection_id.to_string())
            {
                continue;
            }

            let peer_id_str = rep.peer_id_str().to_string();
            if peer_id_str.is_empty() {
                continue;
            }
            let peer_id = PeerId::new(peer_id_str);

            let mut request = PushLogRequest::new(
                doc_id.to_string(),
                Bytes::from(cid.to_bytes()),
                collection_id.to_string(),
                creator.to_string(),
                Bytes::copy_from_slice(block),
            );

            if let Err(e) = sign_with_transport(&self.runtime.transport, &mut request) {
                tracing::debug!(error = %e, "Failed to sign PushLog request");
                continue;
            }

            let transport = self.runtime.transport.clone();
            let cid_clone = *cid;
            let failure_tx = self.runtime.failure_tx.clone();
            let doc_id_owned = doc_id.to_string();
            let collection_id_owned = collection_id.to_string();
            let semaphore = self.runtime.push_semaphore.clone();
            let peer_id_clone = peer_id.clone();
            tokio::spawn(async move {
                let _permit = semaphore.acquire().await;
                if let Err(e) = transport
                    .send_two_stream_request(&peer_id_clone, request)
                    .await
                {
                    tracing::debug!(
                        peer_id = %peer_id_clone,
                        cid = %cid_clone,
                        error = %e,
                        "PushLog to replicator failed"
                    );
                    Self::report_push_failure(
                        &failure_tx,
                        &peer_id_clone,
                        doc_id_owned,
                        collection_id_owned,
                    );
                }
            });
        }
    }

    /// Load every transitive block in a document DAG, with dependencies first.
    async fn load_dag_blocks(&self, root_cid: Cid, root_bytes: Bytes) -> Vec<(Cid, Bytes)> {
        let mut ordered = Vec::new();
        let mut visited = HashSet::new();
        let mut stack = vec![(root_cid, root_bytes, false)];

        while let Some((cid, data, expanded)) = stack.pop() {
            if expanded {
                ordered.push((cid, data));
                continue;
            }

            if !visited.insert(cid) {
                continue;
            }

            let linked_cids = defra_core::Block::from_dag_cbor(&data)
                .ok()
                .and_then(|block| defra_core::collect_block_links(&block).ok())
                .unwrap_or_default();

            stack.push((cid, data, true));

            for linked_cid in linked_cids.into_iter().rev() {
                match self.blockstore().get(&linked_cid).await {
                    Ok(Some(linked_data)) => stack.push((linked_cid, linked_data, false)),
                    Ok(None) => {
                        tracing::debug!(
                            root_cid = %root_cid,
                            linked_cid = %linked_cid,
                            "Linked DAG block not found in blockstore"
                        );
                    }
                    Err(error) => {
                        tracing::debug!(
                            root_cid = %root_cid,
                            linked_cid = %linked_cid,
                            error = %error,
                            "Failed to load linked DAG block"
                        );
                    }
                }
            }
        }

        ordered
    }

    /// Send PushLog requests to a peer in order via the transport, waiting for each to complete.
    async fn send_ordered_pushlogs_via_transport(
        transport: &T,
        peer_id: &PeerId,
        requests: Vec<(Cid, PushLogRequest)>,
    ) -> bool {
        let mut any_failed = false;
        for (cid, request) in requests {
            match transport.send_two_stream_request(peer_id, request).await {
                Ok(reply) if reply.err_message.is_some() => {
                    tracing::warn!(
                        peer_id = %peer_id,
                        cid = %cid,
                        error = %reply.err_message.as_deref().unwrap_or("unknown pushlog error"),
                        "PushLog to replicator was rejected"
                    );
                    any_failed = true;
                    break;
                }
                Ok(_) => {}
                Err(e) => {
                    if e.is_connection_like() {
                        tracing::debug!(
                            peer_id = %peer_id,
                            cid = %cid,
                            error = %e,
                            "PushLog to replicator failed because the connection became unavailable; stopping replay for this peer"
                        );
                    } else {
                        tracing::debug!(
                            peer_id = %peer_id,
                            cid = %cid,
                            error = %e,
                            "PushLog to replicator failed"
                        );
                    }
                    any_failed = true;
                    break;
                }
            }
        }
        any_failed
    }
}
