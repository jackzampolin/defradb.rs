//! CAR fetch event handling for the sync coordinator.

use blockstore::{verify_block_cid, Blockstore};
use bytes::Bytes;
use cid::Cid;

use crate::bitswap::BlockClass;
use crate::error::Result;
use crate::message::CarFetchRequest;
use crate::sync::car::{collect_dag_blocks, collect_exact_blocks, decode_car, encode_car};
use crate::sync::coordinator::SyncCoordinator;
use crate::transport::{P2PTransport, PeerId};

use super::super::authorizer::AccessAuthorizer;

fn sample_cids(cids: &[Cid]) -> Vec<String> {
    cids.iter().take(4).map(ToString::to_string).collect()
}

async fn sample_cid_presence<B: Blockstore>(
    blockstore: &B,
    cids: &[Cid],
) -> Vec<(String, Option<bool>)> {
    let mut presence = Vec::with_capacity(cids.len().min(4));
    for cid in cids.iter().take(4) {
        presence.push((cid.to_string(), blockstore.has(cid).await.ok()));
    }
    presence
}

impl<B: Blockstore + 'static, T: P2PTransport> SyncCoordinator<B, T> {
    /// Handle an inbound CAR fetch request: collect the DAG and send CARv1 response.
    pub(crate) async fn handle_car_fetch_request(
        &self,
        peer_id: PeerId,
        request: CarFetchRequest,
        token: Option<T::ResponseToken>,
    ) -> Result<()> {
        let root_present = match self.manager.blockstore().has(&request.root_cid).await {
            Ok(present) => Some(present),
            Err(error) => {
                tracing::debug!(
                    root_cid = %request.root_cid,
                    peer_id = %peer_id,
                    error = %error,
                    "CAR handler: failed to check root presence before serving request"
                );
                None
            }
        };

        tracing::debug!(
            root_cid = %request.root_cid,
            peer_id = %peer_id,
            recursive = request.recursive,
            requested_count = request.wanted_cids.len(),
            has_token = token.is_some(),
            "CAR handler: collecting blocks"
        );
        let response_roots = request.response_roots();
        let collected = if request.recursive {
            collect_dag_blocks(self.manager.blockstore().as_ref(), &request.root_cid).await?
        } else {
            collect_exact_blocks(self.manager.blockstore().as_ref(), &request.wanted_cids).await?
        };
        let truncated = collected.truncated();
        let collected_count = collected.blocks.len();
        let blocks = self
            .filter_car_response_blocks(&peer_id, &request.root_cid, collected.blocks)
            .await;
        let kept_count = blocks.len();
        let filtered_count = collected_count.saturating_sub(kept_count);
        let blockstore_miss_count = request.wanted_cids.len().saturating_sub(collected_count);
        self.manager.diagnostics.record_car_serve_counts(
            request.wanted_cids.len(),
            collected_count,
            kept_count,
            filtered_count,
        );

        if blocks.is_empty() {
            self.manager.diagnostics.record_car_no_blocks_served();
            // Normal race: peer asked for blocks we have not yet received
            // ourselves. Noisy at WARN during concurrent replication catch-up;
            // the requester retries until some peer serves the DAG.
            if request.recursive {
                tracing::debug!(
                    root_cid = %request.root_cid,
                    peer_id = %peer_id,
                    recursive = request.recursive,
                    requested_count = request.wanted_cids.len(),
                    root_present = ?root_present,
                    "CAR handler: no blocks found for request"
                );
            } else {
                let requested_presence =
                    sample_cid_presence(self.manager.blockstore().as_ref(), &request.wanted_cids)
                        .await;
                tracing::warn!(
                    root_cid = %request.root_cid,
                    peer_id = %peer_id,
                    root_present = ?root_present,
                    requested_count = request.wanted_cids.len(),
                    blockstore_hit_count = collected_count,
                    blockstore_miss_count,
                    filtered_count,
                    kept_count,
                    requested_cids = ?sample_cids(&request.wanted_cids),
                    requested_presence = ?requested_presence,
                    "CAR handler: no exact blocks served for selective request"
                );
            }
            // Send a header-only CAR so both transports (iroh and libp2p)
            // receive a well-formed response they can decode. Without this,
            // the libp2p response handler errors on empty bytes and the
            // requester-side car_empty_responses counter stays at zero
            // (issue #858 review feedback).
            let car_data = encode_car(&response_roots, &[])?;
            if let Some(token) = token {
                self.runtime
                    .transport
                    .send_car_response_token(token, car_data)
                    .await?;
            } else {
                self.runtime
                    .transport
                    .send_car_response(&peer_id, car_data)
                    .await?;
            }
            return Ok(());
        }

        let block_refs: Vec<(&Cid, &[u8])> = blocks.iter().map(|(c, d)| (c, d.as_ref())).collect();
        let car_data = encode_car(&response_roots, &block_refs)?;

        tracing::debug!(
            root_cid = %request.root_cid,
            peer_id = %peer_id,
            recursive = request.recursive,
            response_roots = response_roots.len(),
            blocks = blocks.len(),
            car_bytes = car_data.len(),
            truncated,
            "Sending CAR response"
        );
        if truncated {
            tracing::warn!(
                root_cid = %request.root_cid,
                peer_id = %peer_id,
                recursive = request.recursive,
                response_roots = response_roots.len(),
                blocks = blocks.len(),
                car_bytes = car_data.len(),
                "CAR response truncated by server-side limits"
            );
        }

        if let Some(token) = token {
            self.runtime
                .transport
                .send_car_response_token(token, car_data)
                .await?;
        } else {
            self.runtime
                .transport
                .send_car_response(&peer_id, car_data)
                .await?;
        }
        Ok(())
    }

    async fn filter_car_response_blocks(
        &self,
        peer_id: &PeerId,
        root_cid: &Cid,
        blocks: Vec<(Cid, Bytes)>,
    ) -> Vec<(Cid, Bytes)> {
        if self.access.access_mode.is_open() {
            return blocks;
        }

        let peer_str = peer_id.to_string();
        let serve = self.serve_acp.get();
        let mut identity: Option<acp::Identity> = None;
        let mut kept = Vec::with_capacity(blocks.len());
        let rooted_grant = self
            .runtime
            .selective_car_access
            .allows_root(peer_id, root_cid)
            || self
                .has_restart_safe_root_authority(peer_id, root_cid)
                .await;
        let granted_cids = if rooted_grant {
            crate::sync::car::collect_dag_cids(
                self.manager.blockstore().as_ref(),
                root_cid,
                crate::sync::car::CAR_MAX_BLOCKS,
            )
            .await
            .ok()
            .map(|cids| cids.into_iter().collect::<std::collections::HashSet<_>>())
        } else {
            None
        };

        for (cid, data) in blocks {
            if granted_cids
                .as_ref()
                .is_some_and(|cids| cids.contains(&cid))
            {
                kept.push((cid, data));
                continue;
            }

            match self.classifier.classify(&cid, &data).await {
                BlockClass::Allow => kept.push((cid, data)),
                BlockClass::Deny => {
                    tracing::debug!(
                        cid = %cid,
                        peer_id = %peer_id,
                        "CAR handler: dropping block denied by classifier"
                    );
                }
                BlockClass::Data(meta) => {
                    if self
                        .access
                        .replicators
                        .is_filtered_replicator(&meta.collection_id, &peer_str)
                    {
                        continue;
                    }
                    if self
                        .access
                        .replicators
                        .is_replicator(&meta.collection_id, &peer_str)
                    {
                        kept.push((cid, data));
                        continue;
                    }

                    let Some(serve) = serve else {
                        continue;
                    };
                    if identity.is_none() {
                        identity = Some(match serve.resolver.resolve(peer_id).await {
                            Some(did) => acp::Identity::Authenticated(did),
                            None => acp::Identity::Anonymous,
                        });
                    }
                    if serve
                        .gate
                        .may_read(identity.as_ref().expect("identity set"), &meta)
                        .await
                    {
                        kept.push((cid, data));
                    }
                }
            }
        }

        kept
    }

    /// Re-derive the authority installed before a head hint after the sender
    /// has restarted or its in-memory grant cache has expired.
    ///
    /// The DB-backed classifier binds the requested root to its collection and
    /// document IDs. Replicator configuration is durable and binds the peer to
    /// that collection. Filter predicates select which roots are announced;
    /// they are not a block-read security boundary (ACP/encryption remains the
    /// security boundary). Consequently the exact requested root reconstructs
    /// the rooted CAR capability without CID-valued sender delivery state.
    async fn has_restart_safe_root_authority(&self, peer_id: &PeerId, root_cid: &Cid) -> bool {
        let Ok(Some(root_data)) = self.manager.blockstore().get(root_cid).await else {
            return false;
        };
        let BlockClass::Data(meta) = self.classifier.classify(root_cid, &root_data).await else {
            return false;
        };
        // Document composite roots resolve to one or more document IDs, while
        // collection-commit roots deliberately resolve to an empty set. Both
        // are valid head-hint scopes. The exact root and the reachability walk
        // below constrain the capability; requiring a document ID here would
        // strand every durably registered collection obligation after restart.
        let configured_replicator = self
            .authorizer
            .peer_authorized_for_collection(peer_id.as_str(), &meta.collection_id)
            .await;
        // Gossip ingress accepts a locally subscribed collection even when
        // the source has no outbound replicator record for that receiver.
        // After a sender restart the receiver advertises that configured
        // subscription again. Treat the observed, exact collection topic as
        // the second restart-reconstructible serving policy; otherwise a
        // success-acked gossip root becomes permanently unserviceable as soon
        // as its process-local SelectiveCarAccess grant expires.
        let configured_subscriber = self
            .access
            .peer_state
            .peer_subscribed_to_collection(peer_id.as_str(), &meta.collection_id);
        let authorized = configured_replicator || configured_subscriber;
        if authorized {
            tracing::debug!(
                peer_id = %peer_id,
                root_cid = %root_cid,
                collection_id = %meta.collection_id,
                configured_replicator,
                configured_subscriber,
                "Re-derived rooted CAR authority from replication scope"
            );
        } else {
            tracing::debug!(
                peer_id = %peer_id,
                root_cid = %root_cid,
                collection_id = %meta.collection_id,
                configured_replicator,
                configured_subscriber,
                "Could not re-derive rooted CAR authority from replication scope"
            );
        }
        authorized
    }

    /// Handle an inbound CAR fetch response: decode and store blocks.
    pub(crate) async fn handle_car_fetch_response(
        &self,
        peer_id: PeerId,
        root_cid: Cid,
        car_data: Vec<u8>,
    ) -> Result<()> {
        let result = self
            .handle_car_fetch_response_inner(peer_id, root_cid, car_data)
            .await;
        self.manager
            .rooted_car_completion_tracker()
            .complete(root_cid, result.is_ok());
        result
    }

    async fn handle_car_fetch_response_inner(
        &self,
        peer_id: PeerId,
        root_cid: Cid,
        car_data: Vec<u8>,
    ) -> Result<()> {
        // Raw-empty: transport received zero bytes (peer had nothing for
        // this root, e.g. the serving side's handle_car_fetch_request
        // returned without writing a body). Count and skip decode.
        if car_data.is_empty() {
            self.manager.diagnostics.record_car_empty_response();
            tracing::debug!(
                root_cid = %root_cid,
                peer_id = %peer_id,
                "Received empty CAR response (raw bytes)"
            );
            return Ok(());
        }

        let (_roots, blocks) = decode_car(&car_data)?;

        if blocks.is_empty() {
            self.manager.diagnostics.record_car_empty_response();
            // Peer replied with a parseable but block-less CAR (provider
            // had nothing for this root). Debug: transport layer surfaces
            // the final "no provider succeeded" outcome via BitswapComplete
            // (see issue #858).
            tracing::debug!(
                root_cid = %root_cid,
                peer_id = %peer_id,
                "Received empty CAR response (decoded 0 blocks)"
            );
            return Ok(());
        }

        // Verify all block CIDs before storing (finding 03-35).
        for (cid, data) in &blocks {
            if let Err(e) = verify_block_cid(cid, data) {
                let p2p_err = crate::error::blockstore_verify_to_p2p(e, cid);
                tracing::warn!(
                    root_cid = %root_cid,
                    block_cid = %cid,
                    peer_id = %peer_id,
                    error = %p2p_err,
                    "CAR block failed CID verification, rejecting entire response"
                );
                return Err(p2p_err);
            }
        }

        // Share the same CID ownership boundary as PushLog processing and
        // merge. Go holds its root sync owner across CAR ingest and merge;
        // Rust additionally owns every contained CID because overlapping DAGs
        // can share a mutable `ToMergeIndexKey` and SSI correctly treats two
        // unsynchronised writers as a conflict.
        let _storage_owners = self
            .manager
            .acquire_car_storage_owners(root_cid, blocks.iter().map(|(cid, _)| *cid).collect())
            .await;
        let block_refs: Vec<(&Cid, &[u8])> = blocks.iter().map(|(c, d)| (c, d.as_ref())).collect();
        self.manager
            .blockstore()
            .as_ref()
            .put_many(&block_refs)
            .await
            .map_err(|e| crate::error::Error::BlockstoreError(e.to_string()))?;

        tracing::debug!(
            root_cid = %root_cid,
            peer_id = %peer_id,
            blocks_stored = blocks.len(),
            car_bytes = car_data.len(),
            "Stored blocks from CAR response"
        );

        Ok(())
    }
}
