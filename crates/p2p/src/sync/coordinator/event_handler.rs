//! Host event handling for the sync coordinator.

use blockstore::Blockstore;
use cid::Cid;

use super::SyncCoordinator;
use crate::error::Result;
use crate::host::HostEvent;
use crate::message::{
    BranchableSyncReply, DocSyncItem, DocSyncReply, PushLogBroadcast, PushLogReply,
};
use crate::signing::sign_message;

impl<B: Blockstore + 'static> SyncCoordinator<B> {
    /// Handle an event from the P2P host.
    ///
    /// This should be called from the event loop that processes HostEvents.
    pub async fn handle_host_event(&self, event: HostEvent) -> Result<()> {
        match event {
            HostEvent::PeerConnected(peer_id) => {
                tracing::debug!(peer_id = %peer_id, "Peer connected");
                self.peer_state.peer_connected(peer_id);
            }
            HostEvent::PeerDisconnected(peer_id) => {
                tracing::debug!(peer_id = %peer_id, "Peer disconnected");
                self.peer_state.peer_disconnected(&peer_id);
            }
            HostEvent::PeerSubscribed { peer_id, topic } => {
                tracing::debug!(peer_id = %peer_id, topic = %topic, "Peer subscribed to topic");
                self.peer_state.peer_subscribed(&peer_id, topic);
            }
            HostEvent::PeerUnsubscribed { peer_id, topic } => {
                tracing::debug!(peer_id = %peer_id, topic = %topic, "Peer unsubscribed from topic");
                self.peer_state.peer_unsubscribed(&peer_id, &topic);
            }
            HostEvent::GossipMessage {
                propagation_source,
                message,
                topic,
                ..
            } => {
                self.handle_gossip_message(propagation_source, message, topic)
                    .await?;
            }
            HostEvent::PushLogRequest {
                peer_id,
                request,
                channel,
            } => {
                self.handle_pushlog_request(peer_id, request, channel)
                    .await?;
            }
            HostEvent::TwoStreamRequest { peer_id, request } => {
                self.handle_two_stream_request(peer_id, request).await?;
            }
            HostEvent::BitswapBlockReceived {
                query_id,
                cid,
                data,
            } => {
                self.handle_bitswap_block_received(query_id, cid, data)
                    .await?;
            }
            HostEvent::BitswapComplete {
                query_id,
                success,
                error,
            } => {
                self.handle_bitswap_complete(query_id, success, error)
                    .await?;
            }
            HostEvent::DocSyncRequest { peer_id, request } => {
                self.handle_doc_sync_request(peer_id, request).await?;
            }
            HostEvent::DocSyncReply { peer_id, reply } => {
                self.handle_doc_sync_reply(peer_id, reply).await?;
            }
            HostEvent::BranchableSyncRequest { peer_id, request } => {
                self.handle_branchable_sync_request(peer_id, request)
                    .await?;
            }
            HostEvent::BranchableSyncReply { peer_id, reply } => {
                self.handle_branchable_sync_reply(peer_id, reply).await?;
            }
            other => {
                // Other events (peer discovery, listening, etc.) don't need sync handling
                tracing::trace!(event = ?other, "Ignoring non-sync host event");
            }
        }
        Ok(())
    }

    async fn handle_gossip_message(
        &self,
        propagation_source: libp2p::PeerId,
        message: PushLogBroadcast,
        topic: String,
    ) -> Result<()> {
        eprintln!(
            "[COORD] GossipMessage from={} doc_id={} collection={} topic={}",
            propagation_source, message.doc_id, message.collection_id, topic
        );
        tracing::debug!(
            doc_id = %message.doc_id,
            collection_id = %message.collection_id,
            topic = %topic,
            "Received GossipSub message"
        );

        // Access control check
        if let Err(e) = self.check_access(&propagation_source, &message.collection_id) {
            tracing::warn!(
                peer_id = %propagation_source,
                collection_id = %message.collection_id,
                doc_id = %message.doc_id,
                "Dropping GossipSub message from unauthorized peer"
            );
            return Err(e);
        }

        // Parse CID - if invalid, return error early
        match Cid::try_from(message.cid.as_slice()) {
            Ok(cid) => {
                self.peer_state.peer_has_cid(&propagation_source, cid);
            }
            Err(e) => {
                tracing::warn!(
                    peer_id = %propagation_source,
                    cid_bytes_len = message.cid.len(),
                    error = %e,
                    "Failed to parse CID from gossip message - skipping message"
                );
                return Err(crate::error::Error::InvalidCid(format!(
                    "Failed to parse CID from gossip message: {}",
                    e
                )));
            }
        }

        self.manager.process_pushlog(&message).await
    }

    async fn handle_pushlog_request(
        &self,
        peer_id: libp2p::PeerId,
        request: crate::message::PushLogRequest,
        channel: crate::host::ResponseChannel,
    ) -> Result<()> {
        eprintln!(
            "[COORD] PushLogRequest from={} doc_id={} collection={}",
            peer_id, request.doc_id, request.collection_id
        );
        tracing::debug!(
            peer_id = %peer_id,
            doc_id = %request.doc_id,
            "Received PushLog request"
        );

        // Access control check
        if let Err(e) = self.check_access(&peer_id, &request.collection_id) {
            tracing::warn!(
                peer_id = %peer_id,
                collection_id = %request.collection_id,
                doc_id = %request.doc_id,
                "Rejecting PushLog request from unauthorized peer"
            );
            let reply = PushLogReply::error(
                &request.metadata.message_id,
                &format!(
                    "access denied: not authorized for collection {}",
                    request.collection_id
                ),
            );
            if let Err(send_err) = self.host.send_pushlog_response(channel, reply).await {
                tracing::warn!(
                    peer_id = %peer_id,
                    error = %send_err,
                    "Failed to send access denied response"
                );
            }
            return Err(e);
        }

        // Parse CID - if invalid, send error response
        let cid = match Cid::try_from(request.cid.as_slice()) {
            Ok(cid) => {
                self.peer_state.peer_has_cid(&peer_id, cid);
                cid
            }
            Err(e) => {
                let error_msg = format!("Failed to parse CID: {}", e);
                tracing::warn!(
                    peer_id = %peer_id,
                    cid_bytes_len = request.cid.len(),
                    error = %e,
                    "Failed to parse CID from PushLog request - sending error response"
                );
                let reply = PushLogReply::error(&request.metadata.message_id, &error_msg);
                if let Err(send_err) = self.host.send_pushlog_response(channel, reply).await {
                    tracing::warn!(
                        peer_id = %peer_id,
                        error = %send_err,
                        "Failed to send error response for invalid CID"
                    );
                }
                return Err(crate::error::Error::InvalidCid(error_msg));
            }
        };

        tracing::trace!(?cid, "Parsed valid CID from PushLog request");

        // Convert request to broadcast format and process
        let broadcast = PushLogBroadcast::from_request(&request);
        let process_result = self.manager.process_pushlog(&broadcast).await;

        // Send response based on processing result
        let reply = match &process_result {
            Ok(()) => PushLogReply::success(&request.metadata.message_id),
            Err(e) => PushLogReply::error(&request.metadata.message_id, &e.to_string()),
        };

        if let Err(e) = self.host.send_pushlog_response(channel, reply).await {
            tracing::warn!(
                peer_id = %peer_id,
                doc_id = %request.doc_id,
                error = %e,
                "Failed to send PushLog response"
            );
        } else {
            tracing::trace!(
                peer_id = %peer_id,
                doc_id = %request.doc_id,
                "Sent PushLog response"
            );
        }

        process_result
    }

    async fn handle_two_stream_request(
        &self,
        peer_id: libp2p::PeerId,
        request: crate::message::PushLogRequest,
    ) -> Result<()> {
        eprintln!(
            "[COORD] TwoStreamRequest from={} doc_id={} collection={}",
            peer_id, request.doc_id, request.collection_id
        );
        tracing::debug!(
            peer_id = %peer_id,
            doc_id = %request.doc_id,
            message_id = %request.metadata.message_id,
            "Received PushLog request via two-stream protocol (Go compatibility)"
        );

        // Access control check
        if let Err(e) = self.check_access(&peer_id, &request.collection_id) {
            tracing::warn!(
                peer_id = %peer_id,
                collection_id = %request.collection_id,
                doc_id = %request.doc_id,
                "Rejecting two-stream request from unauthorized peer"
            );
            let mut reply = PushLogReply::error(
                &request.metadata.message_id,
                &format!(
                    "access denied: not authorized for collection {}",
                    request.collection_id
                ),
            );
            if let Err(sign_err) = sign_message(self.host.keypair(), &mut reply) {
                tracing::error!(error = %sign_err, "Failed to sign access denied response");
            }
            if let Err(send_err) = self.host.send_two_stream_response(peer_id, reply).await {
                tracing::warn!(
                    peer_id = %peer_id,
                    error = %send_err,
                    "Failed to send access denied response via two-stream"
                );
            }
            return Err(e);
        }

        // Parse CID - if invalid, send error response
        let cid = match Cid::try_from(request.cid.as_slice()) {
            Ok(cid) => {
                self.peer_state.peer_has_cid(&peer_id, cid);
                cid
            }
            Err(e) => {
                let error_msg = format!("Failed to parse CID: {}", e);
                tracing::warn!(
                    peer_id = %peer_id,
                    cid_bytes_len = request.cid.len(),
                    error = %e,
                    "Failed to parse CID from two-stream request - sending error response"
                );
                let mut reply = PushLogReply::error(&request.metadata.message_id, &error_msg);
                if let Err(sign_err) = sign_message(self.host.keypair(), &mut reply) {
                    tracing::error!(error = %sign_err, "Failed to sign invalid CID response");
                }
                if let Err(send_err) = self.host.send_two_stream_response(peer_id, reply).await {
                    tracing::warn!(
                        peer_id = %peer_id,
                        error = %send_err,
                        "Failed to send error response for invalid CID via two-stream"
                    );
                }
                return Err(crate::error::Error::InvalidCid(error_msg));
            }
        };

        tracing::trace!(?cid, "Parsed valid CID from two-stream request");

        // Convert request to broadcast format and process
        let broadcast = PushLogBroadcast::from_request(&request);
        let process_result = self.manager.process_pushlog(&broadcast).await;

        // Send response via two-stream protocol (on a NEW stream)
        let mut reply = match &process_result {
            Ok(()) => PushLogReply::success(&request.metadata.message_id),
            Err(e) => PushLogReply::error(&request.metadata.message_id, &e.to_string()),
        };

        // Sign the response (required for Go compatibility)
        if let Err(e) = sign_message(self.host.keypair(), &mut reply) {
            tracing::error!(
                peer_id = %peer_id,
                error = %e,
                "Failed to sign two-stream response"
            );
            return Err(e);
        }

        if let Err(e) = self.host.send_two_stream_response(peer_id, reply).await {
            tracing::warn!(
                peer_id = %peer_id,
                doc_id = %request.doc_id,
                error = %e,
                "Failed to send two-stream response"
            );
        } else {
            tracing::trace!(
                peer_id = %peer_id,
                doc_id = %request.doc_id,
                "Sent two-stream response"
            );
        }

        process_result
    }

    async fn handle_bitswap_block_received(
        &self,
        query_id: crate::QueryId,
        cid: Cid,
        data: Vec<u8>,
    ) -> Result<()> {
        tracing::info!(
            query_id = query_id.0,
            cid = %cid,
            data_len = data.len(),
            "Storing Bitswap block in blockstore"
        );

        match self.manager.store_bitswap_block(&cid, &data).await {
            Ok(true) => {
                tracing::debug!(
                    query_id = query_id.0,
                    cid = %cid,
                    "Bitswap block stored successfully"
                );
            }
            Ok(false) => {
                tracing::debug!(
                    query_id = query_id.0,
                    cid = %cid,
                    "Bitswap block was already in blockstore"
                );
            }
            Err(e) => {
                tracing::error!(
                    query_id = query_id.0,
                    cid = %cid,
                    error = %e,
                    "Failed to store Bitswap block"
                );
                return Err(e);
            }
        }
        Ok(())
    }

    async fn handle_bitswap_complete(
        &self,
        query_id: crate::QueryId,
        success: bool,
        error: Option<String>,
    ) -> Result<()> {
        eprintln!(
            "[DOCSYNC] BitswapComplete query_id={} success={} error={:?}",
            query_id.0, success, error
        );
        tracing::info!(
            query_id = query_id.0,
            success = success,
            error = ?error,
            "Bitswap fetch completed"
        );

        if success {
            let pending_dags: Vec<Cid> = self.manager.pending_dag_cids();
            eprintln!(
                "[DOCSYNC] BitswapComplete: pending_dags count={}",
                pending_dags.len()
            );

            for root_cid in pending_dags {
                eprintln!("[DOCSYNC] Retrying pending DAG root_cid={}", root_cid);
                match self.manager.retry_pending_dag(&root_cid).await {
                    Ok(true) => {
                        eprintln!("[DOCSYNC] Pending DAG completed root_cid={}", root_cid);
                        tracing::info!(
                            query_id = query_id.0,
                            root_cid = %root_cid,
                            "Pending DAG completed after Bitswap fetch"
                        );
                    }
                    Ok(false) => {
                        let missing = self.manager.pending_dag_missing(&root_cid);
                        if !missing.is_empty() {
                            eprintln!("[DOCSYNC] Pending DAG has {} missing child blocks, fetching via Bitswap root_cid={}", missing.len(), root_cid);
                            let providers = self
                                .peer_state
                                .connected_peers()
                                .into_iter()
                                .collect::<Vec<_>>();
                            if let Err(e) =
                                self.host.bitswap_sync(root_cid, providers, missing).await
                            {
                                eprintln!(
                                    "[DOCSYNC] Failed to start Bitswap for child blocks: {}",
                                    e
                                );
                                tracing::warn!(
                                    root_cid = %root_cid,
                                    error = %e,
                                    "Failed to start Bitswap fetch for child blocks"
                                );
                            }
                        } else {
                            eprintln!("[DOCSYNC] Pending DAG still has missing links but no missing CIDs reported root_cid={}", root_cid);
                        }
                        tracing::debug!(
                            query_id = query_id.0,
                            root_cid = %root_cid,
                            "Pending DAG still has missing links, initiated additional fetch"
                        );
                    }
                    Err(e) => {
                        eprintln!(
                            "[DOCSYNC] Failed to retry pending DAG root_cid={}: {}",
                            root_cid, e
                        );
                        tracing::warn!(
                            query_id = query_id.0,
                            root_cid = %root_cid,
                            error = %e,
                            "Failed to retry pending DAG"
                        );
                    }
                }
            }
        } else if let Some(ref err) = error {
            eprintln!("[DOCSYNC] Bitswap fetch failed: {}", err);
            tracing::warn!(
                query_id = query_id.0,
                error = %err,
                "Bitswap fetch failed"
            );
        }
        Ok(())
    }

    async fn handle_doc_sync_request(
        &self,
        peer_id: libp2p::PeerId,
        request: crate::message::DocSyncRequest,
    ) -> Result<()> {
        eprintln!(
            "[DOCSYNC] Received DocSyncRequest from peer={} doc_ids={:?}",
            peer_id, request.doc_ids
        );
        tracing::debug!(
            peer_id = %peer_id,
            doc_ids = ?request.doc_ids,
            message_id = %request.metadata.message_id,
            "Received DocSync request"
        );

        let mut results: Vec<DocSyncItem> = Vec::new();
        for doc_id in &request.doc_ids {
            eprintln!("[DOCSYNC] Looking up heads for doc_id={}", doc_id);
            match self.head_provider.get_document_heads(doc_id).await {
                Ok(heads) => {
                    eprintln!(
                        "[DOCSYNC] Found {} heads for doc_id={}",
                        heads.len(),
                        doc_id
                    );
                    if !heads.is_empty() {
                        tracing::debug!(
                            doc_id = %doc_id,
                            head_count = heads.len(),
                            "Found document heads for DocSync response"
                        );
                        results.push(DocSyncItem {
                            doc_id: doc_id.clone(),
                            heads: heads.iter().map(|cid| cid.to_bytes()).collect(),
                        });
                    }
                }
                Err(e) => {
                    eprintln!("[DOCSYNC] Error getting heads for doc_id={}: {}", doc_id, e);
                    tracing::warn!(
                        doc_id = %doc_id,
                        error = %e,
                        "Failed to get document heads for DocSync"
                    );
                }
            }
        }

        eprintln!(
            "[DOCSYNC] Sending DocSync response with {} results",
            results.len()
        );
        tracing::debug!(
            peer_id = %peer_id,
            result_count = results.len(),
            "Sending DocSync response"
        );

        let mut reply = DocSyncReply::success(&request.metadata.message_id, results);

        if let Err(e) = crate::signing::sign_message(self.host.keypair(), &mut reply) {
            eprintln!("[DOCSYNC] Failed to sign DocSync response: {}", e);
            tracing::error!(
                peer_id = %peer_id,
                error = %e,
                "Failed to sign DocSync response"
            );
            return Err(e);
        }

        if let Err(e) = self.host.send_doc_sync_response(peer_id, reply).await {
            eprintln!("[DOCSYNC] Failed to send DocSync response: {}", e);
            tracing::warn!(
                peer_id = %peer_id,
                error = %e,
                "Failed to send DocSync response"
            );
        } else {
            eprintln!("[DOCSYNC] Sent DocSync response to peer={}", peer_id);
            tracing::debug!(
                peer_id = %peer_id,
                "Sent DocSync response"
            );
        }
        Ok(())
    }

    async fn handle_doc_sync_reply(
        &self,
        peer_id: libp2p::PeerId,
        reply: DocSyncReply,
    ) -> Result<()> {
        eprintln!(
            "[DOCSYNC] Received DocSyncReply from peer={} message_id={} results_count={}",
            peer_id,
            reply.message_id,
            reply.results.len()
        );
        tracing::info!(
            peer_id = %peer_id,
            message_id = %reply.message_id,
            results_count = reply.results.len(),
            "Processing DocSync reply"
        );

        let mut cids_to_fetch: Vec<(Cid, String)> = Vec::new();
        for item in &reply.results {
            for head_bytes in &item.heads {
                match Cid::try_from(head_bytes.as_slice()) {
                    Ok(cid) => {
                        match self.manager.blockstore().has(&cid).await {
                            Ok(true) => {
                                tracing::debug!(
                                    cid = %cid,
                                    doc_id = %item.doc_id,
                                    "Already have block, skipping fetch"
                                );
                            }
                            Ok(false) => {
                                tracing::debug!(
                                    cid = %cid,
                                    doc_id = %item.doc_id,
                                    "Need to fetch block via Bitswap"
                                );
                                cids_to_fetch.push((cid, item.doc_id.clone()));
                            }
                            Err(e) => {
                                tracing::warn!(
                                    cid = %cid,
                                    error = %e,
                                    "Failed to check if block exists"
                                );
                                cids_to_fetch.push((cid, item.doc_id.clone()));
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            doc_id = %item.doc_id,
                            error = %e,
                            "Failed to parse CID from DocSync reply"
                        );
                    }
                }
            }
        }

        if !cids_to_fetch.is_empty() {
            eprintln!(
                "[DOCSYNC] Initiating Bitswap fetch for {} CIDs",
                cids_to_fetch.len()
            );
            tracing::info!(
                cid_count = cids_to_fetch.len(),
                "Initiating Bitswap fetch for DocSync blocks"
            );

            for (root_cid, doc_id) in &cids_to_fetch {
                eprintln!(
                    "[DOCSYNC] Registering pending DAG cid={} doc_id={}",
                    root_cid, doc_id
                );
                self.manager.register_docsync_dag(*root_cid, doc_id.clone());

                eprintln!("[DOCSYNC] Starting Bitswap sync for cid={}", root_cid);
                if let Err(e) = self
                    .host
                    .bitswap_sync(*root_cid, vec![peer_id], vec![*root_cid])
                    .await
                {
                    eprintln!(
                        "[DOCSYNC] Failed to start Bitswap sync for cid={}: {}",
                        root_cid, e
                    );
                    tracing::warn!(
                        error = %e,
                        cid = %root_cid,
                        doc_id = %doc_id,
                        "Failed to initiate Bitswap sync for DocSync CID"
                    );
                }
            }
        } else {
            eprintln!("[DOCSYNC] No blocks to fetch from DocSync reply (all local)");
            tracing::debug!("No blocks to fetch from DocSync reply (all local)");
        }
        Ok(())
    }

    async fn handle_branchable_sync_request(
        &self,
        peer_id: libp2p::PeerId,
        request: crate::message::BranchableSyncRequest,
    ) -> Result<()> {
        eprintln!(
            "[BRANCHABLE] Received BranchableSyncRequest from peer={} collection={}",
            peer_id, request.collection_id
        );

        let heads = match self
            .head_provider
            .get_collection_heads(&request.collection_id)
            .await
        {
            Ok(heads) => {
                eprintln!(
                    "[BRANCHABLE] Found {} collection heads for {}",
                    heads.len(),
                    request.collection_id
                );
                for h in &heads {
                    eprintln!("[BRANCHABLE]   head CID: {}", h);
                }
                heads.iter().map(|cid| cid.to_bytes()).collect()
            }
            Err(e) => {
                eprintln!("[BRANCHABLE] Failed to get collection heads: {}", e);
                Vec::new()
            }
        };

        let mut reply = BranchableSyncReply::success(
            &request.metadata.message_id,
            &request.collection_id,
            heads,
        );

        if let Err(e) = crate::signing::sign_message(self.host.keypair(), &mut reply) {
            tracing::error!(
                peer_id = %peer_id,
                error = %e,
                "Failed to sign BranchableSync response"
            );
            return Err(e);
        }

        if let Err(e) = self
            .host
            .send_branchable_sync_response(peer_id, reply)
            .await
        {
            tracing::warn!(
                peer_id = %peer_id,
                error = %e,
                "Failed to send BranchableSync response"
            );
        }
        Ok(())
    }

    async fn handle_branchable_sync_reply(
        &self,
        peer_id: libp2p::PeerId,
        reply: crate::message::BranchableSyncReply,
    ) -> Result<()> {
        eprintln!(
            "[BRANCHABLE] Received BranchableSyncReply from peer={} collection={} heads={}",
            peer_id,
            reply.collection_id,
            reply.heads.len()
        );

        if reply.heads.is_empty() {
            eprintln!(
                "[BRANCHABLE] Peer has no heads for collection {}",
                reply.collection_id
            );
            return Ok(());
        }

        let mut cids_to_fetch: Vec<Cid> = Vec::new();
        for head_bytes in &reply.heads {
            match Cid::try_from(head_bytes.as_slice()) {
                Ok(cid) => {
                    eprintln!("[BRANCHABLE] Parsed collection head CID: {}", cid);
                    match self.manager.blockstore().has(&cid).await {
                        Ok(true) => {
                            eprintln!("[BRANCHABLE] Already have block {}", cid);
                        }
                        Ok(false) => {
                            eprintln!("[BRANCHABLE] Need to fetch block {}", cid);
                            cids_to_fetch.push(cid);
                        }
                        Err(e) => {
                            eprintln!("[BRANCHABLE] Error checking block {}: {}", cid, e);
                            cids_to_fetch.push(cid);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("[BRANCHABLE] Failed to parse CID from reply: {}", e);
                }
            }
        }

        if !cids_to_fetch.is_empty() {
            eprintln!(
                "[BRANCHABLE] Initiating Bitswap fetch for {} collection blocks",
                cids_to_fetch.len()
            );

            for root_cid in &cids_to_fetch {
                self.manager
                    .register_branchable_dag(*root_cid, reply.collection_id.clone());

                if let Err(e) = self
                    .host
                    .bitswap_sync(*root_cid, vec![peer_id], vec![*root_cid])
                    .await
                {
                    eprintln!(
                        "[BRANCHABLE] Failed to start Bitswap for {}: {}",
                        root_cid, e
                    );
                }
            }
        } else {
            eprintln!(
                "[BRANCHABLE] All blocks already local for collection {}",
                reply.collection_id
            );
        }
        Ok(())
    }
}
