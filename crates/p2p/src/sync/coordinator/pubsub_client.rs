//! Coordinator-side public API for the pubsub_rpc DocSync /
//! BranchableSync services.
//!
//! - [`SyncCoordinator::start_pubsub_services`] — subscribe the base and
//!   response sub-topics, register them with the transport, and mark the
//!   coordinator as ready to serve inbound requests.
//! - [`SyncCoordinator::pubsub_sync_documents`] — caller-side DocSync
//!   publish; returns collected [`DocSyncReply`]s.
//! - [`SyncCoordinator::pubsub_sync_branchable_collection`] — caller-side
//!   BranchableSync publish.
//!
//! These are all no-ops on transports whose `local_peer_id()` isn't a
//! libp2p PeerId (iroh), so the existing two-stream paths remain the
//! functional ones for those transports.

use std::time::Duration;

use blockstore::Blockstore;
use tracing::{debug, warn};

use super::pubsub_services::{BRANCHABLE_SYNC_TOPIC, DOC_SYNC_TOPIC};
use super::SyncCoordinator;
use crate::error::{Error, Result};
use crate::message::pubsub as wire;
use crate::message::{
    BranchableSyncReply as TwoStreamBranchableSyncReply, DocSyncItem as TwoStreamDocSyncItem,
    DocSyncReply as TwoStreamDocSyncReply,
};
use crate::pubsub_rpc::{PublishOptions, PubsubResponse};
use crate::transport::{P2PTransport, PeerId};

/// Default wait for DocSync / BranchableSync responses before returning
/// whatever has arrived. Matches Go's `5*time.Second` fallback in
/// `sync_doc.go:125`.
const DEFAULT_PUBSUB_SYNC_TIMEOUT: Duration = Duration::from_secs(5);

impl<B: Blockstore + 'static, T: P2PTransport> SyncCoordinator<B, T> {
    /// Subscribe to `doc-sync`, `sync-branchable`, and our per-peer
    /// `<base>/<self>/_response` sub-topics, then register all four with
    /// the transport so inbound messages arrive as
    /// [`crate::transport::TransportEvent::GossipRawMessage`].
    ///
    /// Idempotent: safe to call at startup and again on reconnect.
    /// Silently skips on transports without pubsub_rpc support
    /// (`subscribe_raw` / `register_pubsub_rpc_topic` return `Err`/`Ok`
    /// per the trait defaults; iroh inherits the defaults).
    pub async fn start_pubsub_services(&self) -> Result<()> {
        let Some(services) = self.pubsub_services.as_ref() else {
            debug!("pubsub_rpc services disabled (local peer is not a libp2p PeerId)");
            return Ok(());
        };

        let doc_self = services.doc_sync.self_response_topic().to_string();
        let branch_self = services.branchable_sync.self_response_topic().to_string();

        for topic in [
            DOC_SYNC_TOPIC.to_string(),
            BRANCHABLE_SYNC_TOPIC.to_string(),
            doc_self.clone(),
            branch_self.clone(),
        ] {
            // subscribe_raw returns Err on transports without gossipsub.
            // Treat that as soft-failure so the coordinator can be started
            // uniformly on libp2p + iroh; the raw-message dispatcher only
            // fires when messages actually arrive.
            if let Err(e) = self.runtime.transport.subscribe_raw(topic.clone()).await {
                debug!(topic = %topic, error = %e, "subscribe_raw skipped");
                return Ok(());
            }
            self.runtime
                .transport
                .register_pubsub_rpc_topic(topic)
                .await?;
        }

        debug!(
            doc_sync_topic = DOC_SYNC_TOPIC,
            branchable_topic = BRANCHABLE_SYNC_TOPIC,
            "pubsub_rpc services started"
        );
        Ok(())
    }

    /// Publish a DocSync request over `doc-sync` and wait up to
    /// `timeout` (default 5s) for responses. Matches the behavior of Go's
    /// `SyncDocuments` in `sync_doc.go:61-88`.
    pub async fn pubsub_sync_documents(
        &self,
        doc_ids: Vec<String>,
        timeout: Option<Duration>,
    ) -> Result<Vec<(String, wire::DocSyncReply)>> {
        let Some(services) = self.pubsub_services.as_ref() else {
            return Err(Error::Transport(
                "pubsub_rpc DocSync is not available on this transport".into(),
            ));
        };

        let mut req_bytes = Vec::new();
        ciborium::into_writer(&wire::DocSyncRequest::new(doc_ids), &mut req_bytes)
            .map_err(|e| Error::CborSerialization(e.to_string()))?;

        let mut prep = services.doc_sync.prepare_publish(
            req_bytes,
            PublishOptions {
                multi_response: true,
                ..Default::default()
            },
        );

        if let Err(e) = self
            .runtime
            .transport
            .publish_raw(DOC_SYNC_TOPIC.to_string(), prep.data.clone())
            .await
        {
            warn!(error = %e, "doc-sync publish_raw failed");
            return Err(e);
        }

        let wait = timeout.unwrap_or(DEFAULT_PUBSUB_SYNC_TIMEOUT);
        let mut out = Vec::new();
        while let Ok(Some(resp)) = tokio::time::timeout(wait, prep.responses.recv()).await {
            if let Some(parsed) = parse_doc_sync_response(&resp) {
                let peer_str = resp.from.to_string();
                // Feed the reply into the coordinator's standard handler
                // so DAG fetches and merges trigger just like the
                // two-stream path. Converted to the two-stream struct
                // shape (which has a MetaData header) so we can reuse
                // the existing handle_doc_sync_reply logic.
                let converted = TwoStreamDocSyncReply {
                    version: crate::protocol::MESSAGE_VERSION.to_string(),
                    message_id: String::new(),
                    sender_id: parsed.sender.clone(),
                    pubkey: Vec::new(),
                    signature: None,
                    err_message: None,
                    results: parsed
                        .results
                        .iter()
                        .map(|item| TwoStreamDocSyncItem {
                            doc_id: item.doc_id.clone(),
                            heads: item.heads.clone(),
                        })
                        .collect(),
                };
                if let Err(e) = self
                    .handle_doc_sync_reply(PeerId::new(peer_str.clone()), converted)
                    .await
                {
                    warn!(from = %peer_str, error = %e, "doc-sync: reply processing failed");
                }
                out.push((peer_str, parsed));
            }
        }
        Ok(out)
    }

    /// Publish a BranchableSync request for `collection_id` and wait up
    /// to `timeout` (default 5s) for the first reply. Matches Go's
    /// `SyncBranchableCollection`.
    pub async fn pubsub_sync_branchable_collection(
        &self,
        collection_id: String,
        timeout: Option<Duration>,
    ) -> Result<Option<(String, wire::BranchableSyncReply)>> {
        let Some(services) = self.pubsub_services.as_ref() else {
            return Err(Error::Transport(
                "pubsub_rpc BranchableSync is not available on this transport".into(),
            ));
        };

        let mut req_bytes = Vec::new();
        ciborium::into_writer(
            &wire::BranchableSyncRequest::new(collection_id),
            &mut req_bytes,
        )
        .map_err(|e| Error::CborSerialization(e.to_string()))?;

        let mut prep = services
            .branchable_sync
            .prepare_publish(req_bytes, PublishOptions::default());

        if let Err(e) = self
            .runtime
            .transport
            .publish_raw(BRANCHABLE_SYNC_TOPIC.to_string(), prep.data.clone())
            .await
        {
            warn!(error = %e, "sync-branchable publish_raw failed");
            return Err(e);
        }

        let wait = timeout.unwrap_or(DEFAULT_PUBSUB_SYNC_TIMEOUT);
        match tokio::time::timeout(wait, prep.responses.recv()).await {
            Ok(Some(resp)) => {
                let peer_str = resp.from.to_string();
                let parsed = parse_branchable_sync_response(&resp);
                if let Some(reply) = &parsed {
                    // Feed through the two-stream handler so DAG fetches
                    // schedule the same way.
                    let converted = TwoStreamBranchableSyncReply {
                        version: crate::protocol::MESSAGE_VERSION.to_string(),
                        message_id: String::new(),
                        sender_id: reply.sender.clone(),
                        pubkey: Vec::new(),
                        signature: None,
                        err_message: None,
                        collection_id: reply.collection_id.clone(),
                        heads: reply.heads.clone(),
                    };
                    if let Err(e) = self
                        .handle_branchable_sync_reply(PeerId::new(peer_str.clone()), converted)
                        .await
                    {
                        warn!(from = %peer_str, error = %e, "sync-branchable: reply processing failed");
                    }
                }
                Ok(parsed.map(|reply| (peer_str, reply)))
            }
            _ => Ok(None),
        }
    }
}

fn parse_doc_sync_response(resp: &PubsubResponse) -> Option<wire::DocSyncReply> {
    if let Some(err) = &resp.err {
        warn!(from = %resp.from, error = %err, "doc-sync: peer returned error");
        return None;
    }
    match ciborium::from_reader::<wire::DocSyncReply, _>(resp.data.as_slice()) {
        Ok(r) => Some(r),
        Err(e) => {
            warn!(from = %resp.from, error = %e, "doc-sync: failed to decode reply");
            None
        }
    }
}

fn parse_branchable_sync_response(resp: &PubsubResponse) -> Option<wire::BranchableSyncReply> {
    if let Some(err) = &resp.err {
        warn!(from = %resp.from, error = %err, "sync-branchable: peer returned error");
        return None;
    }
    match ciborium::from_reader::<wire::BranchableSyncReply, _>(resp.data.as_slice()) {
        Ok(r) => Some(r),
        Err(e) => {
            warn!(from = %resp.from, error = %e, "sync-branchable: failed to decode reply");
            None
        }
    }
}
