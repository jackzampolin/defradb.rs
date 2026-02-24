//! `P2PTransport` implementation backed by iroh's QUIC-native networking.
//!
//! `IrohTransport` is a thin facade that sends commands to the background
//! `IrohEndpoint` event loop via an mpsc channel.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use cid::Cid;
use iroh::SecretKey;
use tokio::sync::{mpsc, oneshot};

use crate::error::{Error, Result};
use crate::message::{
    BranchableSyncReply, BranchableSyncRequest, DocSyncReply, DocSyncRequest, PushLogBroadcast,
    PushLogReply, PushLogRequest, PushSEArtifactsRequest,
};
use crate::replicator::ReplicatorInfo;
use crate::topics::DefraTopic;
use crate::transport::{MessageId, P2PTransport, PeerAddr, PeerId, ResponseToken};
use crate::QueryId;

use super::command::IrohCommand;

/// Iroh-backed P2P transport.
///
/// Implements `P2PTransport` by sending commands to a background `IrohEndpoint`
/// task via an mpsc channel. The endpoint owns all iroh state (QUIC connections,
/// gossip subscriptions, etc.).
#[derive(Clone)]
pub struct IrohTransport {
    command_tx: mpsc::Sender<IrohCommand>,
    local_peer_id: PeerId,
    local_public_key_bytes: Vec<u8>,
    secret_key: Arc<SecretKey>,
}

impl IrohTransport {
    /// Create a new `IrohTransport` with the given command channel and identity.
    pub fn new(command_tx: mpsc::Sender<IrohCommand>, secret_key: SecretKey) -> Self {
        let node_id = secret_key.public();
        let local_peer_id = PeerId::new(node_id.to_string());
        let local_public_key_bytes = node_id.as_bytes().to_vec();

        Self {
            command_tx,
            local_peer_id,
            local_public_key_bytes,
            secret_key: Arc::new(secret_key),
        }
    }

    /// Send a command and await the oneshot reply.
    async fn send_command<T>(
        &self,
        make_cmd: impl FnOnce(oneshot::Sender<Result<T>>) -> IrohCommand,
    ) -> Result<T> {
        let (tx, rx) = oneshot::channel();
        self.command_tx
            .send(make_cmd(tx))
            .await
            .map_err(|_| Error::ChannelSend)?;
        rx.await.map_err(|_| Error::ChannelReceive)?
    }
}

#[async_trait]
impl P2PTransport for IrohTransport {
    fn local_peer_id(&self) -> &PeerId {
        &self.local_peer_id
    }

    fn local_public_key_proto(&self) -> &[u8] {
        &self.local_public_key_bytes
    }

    fn sign(&self, data: &[u8]) -> Result<Vec<u8>> {
        let signature = self.secret_key.sign(data);
        Ok(signature.to_bytes().to_vec())
    }

    async fn dial(&self, peer_id: &PeerId, addrs: Vec<PeerAddr>) -> Result<()> {
        self.send_command(|reply| IrohCommand::Dial {
            peer_id: peer_id.clone(),
            addrs,
            reply,
        })
        .await
    }

    async fn listen(&self, addr: PeerAddr) -> Result<()> {
        self.send_command(|reply| IrohCommand::Listen { addr, reply })
            .await
    }

    async fn connected_peers(&self) -> Result<Vec<PeerId>> {
        self.send_command(|reply| IrohCommand::ConnectedPeers { reply })
            .await
    }

    async fn listen_addresses(&self) -> Result<Vec<PeerAddr>> {
        self.send_command(|reply| IrohCommand::ListenAddresses { reply })
            .await
    }

    async fn poll_until_connected(&self, peer_id: &PeerId, timeout: Duration) -> Result<()> {
        let start = std::time::Instant::now();
        loop {
            let peers = self.connected_peers().await?;
            if peers.iter().any(|p| p.as_str() == peer_id.as_str()) {
                return Ok(());
            }
            if start.elapsed() >= timeout {
                return Err(Error::ConnectionTimeout(peer_id.to_string()));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn peer_addresses(&self) -> Result<Vec<String>> {
        self.send_command(|reply| IrohCommand::PeerAddresses { reply })
            .await
    }

    async fn subscribe(&self, topic: DefraTopic) -> Result<bool> {
        self.send_command(|reply| IrohCommand::Subscribe { topic, reply })
            .await
    }

    async fn unsubscribe(&self, topic: DefraTopic) -> Result<bool> {
        self.send_command(|reply| IrohCommand::Unsubscribe { topic, reply })
            .await
    }

    async fn publish(&self, topic: DefraTopic, msg: PushLogBroadcast) -> Result<MessageId> {
        self.send_command(|reply| IrohCommand::Publish { topic, msg, reply })
            .await
    }

    async fn send_pushlog_response(&self, token: ResponseToken, reply: PushLogReply) -> Result<()> {
        let send_stream: iroh::endpoint::SendStream = token
            .downcast::<iroh::endpoint::SendStream>()
            .ok_or_else(|| Error::ResponseSend("invalid response token type".to_string()))?;

        self.send_command(|r| IrohCommand::SendPushLogResponse {
            send_stream,
            reply_msg: reply,
            reply: r,
        })
        .await
    }

    async fn send_two_stream_request(
        &self,
        peer_id: &PeerId,
        req: PushLogRequest,
    ) -> Result<PushLogReply> {
        self.send_command(|reply| IrohCommand::SendTwoStreamRequest {
            peer_id: peer_id.clone(),
            request: req,
            reply,
        })
        .await
    }

    async fn send_two_stream_response(&self, peer_id: &PeerId, reply: PushLogReply) -> Result<()> {
        self.send_command(|r| IrohCommand::SendTwoStreamResponse {
            peer_id: peer_id.clone(),
            reply_msg: reply,
            reply: r,
        })
        .await
    }

    async fn send_doc_sync_request(&self, peer_id: &PeerId, req: DocSyncRequest) -> Result<()> {
        self.send_command(|reply| IrohCommand::SendDocSyncRequest {
            peer_id: peer_id.clone(),
            request: req,
            reply,
        })
        .await
    }

    async fn send_doc_sync_response(&self, peer_id: &PeerId, reply: DocSyncReply) -> Result<()> {
        self.send_command(|r| IrohCommand::SendDocSyncResponse {
            peer_id: peer_id.clone(),
            reply_msg: reply,
            reply: r,
        })
        .await
    }

    async fn send_branchable_sync_request(
        &self,
        peer_id: &PeerId,
        req: BranchableSyncRequest,
    ) -> Result<()> {
        self.send_command(|reply| IrohCommand::SendBranchableSyncRequest {
            peer_id: peer_id.clone(),
            request: req,
            reply,
        })
        .await
    }

    async fn send_branchable_sync_response(
        &self,
        peer_id: &PeerId,
        reply: BranchableSyncReply,
    ) -> Result<()> {
        self.send_command(|r| IrohCommand::SendBranchableSyncResponse {
            peer_id: peer_id.clone(),
            reply_msg: reply,
            reply: r,
        })
        .await
    }

    async fn send_car_request(&self, peer_id: &PeerId, root_cid: Cid) -> Result<()> {
        self.send_command(|reply| IrohCommand::SendCarRequest {
            peer_id: peer_id.clone(),
            root_cid,
            reply,
        })
        .await
    }

    async fn send_car_response(&self, peer_id: &PeerId, car_data: Vec<u8>) -> Result<()> {
        self.send_command(|reply| IrohCommand::SendCarResponse {
            peer_id: peer_id.clone(),
            car_data,
            reply,
        })
        .await
    }

    async fn send_car_response_token(&self, token: ResponseToken, car_data: Vec<u8>) -> Result<()> {
        let mut send_stream: iroh::endpoint::SendStream = token
            .downcast::<iroh::endpoint::SendStream>()
            .ok_or_else(|| Error::ResponseSend("invalid response token type".to_string()))?;

        send_stream
            .write_all(&car_data)
            .await
            .map_err(|e| Error::ResponseSend(format!("write CAR data: {}", e)))?;
        send_stream
            .finish()
            .map_err(|e| Error::ResponseSend(format!("finish CAR stream: {}", e)))?;
        Ok(())
    }

    async fn send_se_artifacts(&self, peer_id: &PeerId, req: PushSEArtifactsRequest) -> Result<()> {
        self.send_command(|reply| IrohCommand::SendSEArtifacts {
            peer_id: peer_id.clone(),
            request: req,
            reply,
        })
        .await
    }

    async fn sync_blocks(
        &self,
        root: Cid,
        providers: Vec<PeerId>,
        missing: Vec<Cid>,
    ) -> Result<QueryId> {
        self.send_command(|reply| IrohCommand::SyncBlocks {
            root,
            providers,
            missing,
            reply,
        })
        .await
    }

    async fn cancel_sync(&self, query_id: QueryId) -> Result<bool> {
        self.send_command(|reply| IrohCommand::CancelSync { query_id, reply })
            .await
    }

    async fn create_replicator(&self, peer_id: &PeerId, collections: Vec<String>) -> Result<()> {
        self.send_command(|reply| IrohCommand::CreateReplicator {
            peer_id: peer_id.clone(),
            collections,
            reply,
        })
        .await
    }

    async fn delete_replicator(&self, peer_id: &PeerId) -> Result<()> {
        self.send_command(|reply| IrohCommand::DeleteReplicator {
            peer_id: peer_id.clone(),
            reply,
        })
        .await
    }

    async fn list_replicators(&self) -> Result<Vec<ReplicatorInfo>> {
        self.send_command(|reply| IrohCommand::ListReplicators { reply })
            .await
    }

    async fn get_replicator(&self, peer_id: &PeerId) -> Result<Option<ReplicatorInfo>> {
        self.send_command(|reply| IrohCommand::GetReplicator {
            peer_id: peer_id.clone(),
            reply,
        })
        .await
    }

    async fn remove_replicator_collections(
        &self,
        peer_id: &PeerId,
        collections: Vec<String>,
    ) -> Result<bool> {
        self.send_command(|reply| IrohCommand::RemoveReplicatorCollections {
            peer_id: peer_id.clone(),
            collections,
            reply,
        })
        .await
    }

    async fn shutdown(&self) -> Result<()> {
        self.send_command(|reply| IrohCommand::Shutdown { reply })
            .await
    }
}
