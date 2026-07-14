use std::collections::HashSet;
use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;

use bytes::Bytes;
use iroh::SecretKey;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::timeout;

use super::{spawn_endpoint, IrohDiscoveryConfig, IrohEndpointConfig, IrohTransport};
use crate::message::{PushLogReply, PushLogRequest};
use crate::signing::sign_with_transport;
use crate::transport::{P2PTransport, PeerId, TransportEvent};

const IO_TIMEOUT: Duration = Duration::from_secs(5);

struct TestNode {
    transport: IrohTransport,
    events: mpsc::Receiver<TransportEvent<iroh::endpoint::SendStream>>,
    task: JoinHandle<()>,
}

impl TestNode {
    async fn spawn() -> Self {
        let secret_key = SecretKey::generate();
        let config = IrohEndpointConfig {
            secret_key: secret_key.clone(),
            relay_mode: crate::iroh::IrohRelayModeConfig::Disabled,
            discovery: IrohDiscoveryConfig::Disabled,
            bind_port: None,
            bind_addr: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            gossip_heal: Default::default(),
        };
        let (command_tx, events, _replicators, task) = spawn_endpoint(config).await.unwrap();
        Self {
            transport: IrohTransport::new(command_tx, secret_key),
            events,
            task,
        }
    }

    async fn shutdown(self) {
        self.transport.shutdown().await.unwrap();
        self.task.await.unwrap();
    }
}

async fn connect(sender: &IrohTransport, receiver: &IrohTransport) {
    sender
        .dial(
            receiver.local_peer_id(),
            receiver.listen_addresses().await.unwrap(),
        )
        .await
        .unwrap();
    sender
        .poll_until_connected(receiver.local_peer_id(), IO_TIMEOUT)
        .await
        .unwrap();
    receiver
        .poll_until_connected(sender.local_peer_id(), IO_TIMEOUT)
        .await
        .unwrap();
}

fn signed_request(transport: &IrohTransport, label: &str) -> PushLogRequest {
    let mut request = PushLogRequest::new(
        format!("doc-{label}"),
        Bytes::from(vec![1, 2, 3]),
        "collection1".to_string(),
        "creator1".to_string(),
        Bytes::from(format!("block-{label}").into_bytes()),
    );
    sign_with_transport(transport, &mut request).unwrap();
    request
}

async fn next_two_stream_request(
    events: &mut mpsc::Receiver<TransportEvent<iroh::endpoint::SendStream>>,
) -> (PeerId, PushLogRequest, iroh::endpoint::SendStream) {
    loop {
        let event = timeout(IO_TIMEOUT, events.recv())
            .await
            .expect("timed out waiting for two-stream request")
            .expect("iroh event channel closed");
        if let TransportEvent::TwoStreamRequest {
            peer_id,
            request,
            token: Some(token),
            ..
        } = event
        {
            return (peer_id, request, token);
        }
    }
}

#[tokio::test]
async fn two_stream_request_receives_reply_on_request_stream() {
    let sender = TestNode::spawn().await;
    let mut receiver = TestNode::spawn().await;
    connect(&sender.transport, &receiver.transport).await;

    let request = signed_request(&sender.transport, "same-stream");
    let message_id = request.message_id.clone();
    let target = receiver.transport.local_peer_id().clone();
    let sender_transport = sender.transport.clone();
    let send_task = tokio::spawn(async move {
        sender_transport
            .send_two_stream_request(&target, request)
            .await
    });

    let (peer_id, received, token) = next_two_stream_request(&mut receiver.events).await;
    assert_eq!(peer_id, *sender.transport.local_peer_id());
    assert_eq!(received.message_id, message_id);
    assert!(received.supports_same_stream_reply);

    let mut reply = PushLogReply::success(&message_id);
    sign_with_transport(&receiver.transport, &mut reply).unwrap();
    receiver
        .transport
        .send_pushlog_response(token, reply)
        .await
        .unwrap();

    let received_reply = timeout(Duration::from_secs(1), send_task)
        .await
        .expect("same-stream two-stream reply timed out")
        .unwrap()
        .unwrap();
    assert_eq!(received_reply.message_id, message_id);

    sender.shutdown().await;
    receiver.shutdown().await;
}

#[tokio::test]
async fn two_stream_request_still_accepts_legacy_reverse_stream_reply() {
    let sender = TestNode::spawn().await;
    let mut receiver = TestNode::spawn().await;
    connect(&sender.transport, &receiver.transport).await;

    let request = signed_request(&sender.transport, "legacy-reply");
    let message_id = request.message_id.clone();
    let target = receiver.transport.local_peer_id().clone();
    let sender_transport = sender.transport.clone();
    let send_task = tokio::spawn(async move {
        sender_transport
            .send_two_stream_request(&target, request)
            .await
    });

    let (_, received, _) = next_two_stream_request(&mut receiver.events).await;
    assert_eq!(received.message_id, message_id);
    assert!(received.supports_same_stream_reply);

    let mut reply = PushLogReply::success(&message_id);
    sign_with_transport(&receiver.transport, &mut reply).unwrap();
    receiver
        .transport
        .send_two_stream_response(sender.transport.local_peer_id(), reply)
        .await
        .unwrap();

    let received_reply = timeout(Duration::from_secs(1), send_task)
        .await
        .expect("legacy reverse-stream reply timed out")
        .unwrap()
        .unwrap();
    assert_eq!(received_reply.message_id, message_id);

    sender.shutdown().await;
    receiver.shutdown().await;
}

#[tokio::test]
async fn concurrent_two_stream_fan_in_replies_on_request_streams() {
    const SENDERS: usize = 8;

    let mut receiver = TestNode::spawn().await;
    let mut senders = Vec::with_capacity(SENDERS);
    for index in 0..SENDERS {
        let sender = TestNode::spawn().await;
        connect(&sender.transport, &receiver.transport).await;
        let request = signed_request(&sender.transport, &format!("fan-in-{index}"));
        let target = receiver.transport.local_peer_id().clone();
        let sender_transport = sender.transport.clone();
        let send_task = tokio::spawn(async move {
            sender_transport
                .send_two_stream_request(&target, request)
                .await
        });
        senders.push((sender, send_task));
    }

    let mut peers = HashSet::with_capacity(SENDERS);
    for _ in 0..SENDERS {
        let (peer_id, request, token) = next_two_stream_request(&mut receiver.events).await;
        peers.insert(peer_id);
        assert!(request.supports_same_stream_reply);
        let mut reply = PushLogReply::success(&request.message_id);
        sign_with_transport(&receiver.transport, &mut reply).unwrap();
        receiver
            .transport
            .send_pushlog_response(token, reply)
            .await
            .unwrap();
    }
    assert_eq!(peers.len(), SENDERS);

    for (_, send_task) in &mut senders {
        let reply = timeout(Duration::from_secs(1), send_task)
            .await
            .expect("fan-in two-stream reply timed out")
            .unwrap()
            .unwrap();
        assert!(reply.err_message.is_none());
    }

    for (sender, _) in senders {
        sender.shutdown().await;
    }
    receiver.shutdown().await;
}
