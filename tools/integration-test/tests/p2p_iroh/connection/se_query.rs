//! Iroh direct-stream SE query transport tests.
//!
//! Run with:
//!   cargo test -p integration-test --test p2p_iroh -- connection::se_query

use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;

use p2p::iroh::{
    load_or_generate_secret_key, spawn_endpoint, IrohDiscoveryConfig, IrohEndpointConfig,
    IrohRelayModeConfig, IrohTransport,
};
use p2p::message::{QuerySEArtifactsReply, QuerySEArtifactsRequest, SEFieldQuery};
use p2p::{sign_with_transport, P2PTransport, TransportEvent};
use tokio::time::timeout;

async fn test_config() -> IrohEndpointConfig {
    IrohEndpointConfig {
        secret_key: load_or_generate_secret_key(None)
            .await
            .expect("generate iroh key"),
        relay_mode: IrohRelayModeConfig::Disabled,
        discovery: IrohDiscoveryConfig::Disabled,
        bind_port: None,
        bind_addr: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
    }
}

#[tokio::test]
async fn roundtrip_emits_request_and_reply_events() {
    let config0 = test_config().await;
    let config1 = test_config().await;
    let key0 = config0.secret_key.clone();
    let key1 = config1.secret_key.clone();
    let (command_tx0, mut events0, _replicators0, task0) =
        spawn_endpoint(config0).await.expect("spawn endpoint 0");
    let (command_tx1, mut events1, _replicators1, task1) =
        spawn_endpoint(config1).await.expect("spawn endpoint 1");
    let transport0 = IrohTransport::new(command_tx0, key0);
    let transport1 = IrohTransport::new(command_tx1, key1);

    transport0
        .dial(
            transport1.local_peer_id(),
            transport1.listen_addresses().await.expect("listen addrs 1"),
        )
        .await
        .expect("dial endpoint 1");
    transport0
        .poll_until_connected(transport1.local_peer_id(), Duration::from_secs(5))
        .await
        .expect("endpoint 0 sees endpoint 1");
    transport1
        .poll_until_connected(transport0.local_peer_id(), Duration::from_secs(5))
        .await
        .expect("endpoint 1 sees endpoint 0");

    let mut request = QuerySEArtifactsRequest::new(
        "collection1",
        vec![SEFieldQuery::new("name", "name", vec![1, 2, 3])],
    );
    sign_with_transport(&transport0, &mut request).expect("sign request");
    let request_message_id = request.message_id.clone();

    transport0
        .send_se_query_request(transport1.local_peer_id(), request)
        .await
        .expect("send request");

    let received_request = loop {
        let event = timeout(Duration::from_secs(5), events1.recv())
            .await
            .expect("timed out waiting for SE query request")
            .expect("iroh event channel closed");
        if let TransportEvent::SEQueryRequest { peer_id, request } = event {
            assert_eq!(peer_id, *transport0.local_peer_id());
            break request;
        }
    };
    assert_eq!(received_request.message_id, request_message_id);
    assert_eq!(
        received_request.sender_id,
        transport0.local_peer_id().to_string()
    );
    assert_eq!(received_request.collection_id, "collection1");
    assert_eq!(received_request.queries.len(), 1);
    assert_eq!(received_request.queries[0].search_tag, vec![1, 2, 3]);

    let mut reply =
        QuerySEArtifactsReply::success(&request_message_id, vec!["doc1".into(), "doc2".into()]);
    sign_with_transport(&transport1, &mut reply).expect("sign reply");
    transport1
        .send_se_query_response(transport0.local_peer_id(), reply)
        .await
        .expect("send reply");

    loop {
        let event = timeout(Duration::from_secs(5), events0.recv())
            .await
            .expect("timed out waiting for SE query reply")
            .expect("iroh event channel closed");
        if let TransportEvent::SEQueryReply { peer_id, reply } = event {
            assert_eq!(peer_id, *transport1.local_peer_id());
            assert_eq!(reply.message_id, request_message_id);
            assert_eq!(reply.sender_id, transport1.local_peer_id().to_string());
            assert_eq!(reply.doc_ids, vec!["doc1".to_string(), "doc2".to_string()]);
            break;
        }
    }

    transport0.shutdown().await.expect("shutdown endpoint 0");
    transport1.shutdown().await.expect("shutdown endpoint 1");
    task0.await.expect("endpoint task 0");
    task1.await.expect("endpoint task 1");
}
