#![cfg(feature = "iroh-transport")]

use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;

use multihash_codetable::{Code, MultihashDigest};
use p2p::iroh::{
    spawn_endpoint, IrohDiscoveryConfig, IrohEndpointConfig, IrohRelayModeConfig, IrohTransport,
};
use p2p::transport::P2PTransport;
use tokio::task::JoinHandle;
use tokio::time::timeout;

fn config() -> IrohEndpointConfig {
    IrohEndpointConfig {
        relay_mode: IrohRelayModeConfig::Disabled,
        discovery: IrohDiscoveryConfig::Disabled,
        bind_addr: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        ..Default::default()
    }
}

async fn assert_endpoint_stopped(mut task: JoinHandle<()>) {
    let result = timeout(Duration::from_secs(5), &mut task).await;
    if result.is_err() {
        task.abort();
        let _ = task.await;
    }
    result
        .expect("endpoint kept running after its last command sender was dropped")
        .expect("endpoint task panicked");
}

#[tokio::test]
async fn endpoint_stops_when_last_transport_clone_is_dropped() {
    let config = config();
    let key = config.secret_key.clone();
    let (commands, _events, _replicators, task) = spawn_endpoint(config).await.unwrap();
    let transport = IrohTransport::new(commands, key);
    let last_transport = transport.clone();
    drop(transport);

    assert!(last_transport.connected_peers().await.unwrap().is_empty());
    drop(last_transport);

    assert_endpoint_stopped(task).await;
}

#[tokio::test]
async fn endpoint_stops_when_command_sender_is_dropped_before_startup() {
    let (commands, _events, _replicators, task) = spawn_endpoint(config()).await.unwrap();
    drop(commands);

    assert_endpoint_stopped(task).await;
}

#[tokio::test]
async fn shutdown_joins_subscriptions_and_syncs_with_a_full_event_queue() {
    let config = config();
    let key = config.secret_key.clone();
    let (commands, events, _replicators, task) = spawn_endpoint(config).await.unwrap();
    let transport = IrohTransport::new(commands, key);
    transport.subscribe(p2p::DefraTopic::DocSync).await.unwrap();
    transport
        .subscribe_raw("shutdown-test".into())
        .await
        .unwrap();

    // Keep the receiver idle so failed syncs block delivering their completion.
    let root = cid::Cid::new_v1(0x55, Code::Sha2_256.digest(b"shutdown"));
    timeout(Duration::from_secs(5), async {
        for _ in 0..300 {
            transport.sync_blocks(root, vec![], vec![]).await.unwrap();
        }
        while events.capacity() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("syncs did not fill the event queue");
    drop(transport);

    assert_endpoint_stopped(task).await;
    assert!(
        events.is_closed(),
        "a task retained the endpoint's event sender"
    );
}
