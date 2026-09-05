#![cfg(feature = "iroh-transport")]

use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;

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
