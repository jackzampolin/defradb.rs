use super::*;
use crate::iroh::{IrohDiscoveryConfig, IrohRelayModeConfig};
use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;

#[tokio::test]
async fn endpoint_discards_buffered_commands_when_last_sender_drops() {
    let config = IrohEndpointConfig {
        relay_mode: IrohRelayModeConfig::Disabled,
        discovery: IrohDiscoveryConfig::Disabled,
        bind_addr: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        ..Default::default()
    };
    let (commands, _events, _replicators, mut task) = spawn_endpoint(config).await.unwrap();
    let (reply, response) = oneshot::channel();
    // The current-thread runtime cannot dispatch this command before the drop.
    commands
        .try_send(IrohCommand::ConnectedPeers { reply })
        .unwrap();
    drop(commands);

    let stopped = tokio::time::timeout(Duration::from_secs(5), &mut task).await;
    if stopped.is_err() {
        task.abort();
        let _ = task.await;
    }
    stopped.expect("endpoint did not stop").unwrap();
    assert!(response.await.is_err(), "queued command was dispatched");
}
