#[path = "p2p/connection_manager.rs"]
mod connection_manager;
#[path = "p2p/document.rs"]
mod document;
#[path = "p2p/feature_binaries.rs"]
mod feature_binaries;
#[path = "p2p/filtered_replication.rs"]
mod filtered_replication;
#[path = "p2p/idempotent_replay.rs"]
mod idempotent_replay;
#[path = "p2p/manage_relay.rs"]
mod manage_relay;
#[path = "manage_relay_common.rs"]
mod manage_relay_common;
#[path = "p2p/management.rs"]
mod management;
#[path = "p2p/quarantine.rs"]
mod quarantine;
#[path = "p2p/receiver_pull.rs"]
mod receiver_pull;
#[path = "p2p/replication.rs"]
mod replication;
#[path = "p2p/replication_advanced.rs"]
mod replication_advanced;
#[path = "p2p/resilience.rs"]
mod resilience;
#[path = "p2p/sync.rs"]
mod sync;
#[path = "p2p/transports.rs"]
mod transports;
#[path = "p2p/trust_boundary.rs"]
mod trust_boundary;
#[path = "p2p/write_contention.rs"]
mod write_contention;

#[path = "replicator_retry_common.rs"]
mod replicator_retry_common;

#[tokio::test]
async fn rust_replicator_retry_intervals() {
    replicator_retry_common::retry_intervals_test(
        integration_test::TestCluster::builder().with_p2p(),
    )
    .await;
}
