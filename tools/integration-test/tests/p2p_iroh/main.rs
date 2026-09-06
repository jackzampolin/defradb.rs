mod acp;
#[path = "../client_authored_common.rs"]
mod client_authored_common;
mod connection;
#[path = "../manage_relay_common.rs"]
mod manage_relay_common;
mod peer;
mod replication;
#[path = "../replicator_retry_common.rs"]
mod replicator_retry_common;
mod schema;
mod support;
mod sync;

#[tokio::test]
async fn iroh_replicator_retry_intervals() {
    replicator_retry_common::retry_intervals_test(
        integration_test::TestCluster::builder().with_iroh_transport(),
    )
    .await;
}
