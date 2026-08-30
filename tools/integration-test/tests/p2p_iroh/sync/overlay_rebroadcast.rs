//! With post-merge gossip rebroadcast on, a document reaches the third node
//! of an A -> B -> C chain through B alone.
//!
//! Same topology as `overlay_origin`: B dials A, C dials B, all subscribed
//! to one collection. A's own hints reach C over the iroh overlay but are
//! dropped as advisory, because C's transport has no route to A. B, having
//! merged the complete DAG, re-announces each head with itself as the signed
//! origin; C's transport does know B, so that hint registers a fetchable root
//! and C converges without ever talking to A.
//!
//! `DEFRA_P2P_REBROADCAST_ON_MERGE` is process-global and inherited by the
//! spawned nodes, so the test is `#[serial]` and clears the variable on exit.
//!
//! Run with:
//!   cargo test --test p2p_iroh -- sync::overlay_rebroadcast::

use std::time::Duration;

use integration_test::{extract_p2p_addr, poll_until, TestCluster};
use serial_test::serial;

const SCHEMA: &str = "type Note { title: String }";
const P2P_TIMEOUT: Duration = Duration::from_secs(15);
const DOC_COUNT: usize = 6;
const REBROADCAST_ENV: &str = "DEFRA_P2P_REBROADCAST_ON_MERGE";

struct EnvGuard(&'static str);

impl Drop for EnvGuard {
    fn drop(&mut self) {
        std::env::remove_var(self.0);
    }
}

async fn sync_status(cluster: &TestCluster, node: usize) -> serde_json::Value {
    reqwest::get(format!("{}/api/v0/p2p/sync/status", cluster.api_url(node)))
        .await
        .expect("sync status request")
        .json()
        .await
        .expect("sync status json")
}

fn note_count(client: &integration_test::DefraClient) -> usize {
    client
        .query("query { Note { title } }")
        .ok()
        .and_then(|r| r["Note"].as_array().map(Vec::len))
        .unwrap_or(0)
}

#[tokio::test]
#[serial]
async fn third_hop_converges_through_rebroadcasting_relay() {
    std::env::set_var(REBROADCAST_ENV, "true");
    let _guard = EnvGuard(REBROADCAST_ENV);

    let cluster = TestCluster::builder()
        .rust_nodes(3)
        .with_iroh_transport()
        .build()
        .await
        .expect("cluster start");
    for i in 0..3 {
        cluster
            .wait_for_log(i, "p2p_listening", P2P_TIMEOUT)
            .await
            .unwrap_or_else(|_| panic!("node{i} P2P listener"));
    }
    let node_a = cluster.client(0);
    let node_b = cluster.client(1);
    let node_c = cluster.client(2);
    for node in [&node_a, &node_b, &node_c] {
        node.schema_add(SCHEMA).expect("schema add");
        node.p2p_collection_add(&["Note"]).expect("subscribe");
    }

    // Chain: B dials A, C dials B. C never dials A.
    node_b
        .p2p_connect(&[&extract_p2p_addr(&cluster, 0)])
        .expect("connect B -> A");
    node_c
        .p2p_connect(&[&extract_p2p_addr(&cluster, 1)])
        .expect("connect C -> B");
    cluster
        .wait_for_log(0, "peer_connected", P2P_TIMEOUT)
        .await
        .expect("A never saw B connect");
    cluster
        .wait_for_log(2, "peer_connected", P2P_TIMEOUT)
        .await
        .expect("C never saw B connect");
    tokio::time::sleep(Duration::from_secs(3)).await;

    for i in 0..DOC_COUNT {
        node_a
            .query(&format!(
                r#"mutation {{ add_Note(input: {{title: "note-{i}"}}) {{ _docID }} }}"#
            ))
            .expect("create on A");
    }

    let node_b_ref = &node_b;
    poll_until(
        || note_count(node_b_ref) >= DOC_COUNT,
        Duration::from_secs(30),
        Duration::from_millis(300),
        "A's documents did not reach B over gossip",
    )
    .await;

    // The hop under test: C is only transport-connected to B.
    let node_c_ref = &node_c;
    poll_until(
        || note_count(node_c_ref) >= DOC_COUNT,
        Duration::from_secs(60),
        Duration::from_millis(500),
        "A's documents did not reach C through B's rebroadcast",
    )
    .await;

    let status = sync_status(&cluster, 2).await;
    assert_eq!(
        status["pending_dags"].as_u64(),
        Some(0),
        "C converged but still holds pending roots: {status}"
    );
    assert_eq!(
        status["pending_dag_fetch_deferred_unavailable"].as_u64(),
        Some(0),
        "C deferred a fetch for an unreachable provider on the way: {status}"
    );
}
