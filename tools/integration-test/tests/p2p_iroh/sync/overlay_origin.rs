//! Head hints that arrive over the iroh gossip overlay from a peer the
//! transport never dialed must stay advisory.
//!
//! Three nodes chained A -> B -> C: B dials A, C dials B, all subscribed to
//! the same collection. iroh-gossip keeps its own overlay and meshes C
//! straight to A, so A's head hints reach C carrying A as both propagation
//! hop and signed origin while C's transport only knows B. A hint accepted
//! as routable from such an origin would bind a durable fetch obligation to
//! A that the fetcher defers on every clock tick ("until a qualified
//! provider reconnects") — a pending root that can never resolve.
//!
//! The fence: once A's documents have provably reached B (so the gossip path
//! ran), C must hold no pending roots and must never have deferred a fetch
//! for lack of a reachable provider. This is deliberately not a data-delivery
//! test: with post-merge gossip rebroadcast off, C does not receive the
//! documents here, and that is expected.
//!
//! Run with:
//!   cargo test --test p2p_iroh -- sync::overlay_origin::

use std::time::Duration;

use integration_test::{extract_p2p_addr, poll_until, TestCluster};
use serial_test::serial;

const SCHEMA: &str = "type Note { title: String }";
const P2P_TIMEOUT: Duration = Duration::from_secs(15);
const DOC_COUNT: usize = 6;
/// Longer than several receiver-clock ticks (2 s base, doubling), so a
/// permanently deferred root would have been re-dispatched — and counted —
/// well inside it.
const OBSERVATION_WINDOW: Duration = Duration::from_secs(12);

struct Snapshot {
    pending_dags: u64,
    deferred_unavailable: u64,
}

async fn sync_status(cluster: &TestCluster, node: usize) -> Snapshot {
    let status: serde_json::Value =
        reqwest::get(format!("{}/api/v0/p2p/sync/status", cluster.api_url(node)))
            .await
            .expect("sync status request")
            .json()
            .await
            .expect("sync status json");
    Snapshot {
        pending_dags: status["pending_dags"]
            .as_u64()
            .expect("pending_dags field present"),
        deferred_unavailable: status["pending_dag_fetch_deferred_unavailable"]
            .as_u64()
            .expect("pending_dag_fetch_deferred_unavailable field present"),
    }
}

#[tokio::test]
#[serial]
async fn overlay_delivered_hint_from_unknown_origin_leaves_no_pending_root() {
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
    // Let the gossip overlay form beyond the transport links.
    tokio::time::sleep(Duration::from_secs(3)).await;

    for i in 0..DOC_COUNT {
        node_a
            .query(&format!(
                r#"mutation {{ add_Note(input: {{title: "note-{i}"}}) {{ _docID }} }}"#
            ))
            .expect("create on A");
    }

    // Anti-vacuity: the gossip path ran end to end at least as far as B.
    let node_b_ref = &node_b;
    poll_until(
        || {
            node_b_ref
                .query("query { Note { title } }")
                .ok()
                .and_then(|r| r["Note"].as_array().map(|n| n.len() >= DOC_COUNT))
                .unwrap_or(false)
        },
        Duration::from_secs(30),
        Duration::from_millis(300),
        "A's documents did not reach B over gossip",
    )
    .await;

    // Hold C under observation across several receiver-clock ticks.
    tokio::time::sleep(OBSERVATION_WINDOW).await;
    let status = sync_status(&cluster, 2).await;
    assert_eq!(
        status.deferred_unavailable, 0,
        "C deferred a pending-DAG fetch for lack of a reachable provider: a hint from a \
         peer the transport never dialed was accepted as routable"
    );
    assert_eq!(
        status.pending_dags, 0,
        "C is holding {} pending root(s) it can never resolve",
        status.pending_dags
    );
}
