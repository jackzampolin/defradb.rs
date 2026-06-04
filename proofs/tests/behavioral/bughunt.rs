//! BUG-HUNT (ignored, reporting) — convergence/concurrency probes against the
//! real binary, on the surface where the LWW priority-reconcile bug lived. These
//! report each node's converged state (not assert) so divergences are visible.
//!
//! Run: cargo test -p conformance --test tla_conformance bughunt:: \
//!        -- --ignored --test-threads=1 --nocapture

use crate::support;
use defra_harness::{DefraClient, TestCluster};
use std::time::{Duration, Instant};

fn node_addr(cluster: &TestCluster, i: usize) -> String {
    let info = cluster.client(i).p2p_info().expect("p2p info");
    info.as_array()
        .and_then(|a| a.first())
        .and_then(|v| v.as_str())
        .expect("p2p address")
        .to_string()
}

fn hits(node: &DefraClient) -> i64 {
    node.query("query { Tally { hits } }").expect("query Tally")["Tally"][0]["hits"]
        .as_i64()
        .unwrap_or(-1)
}

async fn wire(cluster: &TestCluster) {
    let (a0, a1) = (node_addr(cluster, 0), node_addr(cluster, 1));
    cluster.client(0).p2p_connect(&[a1.as_str()]).ok();
    cluster.client(1).p2p_connect(&[a0.as_str()]).ok();
    cluster.client(0).p2p_collection_add(&["Tally"]).ok();
    cluster.client(1).p2p_collection_add(&["Tally"]).ok();
    cluster.client(0).p2p_replicator_set(&["Tally"], &a1).ok();
    cluster.client(1).p2p_replicator_set(&["Tally"], &a0).ok();
}

/// Concurrent PCounter increments (node0 +45, node1 +45) — optionally across a
/// restart-partition — must converge to the SUM (90) on both nodes. Counters are
/// non-idempotent (applied-CID dedup), so a re-merge that drops or double-counts
/// an increment is a serious, visible bug.
async fn run_counter(label: &str, restart: Option<usize>) {
    let mut cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .with_store("redb")
        // Stable peer-id across restart (so a peer-id change can't confound).
        .with_keyring()
        .with_rust_binary(support::release_binary())
        .build()
        .await
        .expect("2-node cluster");

    let schema = "type Tally { name: String  hits: Int @crdt(type: pcounter) }";
    cluster.client(0).schema_add(schema).expect("schema node0");
    cluster.client(1).schema_add(schema).expect("schema node1");
    wire(&cluster).await;

    let created = cluster
        .client(0)
        .query(r#"mutation { add_Tally(input: {name: "t", hits: 0}) { _docID } }"#)
        .expect("create");
    let id = created["add_Tally"][0]["_docID"]
        .as_str()
        .expect("_docID")
        .to_string();
    tokio::time::sleep(Duration::from_secs(3)).await; // converge seed

    if let Some(idx) = restart {
        cluster
            .restart_node(idx, Duration::from_secs(30))
            .await
            .expect("restart node");
    }

    let inc = |node: &DefraClient| {
        node.query(&format!(
            r#"mutation {{ update_Tally(docID: "{id}", input: {{hits: 45}}) {{ _docID }} }}"#
        ))
        .expect("increment");
    };
    inc(&cluster.client(0));
    inc(&cluster.client(1));

    if restart.is_some() {
        let (a0b, a1b) = (node_addr(&cluster, 0), node_addr(&cluster, 1));
        cluster.client(0).p2p_connect(&[a1b.as_str()]).ok();
        cluster.client(1).p2p_connect(&[a0b.as_str()]).ok();
        cluster.client(0).p2p_collection_add(&["Tally"]).ok();
        cluster.client(1).p2p_collection_add(&["Tally"]).ok();
        cluster
            .client(0)
            .p2p_replicator_delete(&["Tally"], Some(&a1b))
            .ok();
        cluster
            .client(1)
            .p2p_replicator_delete(&["Tally"], Some(&a0b))
            .ok();
        cluster.client(0).p2p_replicator_set(&["Tally"], &a1b).ok();
        cluster.client(1).p2p_replicator_set(&["Tally"], &a0b).ok();
    }

    // Poll for convergence up to a deadline (counters merge by accumulating
    // unique deltas, so give it time).
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let (h0, h1) = (hits(&cluster.client(0)), hits(&cluster.client(1)));
        if (h0 == h1 && h0 == 90) || Instant::now() >= deadline {
            let converged = h0 == h1 && h0 == 90;
            eprintln!(
                "BUGHUNT[{label}] node0.hits={h0} node1.hits={h1} | expected 90 | CONVERGED={converged}"
            );
            return;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

#[ignore = "bug-hunt probe; run with --ignored --nocapture"]
#[tokio::test]
async fn bughunt_counter_live() {
    run_counter("counter_live", None).await;
}

#[ignore = "bug-hunt probe; run with --ignored --nocapture"]
#[tokio::test]
async fn bughunt_counter_restart() {
    run_counter("counter_restart(node1)", Some(1)).await;
}
