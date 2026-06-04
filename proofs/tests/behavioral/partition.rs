//! DAG convergence under partition — two nodes each make an independent write
//! while partitioned (node1 is restarted to sever the link), and once
//! reconnected BOTH nodes converge to holding BOTH writes. This is the core
//! convergence guarantee — every node eventually receives every delta — across a
//! real partition in both directions. Distinct from the live-forward
//! (`replication.rs`) and one-way resume (`replicator_lifecycle.rs`) tests.
//! Model: `MC_Conv_Eventual` / `MC_Conv_RestartEviction` (proofs/tla).

use crate::support;
use defra_harness::{DefraClient, TestCluster};
use std::time::{Duration, Instant};

fn node_addr(cluster: &TestCluster, index: usize) -> String {
    let info = cluster.client(index).p2p_info().expect("p2p info");
    info.as_array()
        .and_then(|a| a.first())
        .and_then(|v| v.as_str())
        .expect("p2p address")
        .to_string()
}

fn names(node: &DefraClient) -> Vec<String> {
    let r = node.query("query { User { name } }").expect("query User");
    r["User"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|u| u["name"].as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

async fn poll_has_both(node: &DefraClient, a: &str, b: &str, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        let ns = names(node);
        if ns.iter().any(|n| n == a) && ns.iter().any(|n| n == b) {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn create(node: &DefraClient, name: &str) {
    node.query(&format!(
        r#"mutation {{ add_User(input: {{name: "{name}", age: 1}}) {{ _docID }} }}"#
    ))
    .expect("create user");
}

fn user_doc(node: &DefraClient) -> serde_json::Value {
    let r = node
        .query("query { User { _docID name age city } }")
        .expect("query User");
    r["User"][0].clone()
}

async fn poll_field_state(node: &DefraClient, age: i64, city: &str, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        let d = user_doc(node);
        if d["age"] == age && d["city"] == city {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test]
async fn convergence_partition_both_directions_merge() {
    let mut cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        // Disk store so each node's state survives the restart that creates the
        // partition.
        .with_store("redb")
        .with_rust_binary(support::release_binary())
        .build()
        .await
        .expect("build 2-node p2p cluster");

    let schema = "type User { name: String  age: Int }";
    cluster.client(0).schema_add(schema).expect("schema node0");
    cluster.client(1).schema_add(schema).expect("schema node1");

    // Bidirectional replication so each node's writes reach the other.
    let (a0, a1) = (node_addr(&cluster, 0), node_addr(&cluster, 1));
    cluster
        .client(0)
        .p2p_connect(&[a1.as_str()])
        .expect("connect 0->1");
    cluster
        .client(1)
        .p2p_connect(&[a0.as_str()])
        .expect("connect 1->0");
    cluster
        .client(0)
        .p2p_collection_add(&["User"])
        .expect("subscribe node0");
    cluster
        .client(1)
        .p2p_collection_add(&["User"])
        .expect("subscribe node1");
    cluster
        .client(0)
        .p2p_replicator_set(&["User"], &a1)
        .expect("replicator 0->1");
    cluster
        .client(1)
        .p2p_replicator_set(&["User"], &a0)
        .expect("replicator 1->0");

    // Baseline: a write on node0 reaches node1 (sanity that replication is live).
    create(&cluster.client(0), "Seed");
    assert!(
        poll_has_both(&cluster.client(1), "Seed", "Seed", Duration::from_secs(20)).await,
        "seed write must replicate to node1 before the partition"
    );

    // PARTITION: restart node1 to sever the connection.
    cluster
        .restart_node(1, Duration::from_secs(30))
        .await
        .expect("restart node1");

    // Independent writes on each side while partitioned — different documents,
    // so this exercises delta delivery (DAG completeness), not same-doc merge.
    create(&cluster.client(0), "FromNode0");
    create(&cluster.client(1), "FromNode1");

    // HEAL: reconnect and re-establish replication both ways. Delete first so the
    // re-set re-enumerates and backfills the writes made during the partition.
    let (a0b, a1b) = (node_addr(&cluster, 0), node_addr(&cluster, 1));
    cluster.client(0).p2p_connect(&[a1b.as_str()]).ok();
    cluster.client(1).p2p_connect(&[a0b.as_str()]).ok();
    cluster.client(0).p2p_collection_add(&["User"]).ok();
    cluster.client(1).p2p_collection_add(&["User"]).ok();
    cluster
        .client(0)
        .p2p_replicator_delete(&["User"], Some(&a1b))
        .ok();
    cluster
        .client(1)
        .p2p_replicator_delete(&["User"], Some(&a0b))
        .ok();
    cluster
        .client(0)
        .p2p_replicator_set(&["User"], &a1b)
        .expect("re-replicator 0->1");
    cluster
        .client(1)
        .p2p_replicator_set(&["User"], &a0b)
        .expect("re-replicator 1->0");

    // CONVERGE: both nodes hold BOTH partition writes (every node receives every
    // delta once connectivity is restored).
    if !poll_has_both(
        &cluster.client(0),
        "FromNode0",
        "FromNode1",
        Duration::from_secs(30),
    )
    .await
    {
        panic!(
            "node0 did not converge; node0 names = {:?}",
            names(&cluster.client(0))
        );
    }
    if !poll_has_both(
        &cluster.client(1),
        "FromNode0",
        "FromNode1",
        Duration::from_secs(30),
    )
    .await
    {
        panic!(
            "node1 did not converge; node1 names = {:?}",
            names(&cluster.client(1))
        );
    }
}

/// Convergence under CONCURRENT edits to the SAME document — the strongest form:
/// while partitioned, node0 updates one field and node1 updates another on the
/// same document, and after reconnect both replicas must materialize the SAME
/// merged state (both edits present) — order-independent merge (commutativity),
/// the exact property the Lean LWW proofs assert.
///
/// Regression guard for a real convergence bug this harness FOUND AND FIXED. The
/// DAGs converged byte-identically, but the restarted node materialized the
/// wrong value (node0 -> city=LA, node1 -> city=NYC) — a permanent divergence
/// that all unit tests missed, and that go<->go does NOT exhibit (it was a Rust
/// regression from Go; see parity.rs).
///
/// Root cause: a field's priority lives in two stores — the headstore (advanced
/// by BOTH local writes and merges) and the datastore LWW priority (advanced
/// ONLY by merges). A local write (node1's `city=LA`) pushes the headstore to
/// priority 2 but leaves the datastore LWW stale at the replicated seed's
/// priority 1; a restart then clears the in-memory `merged_composites` dedup and
/// re-walks the create composite, whose `city=NYC` (priority 1) ties the stale
/// datastore entry and wins the lexicographic tie-break. Fixed in
/// `crates/db-merge/src/merge_handler/lww.rs` `seed_lww_from_existing_doc`: it
/// now re-seeds the datastore LWW from the authoritative materialized doc + head
/// priority whenever the headstore is ahead, so the merge resolves against the
/// true current state. Verified: rust<->rust, go<->go, and go<->rust all converge.
#[tokio::test]
async fn convergence_concurrent_same_doc_writes_merge() {
    let mut cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .with_store("redb")
        .with_rust_binary(support::release_binary())
        .build()
        .await
        .expect("build 2-node p2p cluster");

    let schema = "type User { name: String  age: Int  city: String }";
    cluster.client(0).schema_add(schema).expect("schema node0");
    cluster.client(1).schema_add(schema).expect("schema node1");

    let (a0, a1) = (node_addr(&cluster, 0), node_addr(&cluster, 1));
    cluster
        .client(0)
        .p2p_connect(&[a1.as_str()])
        .expect("connect 0->1");
    cluster
        .client(1)
        .p2p_connect(&[a0.as_str()])
        .expect("connect 1->0");
    cluster
        .client(0)
        .p2p_collection_add(&["User"])
        .expect("subscribe node0");
    cluster
        .client(1)
        .p2p_collection_add(&["User"])
        .expect("subscribe node1");
    cluster
        .client(0)
        .p2p_replicator_set(&["User"], &a1)
        .expect("replicator 0->1");
    cluster
        .client(1)
        .p2p_replicator_set(&["User"], &a0)
        .expect("replicator 1->0");

    // Seed one document and converge it to both nodes.
    let created = cluster
        .client(0)
        .query(r#"mutation { add_User(input: {name: "Alice", age: 30, city: "NYC"}) { _docID } }"#)
        .expect("create");
    let id = created["add_User"][0]["_docID"]
        .as_str()
        .expect("_docID")
        .to_string();
    assert!(
        poll_field_state(&cluster.client(1), 30, "NYC", Duration::from_secs(20)).await,
        "seed document must replicate to node1 before the partition"
    );

    // PARTITION.
    cluster
        .restart_node(1, Duration::from_secs(30))
        .await
        .expect("restart node1");

    // Concurrent edits to the SAME doc, different fields: node0 -> age, node1 -> city.
    cluster
        .client(0)
        .query(&format!(
            r#"mutation {{ update_User(docID: "{id}", input: {{age: 31}}) {{ _docID }} }}"#
        ))
        .expect("node0 updates age");
    cluster
        .client(1)
        .query(&format!(
            r#"mutation {{ update_User(docID: "{id}", input: {{city: "LA"}}) {{ _docID }} }}"#
        ))
        .expect("node1 updates city");

    // HEAL.
    let (a0b, a1b) = (node_addr(&cluster, 0), node_addr(&cluster, 1));
    cluster.client(0).p2p_connect(&[a1b.as_str()]).ok();
    cluster.client(1).p2p_connect(&[a0b.as_str()]).ok();
    cluster.client(0).p2p_collection_add(&["User"]).ok();
    cluster.client(1).p2p_collection_add(&["User"]).ok();
    cluster
        .client(0)
        .p2p_replicator_delete(&["User"], Some(&a1b))
        .ok();
    cluster
        .client(1)
        .p2p_replicator_delete(&["User"], Some(&a0b))
        .ok();
    cluster
        .client(0)
        .p2p_replicator_set(&["User"], &a1b)
        .expect("re-replicator 0->1");
    cluster
        .client(1)
        .p2p_replicator_set(&["User"], &a0b)
        .expect("re-replicator 1->0");

    // CONVERGE: both replicas materialize the SAME merged state — both concurrent
    // edits present (age=31 AND city=LA), order-independently.
    if !poll_field_state(&cluster.client(0), 31, "LA", Duration::from_secs(30)).await {
        panic!(
            "node0 did not converge; node0 = {}",
            user_doc(&cluster.client(0))
        );
    }
    if !poll_field_state(&cluster.client(1), 31, "LA", Duration::from_secs(30)).await {
        panic!(
            "node1 did not converge; node1 = {}",
            user_doc(&cluster.client(1))
        );
    }
    assert_eq!(
        user_doc(&cluster.client(0)),
        user_doc(&cluster.client(1)),
        "replicas must materialize identical document state after merging concurrent edits"
    );
}
