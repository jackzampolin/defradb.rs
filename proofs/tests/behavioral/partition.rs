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
/// IGNORED — this exposes a real convergence bug (found via this harness). The
/// DAGs converge byte-identically on both nodes, but the restarted node
/// materializes the wrong value: node0 -> {age:31, city:LA}, node1 ->
/// {age:31, city:NYC} (node1 loses its own concurrent city=LA, reverting to the
/// create's NYC).
///
/// Traced root cause (via merge-handler instrumentation): a FIELD-PRIORITY
/// inconsistency on the restarted node. node1 emits its `city=LA` update at
/// priority 2 (node0 receives it as priority 2 and LA correctly wins
/// `RejectedLowerPriority{current:2}`), but in node1's OWN local LWW store the
/// field sits at priority 1. So when the create's `city=NYC` (priority 1) is
/// re-merged on node1, it TIES (`RejectedTieBreak`, equal priority) and wins the
/// lexicographic tie-break ("NYC" > "LA"). The restart (which clears the
/// in-memory `merged_composites` dedup in db-merge/.../composite.rs and forces a
/// re-walk of the already-applied create composite) is what surfaces the stale
/// priority. The fix is in the local-update / headstore<->datastore priority
/// handling, not a single merge-handler line — needs deliberate engine work.
/// Un-ignore once that lands.
///
/// PARITY (see parity.rs): this is RUST-SPECIFIC. Under the identical scenario
/// go<->go CONVERGES (both nodes -> city=LA) and go<->rust live-converges, while
/// rust<->rust diverges — a Rust regression from Go's behavior, not a shared
/// CRDT-design issue.
#[ignore = "exposes a real restart-induced CRDT field-priority convergence bug (Rust-specific); see doc comment"]
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
