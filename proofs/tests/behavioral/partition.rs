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

fn tally_hits(node: &DefraClient) -> i64 {
    node.query("query { Tally { hits } }").expect("query Tally")["Tally"][0]["hits"]
        .as_i64()
        .unwrap_or(-1)
}

async fn poll_hits(node: &DefraClient, want: i64, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if tally_hits(node) == want {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn wire_bidirectional(cluster: &TestCluster, collection: &str) {
    let (a0, a1) = (node_addr(cluster, 0), node_addr(cluster, 1));
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
        .p2p_collection_add(&[collection])
        .expect("subscribe node0");
    cluster
        .client(1)
        .p2p_collection_add(&[collection])
        .expect("subscribe node1");
    cluster
        .client(0)
        .p2p_replicator_set(&[collection], &a1)
        .expect("replicator 0->1");
    cluster
        .client(1)
        .p2p_replicator_set(&[collection], &a0)
        .expect("replicator 1->0");
}

fn mixed_state(node: &DefraClient) -> (String, i64) {
    let r = node
        .query("query { Mixed { name views } }")
        .expect("query Mixed");
    r["Mixed"]
        .as_array()
        .and_then(|rows| rows.first())
        .map(|doc| {
            (
                doc["name"].as_str().unwrap_or("<none>").to_string(),
                doc["views"].as_i64().unwrap_or(-1),
            )
        })
        .unwrap_or_else(|| ("<missing>".to_string(), -1))
}

async fn poll_mixed_state(node: &DefraClient, name: &str, views: i64, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if mixed_state(node) == (name.to_string(), views) {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Concurrent PCounter increments must converge to the SUM. Two nodes share a
/// `hits=0` document over a live bidirectional connection; each increments by 45,
/// concurrently; both must converge to 90.
///
/// REGRESSION: the twin of the LWW priority bug above. A counter's value lives in
/// two places — the materialized document blob (advanced by BOTH local increments
/// and merges) and the CRDT accumulation store (`value_key`, advanced ONLY by
/// merges). A *local* increment updates only the blob; the node that received the
/// document by replication first already has an initialized accumulation store, so
/// a remote delta re-materializes the blob from that stale store and silently
/// drops the node's own increment — converging to 45 instead of 90. Fixed in
/// `crates/db-merge/src/merge_handler/counter.rs` by reconciling the store up to
/// the committed blob before every merge (`Counter::reconcile_int64`). Verified:
/// rust<->rust, go<->go, and rust<->go all converge to 90.
#[tokio::test]
async fn convergence_concurrent_counter_increments_sum() {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .with_store("redb")
        .with_rust_binary(support::release_binary())
        .build()
        .await
        .expect("build 2-node p2p cluster");

    let schema = "type Tally { name: String  hits: Int @crdt(type: pcounter) }";
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
        .p2p_collection_add(&["Tally"])
        .expect("subscribe node0");
    cluster
        .client(1)
        .p2p_collection_add(&["Tally"])
        .expect("subscribe node1");
    cluster
        .client(0)
        .p2p_replicator_set(&["Tally"], &a1)
        .expect("replicator 0->1");
    cluster
        .client(1)
        .p2p_replicator_set(&["Tally"], &a0)
        .expect("replicator 1->0");

    // Seed `hits=0` from node0; node1 receives the document by replication first
    // (the precondition that exposed the bug).
    let created = cluster
        .client(0)
        .query(r#"mutation { add_Tally(input: {name: "t", hits: 0}) { _docID } }"#)
        .expect("create");
    let id = created["add_Tally"][0]["_docID"]
        .as_str()
        .expect("_docID")
        .to_string();
    assert!(
        poll_hits(&cluster.client(1), 0, Duration::from_secs(20)).await,
        "seed document must replicate to node1 before the concurrent increments"
    );

    // Concurrent increments: each node adds 45.
    for n in [0usize, 1] {
        cluster
            .client(n)
            .query(&format!(
                r#"mutation {{ update_Tally(docID: "{id}", input: {{hits: 45}}) {{ _docID }} }}"#
            ))
            .expect("increment");
    }

    // CONVERGE: both replicas accumulate BOTH increments → 90.
    if !poll_hits(&cluster.client(0), 90, Duration::from_secs(30)).await {
        panic!(
            "node0 did not converge to 90; hits = {}",
            tally_hits(&cluster.client(0))
        );
    }
    if !poll_hits(&cluster.client(1), 90, Duration::from_secs(30)).await {
        panic!(
            "node1 did not converge to 90; hits = {}",
            tally_hits(&cluster.client(1))
        );
    }
}

/// Concurrent PNCounter updates must accumulate signed deltas, not just positive
/// increments. node0 contributes `+50` while node1 contributes `-30`; after
/// merging, both materialized documents must show `20`.
///
/// This extends the PCounter two-store regression guard above to the
/// decrement-capable counter variant. A merge path that clamps, drops, or
/// re-materializes from a stale accumulation store would converge to the wrong
/// value even if both replicas agree.
#[tokio::test]
async fn convergence_concurrent_pncounter_signed_deltas_sum() {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .with_store("redb")
        .with_rust_binary(support::release_binary())
        .build()
        .await
        .expect("build 2-node p2p cluster");

    let schema = "type Tally { name: String  hits: Int @crdt(type: pncounter) }";
    cluster.client(0).schema_add(schema).expect("schema node0");
    cluster.client(1).schema_add(schema).expect("schema node1");
    wire_bidirectional(&cluster, "Tally");

    let created = cluster
        .client(0)
        .query(r#"mutation { add_Tally(input: {name: "t", hits: 0}) { _docID } }"#)
        .expect("create");
    let id = created["add_Tally"][0]["_docID"]
        .as_str()
        .expect("_docID")
        .to_string();
    assert!(
        poll_hits(&cluster.client(1), 0, Duration::from_secs(20)).await,
        "seed document must replicate to node1 before the signed counter updates"
    );

    cluster
        .client(0)
        .query(&format!(
            r#"mutation {{ update_Tally(docID: "{id}", input: {{hits: 50}}) {{ _docID }} }}"#
        ))
        .expect("node0 increment");
    cluster
        .client(1)
        .query(&format!(
            r#"mutation {{ update_Tally(docID: "{id}", input: {{hits: -30}}) {{ _docID }} }}"#
        ))
        .expect("node1 decrement");

    if !poll_hits(&cluster.client(0), 20, Duration::from_secs(30)).await {
        panic!(
            "node0 did not converge to 20; hits = {}",
            tally_hits(&cluster.client(0))
        );
    }
    if !poll_hits(&cluster.client(1), 20, Duration::from_secs(30)).await {
        panic!(
            "node1 did not converge to 20; hits = {}",
            tally_hits(&cluster.client(1))
        );
    }
}

/// Mixed-field convergence: the same document carries an LWW field and a
/// counter field, and different nodes update different CRDT families
/// concurrently. Persisting one linked field must not re-materialize the
/// document from stale state and clobber the other field's local write.
#[tokio::test]
async fn convergence_concurrent_mixed_lww_and_counter_fields_merge() {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .with_store("redb")
        .with_rust_binary(support::release_binary())
        .build()
        .await
        .expect("build 2-node p2p cluster");

    let schema = "type Mixed { name: String  views: Int @crdt(type: pcounter) }";
    cluster.client(0).schema_add(schema).expect("schema node0");
    cluster.client(1).schema_add(schema).expect("schema node1");
    wire_bidirectional(&cluster, "Mixed");

    let created = cluster
        .client(0)
        .query(r#"mutation { add_Mixed(input: {name: "seed", views: 0}) { _docID } }"#)
        .expect("create");
    let id = created["add_Mixed"][0]["_docID"]
        .as_str()
        .expect("_docID")
        .to_string();
    assert!(
        poll_mixed_state(&cluster.client(1), "seed", 0, Duration::from_secs(20)).await,
        "seed document must replicate to node1 before mixed-field updates"
    );

    cluster
        .client(0)
        .query(&format!(
            r#"mutation {{ update_Mixed(docID: "{id}", input: {{name: "alice"}}) {{ _docID }} }}"#
        ))
        .expect("node0 name update");
    cluster
        .client(1)
        .query(&format!(
            r#"mutation {{ update_Mixed(docID: "{id}", input: {{views: 10}}) {{ _docID }} }}"#
        ))
        .expect("node1 views increment");

    if !poll_mixed_state(&cluster.client(0), "alice", 10, Duration::from_secs(30)).await {
        panic!(
            "node0 did not converge to name=alice views=10; state = {:?}",
            mixed_state(&cluster.client(0))
        );
    }
    if !poll_mixed_state(&cluster.client(1), "alice", 10, Duration::from_secs(30)).await {
        panic!(
            "node1 did not converge to name=alice views=10; state = {:?}",
            mixed_state(&cluster.client(1))
        );
    }
    assert_eq!(
        mixed_state(&cluster.client(0)),
        mixed_state(&cluster.client(1)),
        "replicas must materialize identical mixed-field state"
    );
}
