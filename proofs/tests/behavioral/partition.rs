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

async fn rust_storm_cluster(nodes: usize) -> TestCluster {
    TestCluster::builder()
        .rust_nodes(nodes)
        .with_p2p()
        .with_store("redb")
        .with_keyring()
        .with_rust_binary(support::release_binary())
        .build()
        .await
        .expect("build rust p2p cluster")
}

/// PCounter same-doc merge STORM. Three nodes each fire bursts of +1 at ONE doc,
/// concurrent across a full mesh; every node must converge to the EXACT running sum
/// each round (below = a delta dropped, above = one double-applied).
///
/// REGRESSION (#1021): the counter value lived in two stores (materialized blob +
/// CRDT accumulation store); local writes advanced only the blob while merges
/// reconciled the store FROM a concurrently-stale blob, dropping increments while
/// the commit DAG still converged (identical `_commits`, divergent value). Fixed by
/// the Go-parity single store (local writes + merges RMW the authoritative store by
/// delta; reconcile init-if-absent / PCounter migrate-via-max) serialized per-doc
/// (`crates/db/src/doc_write_queue.rs`). Modeled by `proofs/tla/TwoStoreCounter` +
/// `DefraConvergence.CounterReconcile`. Driver: `support::run_counter_storm` (the
/// cluster-agnostic harness shared with the Go-parity storm in `parity.rs`).
#[tokio::test]
async fn convergence_concurrent_same_doc_merge_storm() {
    let cluster = rust_storm_cluster(3).await;
    support::run_counter_storm(&cluster, "pcounter", "Int", &[1.0, 1.0, 1.0], 4, 4).await;
}

/// PNCounter: mixed signed deltas (two +1, one -1) under the storm — the signed
/// running sum must converge exactly, exercising the decrement path and the
/// PNCounter (strict init-if-absent) reconcile branch under concurrency.
#[tokio::test]
async fn convergence_concurrent_pncounter_same_doc_merge_storm() {
    let cluster = rust_storm_cluster(3).await;
    support::run_counter_storm(&cluster, "pncounter", "Int", &[1.0, 1.0, -1.0], 4, 4).await;
}

/// Float PCounter: exactly-representable +1.0 increments converge to the exact sum.
/// (Float counters are only order-independent for exactly-representable deltas; the
/// non-associativity of general f64 sums is a documented Go-parity limitation, not
/// asserted here.)
#[tokio::test]
async fn convergence_concurrent_float_counter_same_doc_merge_storm() {
    let cluster = rust_storm_cluster(3).await;
    support::run_counter_storm(&cluster, "pcounter", "Float", &[1.0, 1.0, 1.0], 4, 4).await;
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

/// THREE-node counter accumulation: each of three fully-meshed nodes increments
/// the same `hits=0` counter by 10, concurrently; all three must converge to 30.
///
/// REGRESSION: extends the two-node counter guard above to a topology where each
/// node's delta reaches the others via TWO distinct peers. A third node exposes
/// failures the symmetric two-node reconcile cannot: a cross-peer delta dropped
/// by applied-CID dedup, a missing-field-block fetch routed to a provider that
/// lacks it, or a local-increment-vs-concurrent-merge clobber on the creator.
/// node0 (the creator) under-counting while node1/node2 reach 30 is the
/// signature this guards against (the shape seen when #894's node-construction
/// rewrite re-split the blob vs accumulation stores). Convergence here requires
/// both the counter store reconcile and reliable cross-peer DAG delivery.
#[tokio::test]
async fn convergence_concurrent_counter_3node_full_mesh_sum() {
    let cluster = TestCluster::builder()
        .rust_nodes(3)
        .with_p2p()
        .with_store("redb")
        // Stable peer-ids so a peer-id churn can't confound the cross-peer fetch.
        .with_keyring()
        .with_rust_binary(support::release_binary())
        .build()
        .await
        .expect("build 3-node p2p cluster");

    let schema = "type Tally { name: String  hits: Int @crdt(type: pcounter) }";
    let addr: Vec<String> = (0..3).map(|n| node_addr(&cluster, n)).collect();
    for n in 0..3 {
        cluster.client(n).schema_add(schema).expect("schema");
        cluster
            .client(n)
            .p2p_collection_add(&["Tally"])
            .expect("subscribe");
    }
    // Full mesh: every node replicates to the other two.
    for i in 0..3 {
        for (j, peer_addr) in addr.iter().enumerate() {
            if i != j {
                cluster
                    .client(i)
                    .p2p_connect(&[peer_addr.as_str()])
                    .expect("connect");
                cluster
                    .client(i)
                    .p2p_replicator_set(&["Tally"], peer_addr)
                    .expect("replicator");
            }
        }
    }

    let created = cluster
        .client(0)
        .query(r#"mutation { add_Tally(input: {name: "t", hits: 0}) { _docID } }"#)
        .expect("create");
    let id = created["add_Tally"][0]["_docID"]
        .as_str()
        .expect("_docID")
        .to_string();

    // BARRIER: every node must hold the seed (hits==0) before any increment, so a
    // node can't build its increment on a pre-seed base — isolating a genuine
    // merge/dedup/delivery failure from seed-propagation timing.
    for n in 0..3 {
        assert!(
            poll_hits(&cluster.client(n), 0, Duration::from_secs(30)).await,
            "seed document must replicate to node{n} before the concurrent increments"
        );
    }

    // Concurrent increments: each node adds 10.
    for n in 0..3 {
        cluster
            .client(n)
            .query(&format!(
                r#"mutation {{ update_Tally(docID: "{id}", input: {{hits: 10}}) {{ _docID }} }}"#
            ))
            .expect("increment");
    }

    // CONVERGE: every replica accumulates ALL THREE increments → 30.
    for n in 0..3 {
        if !poll_hits(&cluster.client(n), 30, Duration::from_secs(40)).await {
            panic!(
                "node{n} did not converge to 30; hits = {}",
                tally_hits(&cluster.client(n))
            );
        }
    }
}

fn vault_secret(node: &DefraClient) -> String {
    node.query("query { Vault { secret } }").unwrap_or_default()["Vault"][0]["secret"]
        .as_str()
        .unwrap_or("<none>")
        .to_string()
}

async fn poll_vault_secret(node: &DefraClient, want: &str, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if vault_secret(node) == want {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// ENCRYPTED-field LWW convergence across a restart-partition. An encrypted field
/// routes its delta through the KMS write path (a random per-write key gossiped
/// over the encryption topic) rather than the plain block path. node1 is
/// restarted to sever the link, both nodes concurrently update the same encrypted
/// field, then reconnect; both must materialize the same plaintext LWW winner
/// ("zzz" > "aaa" on the equal-priority lexicographic tie-break).
///
/// The two replicas guard different halves, deliberately:
/// - node0 (locally wrote the LOSER "aaa") is the end-to-end leg: to reach "zzz"
///   it must RECEIVE node1's winning ciphertext AND obtain node1's gossiped
///   per-write key to decrypt it — the genuine cross-node merge + KMS key-delivery
///   path.
/// - node1 (received the seed by replication, then locally wrote the WINNER "zzz")
///   is the two-store bug-trigger: its local winner must survive the merge of
///   node0's delta rather than being clobbered by a re-materialization from a
///   stale priority store. A commit-DAG convergence gate forces that merge to
///   actually occur first, so this leg is not satisfied by node1's local write
///   alone.
///
/// REGRESSION: this is the encrypted twin of the LWW priority two-store bug. The
/// plaintext lives in the materialized blob; the LWW priority lives in the CRDT
/// store, advanced only by merges. A local update across a restart that strands
/// its value in the blob — or an encryption key that fails to reach the peer so
/// the winning ciphertext can't be decrypted — diverges here. Convergence
/// requires both the LWW store reconcile AND correct key delivery over the
/// restart.
#[tokio::test]
async fn convergence_encrypted_lww_restart_merge() {
    let mut cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .with_encryption()
        .with_store("redb")
        // Stable peer-id across the restart (so a peer-id change can't confound).
        .with_keyring()
        .with_rust_binary(support::release_binary())
        .build()
        .await
        .expect("build 2-node encrypted p2p cluster");

    let schema = "type Vault { name: String  secret: String }";
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
        .p2p_collection_add(&["Vault"])
        .expect("subscribe node0");
    cluster
        .client(1)
        .p2p_collection_add(&["Vault"])
        .expect("subscribe node1");
    cluster
        .client(0)
        .p2p_replicator_set(&["Vault"], &a1)
        .expect("replicator 0->1");
    cluster
        .client(1)
        .p2p_replicator_set(&["Vault"], &a0)
        .expect("replicator 1->0");

    let created = cluster
        .client(0)
        .query(
            r#"mutation { add_Vault(input: {name: "v", secret: "s0"}, encryptFields: [secret]) { _docID } }"#,
        )
        .expect("create encrypted");
    let id = created["add_Vault"][0]["_docID"]
        .as_str()
        .expect("_docID")
        .to_string();
    assert!(
        poll_vault_secret(&cluster.client(1), "s0", Duration::from_secs(20)).await,
        "encrypted seed must replicate and decrypt on node1 before the concurrent updates"
    );

    // Sever the link by restarting node1, then update concurrently.
    cluster
        .restart_node(1, Duration::from_secs(30))
        .await
        .expect("restart node1");

    cluster
        .client(0)
        .query(&format!(
            r#"mutation {{ update_Vault(docID: "{id}", input: {{secret: "aaa"}}) {{ _docID }} }}"#
        ))
        .expect("node0 update");
    cluster
        .client(1)
        .query(&format!(
            r#"mutation {{ update_Vault(docID: "{id}", input: {{secret: "zzz"}}) {{ _docID }} }}"#
        ))
        .expect("node1 update");

    // Reconnect after the restart-partition.
    let (a0b, a1b) = (node_addr(&cluster, 0), node_addr(&cluster, 1));
    cluster.client(0).p2p_connect(&[a1b.as_str()]).ok();
    cluster.client(1).p2p_connect(&[a0b.as_str()]).ok();
    cluster.client(0).p2p_collection_add(&["Vault"]).ok();
    cluster.client(1).p2p_collection_add(&["Vault"]).ok();
    cluster
        .client(0)
        .p2p_replicator_delete(&["Vault"], Some(&a1b))
        .ok();
    cluster
        .client(1)
        .p2p_replicator_delete(&["Vault"], Some(&a0b))
        .ok();
    cluster.client(0).p2p_replicator_set(&["Vault"], &a1b).ok();
    cluster.client(1).p2p_replicator_set(&["Vault"], &a0b).ok();

    // MERGE PROOF: both replicas must hold the IDENTICAL commit DAG. node1
    // locally wrote the winner ("zzz"), so its assertion below is satisfied by
    // its own write; it only guards the two-store clobber once node1 has actually
    // MERGED node0's delta. Equal commit sets prove the merge happened (the
    // ciphertext blocks crossed), so the winner-survives checks aren't inert.
    assert!(
        support::poll_dags_converged(
            &cluster.client(0),
            &cluster.client(1),
            &id,
            Duration::from_secs(40)
        )
        .await,
        "encrypted-LWW DAGs did not converge: a replica never merged the other's delta, so the winner-survives-merge check would be inert"
    );

    // CONVERGE: both replicas decrypt and materialize the LWW winner "zzz".
    if !poll_vault_secret(&cluster.client(0), "zzz", Duration::from_secs(40)).await {
        panic!(
            "node0 did not converge to \"zzz\"; secret = {:?}",
            vault_secret(&cluster.client(0))
        );
    }
    if !poll_vault_secret(&cluster.client(1), "zzz", Duration::from_secs(40)).await {
        panic!(
            "node1 did not converge to \"zzz\"; secret = {:?}",
            vault_secret(&cluster.client(1))
        );
    }
}

/// Index reconciles correctly iff the winner (99) is the only indexed value and
/// neither the loser (20) nor the stale seed (10) is still resolvable by index.
fn index_reconciled(node: &DefraClient) -> bool {
    support::indexed_age(node) == 99
        && support::count_by_index(node, 99) == 1
        && support::count_by_index(node, 20) == 0
        && support::count_by_index(node, 10) == 0
}

async fn poll_index_reconciled(node: &DefraClient, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if index_reconciled(node) {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// The seed (age=10) is present AND index-resolvable — the precondition before
/// the concurrent edits, so node1 cannot build its update on a pre-seed base.
async fn poll_index_reconciled_seed(node: &DefraClient, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if support::indexed_age(node) == 10 && support::count_by_index(node, 10) == 1 {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// SECONDARY-INDEX reconciliation across a restart-partition. An `@index`ed LWW
/// field is updated concurrently after node1 is restarted (node0 -> 20, node1 ->
/// 99); the higher value wins, and both replicas must end at age=99 resolving
/// ONLY 99 — and not the stale seed (10) or losing edit (20) — through the index.
///
/// The two replicas guard different halves, deliberately:
/// - node1 (received the seed by replication, then locally wrote the WINNER 99)
///   is the two-store bug-trigger: its local winner must SURVIVE the merge of
///   node0's delta rather than being clobbered by a re-materialization from a
///   stale priority store. A commit-DAG convergence gate forces that merge to
///   actually occur first, so this leg is not satisfied by node1's local write
///   alone (the loser value is dominated and otherwise invisible).
/// - node0 (locally wrote the LOSER 20) must MERGE node1's winning 99, flipping
///   its index from 20 to 99 — the genuine cross-node merge + index-follow leg.
///
/// REGRESSION: the index is a THIRD store layered on the two-store seam. A local
/// write advances the materialized blob and writes its own index entry, while the
/// LWW priority store advances only on merge. If a merge re-materializes the blob
/// from a stale priority store (the LWW two-store bug) the index can be left
/// pointing at an orphaned value — the document shows 99 but a filtered query
/// still returns the loser, or returns nothing. This guards that index
/// maintenance follows the reconciled merge, not the clobbered local write.
#[tokio::test]
async fn convergence_indexed_lww_restart_merge() {
    let mut cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .with_store("redb")
        // Stable peer-id across the restart (so a peer-id change can't confound).
        .with_keyring()
        .with_rust_binary(support::release_binary())
        .build()
        .await
        .expect("build 2-node p2p cluster");

    let schema = "type User { name: String  age: Int @index }";
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

    let created = cluster
        .client(0)
        .query(r#"mutation { add_User(input: {name: "Alice", age: 10}) { _docID } }"#)
        .expect("create");
    let id = created["add_User"][0]["_docID"]
        .as_str()
        .expect("_docID")
        .to_string();
    assert!(
        poll_index_reconciled_seed(&cluster.client(1), Duration::from_secs(20)).await,
        "seed document (age=10) must replicate and be index-resolvable on node1"
    );

    // Sever the link by restarting node1, then update concurrently.
    cluster
        .restart_node(1, Duration::from_secs(30))
        .await
        .expect("restart node1");

    cluster
        .client(0)
        .query(&format!(
            r#"mutation {{ update_User(docID: "{id}", input: {{age: 20}}) {{ _docID }} }}"#
        ))
        .expect("node0 update");
    cluster
        .client(1)
        .query(&format!(
            r#"mutation {{ update_User(docID: "{id}", input: {{age: 99}}) {{ _docID }} }}"#
        ))
        .expect("node1 update");

    // Reconnect after the restart-partition.
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
    cluster.client(0).p2p_replicator_set(&["User"], &a1b).ok();
    cluster.client(1).p2p_replicator_set(&["User"], &a0b).ok();

    // MERGE PROOF: both replicas must hold the IDENTICAL commit DAG. node1
    // locally wrote the winner (99), so its index-reconciled assertion below is
    // satisfied by its own write; it only genuinely guards the two-store clobber
    // once node1 has actually MERGED node0's delta. Equal commit sets prove that
    // merge happened (and that node0 received node1's), so the winner-survives
    // checks become meaningful rather than inert.
    assert!(
        support::poll_dags_converged(
            &cluster.client(0),
            &cluster.client(1),
            &id,
            Duration::from_secs(40)
        )
        .await,
        "indexed-LWW DAGs did not converge: a replica never merged the other's delta, so the winner-survives-merge check would be inert"
    );

    // CONVERGE: both replicas materialize 99 and resolve ONLY 99 by index.
    if !poll_index_reconciled(&cluster.client(0), Duration::from_secs(40)).await {
        panic!(
            "node0 index did not reconcile; age={} idx99={} idx20={} idx10={}",
            support::indexed_age(&cluster.client(0)),
            support::count_by_index(&cluster.client(0), 99),
            support::count_by_index(&cluster.client(0), 20),
            support::count_by_index(&cluster.client(0), 10),
        );
    }
    if !poll_index_reconciled(&cluster.client(1), Duration::from_secs(40)).await {
        panic!(
            "node1 index did not reconcile; age={} idx99={} idx20={} idx10={}",
            support::indexed_age(&cluster.client(1)),
            support::count_by_index(&cluster.client(1), 99),
            support::count_by_index(&cluster.client(1), 20),
            support::count_by_index(&cluster.client(1), 10),
        );
    }

    // HONESTY CHECK: confirm the filter actually plans an index scan, so the
    // counts above exercise index maintenance rather than a full collection scan.
    let index_used = cluster
        .client(0)
        .query("query @explain(type: simple) { User(filter: {age: {_eq: 99}}) { name } }")
        .map(|v| v.to_string().to_lowercase().contains("index"))
        .unwrap_or(false);
    assert!(
        index_used,
        "filtered query must plan an index scan, else the index counts prove nothing"
    );
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
