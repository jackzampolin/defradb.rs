use std::time::Duration;

use integration_test::{for_each_p2p_topology, for_each_p2p_topology_3, poll_until, TestCluster};

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Set up a full P2P replication link from `source_idx` to `target_idx` within
/// `cluster`, sharing the given `collections`.
///
/// Returns the multiaddr of the target node.
async fn setup_replication_link(
    cluster: &TestCluster,
    source_idx: usize,
    target_idx: usize,
    collections: &[&str],
) -> String {
    let source = cluster.client(source_idx);
    let target = cluster.client(target_idx);

    let timeout = Duration::from_secs(15);
    cluster
        .wait_for_log(source_idx, "p2p_listening", timeout)
        .await
        .unwrap_or_else(|_| panic!("node{} P2P listener did not start", source_idx));
    cluster
        .wait_for_log(target_idx, "p2p_listening", timeout)
        .await
        .unwrap_or_else(|_| panic!("node{} P2P listener did not start", target_idx));

    let info = target.p2p_info().expect("p2p_info");
    let addr = info
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|v| v.as_str())
        .expect("target has no P2P address")
        .to_string();

    source.p2p_connect(&[&addr]).unwrap();
    source.p2p_collection_add(collections).unwrap();
    target.p2p_collection_add(collections).unwrap();
    source.p2p_replicator_set(collections, &addr).unwrap();

    addr
}

/// Count documents of `collection` on the node at `idx`.
fn count_docs(cluster: &TestCluster, idx: usize, collection: &str) -> usize {
    let client = cluster.client(idx);
    let query = format!("query {{ {} {{ _docID }} }}", collection);
    client
        .query(&query)
        .ok()
        .and_then(|v| v[collection].as_array().map(|a| a.len()))
        .unwrap_or(0)
}

// ─── Test 1: car_bomb_protection ─────────────────────────────────────────────

/// Verifies the CAR sync pipeline survives a burst of document writes without
/// crashing either node and without losing any documents.
async fn car_bomb_protection_test(cluster: TestCluster) {
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    // Deploy schema
    node0
        .schema_add("type Packet { payload: String  seq: Int }")
        .expect("schema node0");
    node1
        .schema_add("type Packet { payload: String  seq: Int }")
        .expect("schema node1");

    setup_replication_link(&cluster, 0, 1, &["Packet"]).await;

    // Baseline: one doc replicates correctly before the burst
    let first = node0
        .query(r#"mutation { create_Packet(input: {payload: "baseline", seq: 0}) { _docID } }"#)
        .expect("create baseline");
    let baseline_id = first["create_Packet"][0]["_docID"]
        .as_str()
        .expect("baseline _docID")
        .to_string();

    poll_until(
        || count_docs(&cluster, 1, "Packet") >= 1,
        Duration::from_secs(15),
        Duration::from_millis(200),
        "baseline doc did not replicate to node1",
    )
    .await;

    let check = node1
        .query("query { Packet { _docID } }")
        .expect("node1 query");
    assert!(
        check["Packet"]
            .as_array()
            .unwrap()
            .iter()
            .any(|d| d["_docID"].as_str() == Some(&baseline_id)),
        "baseline doc ID mismatch on node1"
    );

    // Burst: create 50 documents rapidly on node0 to stress the CAR pipeline
    const BURST_SIZE: usize = 50;
    for i in 0..BURST_SIZE {
        node0
            .query(&format!(
                r#"mutation {{ create_Packet(input: {{payload: "burst-{i}", seq: {i}}}) {{ _docID }} }}"#,
            ))
            .unwrap_or_else(|_| panic!("burst create {}", i));
    }

    // All burst docs must arrive on node1 — neither node may crash
    poll_until(
        || count_docs(&cluster, 1, "Packet") >= BURST_SIZE + 1,
        Duration::from_secs(60),
        Duration::from_millis(500),
        "burst docs did not all replicate to node1",
    )
    .await;

    // Both nodes remain healthy: queries return valid responses
    let n0_count = count_docs(&cluster, 0, "Packet");
    let n1_count = count_docs(&cluster, 1, "Packet");
    assert_eq!(
        n0_count,
        BURST_SIZE + 1,
        "node0 should have {} docs, has {}",
        BURST_SIZE + 1,
        n0_count
    );
    assert_eq!(
        n1_count,
        BURST_SIZE + 1,
        "node1 should have {} docs after replication, has {}",
        BURST_SIZE + 1,
        n1_count
    );

    // Post-burst: a fresh document replicates normally
    let after = node0
        .query(
            r#"mutation { create_Packet(input: {payload: "after-burst", seq: 9999}) { _docID } }"#,
        )
        .expect("post-burst create");
    let after_id = after["create_Packet"][0]["_docID"]
        .as_str()
        .expect("post-burst _docID")
        .to_string();

    poll_until(
        || {
            cluster
                .client(1)
                .query("query { Packet { _docID } }")
                .ok()
                .and_then(|v| {
                    v["Packet"]
                        .as_array()
                        .map(|a| a.iter().any(|d| d["_docID"].as_str() == Some(&after_id)))
                })
                .unwrap_or(false)
        },
        Duration::from_secs(30),
        Duration::from_millis(200),
        "post-burst doc did not replicate to node1",
    )
    .await;
}

for_each_p2p_topology!(car_bomb_protection, car_bomb_protection_test, .with_p2p());

// ─── Test 2: rate_limiter_saturation ─────────────────────────────────────────

/// Verifies that per-peer rate limiting on node0 does not block a legitimate
/// peer (node2) while node1 is flooding node0 with documents.
///
/// Topology: node1 → node0 ← node2
/// node0 is the target; node1 is the spammer; node2 is the bystander.
async fn rate_limiter_saturation_test(cluster: TestCluster) {
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);
    let node2 = cluster.client(2);

    let schema = "type Event { label: String  src: Int }";
    node0.schema_add(schema).expect("schema node0");
    node1.schema_add(schema).expect("schema node1");
    node2.schema_add(schema).expect("schema node2");

    // node1 → node0: spammer replicates into victim
    setup_replication_link(&cluster, 1, 0, &["Event"]).await;
    // node2 → node0: bystander replicates into victim
    setup_replication_link(&cluster, 2, 0, &["Event"]).await;

    // Baseline: one doc from node1 lands on node0
    node1
        .query(r#"mutation { create_Event(input: {label: "baseline", src: 1}) { _docID } }"#)
        .expect("baseline from node1");

    poll_until(
        || count_docs(&cluster, 0, "Event") >= 1,
        Duration::from_secs(15),
        Duration::from_millis(200),
        "baseline doc did not reach node0",
    )
    .await;

    // Spam: 50 documents from node1 to saturate node0's rate limiter
    const SPAM_COUNT: usize = 50;
    for i in 0..SPAM_COUNT {
        node1
            .query(&format!(
                r#"mutation {{ create_Event(input: {{label: "spam-{i}", src: 1}}) {{ _docID }} }}"#,
            ))
            .unwrap_or_else(|_| panic!("spam create {}", i));
    }

    // Immediately create node2's doc (the bystander)
    let bystander = node2
        .query(r#"mutation { create_Event(input: {label: "from-node2", src: 2}) { _docID } }"#)
        .expect("bystander doc");
    let bystander_id = bystander["create_Event"][0]["_docID"]
        .as_str()
        .expect("bystander _docID")
        .to_string();

    // Node0 must receive node2's doc within a bounded window even while node1
    // is flooding it — per-peer rate limiting must not block innocent peers.
    let bystander_id_clone = bystander_id.clone();
    poll_until(
        || {
            cluster
                .client(0)
                .query("query { Event { _docID } }")
                .ok()
                .and_then(|v| {
                    v["Event"].as_array().map(|a| {
                        a.iter()
                            .any(|d| d["_docID"].as_str() == Some(&bystander_id_clone))
                    })
                })
                .unwrap_or(false)
        },
        Duration::from_secs(30),
        Duration::from_millis(200),
        "node2's doc did not reach node0 — rate limiter may have blocked innocent peer",
    )
    .await;

    // All of node1's spam must also eventually arrive on node0
    poll_until(
        || count_docs(&cluster, 0, "Event") >= SPAM_COUNT + 2,
        Duration::from_secs(90),
        Duration::from_millis(500),
        "not all spam docs reached node0",
    )
    .await;

    // All three nodes remain healthy
    assert!(
        node0.query("query { Event { _docID } }").is_ok(),
        "node0 is unhealthy after rate limiter stress"
    );
    assert!(
        node1.query("query { Event { _docID } }").is_ok(),
        "node1 is unhealthy after burst"
    );
    assert!(
        node2.query("query { Event { _docID } }").is_ok(),
        "node2 is unhealthy"
    );
}

for_each_p2p_topology_3!(rate_limiter_saturation, rate_limiter_saturation_test, .with_p2p());

// ─── Test 3: dag_semaphore_exhaustion ────────────────────────────────────────

/// Verifies that flooding the DAG fetch pipeline from one peer (node1) does not
/// permanently exhaust the semaphore on node0, allowing a legitimate peer
/// (node2) to still get its document fetched promptly.
///
/// Topology: node1 → node0 ← node2
/// 16+ concurrent DAG fetches from node1 must not starve node2.
async fn dag_semaphore_exhaustion_test(cluster: TestCluster) {
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);
    let node2 = cluster.client(2);

    let schema = "type Block { data: String  idx: Int }";
    node0.schema_add(schema).expect("schema node0");
    node1.schema_add(schema).expect("schema node1");
    node2.schema_add(schema).expect("schema node2");

    // node1 → node0: flooder pushes into target
    setup_replication_link(&cluster, 1, 0, &["Block"]).await;
    // node2 → node0: legitimate peer pushes into target
    setup_replication_link(&cluster, 2, 0, &["Block"]).await;

    // Create 20 documents on node1 rapidly — exceeds MAX_CONCURRENT_DAG_FETCHES=16
    // to ensure the semaphore is fully occupied when node2's doc arrives.
    const FLOOD_COUNT: usize = 20;
    for i in 0..FLOOD_COUNT {
        node1
            .query(&format!(
                r#"mutation {{ create_Block(input: {{data: "flood-{i}", idx: {i}}}) {{ _docID }} }}"#,
            ))
            .unwrap_or_else(|_| panic!("flood create {}", i));
    }

    // Immediately create node2's legitimate document
    let legit = node2
        .query(r#"mutation { create_Block(input: {data: "legit", idx: 9999}) { _docID } }"#)
        .expect("legit doc");
    let legit_id = legit["create_Block"][0]["_docID"]
        .as_str()
        .expect("legit _docID")
        .to_string();

    // Node0 must receive node2's doc within 20 seconds — the semaphore must not
    // be permanently exhausted by node1's flood.
    let legit_id_clone = legit_id.clone();
    poll_until(
        || {
            cluster
                .client(0)
                .query("query { Block { _docID } }")
                .ok()
                .and_then(|v| {
                    v["Block"].as_array().map(|a| {
                        a.iter()
                            .any(|d| d["_docID"].as_str() == Some(&legit_id_clone))
                    })
                })
                .unwrap_or(false)
        },
        Duration::from_secs(20),
        Duration::from_millis(200),
        "node2's doc did not reach node0 — DAG semaphore may be exhausted",
    )
    .await;

    // All flood docs must also eventually arrive on node0
    poll_until(
        || count_docs(&cluster, 0, "Block") >= FLOOD_COUNT + 1,
        Duration::from_secs(60),
        Duration::from_millis(500),
        "not all flood docs reached node0",
    )
    .await;

    // Node0 remains healthy — query returns a valid response
    let health = node0
        .query("query { Block { _docID data idx } }")
        .expect("node0 health check query");
    assert!(
        health["Block"].as_array().is_some(),
        "node0 Block query did not return an array — node may be unhealthy"
    );
}

for_each_p2p_topology_3!(dag_semaphore_exhaustion, dag_semaphore_exhaustion_test, .with_p2p());
