use std::time::{Duration, Instant};

use integration_test::{for_each_p2p_topology, poll_until, DefraClient, TestCluster};

async fn write_with_conflict_retry(client: &DefraClient, query: &str, label: &str) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match client.query(query) {
            Ok(_) => return,
            Err(error) => {
                let message = format!("{error:#}");
                assert!(
                    message.contains("transaction conflict. Please retry"),
                    "{label}: {message}"
                );
                assert!(
                    Instant::now() < deadline,
                    "{label} remained conflicted until the retry deadline: {message}"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

/// Set up a full P2P replication link between two nodes sharing given collections.
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

fn count_docs(cluster: &TestCluster, idx: usize, collection: &str) -> usize {
    let client = cluster.client(idx);
    let query = format!("query {{ {} {{ _docID }} }}", collection);
    client
        .query(&query)
        .ok()
        .and_then(|v| v[collection].as_array().map(|a| a.len()))
        .unwrap_or(0)
}

fn query_record_docs(cluster: &TestCluster, idx: usize) -> Result<Vec<serde_json::Value>, String> {
    let response = cluster
        .client(idx)
        .query("query { Record { _docID origin idx } }")
        .map_err(|error| format!("{error:#}"))?;
    let mut docs = response["Record"]
        .as_array()
        .cloned()
        .ok_or_else(|| format!("unexpected query response: {response}"))?;
    docs.sort_by(|left, right| left["_docID"].as_str().cmp(&right["_docID"].as_str()));
    Ok(docs)
}

// ─── Test 1: write_after_replication ─────────────────────────────────────────

/// Reproduces the P2P write contention deadlock pattern:
///   1. Node A creates a document
///   2. P2P replicates it to node B
///   3. Node B creates a new document while the merge is processing
///   4. B's document replicates back to A
///
/// Before the fix, step 3 could deadlock: A's broadcast held the write lock
/// while the merge handler on A also needed it for B's incoming document,
/// causing a 30s timeout.
async fn write_after_replication_test(cluster: TestCluster) {
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    node0
        .schema_add("type Metric { label: String  value: Int }")
        .expect("schema node0");
    node1
        .schema_add("type Metric { label: String  value: Int }")
        .expect("schema node1");

    // Bidirectional replication
    setup_replication_link(&cluster, 0, 1, &["Metric"]).await;

    let info0 = node0.p2p_info().expect("p2p_info node0");
    let addr0 = info0
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|v| v.as_str())
        .expect("node0 has no P2P address");
    node1
        .p2p_replicator_set(&["Metric"], addr0)
        .expect("replicator 1->0");

    // Step 1: A creates a document
    let data_a = node0
        .query(r#"mutation { add_Metric(input: {label: "from-a", value: 10}) { _docID } }"#)
        .expect("create on node0");
    let doc_id_a = data_a["add_Metric"][0]["_docID"]
        .as_str()
        .expect("missing _docID from node0")
        .to_string();

    // Step 2: Wait for A's doc to replicate to B
    poll_until(
        || {
            let result = cluster
                .client(1)
                .query("query { Metric { _docID } }")
                .unwrap();
            result["Metric"]
                .as_array()
                .map(|arr| arr.iter().any(|d| d["_docID"].as_str() == Some(&doc_id_a)))
                .unwrap_or(false)
        },
        Duration::from_secs(15),
        Duration::from_millis(200),
        "A's document did not replicate to B",
    )
    .await;

    // Step 3: B creates a response document (the critical contention point)
    let data_b = node1
        .query(r#"mutation { add_Metric(input: {label: "from-b", value: 20}) { _docID } }"#)
        .expect("create on node1 — would deadlock before fix");
    let doc_id_b = data_b["add_Metric"][0]["_docID"]
        .as_str()
        .expect("missing _docID from node1")
        .to_string();

    // Step 4: B's document replicates back to A
    poll_until(
        || {
            let result = cluster
                .client(0)
                .query("query { Metric { _docID } }")
                .unwrap();
            result["Metric"]
                .as_array()
                .map(|arr| arr.iter().any(|d| d["_docID"].as_str() == Some(&doc_id_b)))
                .unwrap_or(false)
        },
        Duration::from_secs(15),
        Duration::from_millis(200),
        "B's document did not replicate back to A",
    )
    .await;

    // Both nodes see both documents
    assert_eq!(count_docs(&cluster, 0, "Metric"), 2);
    assert_eq!(count_docs(&cluster, 1, "Metric"), 2);
}

for_each_p2p_topology!(write_after_replication, write_after_replication_test, .with_p2p());

// ─── Test 2: simultaneous_writes ─────────────────────────────────────────────

/// Both nodes write simultaneously to the same collection, then both documents
/// must replicate to both nodes. This exercises concurrent merge + broadcast.
async fn simultaneous_writes_test(cluster: TestCluster) {
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    node0
        .schema_add("type Event { src: String  seq: Int }")
        .expect("schema node0");
    node1
        .schema_add("type Event { src: String  seq: Int }")
        .expect("schema node1");

    // Bidirectional replication
    setup_replication_link(&cluster, 0, 1, &["Event"]).await;

    let info0 = node0.p2p_info().expect("p2p_info node0");
    let addr0 = info0
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|v| v.as_str())
        .expect("node0 has no P2P address");
    node1
        .p2p_replicator_set(&["Event"], addr0)
        .expect("replicator 1->0");

    // Both nodes write at the same time
    node0
        .query(r#"mutation { add_Event(input: {src: "node0", seq: 1}) { _docID } }"#)
        .expect("write on node0");
    node1
        .query(r#"mutation { add_Event(input: {src: "node1", seq: 2}) { _docID } }"#)
        .expect("write on node1");

    // Both nodes should eventually see both documents
    poll_until(
        || count_docs(&cluster, 0, "Event") >= 2 && count_docs(&cluster, 1, "Event") >= 2,
        Duration::from_secs(30),
        Duration::from_millis(300),
        "not all documents replicated to both nodes",
    )
    .await;

    assert_eq!(count_docs(&cluster, 0, "Event"), 2);
    assert_eq!(count_docs(&cluster, 1, "Event"), 2);
}

for_each_p2p_topology!(simultaneous_writes, simultaneous_writes_test, .with_p2p());

// ─── Test 3: burst_writes_with_bidirectional_replication ─────────────────────

/// Rapid burst of writes on node0 while node1 is also writing, with
/// bidirectional replication. This is the stress test for the MergeQueue
/// and decoupled broadcast.
async fn burst_bidirectional_test(cluster: TestCluster) {
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    node0
        .schema_add("type Record { origin: String  idx: Int }")
        .expect("schema node0");
    node1
        .schema_add("type Record { origin: String  idx: Int }")
        .expect("schema node1");

    // Bidirectional replication
    setup_replication_link(&cluster, 0, 1, &["Record"]).await;

    let info0 = node0.p2p_info().expect("p2p_info node0");
    let addr0 = info0
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|v| v.as_str())
        .expect("node0 has no P2P address");
    node1
        .p2p_replicator_set(&["Record"], addr0)
        .expect("replicator 1->0");

    // Node0 creates 10 documents rapidly
    const N0_COUNT: usize = 10;
    for i in 0..N0_COUNT {
        write_with_conflict_retry(
            &node0,
            &format!(
                r#"mutation {{ add_Record(input: {{origin: "n0", idx: {i}}}) {{ _docID }} }}"#,
            ),
            &format!("node0 burst write {i}"),
        )
        .await;
    }

    // Node1 creates 5 documents in parallel
    const N1_COUNT: usize = 5;
    for i in 0..N1_COUNT {
        write_with_conflict_retry(
            &node1,
            &format!(
                r#"mutation {{ add_Record(input: {{origin: "n1", idx: {i}}}) {{ _docID }} }}"#,
            ),
            &format!("node1 burst write {i}"),
        )
        .await;
    }

    let total = N0_COUNT + N1_COUNT;

    // Keep the final document states so a timeout distinguishes lag from divergence.
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let node0_docs = query_record_docs(&cluster, 0);
        let node1_docs = query_record_docs(&cluster, 1);

        if let (Ok(node0_docs), Ok(node1_docs)) = (&node0_docs, &node1_docs) {
            if node0_docs.len() >= total && node1_docs.len() >= total {
                assert_eq!(node0_docs.len(), total, "node0 records: {node0_docs:#?}");
                assert_eq!(node1_docs.len(), total, "node1 records: {node1_docs:#?}");
                break;
            }
        }

        assert!(
            Instant::now() < deadline,
            "burst writes did not converge on both nodes within 60s\nnode0: {node0_docs:#?}\nnode1: {node1_docs:#?}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // Post-burst health check: both nodes can still write and query
    write_with_conflict_retry(
        &node0,
        r#"mutation { add_Record(input: {origin: "post-burst", idx: 9999}) { _docID } }"#,
        "post-burst write on node0",
    )
    .await;

    poll_until(
        || count_docs(&cluster, 1, "Record") > total,
        Duration::from_secs(15),
        Duration::from_millis(200),
        "post-burst doc did not replicate",
    )
    .await;
}

for_each_p2p_topology!(burst_bidirectional, burst_bidirectional_test, .with_p2p());
