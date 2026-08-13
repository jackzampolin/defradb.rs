use std::time::{Duration, Instant};

use integration_test::{for_each_p2p_topology, TestCluster};

const DOWNSAMPLE_RAW_SCHEMA: &str = "type Metric { label: String  ts: DateTime  value: Int }";
const CONTROL_SCHEMA: &str = "type User { name: String  age: Int }";
const DOWNSAMPLE_ROLLUP_SDL: &str = r#"
type Metric1m @downsample(interval: "1m", timeField: "ts") {
  label: String
  source_doc_id: String
  source_height: Int
  window_start: DateTime
  window_end: DateTime
  count: Int
  sum: Int
  avg: Float
  min: Int
  max: Int
}
"#;

async fn replication_test(cluster: TestCluster) {
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    // Wait for P2P listeners to be ready on both nodes before calling p2p info
    let timeout = Duration::from_secs(15);
    cluster
        .wait_for_log(0, "p2p_listening", timeout)
        .await
        .expect("node0 P2P listener did not start");
    cluster
        .wait_for_log(1, "p2p_listening", timeout)
        .await
        .expect("node1 P2P listener did not start");

    // Get full multiaddrs (with peer ID) from p2p info
    // Response is a flat JSON array: ["/ip4/127.0.0.1/tcp/PORT/p2p/PEERID"]
    let info1 = node1.p2p_info().expect("failed to get node1 p2p info");
    let addr1 = info1
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|v| v.as_str())
        .expect("node1 has no P2P address");

    // Deploy schema on both nodes
    node0
        .schema_add("type User { name: String  age: Int }")
        .unwrap();
    node1
        .schema_add("type User { name: String  age: Int }")
        .unwrap();

    // Connect peers (HTTP call is synchronous — returns after connection is established)
    node0.p2p_connect(&[addr1]).unwrap();

    // Enable collection sync + set replicator
    node0.p2p_collection_add(&["User"]).unwrap();
    node1.p2p_collection_add(&["User"]).unwrap();
    node0.p2p_replicator_set(&["User"], addr1).unwrap();

    // Create doc on node 0
    let data = node0
        .query(r#"mutation { add_User(input: {name: "Alice", age: 30}) { _docID name age } }"#)
        .unwrap();
    let doc_id = data["add_User"][0]["_docID"]
        .as_str()
        .expect("missing _docID");

    // Poll-based: query node 1 until doc appears
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let result = node1.query("query { User { _docID name age } }").unwrap();
        if let Some(users) = result["User"].as_array() {
            if !users.is_empty() {
                assert_eq!(users[0]["_docID"].as_str().unwrap(), doc_id);
                assert_eq!(users[0]["name"], "Alice");
                assert_eq!(users[0]["age"], 30);
                break;
            }
        }
        assert!(
            Instant::now() < deadline,
            "doc did not replicate within timeout"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test]
async fn rust_to_go_replicator_backfills_existing_lww_document() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .go_nodes(1)
        .with_p2p()
        .build()
        .await
        .unwrap();
    let source = cluster.client(0);
    let target = cluster.client(1);

    source
        .schema_add(CONTROL_SCHEMA)
        .expect("add User schema on Rust node");
    target
        .schema_add(CONTROL_SCHEMA)
        .expect("add User schema on Go node");

    // Create before connecting so only replicator backfill can deliver this document.
    let created = source
        .query(r#"mutation { add_User(input: {name: "Alice", age: 30}) { _docID } }"#)
        .expect("create User on Rust node");
    let doc_id = created["add_User"][0]["_docID"]
        .as_str()
        .expect("created User has no _docID")
        .to_string();

    let timeout = Duration::from_secs(15);
    cluster
        .wait_for_log(0, "p2p_listening", timeout)
        .await
        .expect("Rust P2P listener did not start");
    cluster
        .wait_for_log(1, "p2p_listening", timeout)
        .await
        .expect("Go P2P listener did not start");
    let target_info = target.p2p_info().expect("get Go P2P info");
    let target_addr = target_info
        .as_array()
        .and_then(|addresses| addresses.first())
        .and_then(|address| address.as_str())
        .expect("Go node has no P2P address");

    source.p2p_connect(&[target_addr]).expect("connect peers");
    source
        .p2p_collection_add(&["User"])
        .expect("subscribe Rust node to User");
    target
        .p2p_collection_add(&["User"])
        .expect("subscribe Go node to User");
    source
        .p2p_replicator_set(&["User"], target_addr)
        .expect("add Go replicator");

    let deadline = Instant::now() + timeout;
    loop {
        let users = target
            .query("query { User { _docID name age } }")
            .expect("query User on Go node");
        let replicated = users["User"].as_array().is_some_and(|rows| {
            rows.iter().any(|row| {
                row["_docID"].as_str() == Some(&doc_id)
                    && row["name"].as_str() == Some("Alice")
                    && row["age"].as_i64() == Some(30)
            })
        });
        if replicated {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "existing Rust document did not backfill to Go: {users}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test]
async fn rust_rust_replication_rejects_downsample_source() {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .build()
        .await
        .unwrap();

    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    let timeout = Duration::from_secs(15);
    cluster
        .wait_for_log(0, "p2p_listening", timeout)
        .await
        .expect("node0 P2P listener did not start");
    cluster
        .wait_for_log(1, "p2p_listening", timeout)
        .await
        .expect("node1 P2P listener did not start");

    let info1 = node1.p2p_info().expect("failed to get node1 p2p info");
    let addr1 = info1
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|v| v.as_str())
        .expect("node1 has no P2P address");

    node0
        .schema_add(DOWNSAMPLE_RAW_SCHEMA)
        .expect("add Metric schema on node0");
    node1
        .schema_add(DOWNSAMPLE_RAW_SCHEMA)
        .expect("add Metric schema on node1");
    node0
        .schema_add(CONTROL_SCHEMA)
        .expect("add User schema on node0");
    node1
        .schema_add(CONTROL_SCHEMA)
        .expect("add User schema on node1");
    node1
        .view_add("Metric { label ts value }", DOWNSAMPLE_ROLLUP_SDL)
        .expect("add downsample view on node1");

    node0.p2p_connect(&[addr1]).expect("connect peers");
    node0
        .p2p_collection_add(&["Metric", "User"])
        .expect("collection add node0");
    node1
        .p2p_collection_add(&["Metric", "User"])
        .expect("collection add node1");
    node0
        .p2p_replicator_set(&["Metric", "User"], addr1)
        .expect("replicator 0->1");

    let control = node0
        .query(r#"mutation { add_User(input: {name: "Alice", age: 30}) { _docID } }"#)
        .expect("create user on node0");
    let control_doc_id = control["add_User"][0]["_docID"]
        .as_str()
        .expect("missing control _docID")
        .to_string();

    let control_deadline = Instant::now() + timeout;
    loop {
        let replicated = node1
            .query("query { User { _docID name age } }")
            .expect("query User on node1");
        let found = replicated["User"].as_array().is_some_and(|rows| {
            rows.iter().any(|row| {
                row["_docID"].as_str() == Some(control_doc_id.as_str())
                    && row["name"].as_str() == Some("Alice")
                    && row["age"].as_i64() == Some(30)
            })
        });
        if found {
            break;
        }
        assert!(
            Instant::now() < control_deadline,
            "control User collection did not replicate: {}",
            serde_json::to_string_pretty(&replicated).unwrap()
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let metric = node0
        .query(
            r#"mutation {
                add_Metric(input: {
                    label: "cpu"
                    ts: "2026-03-12T00:00:00Z"
                    value: 42
                }) {
                    _docID
                }
            }"#,
        )
        .expect("create metric on node0");
    assert!(
        metric["add_Metric"]
            .as_array()
            .is_some_and(|rows| !rows.is_empty()),
        "expected Metric to be created on node0"
    );

    let rejection_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let replicated_metric = node1
            .query("query { Metric { _docID label value } }")
            .expect("query Metric on node1");
        let rollup = node1
            .query("query { Metric1m { _docID label count sum avg } }")
            .expect("query Metric1m on node1");

        let metric_empty = replicated_metric["Metric"]
            .as_array()
            .is_some_and(|rows| rows.is_empty());
        let rollup_empty = rollup["Metric1m"]
            .as_array()
            .is_some_and(|rows| rows.is_empty());

        assert!(
            metric_empty,
            "replicated Metric should be rejected on node1: {}",
            serde_json::to_string_pretty(&replicated_metric).unwrap()
        );
        assert!(
            rollup_empty,
            "Metric1m should remain empty after rejecting replicated source writes: {}",
            serde_json::to_string_pretty(&rollup).unwrap()
        );

        if Instant::now() >= rejection_deadline {
            break;
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

for_each_p2p_topology!(replication, replication_test, .with_p2p());
