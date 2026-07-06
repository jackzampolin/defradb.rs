//! #1088 W4 liveness: a deep full-DAG push must complete under intake rate
//! limiting instead of sawtoothing against a lockout ladder.
//!
//! Filtered replicators receive the full transitive DAG on every update with
//! no client-side pacer (unlike the initial-replay path, whose ReplayPushGate
//! self-paces). With the pre-split limiter, the first over-budget block armed
//! a 30s lockout: the pusher's ~2.3s of in-batch retries all fell inside it,
//! the batch aborted, and every ladder-spaced re-push restarted from block one
//! — re-burning the burst on already-sent blocks — so any DAG deeper than
//! roughly the burst never converged. The request-intake limiter now paces at
//! the token-refill horizon, so the push throttles to the refill rate and
//! completes.
//!
//! Own test binary: the burst/rate env overrides must not leak into other
//! tests' clusters.

use std::process::Command;
use std::time::{Duration, Instant};

use integration_test::TestCluster;

const SCHEMA: &str = "type User { name: String @immutable  age: Int }";
const UPDATES: usize = 50;

fn socket_addr(cluster: &TestCluster, node: usize) -> String {
    cluster.api_url(node).replace("http://", "")
}

fn add_filtered_replicator(cluster: &TestCluster, node: usize, addr: &str) {
    let client = cluster.client(node);
    let output = Command::new(client.binary_path())
        .arg("--url")
        .arg(socket_addr(cluster, node))
        .args([
            "client",
            "p2p",
            "replicator",
            "add",
            "-c",
            "User",
            "--filter-field",
            "name",
            "--filter-value",
            "deep",
            addr,
        ])
        .output()
        .expect("exec filtered replicator add");
    assert!(
        output.status.success(),
        "filtered replicator add failed: status={} stderr={}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test]
async fn deep_dag_push_completes_under_intake_rate_limiting() {
    // Hub bucket far smaller than the DAG so the live full-DAG push must be
    // paced across many refill windows: ~150 blocks vs a 50-token burst.
    std::env::set_var("DEFRA_P2P_RATE_LIMIT_BURST", "50");
    std::env::set_var("DEFRA_P2P_RATE_LIMIT_RATE", "20");

    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .build()
        .await
        .expect("cluster start");

    let startup_timeout = Duration::from_secs(30);
    for node in 0..2 {
        cluster
            .wait_for_log(node, "p2p_listening", startup_timeout)
            .await
            .unwrap_or_else(|e| panic!("node{node} P2P listener did not start: {e}"));
    }

    let pusher = cluster.client(0);
    let hub = cluster.client(1);
    let hub_info = hub.p2p_info().expect("hub p2p info");
    let hub_addr = hub_info
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|v| v.as_str())
        .expect("hub has no P2P address")
        .to_string();

    pusher.schema_add(SCHEMA).expect("pusher schema");
    hub.schema_add(SCHEMA).expect("hub schema");
    pusher.p2p_connect(&[&hub_addr]).expect("connect to hub");

    // Build a deep DAG before any replication: 1 create + UPDATES updates,
    // each adding a composite + field block to the document's history.
    let created = pusher
        .query(r#"mutation { add_User(input: {name: "deep", age: 0}) { _docID } }"#)
        .expect("create doc");
    let doc_id = created["add_User"][0]["_docID"]
        .as_str()
        .expect("missing _docID")
        .to_string();
    for age in 1..=UPDATES {
        let mutation = format!(
            r#"mutation {{ update_User(docID: "{doc_id}", input: {{age: {age}}}) {{ _docID }} }}"#
        );
        pusher.query(&mutation).expect("update doc");
    }

    add_filtered_replicator(&cluster, 0, &hub_addr);
    // Let the initial replay attempt (client-paced at defaults, which exceed
    // this hub's shrunken bucket) finish nacking and the bucket refill.
    tokio::time::sleep(Duration::from_secs(3)).await;

    // One live update pushes the ENTIRE transitive DAG (~3 blocks per update
    // deep by now) through send_ordered_pushlogs_via_transport: it must pace
    // through the limiter and converge, not wedge.
    let final_age = 999;
    let mutation = format!(
        r#"mutation {{ update_User(docID: "{doc_id}", input: {{age: {final_age}}}) {{ _docID }} }}"#
    );
    pusher.query(&mutation).expect("final update");

    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let result = hub
            .query("query { User { _docID age } }")
            .expect("query hub");
        let converged = result["User"].as_array().is_some_and(|rows| {
            rows.iter().any(|row| {
                row["_docID"].as_str() == Some(doc_id.as_str())
                    && row["age"].as_i64() == Some(final_age)
            })
        });
        if converged {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "deep-DAG push did not converge under intake rate limiting (sawtooth/wedge): {}",
            serde_json::to_string_pretty(&result).unwrap()
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
