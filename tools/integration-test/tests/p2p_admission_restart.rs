//! #1099: hub restart must not lose success-acked pending-DAG registrations.
//!
//! Own binary: `DEFRA_P2P_MAX_PENDING_DAGS`, `DEFRA_P2P_RATE_LIMIT_BURST`,
//! and `RUST_LOG` are injected via process-global `set_var` — every node
//! spawned by this process inherits them, and the burst limit is re-toggled
//! mid-test around node restarts, so no other test may share this process.

use std::time::{Duration, Instant};

use integration_test::TestCluster;

const SCHEMA: &str = "type User { name: String  age: Int }";
const PERSISTED_REGISTRATION: &str = "Persisted pending DAG registration";
const ACCEPTED_PUSH: &str = "PushLog head hint accepted by replicator";

async fn pending_dags(hub_api: &str) -> u64 {
    let Ok(response) = reqwest::get(format!("{hub_api}/api/v0/p2p/sync/status")).await else {
        return 0;
    };
    response
        .json::<serde_json::Value>()
        .await
        .ok()
        .and_then(|status| status["pending_dags"].as_u64())
        .unwrap_or(0)
}

fn latest_log_line(path: &std::path::Path, marker: &str) -> Option<String> {
    std::fs::read_to_string(path)
        .ok()?
        .lines()
        .rev()
        .find(|line| line.contains(marker))
        .map(str::to_string)
}

fn log_field<'a>(line: &'a str, field: &str) -> Option<&'a str> {
    line.split_whitespace()
        .find_map(|value| value.strip_prefix(field))
}

fn log_contains_accepted_cid(path: &std::path::Path, cid: &str) -> bool {
    let cid_field = format!("cid={cid}");
    std::fs::read_to_string(path).is_ok_and(|log| {
        log.lines()
            .any(|line| line.contains(ACCEPTED_PUSH) && line.contains(&cid_field))
    })
}

/// The source starts with inbound P2P requests disabled by a zero-token
/// request bucket. Its root PushLog can still be success-acked by the hub, but
/// the hub cannot fetch the linked field blocks, leaving a stable durable
/// registration to crash over. After the hub restarts, the same persistent
/// source is restarted with normal request intake so the exact restored root
/// can complete.
#[tokio::test]
async fn hub_restart_recovers_success_acked_pending_dags() {
    std::env::set_var("DEFRA_P2P_MAX_PENDING_DAGS", "1");
    std::env::set_var("DEFRA_P2P_RATE_LIMIT_BURST", "500");
    std::env::set_var("RUST_LOG", "info,p2p::sync::restart_recovery=debug");

    // Both nodes need stable stores and identities because each is restarted.
    let mut cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_store("redb")
        .with_keyring()
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

    let hub = cluster.client(0);
    let hub_info = hub.p2p_info().expect("hub p2p info");
    let hub_addr = hub_info
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|value| value.as_str())
        .expect("hub has no P2P address")
        .to_string();

    hub.schema_add(SCHEMA).expect("hub schema");
    let pusher = cluster.client(1);
    pusher.schema_add(SCHEMA).expect("pusher schema");

    // Restart only the source with a zero-capacity inbound request bucket.
    // The hub remains at the normal limit and can therefore admit PushLogs.
    cluster.nodes[1].process.kill();
    std::env::set_var("DEFRA_P2P_RATE_LIMIT_BURST", "0");
    cluster
        .restart_node(1, Duration::from_secs(60))
        .await
        .expect("restart source with CAR serving disabled");
    std::env::set_var("DEFRA_P2P_RATE_LIMIT_BURST", "500");

    let pusher = cluster.client(1);
    pusher.p2p_connect(&[&hub_addr]).expect("connect to hub");
    pusher
        .p2p_replicator_set(&["User"], &hub_addr)
        .expect("replicator pusher -> hub");

    let data = pusher
        .query(r#"mutation { add_User(input: {name: "pending", age: 1}) { _docID } }"#)
        .expect("create document on pusher");
    let created_doc_id = data["add_User"][0]["_docID"]
        .as_str()
        .expect("missing _docID");

    let node_log = |node: usize| {
        cluster.nodes[node]
            .rootdir
            .parent()
            .expect("node rootdir has a parent")
            .join("logs/stdout.log")
    };
    let hub_log = node_log(0);
    let pusher_log = node_log(1);

    // Correlate the receiver's durable registration with the sender's success
    // reply before killing the hub.
    let registration_deadline = Instant::now() + Duration::from_secs(30);
    let (expected_cid, expected_doc_id) = loop {
        assert!(
            Instant::now() < registration_deadline,
            "hub never durably admitted a success-acked pending DAG"
        );
        let Some(registration) = latest_log_line(&hub_log, PERSISTED_REGISTRATION) else {
            tokio::time::sleep(Duration::from_millis(25)).await;
            continue;
        };
        let cid = log_field(&registration, "cid=").expect("registration CID");
        if !log_contains_accepted_cid(&pusher_log, cid) {
            tokio::time::sleep(Duration::from_millis(25)).await;
            continue;
        }
        break (
            cid.to_string(),
            log_field(&registration, "doc_id=")
                .expect("registration document ID")
                .to_string(),
        );
    };
    assert_eq!(expected_doc_id, created_doc_id);

    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        pending_dags(cluster.api_url(0)).await,
        1,
        "hub's pending-DAG count is not holding at 1: the source unexpectedly \
         served the pending DAG before the crash"
    );

    cluster.nodes[0].process.kill();
    cluster
        .restart_node(0, Duration::from_secs(60))
        .await
        .expect("restart hub on its rootdir");

    // Anti-vacuity: the registration must have survived the kill and loaded
    // from the durable pending store.
    let restore_deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let log = std::fs::read_to_string(&hub_log).unwrap_or_default();
        if log.contains("restored persisted pending DAG registrations") {
            break;
        }
        assert!(
            Instant::now() < restore_deadline,
            "hub restart never restored persisted pending DAG registrations"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    // Re-enable CAR serving only after the hub has restored the obligation.
    cluster.nodes[1].process.kill();
    cluster
        .restart_node(1, Duration::from_secs(60))
        .await
        .expect("restart source with normal request intake");
    cluster
        .client(1)
        .p2p_connect(&[&hub_addr])
        .expect("reconnect source to hub");

    let ready_deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let log = std::fs::read_to_string(&hub_log).unwrap_or_default();
        let recovered = log.lines().any(|line| {
            line.contains("DAG complete, emitting DagReady")
                && line.contains(&format!("root_cid={expected_cid}"))
                && line.contains(&format!("doc_id={expected_doc_id}"))
        });
        if recovered {
            break;
        }
        assert!(
            Instant::now() < ready_deadline,
            "restored pending root {expected_cid} did not become ready after hub restart"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    let hub = cluster.client(0);
    let merge_deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let recovered = hub
            .query("query { User { _docID } }")
            .ok()
            .and_then(|result| result["User"].as_array().cloned())
            .is_some_and(|rows| {
                rows.iter()
                    .any(|row| row["_docID"].as_str() == Some(expected_doc_id.as_str()))
            });
        if recovered {
            break;
        }
        assert!(
            Instant::now() < merge_deadline,
            "success-acked pending document {expected_doc_id} was not merged after recovery"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}
