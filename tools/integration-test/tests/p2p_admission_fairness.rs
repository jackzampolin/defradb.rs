//! #1180 W2: per-source-peer pending-DAG quotas keep one noisy pusher from
//! starving a healthy one.
//!
//! Own binary: injects `DEFRA_P2P_MAX_PENDING_DAGS`, inherited by every node
//! spawned here, so it must not leak into other tests' clusters.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use integration_test::TestCluster;

const SCHEMA: &str = "type User { name: String  age: Int }";
// Match the healthy peer's two-slot quota. A larger batch also overloads the
// healthy peer and turns this isolation fence into a retry-ladder timing test.
const HEALTHY_DOCS: usize = 2;

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

/// With `MAX_PENDING_DAGS = 8` the per-peer quota is `max(8/4, 1) = 2`, well
/// below the global cap. One noisy pusher floods head-only writes it never lets
/// resolve (it is frozen), so it can occupy at most its 2-slot quota; a healthy
/// pusher filling its own two-slot quota must still land every document.
///
/// Anti-vacuity: only the noisy peer is active before the freeze, and the
/// global cap (8) can never fill from a single peer capped at 2 — so any
/// "Pending DAGs at capacity" rejection the hub logs is necessarily the
/// per-peer quota, and the noisy peer's own backlog stays unmerged.
#[tokio::test]
async fn per_peer_quota_prevents_noisy_pusher_starvation() {
    std::env::set_var("DEFRA_P2P_MAX_PENDING_DAGS", "8");
    std::env::set_var("DEFRA_P2P_RATE_LIMIT_BURST", "500");

    let mut cluster = TestCluster::builder()
        .rust_nodes(3) // 0 = hub, 1 = noisy pusher, 2 = healthy pusher
        .with_p2p()
        .build()
        .await
        .expect("cluster start");

    let startup_timeout = Duration::from_secs(30);
    for node in 0..=2 {
        cluster
            .wait_for_log(node, "p2p_listening", startup_timeout)
            .await
            .unwrap_or_else(|e| panic!("node{node} P2P listener did not start: {e}"));
    }

    // Make only the noisy source unable to serve linked blocks. It can still
    // accept local writes and send head hints, but the hub's receiver-owned
    // CAR recovery cannot drain those roots before the two-slot quota is
    // observed. Restart before schema setup because this test uses the
    // process-local store.
    cluster.nodes[1].process.kill();
    std::env::set_var("DEFRA_P2P_RATE_LIMIT_BURST", "0");
    cluster
        .restart_node(1, Duration::from_secs(60))
        .await
        .expect("restart noisy source with CAR serving disabled");
    std::env::set_var("DEFRA_P2P_RATE_LIMIT_BURST", "500");

    let hub = cluster.client(0);
    let hub_info = hub.p2p_info().expect("hub p2p info");
    let hub_addr = hub_info
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|v| v.as_str())
        .expect("hub has no P2P address")
        .to_string();

    hub.schema_add(SCHEMA).expect("hub schema");
    for pusher in 1..=2 {
        let client = cluster.client(pusher);
        client.schema_add(SCHEMA).expect("pusher schema");
        client.p2p_connect(&[&hub_addr]).expect("connect to hub");
        client
            .p2p_replicator_set(&["User"], &hub_addr)
            .expect("replicator pusher -> hub");
    }

    // The noisy pusher writes continuously so several of its head-only pushes
    // are pending on the hub at once — enough to trip its 2-slot quota.
    let stop_noisy = Arc::new(AtomicBool::new(false));
    let noisy_writer = {
        let client = cluster.client(1);
        let stop = Arc::clone(&stop_noisy);
        std::thread::spawn(move || {
            let mut doc = 0usize;
            while !stop.load(Ordering::Relaxed) {
                let mutation = format!(
                    r#"mutation {{ add_User(input: {{name: "noisy-d{doc}", age: {doc}}}) {{ _docID }} }}"#
                );
                let _ = client.query(&mutation);
                doc += 1;
                std::thread::sleep(Duration::from_millis(10));
            }
        })
    };

    // Wait until the hub has actually rejected a noisy registration on the
    // per-peer quota. Because only the noisy peer is pushing and the global cap
    // is 8, an "at capacity" log can only be the quota tripping.
    let hub_log = cluster.nodes[0]
        .rootdir
        .parent()
        .expect("hub rootdir has a parent")
        .join("logs/stdout.log");
    let quota_deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let log = std::fs::read_to_string(&hub_log).unwrap_or_default();
        if log.contains("Pending DAGs at capacity, rejecting PushLog DAG registration") {
            break;
        }
        assert!(
            Instant::now() < quota_deadline,
            "noisy pusher never tripped the per-peer pending-DAG quota"
        );
        assert!(
            pending_dags(cluster.api_url(0)).await <= 8,
            "global cap exceeded"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Stop producing new noisy roots. Its quota slots remain occupied because
    // that source's inbound request bucket cannot serve the receiver's CAR.
    stop_noisy.store(true, Ordering::Relaxed);
    noisy_writer.join().expect("noisy writer thread panicked");

    // The healthy pusher writes a fixed batch; every document must land despite
    // the noisy peer holding its quota.
    let healthy = cluster.client(2);
    let mut healthy_ids = Vec::with_capacity(HEALTHY_DOCS);
    for doc in 0..HEALTHY_DOCS {
        let data = healthy
            .query(&format!(
                r#"mutation {{ add_User(input: {{name: "healthy-d{doc}", age: {doc}}}) {{ _docID }} }}"#
            ))
            .expect("create healthy doc");
        healthy_ids.push(
            data["add_User"][0]["_docID"]
                .as_str()
                .expect("missing _docID")
                .to_string(),
        );
    }

    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let present: std::collections::HashSet<String> = hub
            .query("query { User { _docID } }")
            .ok()
            .and_then(|result| {
                result["User"].as_array().map(|rows| {
                    rows.iter()
                        .filter_map(|row| row["_docID"].as_str().map(str::to_string))
                        .collect()
                })
            })
            .unwrap_or_default();

        let missing: Vec<&String> = healthy_ids
            .iter()
            .filter(|id| !present.contains(id.as_str()))
            .collect();
        if missing.is_empty() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "{} of {} healthy-pusher documents were starved by the noisy pusher \
             (per-peer quota not enforced): {:?}",
            missing.len(),
            healthy_ids.len(),
            missing
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
