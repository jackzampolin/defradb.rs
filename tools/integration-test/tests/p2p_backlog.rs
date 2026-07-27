//! #1099: outbound push-backlog bounds, per-peer fairness, and overload
//! observability under fan-out with a nonresponsive peer.
//!
//! Lives in its own test binary because the caps are injected via
//! `DEFRA_P2P_PUSH_QUEUE_*` env vars, which every node spawned by this
//! process inherits — they must not leak into other tests' clusters.

use std::time::{Duration, Instant};

use integration_test::TestCluster;

const SCHEMA: &str = "type User { name: String  age: Int }";
const DOCS: usize = 120;
const QUEUE_CAPACITY: u64 = 8;
const QUEUE_BYTE_CAPACITY: u64 = 262_144;

fn signal(pid: u32, signal: &str) {
    let status = std::process::Command::new("kill")
        .arg(signal)
        .arg(pid.to_string())
        .status()
        .expect("spawn kill");
    assert!(status.success(), "kill {signal} {pid} failed");
}

async fn sync_status(api_url: &str) -> serde_json::Value {
    reqwest::get(format!("{api_url}/api/v0/p2p/sync/status"))
        .await
        .expect("sync status request")
        .json()
        .await
        .expect("sync status json")
}

/// One pusher replicates a write burst far beyond every cap to one healthy
/// and one SIGSTOPped target. Asserts the #1099 resource contract end to end:
///
/// - queued items/bytes, active workers, and retained task handles stay
///   within their configured bounds throughout the burst;
/// - overload is observable (`rejected_*_total` counters advance) and the
///   nonresponsive peer's starvation is visible in the per-peer snapshot;
/// - the healthy target converges while the dead one is stopped (fairness);
/// - after SIGCONT, the deferred pushes drain through the persisted retry
///   ladder so the previously dead target converges too (durable overflow
///   outcome — nothing was silently dropped).
#[tokio::test]
async fn outbound_backlog_bounded_under_fanout_with_dead_peer() {
    std::env::set_var("DEFRA_P2P_PUSH_QUEUE_CAPACITY", QUEUE_CAPACITY.to_string());
    std::env::set_var(
        "DEFRA_P2P_PUSH_QUEUE_BYTES",
        QUEUE_BYTE_CAPACITY.to_string(),
    );
    std::env::set_var("DEFRA_P2P_MAX_ACTIVE_PUSHES_PER_PEER", "2");

    let cluster = TestCluster::builder()
        .rust_nodes(3)
        .with_p2p()
        .build()
        .await
        .expect("cluster start");

    let startup_timeout = Duration::from_secs(30);
    for node in 0..3 {
        cluster
            .wait_for_log(node, "p2p_listening", startup_timeout)
            .await
            .unwrap_or_else(|e| panic!("node{node} P2P listener did not start: {e}"));
    }

    let pusher = cluster.client(0);
    pusher.schema_add(SCHEMA).expect("pusher schema");

    let mut target_addrs = Vec::new();
    for target in 1..3 {
        let client = cluster.client(target);
        client.schema_add(SCHEMA).expect("target schema");
        let info = client.p2p_info().expect("target p2p info");
        let addr = info
            .as_array()
            .and_then(|arr| arr.first())
            .and_then(|v| v.as_str())
            .expect("target has no P2P address")
            .to_string();
        pusher.p2p_connect(&[&addr]).expect("connect to target");
        pusher
            .p2p_replicator_set(&["User"], &addr)
            .expect("replicator pusher -> target");
        target_addrs.push(addr);
    }

    // Freeze target 2: a live TCP peer whose process never responds — the
    // deterministic nonresponsive peer.
    let dead_pid = cluster.nodes[2].process.id().expect("target 2 pid");
    signal(dead_pid, "-STOP");

    let pusher_api = cluster.api_url(0).to_string();

    // Sustained write burst far beyond the queue cap, sampling the resource
    // snapshot throughout.
    let mut expected_doc_ids = Vec::with_capacity(DOCS);
    let mut saw_rejection = false;
    for doc in 0..DOCS {
        let mutation = format!(
            r#"mutation {{ add_User(input: {{name: "doc-{doc}", age: {doc}}}) {{ _docID }} }}"#
        );
        let data = pusher.query(&mutation).expect("create doc on pusher");
        expected_doc_ids.push(
            data["add_User"][0]["_docID"]
                .as_str()
                .expect("missing _docID")
                .to_string(),
        );

        if doc % 10 == 0 {
            let status = sync_status(&pusher_api).await;
            let backlog = &status["push_backlog"];
            let queued_items = backlog["queued_items"].as_u64().unwrap();
            let queued_bytes = backlog["queued_bytes"].as_u64().unwrap();
            let active_jobs = backlog["active_jobs"].as_u64().unwrap();
            let worker_count = backlog["worker_count"].as_u64().unwrap();
            let retained = status["retained_background_tasks"].as_u64().unwrap();
            assert!(
                queued_items <= QUEUE_CAPACITY,
                "queued_items {queued_items} exceeded cap {QUEUE_CAPACITY}"
            );
            assert!(
                queued_bytes <= QUEUE_BYTE_CAPACITY,
                "queued_bytes {queued_bytes} exceeded cap {QUEUE_BYTE_CAPACITY}"
            );
            assert!(
                active_jobs <= worker_count,
                "active_jobs {active_jobs} exceeded workers {worker_count}"
            );
            // Fixed workers + transient fetch/replay tasks; a growing value
            // here would be the #1099 handle-retention leak.
            assert!(
                retained <= worker_count + 32,
                "retained task handles grew unbounded: {retained}"
            );
            saw_rejection |= backlog["rejected_items_total"].as_u64().unwrap() > 0
                || backlog["rejected_bytes_total"].as_u64().unwrap() > 0;
        }
    }

    // Anti-vacuity: the burst must actually have overflowed admission, and
    // overflow must be observable in the counters.
    let overload_deadline = Instant::now() + Duration::from_secs(60);
    while !saw_rejection {
        let status = sync_status(&pusher_api).await;
        let backlog = &status["push_backlog"];
        saw_rejection = backlog["rejected_items_total"].as_u64().unwrap() > 0
            || backlog["rejected_bytes_total"].as_u64().unwrap() > 0;
        assert!(
            Instant::now() < overload_deadline,
            "burst never overflowed the push backlog; caps were not stressed"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    // The dead peer's stall must become visible in the per-peer snapshot
    // (source-inc/gents#630: slot starvation was invisible to diagnostics).
    let visibility_deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let status = sync_status(&pusher_api).await;
        let starving_peer_visible = status["push_backlog"]["per_peer"]
            .as_array()
            .map(|peers| {
                peers.iter().any(|peer| {
                    peer["consecutive_failures"].as_u64().unwrap_or(0) > 0
                        || peer["active_jobs"].as_u64().unwrap_or(0) > 0
                        || peer["queued_items"].as_u64().unwrap_or(0) > 0
                })
            })
            .unwrap_or(false);
        if starving_peer_visible {
            break;
        }
        assert!(
            Instant::now() < visibility_deadline,
            "nonresponsive peer never appeared in per-peer diagnostics"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // Fairness: the healthy target converges while the dead peer is stopped.
    let healthy = cluster.client(1);
    wait_for_docs(
        &healthy,
        &expected_doc_ids,
        Duration::from_secs(120),
        "healthy target",
    )
    .await;

    // Resume the dead target: every deferred/rejected push must drain through
    // the persisted retry ladder — the explicit overload outcome is durable.
    signal(dead_pid, "-CONT");
    let revived = cluster.client(2);
    wait_for_docs(
        &revived,
        &expected_doc_ids,
        Duration::from_secs(300),
        "revived target",
    )
    .await;
}

async fn wait_for_docs(
    client: &integration_test::DefraClient,
    expected_doc_ids: &[String],
    deadline: Duration,
    label: &str,
) {
    let deadline = Instant::now() + deadline;
    loop {
        let present: std::collections::HashSet<String> = client
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

        let missing = expected_doc_ids
            .iter()
            .filter(|id| !present.contains(*id))
            .count();
        if missing == 0 {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{label}: {missing} of {} documents never replicated",
            expected_doc_ids.len()
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
