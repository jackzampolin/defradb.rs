//! #1088 W5: replicator fan-in against a hub with a tiny pending-DAG cap.
//!
//! Lives in its own test binary because the cap is injected via the
//! `DEFRA_P2P_MAX_PENDING_DAGS` env var, which every node spawned by this
//! process inherits — it must not leak into other tests' clusters.

use std::time::{Duration, Instant};

use integration_test::{DefraClient, TestCluster};

const SCHEMA: &str = "type User { name: String  age: Int }";
const PUSHERS: usize = 8;
const DOCS_PER_PUSHER: usize = 8;

/// N pushers replicate concurrent document-create bursts into one hub whose
/// pending-DAG map holds a single slot, so almost every push overflows
/// admission (each unfiltered replicator push carries only the head block;
/// its field links are missing on the hub and need a pending registration).
///
/// The #1088 M1 invariant under test: **no document may be success-acked on a
/// pusher while unmerged and unregistered on the hub.** Before the W1 fix the
/// hub acked success on overflow and the pusher deleted its retry record, so
/// the finite-run obligation balance fell below the number of committed docs.
///
/// Stage 3 intentionally replaced immediate per-request nack retries with the
/// Go-compatible durable 30s..32m peer ladder. With 64 obligations racing for
/// one receiver slot, complete convergence is therefore not a meaningful
/// three-minute assertion: only one sender can win each synchronized retry
/// wave. This fence instead checks the actual safety boundary, then witnesses
/// forward progress through that durable retry path: every committed doc is
/// either merged, durably registered at the receiver, or retained as a sender
/// scope marker.
#[tokio::test]
async fn fan_in_pushlog_admission_no_silent_divergence() {
    std::env::set_var("DEFRA_P2P_MAX_PENDING_DAGS", "1");

    let cluster = TestCluster::builder()
        .rust_nodes(1 + PUSHERS)
        .with_p2p()
        .build()
        .await
        .expect("cluster start");

    let startup_timeout = Duration::from_secs(30);
    for node in 0..=PUSHERS {
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
        .and_then(|v| v.as_str())
        .expect("hub has no P2P address")
        .to_string();

    hub.schema_add(SCHEMA).expect("hub schema");
    for pusher in 1..=PUSHERS {
        let client = cluster.client(pusher);
        client.schema_add(SCHEMA).expect("pusher schema");
        client.p2p_connect(&[&hub_addr]).expect("connect to hub");
        client
            .p2p_replicator_set(&["User"], &hub_addr)
            .expect("replicator pusher -> hub");
    }

    // Concurrent create bursts, one thread per pusher: each create commits
    // locally and pushes its head block to the hub from a background task, so
    // pushes from all 8 pushers race for the hub's single pending-DAG slot.
    // The hub handles transport events serially, so any push that arrives
    // while another registration is still waiting on its Bitswap round trip
    // overflows admission.
    let pusher_clients: Vec<DefraClient> = (1..=PUSHERS).map(|i| cluster.client(i)).collect();
    let expected_doc_ids: Vec<String> = std::thread::scope(|scope| {
        let handles: Vec<_> = pusher_clients
            .into_iter()
            .enumerate()
            .map(|(idx, client)| {
                scope.spawn(move || {
                    let pusher = idx + 1;
                    (0..DOCS_PER_PUSHER)
                        .map(|doc| {
                            let mutation = format!(
                                r#"mutation {{ add_User(input: {{name: "p{pusher}-d{doc}", age: {doc}}}) {{ _docID }} }}"#
                            );
                            let data = client.query(&mutation).expect("create doc on pusher");
                            data["add_User"][0]["_docID"]
                                .as_str()
                                .expect("missing _docID")
                                .to_string()
                        })
                        .collect::<Vec<String>>()
                })
            })
            .collect();
        handles
            .into_iter()
            .flat_map(|handle| handle.join().expect("pusher thread panicked"))
            .collect()
    });
    assert_eq!(expected_doc_ids.len(), PUSHERS * DOCS_PER_PUSHER);

    // Anti-vacuity: the hub must actually have hit admission capacity —
    // otherwise this test would pass without exercising the overflow path.
    // (`wait_for_log` only knows pre-registered named patterns, so read the
    // hub's stdout log directly: it lives at <node_dir>/logs/stdout.log next
    // to the <node_dir>/data rootdir.)
    let hub_log = cluster.nodes[0]
        .rootdir
        .parent()
        .expect("hub rootdir has a parent")
        .join("logs/stdout.log");
    let capacity_deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let log = std::fs::read_to_string(&hub_log).unwrap_or_default();
        if log.contains("Pending DAGs at capacity") {
            break;
        }
        assert!(
            Instant::now() < capacity_deadline,
            "hub never hit pending-DAG capacity; the fan-in did not stress admission"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let status: serde_json::Value =
        reqwest::get(format!("{}/api/v0/p2p/sync/status", cluster.api_url(0)))
            .await
            .expect("hub sync status request")
            .json()
            .await
            .expect("hub sync status json");
    assert!(
        status["pending_dag_capacity_shed"]
            .as_u64()
            .is_some_and(|count| count > 0),
        "fan-in capacity exits were not counted: {status}"
    );
    assert!(status["single_flight_suppressed"].is_u64());
    assert!(status["already_merged_fast_path"].is_u64());

    // Wait for the first live-send wave to drain so the next merge must come
    // from the durable sender ladder rather than a still-queued initial hint.
    let send_waves = DOCS_PER_PUSHER.div_ceil(p2p::sync::DEFAULT_MAX_ACTIVE_PUSHES_PER_PEER);
    let settle_timeout = p2p::sync::DEFAULT_PUSH_SEND_TIMEOUT * (send_waves as u32 + 1);
    let settle_deadline = Instant::now() + settle_timeout;
    loop {
        let mut settled = true;
        for pusher in 1..=PUSHERS {
            let status: serde_json::Value = reqwest::get(format!(
                "{}/api/v0/p2p/sync/status",
                cluster.api_url(pusher)
            ))
            .await
            .expect("pusher sync status request")
            .json()
            .await
            .expect("pusher sync status json");
            settled &= status["push_backlog"]["active_jobs"].as_u64() == Some(0)
                && status["push_backlog"]["queued_items"].as_u64() == Some(0);
        }
        if settled {
            break;
        }
        assert!(
            Instant::now() < settle_deadline,
            "initial head-hint wave did not drain"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    let initial_result = hub
        .query("query { User { _docID } }")
        .expect("query initial hub Users");
    let initially_merged = initial_result["User"].as_array().map_or(0, Vec::len);

    // M1: wait for at least one later retry to merge, then require exact
    // obligation conservation across sender markers, durable receiver roots,
    // and merged documents. The 90s bound covers the first two production
    // ladder rungs without weakening their Go-compatible timing.
    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let result = hub
            .query("query { User { _docID } }")
            .expect("query hub Users");
        let present: std::collections::HashSet<&str> = result["User"]
            .as_array()
            .map(|rows| {
                rows.iter()
                    .filter_map(|row| row["_docID"].as_str())
                    .collect()
            })
            .unwrap_or_default();

        let hub_status: serde_json::Value =
            reqwest::get(format!("{}/api/v0/p2p/sync/status", cluster.api_url(0)))
                .await
                .expect("hub sync status request")
                .json()
                .await
                .expect("hub sync status json");
        let receiver_obligations = hub_status["persisted_pending_dags"]
            .as_u64()
            .expect("persisted_pending_dags counter") as usize;

        let mut sender_markers = 0usize;
        let mut sender_jobs = 0u64;
        for pusher in 1..=PUSHERS {
            let status: serde_json::Value = reqwest::get(format!(
                "{}/api/v0/p2p/sync/status",
                cluster.api_url(pusher)
            ))
            .await
            .expect("pusher sync status request")
            .json()
            .await
            .expect("pusher sync status json");
            sender_markers += status["push_retry_markers"]["document_markers"]
                .as_u64()
                .expect("document marker count") as usize;
            sender_jobs += status["push_backlog"]["active_jobs"]
                .as_u64()
                .expect("active job count");
        }

        let balanced =
            present.len() + receiver_obligations + sender_markers == expected_doc_ids.len();
        if present.len() > initially_merged && sender_jobs == 0 && balanced {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "fan-in obligations did not remain balanced while the durable retry ladder made \
             progress: merged={}, receiver={}, sender={}, expected={}, initially_merged={}",
            present.len(),
            receiver_obligations,
            sender_markers,
            expected_doc_ids.len(),
            initially_merged,
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
