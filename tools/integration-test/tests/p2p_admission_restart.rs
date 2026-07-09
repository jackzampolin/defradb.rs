//! #1099: hub restart must not lose success-acked pending-DAG registrations.
//!
//! Own binary: injects `DEFRA_P2P_MAX_PENDING_DAGS`, which every node spawned
//! by this process inherits.

use std::time::{Duration, Instant};

use integration_test::{DefraClient, TestCluster};

const SCHEMA: &str = "type User { name: String  age: Int }";
const PUSHERS: usize = 4;
const DOCS_PER_PUSHER: usize = 6;

/// Pushers burst head-only pushes into a 1-slot hub, the hub is hard-killed
/// while its pending slot is provably occupied by a success-acked
/// registration, and it is respawned on the same rootdir.
///
/// The restart contract under test (PendingDagRestart.tla INV_AckBacked): a
/// success ack destroyed the pusher's retry record, so the doc can only merge
/// if the hub's registration was durable. The test gates on the restore log
/// (durable records actually survived) and then requires full completeness —
/// on process-local registrations the doc occupying the slot at kill time
/// would be silently lost forever.
#[tokio::test]
async fn hub_restart_recovers_success_acked_pending_dags() {
    std::env::set_var("DEFRA_P2P_MAX_PENDING_DAGS", "1");

    let mut cluster = TestCluster::builder()
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

    // Concurrent create bursts: every head-only push has missing field links
    // on the hub, so pushes contend for the single pending-DAG slot and the
    // slot stays occupied by a success-acked registration.
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

    // Kill the hub while its pending slot is occupied: poll the diagnostics
    // endpoint until a registration is live, then hard-kill immediately.
    let hub_api = cluster.api_url(0).to_string();
    let kill_deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let pending = reqwest::get(format!("{hub_api}/api/v0/p2p/sync/status"))
            .await
            .ok();
        let pending = match pending {
            Some(response) => response
                .json::<serde_json::Value>()
                .await
                .ok()
                .and_then(|status| status["pending_dags"].as_u64())
                .unwrap_or(0),
            None => 0,
        };
        if pending >= 1 {
            break;
        }
        assert!(
            Instant::now() < kill_deadline,
            "hub never held a pending-DAG registration; the burst did not exercise the slot"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    cluster.nodes[0].process.kill();

    cluster
        .restart_node(0, Duration::from_secs(60))
        .await
        .expect("restart hub on its rootdir");

    // Anti-vacuity for the recovery path: durable registrations must have
    // survived the kill and been re-driven. Without persistence this log
    // (emitted only when records were loaded) never appears.
    let hub_log = cluster.nodes[0]
        .rootdir
        .parent()
        .expect("hub rootdir has a parent")
        .join("logs/stdout.log");
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

    // Every success-acked document must merge on the restarted hub: the
    // registration that was pending at kill time recovers through the
    // persisted re-drive; nacked docs recover through pusher retry ladders.
    let hub = cluster.client(0);
    let deadline = Instant::now() + Duration::from_secs(240);
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

        let missing: Vec<&String> = expected_doc_ids
            .iter()
            .filter(|id| !present.contains(id.as_str()))
            .collect();
        if missing.is_empty() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "{} of {} documents lost across hub restart (success-acked pending \
             registration not recovered): {:?}",
            missing.len(),
            expected_doc_ids.len(),
            missing
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
