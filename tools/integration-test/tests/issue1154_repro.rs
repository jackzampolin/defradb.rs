//! #1154 at-scale repro: every success-acked document must merge on the
//! restarted hub. This is the pre-rewrite `p2p_admission_restart` workload
//! (writer threads, SIGSTOP freeze, hard kill/restart) with a scale floor
//! (≥500 docs before the freeze hunt) so the pusher retry ladders carry
//! hundreds of nacked pushes.
//!
//! Own binary: injects process-wide node settings inherited by every spawned
//! node.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use integration_test::TestCluster;

const SCHEMA: &str = "type User { name: String  age: Int }";
const PUSHERS: usize = 4;
const MIN_DOCS: usize = 500;

fn signal(pid: u32, signal: &str) {
    let status = std::process::Command::new("kill")
        .arg(signal)
        .arg(pid.to_string())
        .status()
        .expect("spawn kill");
    assert!(status.success(), "kill {signal} {pid} failed");
}

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

async fn sync_status(cluster: &TestCluster, node: usize) -> serde_json::Value {
    reqwest::get(format!("{}/api/v0/p2p/sync/status", cluster.api_url(node)))
        .await
        .expect("sync status request")
        .json()
        .await
        .expect("sync status json")
}

async fn sender_retry_snapshot(cluster: &TestCluster) -> (usize, u64) {
    let mut markers = 0usize;
    let mut active_jobs = 0u64;
    for pusher in 1..=PUSHERS {
        let status = sync_status(cluster, pusher).await;
        markers += status["push_retry_markers"]["document_markers"]
            .as_u64()
            .expect("document marker count") as usize;
        active_jobs += status["push_backlog"]["active_jobs"]
            .as_u64()
            .expect("active sender jobs");
    }
    (markers, active_jobs)
}

fn log_field<'a>(line: &'a str, field: &str) -> Option<&'a str> {
    line.split_whitespace()
        .find_map(|value| value.strip_prefix(field))
}

/// Documents the hub durably registered before acknowledging their push.
///
/// `SyncManager::process_pushlog` commits the record and only then acks, so
/// the ack destroyed the pusher's retry record for exactly these documents:
/// after the kill the hub is their only remaining owner. An at-capacity push
/// returns before this line is logged, so the set holds only registrations
/// that really reached the store. Each document here is created once and
/// never updated, so one document maps to at most one live root.
fn registered_doc_ids(hub_log: &std::path::Path) -> std::collections::HashSet<String> {
    std::fs::read_to_string(hub_log)
        .unwrap_or_default()
        .lines()
        .filter(|line| line.contains("Persisted pending DAG registration"))
        .filter_map(|line| log_field(line, "doc_id=").map(str::to_string))
        .collect()
}

/// Pushers write continuously into a 1-slot hub while the test arranges a
/// deterministic crash window: once a pending registration is observed, the
/// pushers are SIGSTOPped (so Bitswap cannot resolve it), the registration is
/// re-confirmed, and the hub is hard-killed and respawned on its rootdir.
///
/// The restart contract under test (PendingDagRestart.tla INV_AckBacked): the
/// success ack destroyed the pusher's retry record, so the frozen-slot doc
/// can only merge if the hub's registration was durable. The test gates on
/// the restore log (durable records actually survived and were re-driven) and
/// then requires full completeness — with process-local registrations the doc
/// occupying the slot at kill time is silently lost forever.
#[tokio::test]
async fn hub_restart_recovers_success_acked_pending_dags() {
    std::env::set_var("DEFRA_P2P_MAX_PENDING_DAGS", "1");
    std::env::set_var("RUST_LOG", "info,p2p::sync::restart_recovery=debug");

    // The hub must survive a restart with identity and state intact: the
    // harness defaults (memory store, no keyring => ephemeral peer key) would
    // make the respawned hub an empty stranger the pushers cannot dial.
    let mut cluster = TestCluster::builder()
        .rust_nodes(1 + PUSHERS)
        .with_node_store(0, "redb")
        .with_keyring()
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

    // Continuous head-only write load: every live push has missing field
    // links on the hub, so the single pending slot keeps being occupied by a
    // success-acked registration while the writers run.
    let stop_writers = Arc::new(AtomicBool::new(false));
    let doc_ids = Arc::new(Mutex::new(Vec::<String>::new()));
    let writer_handles: Vec<_> = (1..=PUSHERS)
        .map(|pusher| {
            let client = cluster.client(pusher);
            let stop = Arc::clone(&stop_writers);
            let doc_ids = Arc::clone(&doc_ids);
            std::thread::spawn(move || {
                let mut doc = 0usize;
                while !stop.load(Ordering::Relaxed) {
                    let mutation = format!(
                        r#"mutation {{ add_User(input: {{name: "p{pusher}-d{doc}", age: {doc}}}) {{ _docID }} }}"#
                    );
                    let data = client.query(&mutation).expect("create doc on pusher");
                    let doc_id = data["add_User"][0]["_docID"]
                        .as_str()
                        .expect("missing _docID")
                        .to_string();
                    doc_ids.lock().unwrap().push(doc_id);
                    doc += 1;
                    std::thread::sleep(Duration::from_millis(25));
                }
            })
        })
        .collect();

    // Scale floor: make sure the writers have produced hundreds of documents
    // (and therefore hundreds of nacked pushes queued in the pusher retry
    // ladders behind the 1-slot hub) before hunting for the crash window.
    let load_deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let produced = doc_ids.lock().unwrap().len();
        if produced >= MIN_DOCS {
            break;
        }
        assert!(
            Instant::now() < load_deadline,
            "writers only produced {produced} docs before the load deadline"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Deterministic crash window: observe a live registration, freeze the
    // pushers so Bitswap cannot resolve it, and confirm it is still pending
    // after in-flight blocks settle. Only then is the hub killed.
    let hub_api = cluster.api_url(0).to_string();
    let pusher_pids: Vec<u32> = (1..=PUSHERS)
        .map(|pusher| cluster.nodes[pusher].process.id().expect("pusher pid"))
        .collect();
    let freeze_deadline = Instant::now() + Duration::from_secs(90);
    loop {
        assert!(
            Instant::now() < freeze_deadline,
            "hub never held a pending-DAG registration across a pusher freeze"
        );
        if pending_dags(&hub_api).await == 0 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        }
        for pid in &pusher_pids {
            signal(*pid, "-STOP");
        }
        // Let the hub finish processing in-flight blocks: whatever can still
        // resolve, resolves now.
        tokio::time::sleep(Duration::from_millis(500)).await;
        if pending_dags(&hub_api).await >= 1 {
            break;
        }
        for pid in &pusher_pids {
            signal(*pid, "-CONT");
        }
    }

    let hub_log = cluster.nodes[0]
        .rootdir
        .parent()
        .expect("hub rootdir has a parent")
        .join("logs/stdout.log");
    let registered = registered_doc_ids(&hub_log);
    assert!(
        !registered.is_empty(),
        "hub never durably registered a pending DAG before the kill"
    );
    eprintln!(
        "issue1154_repro: {} durably registered documents at kill time",
        registered.len()
    );

    cluster.nodes[0].process.kill();
    stop_writers.store(true, Ordering::Relaxed);

    cluster
        .restart_node(0, Duration::from_secs(60))
        .await
        .expect("restart hub on its rootdir");

    for pid in &pusher_pids {
        signal(*pid, "-CONT");
    }
    for handle in writer_handles {
        handle.join().expect("writer thread panicked");
    }
    let expected_doc_ids = Arc::try_unwrap(doc_ids)
        .expect("writers joined")
        .into_inner()
        .unwrap();
    assert!(
        expected_doc_ids.len() >= PUSHERS,
        "writers never produced load"
    );
    eprintln!(
        "issue1154_repro: {} committed documents expected on restarted hub",
        expected_doc_ids.len()
    );

    // Anti-vacuity for the recovery path: durable registrations must have
    // survived the kill and been re-driven. Without persistence this log
    // (emitted only when records were loaded) never appears.
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

    // Every document the hub durably registered must merge on the restarted
    // hub. Those are the roots whose success ack destroyed the pusher's retry
    // record, so a lost registration strands them with no owner at all -- the
    // #1154 failure. The roots that were actionably nacked instead keep sender
    // markers on the Go-compatible 30s..32m ladder; with a one-slot receiver,
    // requiring hundreds of those to traverse the ladder inside this test's
    // four-minute bound would test wall-clock tuning rather than crash
    // durability, so they are not required to arrive here.
    //
    // The aggregate below is a coverage smoke check only. It cannot carry the
    // per-document claim: `merged` is a set of document ids, but the receiver
    // and sender terms are opaque counts (root-CID cardinality and a
    // `/rep/retry/doc/` prefix scan), so one unit of overlap would buy one
    // unit of slack and hide exactly one orphan. Overlap is real here: the
    // receiver commits its registration before acknowledging the push, so a
    // hub killed in between leaves a document owned twice, by the restored
    // obligation and by the sender marker its unacknowledged push retained.
    let hub = cluster.client(0);
    let converge_start = Instant::now();
    let deadline = Instant::now() + Duration::from_secs(240);
    let mut next_progress_log = Instant::now() + Duration::from_secs(30);
    loop {
        // Bracket the receiver observation with sender snapshots. Marker
        // ownership only decreases after the writers stop, so equal endpoint
        // samples prove that no sender-to-receiver transfer crossed this
        // observation. Likewise, stable receiver terminal/pending counters
        // prove that the document query did not straddle pending-to-merged
        // discharge. A single unbracketed pass can otherwise count one
        // obligation twice (or not at all) across these independent HTTP
        // surfaces even though the protocol conserved it exactly.
        let (sender_markers_before, sender_jobs_before) = sender_retry_snapshot(&cluster).await;
        let hub_status_before = sync_status(&cluster, 0).await;
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
        let hub_status_after = sync_status(&cluster, 0).await;
        let (sender_markers_after, sender_jobs_after) = sender_retry_snapshot(&cluster).await;

        let receiver_obligations = hub_status_after["persisted_pending_dags"]
            .as_u64()
            .expect("persisted pending count") as usize;
        let receiver_terminal_merges = hub_status_after["pending_dag_terminal_merged"]
            .as_u64()
            .expect("terminal merge count");
        let receiver_stable = hub_status_before["persisted_pending_dags"]
            == hub_status_after["persisted_pending_dags"]
            && hub_status_before["pending_dag_terminal_merged"]
                == hub_status_after["pending_dag_terminal_merged"];
        let sender_stable = sender_markers_before == sender_markers_after
            && sender_jobs_before == 0
            && sender_jobs_after == 0;

        let merged = expected_doc_ids
            .iter()
            .filter(|id| present.contains(id.as_str()))
            .count();
        let orphaned: Vec<&str> = registered
            .iter()
            .filter(|doc_id| !present.contains(doc_id.as_str()))
            .map(String::as_str)
            .collect();

        // A dropped obligation must not pass as a merge: both of these retire
        // a root without one, and no amount of further waiting recovers it.
        let fetch_exhausted = hub_status_after["pending_dag_fetch_exhausted"]
            .as_u64()
            .expect("fetch exhausted count");
        let quarantined = hub_status_after["pending_dag_terminal_quarantined"]
            .as_u64()
            .expect("terminal quarantine count");
        assert_eq!(
            (fetch_exhausted, quarantined),
            (0, 0),
            "restored registration retired without merging: exhausted={fetch_exhausted}, \
             quarantined={quarantined}, orphaned={}/{}",
            orphaned.len(),
            registered.len()
        );

        let covered =
            merged + receiver_obligations + sender_markers_after >= expected_doc_ids.len();
        if orphaned.is_empty() && receiver_terminal_merges > 0 && sender_stable && receiver_stable {
            assert!(
                covered,
                "committed documents lost their owner across the restart: merged={merged}, \
                 receiver={receiver_obligations}, sender={sender_markers_after}, expected={}",
                expected_doc_ids.len()
            );
            eprintln!(
                "issue1154_repro: all {} durable registrations merged; \
                 merged={merged}, receiver={receiver_obligations}, sender={sender_markers_after} \
                 {:.1}s after restart",
                registered.len(),
                converge_start.elapsed().as_secs_f64()
            );
            break;
        }
        if Instant::now() >= next_progress_log {
            eprintln!(
                "issue1154_repro: orphaned={}/{}, merged={merged}, \
                 receiver={receiver_obligations}, sender={sender_markers_after}, \
                 terminal_merges={receiver_terminal_merges}, \
                 stable={sender_stable}/{receiver_stable} after {:.1}s",
                orphaned.len(),
                registered.len(),
                converge_start.elapsed().as_secs_f64()
            );
            next_progress_log += Duration::from_secs(30);
        }
        assert!(
            Instant::now() < deadline,
            "durably registered documents never merged on the restarted hub: \
             orphaned={}/{} {:?}, merged={merged}, receiver={receiver_obligations}, \
             sender={sender_markers_after}, expected={}, \
             terminal_merges={receiver_terminal_merges}, \
             stable={sender_stable}/{receiver_stable}",
            orphaned.len(),
            registered.len(),
            orphaned.iter().take(8).collect::<Vec<_>>(),
            expected_doc_ids.len()
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
