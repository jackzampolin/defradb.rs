//! #1128 end-to-end fence: terminal-failure quarantine under fleet-mirroring
//! conditions.
//!
//! Mirrors the shape of the 2026-07-14 fleet incident (hub "Amy" held 6 stuck
//! heads: docs whose unique `session_id` index rejected an incoming twin,
//! re-driven by the 60s resync sweep forever) rather than a minimal repro:
//! a 3-node fan-in (hub B, spokes A and C) with a UNIQUE index, a genuine
//! content collision under concurrent *sound* traffic, and a hub restart that
//! exercises the same resync path the incident's 60s sweep used, on a fast
//! reconnect-triggered timescale instead of waiting out the real interval.
//!
//! Phase 1 (conflict under concurrent sound traffic): B locally owns a
//! `session_id` value under its own unique index. Spoke A pushes a twin
//! document with the identical value over gossip (so the rejection runs
//! through the real pending-DAG -> DagReady -> merge path, not a bundled
//! push that might never touch the live queue) while spoke C concurrently
//! fans in several unrelated, sound documents via a replicator. Asserts the
//! twin is quarantined (counted + durably recorded), sound traffic is
//! unharmed by the poisoned neighbor, and the live `pending_dags` queue
//! drains to 0 while the quarantine gauge does not — the load-bearing
//! contrast with the pre-fix behavior, where the rejected root would stay
//! stuck in the live retry queue forever.
//!
//! Phase 2 (durability across restart): continuous unrelated writes from A
//! travel the same missing-link path until B holds a live registration, then
//! A is frozen mid-fetch (SIGSTOP) so a genuine durable pending-DAG record
//! exists when B is hard-killed and respawned on its own rootdir. On
//! reconnect, `handle_peer_connected` triggers
//! `resync_persisted_pending_dags`, which must (a) restore the genuine
//! frozen registrations and drive them to a real merge (anti-vacuity: the
//! restore log fires AND both pending gauges drain to 0) while (b) NOT
//! re-registering the quarantined root — proven by the fresh process's
//! `pending_dag_terminal_quarantined` counter staying at 0 across a bounded
//! window and through the recovery drain, since any re-registration would
//! re-attempt the merge and re-hit the same deterministic rejection.
//!
//! Run with `DEFRA_E2E_KEEP=1` to retain node directories (including
//! `logs/stdout.log` per node under `target/e2e/`) for post-mortem when a run
//! fails — the harness normally cleans them up on success.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use integration_test::{extract_doc_id, extract_p2p_addr, poll_until, TestCluster};
use serde_json::Value;

const SCHEMA: &str = "type Session { session_id: String  note: String }";
const HUB: usize = 0;
const SPOKE_A: usize = 1;
const SPOKE_C: usize = 2;
const CONFLICT_VALUE: &str = "conflict-v1";
const SOUND_DOC_COUNT: usize = 5;

fn signal(pid: u32, signal: &str) {
    let status = std::process::Command::new("kill")
        .arg(signal)
        .arg(pid.to_string())
        .status()
        .expect("spawn kill");
    assert!(status.success(), "kill {signal} {pid} failed");
}

/// The quarantine-relevant subset of `/api/v0/p2p/sync/status` (#1128).
struct SyncStatusSnapshot {
    pending_dags: usize,
    persisted_pending_dags: usize,
    pending_dag_terminal_quarantined: u64,
    quarantined_pending_dags: usize,
}

async fn fetch_sync_status(cluster: &TestCluster, node: usize) -> SyncStatusSnapshot {
    let status: Value = reqwest::get(format!("{}/api/v0/p2p/sync/status", cluster.api_url(node)))
        .await
        .expect("sync status request")
        .json()
        .await
        .expect("sync status json");

    SyncStatusSnapshot {
        pending_dags: status["pending_dags"]
            .as_u64()
            .expect("pending_dags field present") as usize,
        persisted_pending_dags: status["persisted_pending_dags"]
            .as_u64()
            .expect("persisted_pending_dags field present")
            as usize,
        pending_dag_terminal_quarantined: status["pending_dag_terminal_quarantined"]
            .as_u64()
            .expect("pending_dag_terminal_quarantined field present"),
        quarantined_pending_dags: status["quarantined_pending_dags"]
            .as_u64()
            .expect("quarantined_pending_dags field present")
            as usize,
    }
}

fn doc_ids_present(client: &integration_test::DefraClient) -> HashSet<String> {
    let result = client
        .query("query { Session { _docID } }")
        .unwrap_or_default();
    result["Session"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v["_docID"].as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

fn hub_log_path(cluster: &TestCluster) -> std::path::PathBuf {
    cluster.nodes[HUB]
        .rootdir
        .parent()
        .expect("hub rootdir has a parent")
        .join("logs/stdout.log")
}

/// 3-node fan-in (hub B, spokes A and C), a real unique-index collision under
/// concurrent sound traffic, then a hub restart that must not re-drive the
/// quarantined twin while still recovering a genuinely in-flight registration.
#[tokio::test]
async fn quarantine_survives_fan_in_and_hub_restart() {
    let mut cluster = TestCluster::builder()
        .rust_nodes(3)
        .with_node_store(HUB, "redb")
        .with_keyring()
        .with_p2p()
        .build()
        .await
        .expect("cluster start");

    let startup_timeout = Duration::from_secs(30);
    for node in [HUB, SPOKE_A, SPOKE_C] {
        cluster
            .wait_for_log(node, "p2p_listening", startup_timeout)
            .await
            .unwrap_or_else(|e| panic!("node{node} P2P listener did not start: {e}"));
    }

    let node_b = cluster.client(HUB);
    let node_a = cluster.client(SPOKE_A);
    let node_c = cluster.client(SPOKE_C);

    node_b.schema_add(SCHEMA).expect("schema add B");
    node_a.schema_add(SCHEMA).expect("schema add A");
    node_c.schema_add(SCHEMA).expect("schema add C");

    let addr_b = extract_p2p_addr(&cluster, HUB);
    node_a.p2p_connect(&[&addr_b]).expect("A connects to B");
    node_c.p2p_connect(&[&addr_b]).expect("C connects to B");

    // Subscribe every node to the collection topic: A's twin push (and,
    // later, its frozen post-restart document) deliberately travels the
    // gossip head-announcement path rather than a bundled replicator push, so
    // the receiver must register a pending-DAG entry and Bitswap-fetch the
    // missing field block — the same mechanism the real incident's stuck
    // heads went through, and the only way `pending_dags` genuinely carries
    // the rejected root before quarantine removes it.
    node_b
        .p2p_collection_add(&["Session"])
        .expect("B subscribe");
    node_a
        .p2p_collection_add(&["Session"])
        .expect("A subscribe");
    node_c
        .p2p_collection_add(&["Session"])
        .expect("C subscribe");

    // C fans in via an explicit replicator: deterministic, fast convergence
    // for the "sound traffic unharmed by the poison neighbor" assertion,
    // independent of the gossip mechanism forcing A's twin through the
    // pending-DAG path.
    node_c
        .p2p_replicator_set(&["Session"], &addr_b)
        .expect("replicator C -> B");

    // --- Phase 1: conflict under concurrent sound traffic ---

    // B's unique index is created before any data lands, then B writes its
    // own local doc under the value it owns — the "pre-fix-era twin" setup:
    // a hub that already holds v under an active unique index.
    node_b
        .index_create(
            "Session",
            &["session_id"],
            Some("idx_session_id_unique"),
            true,
        )
        .expect("create unique index on B");
    let x_create = node_b
        .query(&format!(
            r#"mutation {{ add_Session(input: {{session_id: "{CONFLICT_VALUE}", note: "hub-owned"}}) {{ _docID }} }}"#
        ))
        .expect("create X on B");
    let x_doc_id = extract_doc_id(&x_create, "add_Session");

    // A pushes a twin with the identical value; concurrently C fans in
    // several sound documents with distinct values. Real race, not a
    // sequenced repro: both threads fire at the same time.
    let (y_doc_id, sound_doc_ids) = std::thread::scope(|scope| {
        let y_handle = scope.spawn(|| {
            let create = node_a
                .query(&format!(
                    r#"mutation {{ add_Session(input: {{session_id: "{CONFLICT_VALUE}", note: "spoke-twin"}}) {{ _docID }} }}"#
                ))
                .expect("create Y on A");
            extract_doc_id(&create, "add_Session")
        });
        let c_handle = scope.spawn(|| {
            (0..SOUND_DOC_COUNT)
                .map(|i| {
                    let create = node_c
                        .query(&format!(
                            r#"mutation {{ add_Session(input: {{session_id: "sound-{i}", note: "c-sound"}}) {{ _docID }} }}"#
                        ))
                        .expect("create sound doc on C");
                    extract_doc_id(&create, "add_Session")
                })
                .collect::<Vec<String>>()
        });
        (
            y_handle.join().expect("A thread panicked"),
            c_handle.join().expect("C thread panicked"),
        )
    });
    assert_eq!(
        sound_doc_ids.len(),
        SOUND_DOC_COUNT,
        "C never produced its sound-doc load"
    );

    // Anti-vacuity: the scenario must have produced a deterministic merge
    // rejection, not just eventual quiescence.
    let quarantine_deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let status = fetch_sync_status(&cluster, HUB).await;
        if status.pending_dag_terminal_quarantined >= 1 && status.quarantined_pending_dags >= 1 {
            eprintln!(
                "[quarantine e2e] phase 1: twin rejected+quarantined \
                 (pending_dag_terminal_quarantined={}, quarantined_pending_dags={}, \
                 pending_dags={})",
                status.pending_dag_terminal_quarantined,
                status.quarantined_pending_dags,
                status.pending_dags
            );
            break;
        }
        assert!(
            Instant::now() < quarantine_deadline,
            "the scenario did not produce a deterministic merge rejection: B never quarantined \
             the twin session_id={CONFLICT_VALUE} push (pending_dag_terminal_quarantined={}, \
             quarantined_pending_dags={})",
            status.pending_dag_terminal_quarantined,
            status.quarantined_pending_dags
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // The load-bearing realism: sound traffic is unharmed by the poisoned
    // neighbor. All of C's docs converge on B while the rejection is (or was
    // just) being handled.
    poll_until(
        || {
            let present = doc_ids_present(&node_b);
            sound_doc_ids.iter().all(|id| present.contains(id))
        },
        Duration::from_secs(30),
        Duration::from_millis(200),
        "C's sound docs did not all converge on B while the twin rejection was in flight",
    )
    .await;

    // The quarantined root must leave the live retry queue permanently — the
    // exact contrast with the pre-fix incident, where it would stay stuck in
    // `pending_dags`, re-driven by the periodic sweep forever.
    let drain_deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let status = fetch_sync_status(&cluster, HUB).await;
        if status.pending_dags == 0 {
            assert!(
                status.quarantined_pending_dags >= 1,
                "pending_dags drained to 0 but quarantined_pending_dags also dropped to {} \
                 (the quarantine gauge must persist once a root is quarantined)",
                status.quarantined_pending_dags
            );
            break;
        }
        assert!(
            Instant::now() < drain_deadline,
            "B's live pending_dags gauge never drained to 0 ({} still pending) — a quarantined \
             root must leave the live retry queue instead of being re-driven",
            status.pending_dags
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Data-level confirmation: the rejected twin never lands, and the hub's
    // own value survives the conflicting spoke push unchanged.
    let rows = node_b
        .query("query { Session { _docID session_id note } }")
        .expect("query B Session")["Session"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert!(
        !rows
            .iter()
            .any(|r| r["_docID"].as_str() == Some(y_doc_id.as_str())),
        "rejected twin doc Y ({y_doc_id}) must never merge onto B's Session collection"
    );
    let x_row = rows
        .iter()
        .find(|r| r["_docID"].as_str() == Some(x_doc_id.as_str()))
        .expect("hub's own doc X missing from its own collection");
    assert_eq!(
        x_row["note"].as_str(),
        Some("hub-owned"),
        "hub's locally-owned value must survive the conflicting spoke push unchanged"
    );

    // --- Phase 2: durability across restart ---

    let quarantined_before = fetch_sync_status(&cluster, HUB)
        .await
        .quarantined_pending_dags;
    assert!(
        quarantined_before >= 1,
        "phase 1 must leave a quarantined root behind"
    );

    // Force a genuine, still-open pending-DAG registration to exist at kill
    // time. A single fresh document races the freeze — Bitswap can resolve
    // the missing field block before the SIGSTOP even lands, as observed
    // empirically (single-shot attempts never landed). Instead, drive
    // continuous head-only load from A over the same missing-link gossip
    // mechanism proven above for Y, so at least one registration is reliably
    // mid-flight whenever the poll samples it (mirrors
    // p2p_admission_restart.rs's continuous-writer-then-freeze technique,
    // adapted from replicator pushes to gossip-forced ones).
    let a_pid = cluster.nodes[SPOKE_A].process.id().expect("A pid");
    let node_a = cluster.client(SPOKE_A);
    let stop_writer = Arc::new(AtomicBool::new(false));
    let w_doc_ids = Arc::new(Mutex::new(Vec::<String>::new()));
    let writer_handle = {
        let client = cluster.client(SPOKE_A);
        let stop = Arc::clone(&stop_writer);
        let doc_ids = Arc::clone(&w_doc_ids);
        std::thread::spawn(move || {
            let mut n = 0usize;
            while !stop.load(Ordering::Relaxed) {
                n += 1;
                let mutation = format!(
                    r#"mutation {{ add_Session(input: {{session_id: "post-restart-w-{n}", note: "v0"}}) {{ _docID }} }}"#
                );
                if let Ok(create) = client.query(&mutation) {
                    doc_ids
                        .lock()
                        .unwrap()
                        .push(extract_doc_id(&create, "add_Session"));
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        })
    };

    let freeze_deadline = Instant::now() + Duration::from_secs(60);
    loop {
        assert!(
            Instant::now() < freeze_deadline,
            "hub never showed a live pending-DAG registration under continuous gossip-forced \
             load from A; without one, restarting B would prove nothing about restoring real \
             (not synthetic) durable state"
        );
        if fetch_sync_status(&cluster, HUB).await.pending_dags == 0 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        }
        signal(a_pid, "-STOP");
        // Let any in-flight fetch settle: whatever can still resolve,
        // resolves now (mirrors p2p_admission_restart.rs's freeze window).
        tokio::time::sleep(Duration::from_millis(500)).await;
        if fetch_sync_status(&cluster, HUB).await.pending_dags >= 1 {
            break;
        }
        signal(a_pid, "-CONT");
    }

    // Anti-vacuity input: a durable record must exist before the kill, or
    // the restore log below would have nothing genuine to restore.
    let pre_kill_status = fetch_sync_status(&cluster, HUB).await;
    assert!(
        pre_kill_status.persisted_pending_dags >= 1,
        "no durable pending-DAG record existed before the kill (persisted_pending_dags={}) — \
         the restart would prove nothing about the resync path",
        pre_kill_status.persisted_pending_dags
    );
    eprintln!(
        "[quarantine e2e] phase 2 pre-kill: pending_dags={}, persisted_pending_dags={}, \
         quarantined_pending_dags={}",
        pre_kill_status.pending_dags,
        pre_kill_status.persisted_pending_dags,
        pre_kill_status.quarantined_pending_dags
    );

    // Snapshot B's merged documents right before the kill: everything B held
    // must survive the restart on its redb rootdir.
    let pre_kill_docs = doc_ids_present(&node_b);

    cluster.nodes[HUB].process.kill();
    stop_writer.store(true, Ordering::Relaxed);

    cluster
        .restart_node(HUB, Duration::from_secs(60))
        .await
        .expect("restart hub on its rootdir");

    signal(a_pid, "-CONT");
    writer_handle.join().expect("A writer thread panicked");
    let w_doc_ids = Arc::try_unwrap(w_doc_ids)
        .expect("writer joined")
        .into_inner()
        .unwrap();
    assert!(
        !w_doc_ids.is_empty(),
        "A's writer thread never produced load"
    );

    node_a.p2p_connect(&[&addr_b]).expect("A reconnect to B");
    node_c.p2p_connect(&[&addr_b]).expect("C reconnect to B");

    // Anti-vacuity for the restart itself: the resync path must have
    // demonstrably run and done real work (mirrors p2p_admission.rs's
    // "hub never hit capacity" pattern — grep an emitted-only-when-true log
    // line instead of trusting quiescence).
    let hub_log = hub_log_path(&cluster);
    let restore_deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let log = std::fs::read_to_string(&hub_log).unwrap_or_default();
        if log.contains("restored persisted pending DAG registrations") {
            break;
        }
        assert!(
            Instant::now() < restore_deadline,
            "resync path demonstrably never ran after hub restart+reconnect: \"restored \
             persisted pending DAG registrations\" never logged (persisted_pending_dags was {} \
             before the kill, so there was genuine work for it to restore)",
            pre_kill_status.persisted_pending_dags
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    // The quarantine gauge must survive the restart via hydration from
    // `load_quarantined()`, not just in-memory bookkeeping that a fresh
    // process would reset to 0.
    let post_restart_status = fetch_sync_status(&cluster, HUB).await;
    assert!(
        post_restart_status.quarantined_pending_dags >= quarantined_before,
        "quarantined_pending_dags did not survive the restart: was {quarantined_before} before \
         the kill, {} after restart (must hydrate from load_quarantined at startup)",
        post_restart_status.quarantined_pending_dags
    );
    eprintln!(
        "[quarantine e2e] phase 2 post-restart: quarantined_pending_dags={} (was \
         {quarantined_before} pre-kill), pending_dags={}, persisted_pending_dags={}, \
         fresh-process pending_dag_terminal_quarantined={}",
        post_restart_status.quarantined_pending_dags,
        post_restart_status.pending_dags,
        post_restart_status.persisted_pending_dags,
        post_restart_status.pending_dag_terminal_quarantined
    );

    // The quarantined root must NOT be re-registered and re-driven. Because
    // `pending_dag_terminal_quarantined` is a fresh-process counter (reset to
    // 0 by the restart, unlike the durable gauge above), any re-registration
    // of the quarantined root would attempt to re-merge it, re-hit the same
    // deterministic rejection, and tick this counter off 0 — over a window
    // spanning several 2s retry-clock ticks.
    let flat_window_deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < flat_window_deadline {
        let status = fetch_sync_status(&cluster, HUB).await;
        assert_eq!(
            status.pending_dag_terminal_quarantined, 0,
            "the quarantined twin was re-registered and re-rejected after restart (fresh-process \
             pending_dag_terminal_quarantined moved off 0) — suppression did not hold across \
             restart+reconnect"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // The restored registrations must actually RESOLVE, not just restore:
    // both pending gauges drain to 0 once B re-fetches the frozen roots'
    // missing blocks from the reconnected A. Combined with the fresh-process
    // quarantine counter staying 0 (checked above and re-checked below), a
    // drained durable gauge proves every restored root exited through a
    // successful merge — the only other counted exit is quarantine.
    //
    // Note deliberately NOT asserted: that every document A's writer created
    // lands on B. A's docs travel by gossip, which is best-effort — an
    // announcement published while B was dead (or before A re-established
    // the connection) carries no re-delivery obligation. The durable
    // recovery contract covers exactly the roots B had *registered*, and
    // that is what the gauge drain verifies. (C's docs below are different:
    // its replicator has a persistent retry ladder, so full convergence IS
    // its contract.) This poll runs before C's post-restart pushes so fresh
    // registrations from C cannot hold the gauges up.
    let drain_recovery_deadline = Instant::now() + Duration::from_secs(45);
    loop {
        let status = fetch_sync_status(&cluster, HUB).await;
        if status.pending_dags == 0 && status.persisted_pending_dags == 0 {
            break;
        }
        assert!(
            Instant::now() < drain_recovery_deadline,
            "restored pending-DAG registrations never resolved after restart+reconnect \
             (pending_dags={}, persisted_pending_dags={}; {} durable records existed pre-kill)",
            status.pending_dags,
            status.persisted_pending_dags,
            pre_kill_status.persisted_pending_dags
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Post-restart sound traffic from C (replicator path, persistent retry
    // ladder) still converges on the restarted hub.
    let post_restart_c_1 = node_c
        .query(r#"mutation { add_Session(input: {session_id: "post-restart-c-1", note: "c-sound"}) { _docID } }"#)
        .expect("create post-restart doc on C");
    let post_restart_c_1_id = extract_doc_id(&post_restart_c_1, "add_Session");
    let post_restart_c_2 = node_c
        .query(r#"mutation { add_Session(input: {session_id: "post-restart-c-2", note: "c-sound"}) { _docID } }"#)
        .expect("create post-restart doc on C");
    let post_restart_c_2_id = extract_doc_id(&post_restart_c_2, "add_Session");

    let node_b_after = cluster.client(HUB);
    poll_until(
        || {
            let present = doc_ids_present(&node_b_after);
            present.contains(&post_restart_c_1_id) && present.contains(&post_restart_c_2_id)
        },
        Duration::from_secs(30),
        Duration::from_millis(200),
        "post-restart sound traffic from C did not converge on B after reconnect",
    )
    .await;

    // Final state: everything B held pre-kill survived the restart, the
    // quarantined twin stays gone (durable, not just in-memory), and the
    // recovery drain above did not exit through a fresh quarantine.
    let present_final = doc_ids_present(&node_b_after);
    let lost: Vec<&String> = pre_kill_docs
        .iter()
        .filter(|id| !present_final.contains(id.as_str()))
        .collect();
    assert!(
        lost.is_empty(),
        "{} of {} documents merged before the kill are missing after restart: {:?}",
        lost.len(),
        pre_kill_docs.len(),
        lost
    );
    assert!(
        !present_final.contains(&y_doc_id),
        "quarantined twin Y reappeared on B after restart — quarantine must be durable"
    );
    let final_status = fetch_sync_status(&cluster, HUB).await;
    assert_eq!(
        final_status.pending_dag_terminal_quarantined, 0,
        "recovery drained through a fresh quarantine, not through merges"
    );
    assert!(
        final_status.quarantined_pending_dags >= quarantined_before,
        "durable quarantine gauge decayed by the end of the run: {} < {quarantined_before}",
        final_status.quarantined_pending_dags
    );
    eprintln!(
        "[quarantine e2e] final: quarantined_pending_dags={}, fresh-process \
         pending_dag_terminal_quarantined={}, pending_dags={}, docs on B={} \
         ({} pre-kill docs all retained, twin absent)",
        final_status.quarantined_pending_dags,
        final_status.pending_dag_terminal_quarantined,
        final_status.pending_dags,
        present_final.len(),
        pre_kill_docs.len()
    );
}
