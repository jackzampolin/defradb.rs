//! #1126 x #1128 composition fence: canonical-pick convergence with the
//! quarantine guard staying silent.
//!
//! Mirrors the shape of the 2026-07-14 fleet incident (hub "Amy" held 6
//! stuck heads: docs whose unique `session_id` index rejected an incoming
//! twin, re-driven by the 60s resync sweep forever) rather than a minimal
//! repro — 3-node fan-in (hub B, spokes A and C), a UNIQUE index, a genuine
//! content collision under concurrent *sound* traffic, and a hub restart —
//! but proves the POST-#1126 outcome, not the pre-#1126 one.
//!
//! #1126 changed what a live twin unique-index conflict does on the merge
//! path: instead of rejecting (the incident's trigger), it now resolves
//! deterministically — both documents persist, and the lexicographically
//! smallest docID owns the unique index entry (the losing document stays
//! fully readable by scan queries, just not by the index). #1128's
//! quarantine mechanism is trigger-agnostic: it only acts on whatever
//! `MergeOutcome::Rejected` the classification seam still produces. Composed
//! together, the question this fence answers is: does #1126's canonical
//! pick actually convergence through the real fan-in/gossip/merge stack
//! (not just the db-merge unit level), and does #1128's guard stay quiet
//! while it does — i.e. does the fleet incident's *signature* (a stuck head,
//! re-driven forever, `pending_dag_terminal_quarantined` climbing) stay dead
//! under the healed semantics, with no false-positive quarantine of a
//! conflict that now resolves cleanly?
//!
//! Quarantine's OWN producer-level proof — that a `Rejected` outcome (today:
//! only the degenerate arms `IndexManager::conflicting_doc_id` can't
//! identify a holder for) still gets quarantined and never re-driven — lives
//! in the crate tests, not here: `crates/db-merge/src/merge_handler/mod.rs`
//! (`remote_composite_merge_with_corrupted_unique_index_entry_is_rejected`)
//! for the degenerate-arm classification, and
//! `crates/p2p/src/sync/manager/process/pending_dag.rs`
//! (`quarantine_pending_dag_*`, `resync_deletes_live_leftover_of_quarantined_root_*`)
//! for the disposition/suppression mechanics. Post-#1126, no public-API path
//! in this integration harness can manufacture a genuine `Rejected` outcome
//! any more (the live-twin trigger that could is exactly what #1126 healed),
//! so an e2e fence asserting quarantine's OWN behavior would have no way to
//! trigger it without reaching into internals the harness cannot touch —
//! this fence instead proves the two changes compose correctly at the
//! system level, which is the property only an e2e test can check.
//!
//! Phase 1 (composed convergence under concurrent sound traffic): B locally
//! owns a `session_id` value under its own unique index. Spoke A pushes a
//! twin document with the identical value over gossip (so it runs through
//! the real pending-DAG -> DagReady -> merge path, not a bundled push that
//! might never touch the live queue) while spoke C concurrently fans in
//! several unrelated, sound documents via a replicator. Asserts: both
//! twins persist (docID-level presence — canonical pick never drops data);
//! the unique-index lookup on the shared value converges to exactly the
//! lexicographically-smaller docID (computed independently in the test);
//! sound traffic is unharmed by the colliding neighbor; the quarantine
//! guard never fires (`pending_dag_terminal_quarantined == 0`,
//! `quarantined_pending_dags == 0`); and the live `pending_dags` queue
//! still drains to 0 — no wedge, the incident-class signature stays dead.
//!
//! Phase 2 (restart durability of composed state): B is hard-killed
//! (SIGKILL) and respawned on its own rootdir. Asserts the composed state
//! (both twins present, indexed lookup still resolving to the same winner)
//! and the silent guard (`pending_dag_terminal_quarantined == 0`,
//! `quarantined_pending_dags == 0`) both survive the restart, and that
//! ordinary post-restart sound traffic from A and C still converges.
//!
//! Run with `DEFRA_E2E_KEEP=1` to retain node directories (including
//! `logs/stdout.log` per node under `target/e2e/`) for post-mortem when a run
//! fails — the harness normally cleans them up on success.

use std::collections::HashSet;
use std::time::{Duration, Instant};

use integration_test::{extract_doc_id, extract_p2p_addr, poll_until, TestCluster};
use serde_json::Value;

const SCHEMA: &str = "type Session { session_id: String  note: String }";
const HUB: usize = 0;
const SPOKE_A: usize = 1;
const SPOKE_C: usize = 2;
const CONFLICT_VALUE: &str = "conflict-v1";
const SOUND_DOC_COUNT: usize = 5;

/// The quarantine-relevant subset of `/api/v0/p2p/sync/status` (#1128).
struct SyncStatusSnapshot {
    pending_dags: usize,
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
        pending_dag_terminal_quarantined: status["pending_dag_terminal_quarantined"]
            .as_u64()
            .expect("pending_dag_terminal_quarantined field present"),
        quarantined_pending_dags: status["quarantined_pending_dags"]
            .as_u64()
            .expect("quarantined_pending_dags field present")
            as usize,
    }
}

/// Every `_docID` currently present in the `Session` collection.
///
/// Deliberately panics (rather than defaulting to an empty set) on a query
/// error or an unexpected response shape: a transient failure here must
/// fail the test loudly, not silently masquerade as "no documents present"
/// and let a retention assertion downstream pass vacuously against an
/// empty snapshot (review finding, #1128 task 8).
fn doc_ids_present(client: &integration_test::DefraClient) -> HashSet<String> {
    let result = client
        .query("query { Session { _docID } }")
        .expect("Session _docID query failed");
    result["Session"]
        .as_array()
        .unwrap_or_else(|| {
            panic!("Session _docID query did not return an array; full response: {result}")
        })
        .iter()
        .filter_map(|v| v["_docID"].as_str().map(str::to_string))
        .collect()
}

/// docIDs returned by an indexed lookup on `session_id`, driving the same
/// error-loudly contract as [`doc_ids_present`].
fn indexed_session_lookup(client: &integration_test::DefraClient, value: &str) -> Vec<String> {
    let result = client
        .query(&format!(
            r#"query {{ Session(filter: {{session_id: {{_eq: "{value}"}}}}) {{ _docID }} }}"#
        ))
        .expect("indexed session_id lookup query failed");
    result["Session"]
        .as_array()
        .unwrap_or_else(|| {
            panic!("indexed session_id lookup did not return an array; full response: {result}")
        })
        .iter()
        .filter_map(|v| v["_docID"].as_str().map(str::to_string))
        .collect()
}

/// 3-node fan-in (hub B, spokes A and C): a real unique-index collision
/// under concurrent sound traffic converges via #1126's canonical pick
/// without #1128's quarantine guard misfiring, and both properties survive
/// a hub restart.
#[tokio::test]
async fn canonical_pick_converges_and_quarantine_guard_stays_silent() {
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

    // Subscribe every node to the collection topic: A's twin push travels
    // the gossip head-announcement path rather than a bundled replicator
    // push, so the receiver must register a pending-DAG entry and
    // Bitswap-fetch the missing field block — the same mechanism the real
    // incident's stuck heads went through, and the only way the merge
    // genuinely runs through the pending-DAG -> DagReady path instead of a
    // bundled push that might never touch it.
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
    // for the "sound traffic unharmed by the colliding neighbor" assertion,
    // independent of the gossip mechanism forcing A's twin through the
    // pending-DAG path.
    node_c
        .p2p_replicator_set(&["Session"], &addr_b)
        .expect("replicator C -> B");

    // --- Phase 1: composed convergence under concurrent sound traffic ---

    // B's unique index is created before any data lands, then B writes its
    // own local doc under the value it owns — the same setup that, pre
    // #1126, produced the incident's twin rejection; post #1126 it instead
    // sets up a genuine live conflict for the canonical pick to resolve.
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

    // The deterministic winner per #1126: the lexicographically smaller
    // docID (computed here independently of production code, mirroring
    // `IndexManager::save_resolving_unique_conflict`'s `doc_id <
    // holder.as_str()` comparison in
    // crates/db-index/src/index_manager/mod.rs). Arrival order does not
    // matter — the pick is a pure function of the two docIDs.
    let winner_doc_id = if x_doc_id < y_doc_id {
        x_doc_id.clone()
    } else {
        y_doc_id.clone()
    };
    eprintln!(
        "[compose fence] phase 1: X={x_doc_id}, Y={y_doc_id}, computed winner={winner_doc_id}"
    );

    // Composed convergence, polled together: BOTH twins present at the
    // docID level (the positive evidence that a genuine collision
    // occurred and that the CRDT merge never dropped data — anti-vacuity:
    // if Y never shows up, the scenario never exercised the collision at
    // all), C's sound docs all converge, and the unique-index lookup on
    // the shared value resolves to exactly the computed winner (not zero,
    // not both — exactly one).
    let convergence_deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let present = doc_ids_present(&node_b);
        let indexed = indexed_session_lookup(&node_b, CONFLICT_VALUE);
        let x_present = present.contains(&x_doc_id);
        let y_present = present.contains(&y_doc_id);
        let sound_converged = sound_doc_ids.iter().all(|id| present.contains(id));
        let index_converged = indexed == [winner_doc_id.clone()];

        if x_present && y_present && sound_converged && index_converged {
            eprintln!(
                "[compose fence] phase 1 converged: both twins present, {} sound docs converged, \
                 indexed lookup for {CONFLICT_VALUE} returns exactly the winner {winner_doc_id}",
                sound_doc_ids.len()
            );
            break;
        }

        if Instant::now() >= convergence_deadline {
            assert!(
                y_present,
                "anti-vacuity failure: twin Y ({y_doc_id}) never arrived on B — the scenario did \
                 not exercise the unique-index collision at all, so this run proves nothing about \
                 #1126 x #1128 composition"
            );
            assert!(
                x_present,
                "hub's own locally-created doc X ({x_doc_id}) is missing from B"
            );
            assert!(
                sound_converged,
                "C's sound docs did not all converge on B while the twin collision was resolving"
            );
            assert!(
                index_converged,
                "unique-index lookup for {CONFLICT_VALUE} did not converge to exactly the \
                 computed winner {winner_doc_id}: got {indexed:?} — #1126's canonical pick did \
                 not converge through the real fan-in/gossip/merge stack"
            );
            unreachable!(
                "all convergence sub-conditions individually passed but the loop still timed out"
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // The quarantine guard must stay silent: a live twin conflict resolves
    // via #1126's canonical pick now, not via rejection, so #1128's
    // mechanism must never fire for it. `pending_dag_terminal_quarantined`
    // is a monotonic per-process counter, so a single read after
    // convergence is equivalent to it never having moved throughout.
    let status = fetch_sync_status(&cluster, HUB).await;
    assert_eq!(
        status.pending_dag_terminal_quarantined, 0,
        "quarantine guard misfired: {} deterministic rejection(s) counted for a live twin \
         conflict that should have converged via #1126's canonical pick instead of rejecting",
        status.pending_dag_terminal_quarantined
    );
    assert_eq!(
        status.quarantined_pending_dags, 0,
        "quarantine guard misfired: {} root(s) quarantined for a live twin conflict that should \
         have converged via #1126's canonical pick instead of rejecting",
        status.quarantined_pending_dags
    );

    // The incident-class signature stays dead: the live retry queue must
    // still drain to 0 (no wedge), with the quarantine guard remaining
    // silent throughout the drain.
    let drain_deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let status = fetch_sync_status(&cluster, HUB).await;
        if status.pending_dags == 0 {
            assert_eq!(
                status.pending_dag_terminal_quarantined, 0,
                "quarantine guard misfired while draining the live pending-DAG queue"
            );
            assert_eq!(
                status.quarantined_pending_dags, 0,
                "quarantine guard misfired while draining the live pending-DAG queue"
            );
            break;
        }
        assert!(
            Instant::now() < drain_deadline,
            "B's live pending_dags gauge never drained to 0 ({} still pending) — the incident's \
             stuck-head signature is back",
            status.pending_dags
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Data-level confirmation: both documents are fully readable via a
    // non-indexed scan (canonical pick never drops data), and each retains
    // its own content.
    let rows = node_b
        .query("query { Session { _docID session_id note } }")
        .expect("query B Session")["Session"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let x_row = rows
        .iter()
        .find(|r| r["_docID"].as_str() == Some(x_doc_id.as_str()))
        .expect("hub's own doc X missing from a full scan of its own collection");
    assert_eq!(x_row["note"].as_str(), Some("hub-owned"));
    let y_row = rows
        .iter()
        .find(|r| r["_docID"].as_str() == Some(y_doc_id.as_str()))
        .expect("twin doc Y missing from a full scan — canonical pick must not drop data");
    assert_eq!(y_row["note"].as_str(), Some("spoke-twin"));

    // --- Phase 2: restart durability of composed state ---

    // Pre-kill snapshot: everything B holds right now must survive the
    // restart on its redb rootdir. Asserting it contains the known IDs
    // (rather than trusting an unchecked snapshot) is the fix for the
    // review finding: `doc_ids_present` used to swallow query errors into
    // an empty set, which would let the post-restart retention assertion
    // below pass vacuously against an empty "nothing lost" comparison if
    // this snapshot silently came back empty. `doc_ids_present` itself now
    // panics loudly on a query error (see its definition); this assertion
    // is the second line of defense, failing loudly if the snapshot came
    // back incomplete for any other reason.
    let pre_kill_docs = doc_ids_present(&node_b);
    let pre_kill_missing: Vec<&str> = std::iter::once(x_doc_id.as_str())
        .chain(std::iter::once(y_doc_id.as_str()))
        .chain(sound_doc_ids.iter().map(String::as_str))
        .filter(|id| !pre_kill_docs.contains(*id))
        .collect();
    assert!(
        pre_kill_missing.is_empty(),
        "pre-kill snapshot is missing known documents {pre_kill_missing:?} out of {} total docs \
         present — doc_ids_present must never silently return an incomplete set",
        pre_kill_docs.len()
    );

    cluster.nodes[HUB].process.kill();
    cluster
        .restart_node(HUB, Duration::from_secs(60))
        .await
        .expect("restart hub on its rootdir");

    node_a.p2p_connect(&[&addr_b]).expect("A reconnect to B");
    node_c.p2p_connect(&[&addr_b]).expect("C reconnect to B");

    let node_b_after = cluster.client(HUB);

    // Composed state survives the restart: both twins still present, and
    // the indexed lookup still resolves to the same winner (docIDs are
    // immutable, so recomputing the pick from a fresh index on restart is
    // identical to before the kill).
    poll_until(
        || {
            let present = doc_ids_present(&node_b_after);
            let indexed = indexed_session_lookup(&node_b_after, CONFLICT_VALUE);
            present.contains(&x_doc_id)
                && present.contains(&y_doc_id)
                && indexed == [winner_doc_id.clone()]
        },
        Duration::from_secs(45),
        Duration::from_millis(200),
        "composed state (both twins present + indexed winner) did not survive hub restart+reconnect",
    )
    .await;

    // The silent guard survives the restart too: the durable gauge
    // hydrates from `load_quarantined()` at startup (0 in, 0 out — nothing
    // was ever quarantined), and the fresh process's occurrence counter
    // starts at 0 and must stay there.
    let post_restart_status = fetch_sync_status(&cluster, HUB).await;
    assert_eq!(
        post_restart_status.pending_dag_terminal_quarantined, 0,
        "quarantine guard misfired after restart: fresh-process pending_dag_terminal_quarantined \
         moved off 0 for a conflict that should stay resolved via canonical pick"
    );
    assert_eq!(
        post_restart_status.quarantined_pending_dags, 0,
        "quarantine guard misfired after restart: {} root(s) quarantined",
        post_restart_status.quarantined_pending_dags
    );

    // Post-restart sound traffic from both A (gossip) and C (replicator)
    // still converges on the restarted hub. Unlike traffic racing the
    // kill/freeze window, these are ordinary live-network writes issued
    // after reconnection completes, so best-effort gossip delivery is not
    // a concern here.
    let post_restart_a = node_a
        .query(r#"mutation { add_Session(input: {session_id: "post-restart-a", note: "a-sound"}) { _docID } }"#)
        .expect("create post-restart doc on A");
    let post_restart_a_id = extract_doc_id(&post_restart_a, "add_Session");
    let post_restart_c = node_c
        .query(r#"mutation { add_Session(input: {session_id: "post-restart-c", note: "c-sound"}) { _docID } }"#)
        .expect("create post-restart doc on C");
    let post_restart_c_id = extract_doc_id(&post_restart_c, "add_Session");

    poll_until(
        || {
            let present = doc_ids_present(&node_b_after);
            present.contains(&post_restart_a_id) && present.contains(&post_restart_c_id)
        },
        Duration::from_secs(30),
        Duration::from_millis(200),
        "post-restart sound traffic from A and C did not converge on B after reconnect",
    )
    .await;

    // Final retention: everything B held pre-kill survived the restart.
    let present_final = doc_ids_present(&node_b_after);
    let lost: Vec<&String> = pre_kill_docs
        .iter()
        .filter(|id| !present_final.contains(id.as_str()))
        .collect();
    assert!(
        lost.is_empty(),
        "{} of {} documents present before the kill are missing after restart: {:?}",
        lost.len(),
        pre_kill_docs.len(),
        lost
    );

    let final_status = fetch_sync_status(&cluster, HUB).await;
    assert_eq!(final_status.pending_dag_terminal_quarantined, 0);
    assert_eq!(final_status.quarantined_pending_dags, 0);
    eprintln!(
        "[compose fence] final: winner={winner_doc_id}, docs on B={} ({} pre-kill docs all \
         retained, both twins present), quarantine guard silent throughout \
         (pending_dag_terminal_quarantined=0, quarantined_pending_dags=0)",
        present_final.len(),
        pre_kill_docs.len()
    );
}
