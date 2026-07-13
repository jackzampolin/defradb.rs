//! #1116 stage 2: paced receiver-pull convergence fence.
//!
//! Exercises the stage-2 receiver-side machinery against a real 2-node
//! cluster: the per-root retry clock (post-increment ladder 4s -> 60s), the
//! incremental pending-DAG frontier (full verification walk only when the
//! frontier empties), and the three new `/api/v0/p2p/sync/status` fields
//! (`pending_dag_retry_dispatched`, `pending_dag_retry_suppressed`,
//! `next_pending_retry_in_ms`).
//!
//! The forcing phase deliberately routes the update storm through the gossip
//! single-head path (replicator deleted first): the outbound broadcast
//! coalescer folds the burst so B receives head announcements whose parent
//! chains are missing locally — exactly the pending-DAG registration and
//! paced-fetch path this fence exists to gate. A replicator-driven storm
//! would be vacuous: sequential pushes always deliver parents before
//! children, so the pull path never fires.
//!
//! The storm assertion is the #1112 fence: before the incremental frontier,
//! every block arrival re-walked the whole DAG from the root, so
//! `missing_link_retries` grew with `updates * DAG-depth`. After #1112 /
//! stage 2, a full walk only runs when a pending root's frontier empties
//! (once per completion) or when the retry clock re-dispatches a stalled
//! root, so growth should track `updates` linearly, not multiplicatively.

use std::time::Duration;

use integration_test::{extract_doc_id, poll_until, TestCluster};

const SCHEMA: &str = "type Note { title: String }";
const UPDATE_COUNT: usize = 20;

/// The pending-DAG-retry-clock subset of `/api/v0/p2p/sync/status` (#1116
/// stage 2). Read as a typed snapshot so the points-in-time in this test
/// (baseline vs. post-storm) are easy to diff.
struct SyncStatusSnapshot {
    pending_dags: u64,
    missing_link_retries: u64,
    pending_dag_retry_dispatched: u64,
    pending_dag_retry_suppressed: u64,
    next_pending_retry_in_ms: Option<u64>,
}

async fn fetch_sync_status(cluster: &TestCluster, node: usize) -> SyncStatusSnapshot {
    let status: serde_json::Value =
        reqwest::get(format!("{}/api/v0/p2p/sync/status", cluster.api_url(node)))
            .await
            .expect("sync status request")
            .json()
            .await
            .expect("sync status json");

    SyncStatusSnapshot {
        pending_dags: status["pending_dags"]
            .as_u64()
            .expect("pending_dags field present"),
        missing_link_retries: status["missing_link_retries"]
            .as_u64()
            .expect("missing_link_retries field present"),
        pending_dag_retry_dispatched: status["pending_dag_retry_dispatched"]
            .as_u64()
            .expect("pending_dag_retry_dispatched field present"),
        pending_dag_retry_suppressed: status["pending_dag_retry_suppressed"]
            .as_u64()
            .expect("pending_dag_retry_suppressed field present"),
        next_pending_retry_in_ms: status["next_pending_retry_in_ms"].as_u64(),
    }
}

/// Assert the retry-clock invariant: once no pending-DAG entry is registered
/// there is nothing left for the clock to schedule, so the "next due" field
/// must report `null`, never a stale or synthesized deadline.
fn assert_drained_queue_has_no_next_retry(status: &SyncStatusSnapshot, when: &str) {
    if status.pending_dags == 0 {
        assert!(
            status.next_pending_retry_in_ms.is_none(),
            "{when}: pending_dags == 0 but next_pending_retry_in_ms = {:?} \
             (a drained queue must not report a pending deadline)",
            status.next_pending_retry_in_ms
        );
    }
}

async fn wait_for_title(node_b: &integration_test::DefraClient, doc_id: &str, title: &str) {
    poll_until(
        || {
            let result = node_b.query("query { Note { _docID title } }").unwrap();
            result["Note"]
                .as_array()
                .map(|arr| {
                    arr.iter().any(|n| {
                        n["_docID"].as_str() == Some(doc_id) && n["title"].as_str() == Some(title)
                    })
                })
                .unwrap_or(false)
        },
        Duration::from_secs(30),
        Duration::from_millis(200),
        &format!("title {title} did not converge on B"),
    )
    .await;
}

/// 2-node cluster: baseline convergence through an explicit replicator, then
/// the replicator is deleted and a ~20-update storm on the same document must
/// converge through the gossip single-head + paced receiver-pull path alone.
/// Asserts the new sync-status retry-clock fields are present and internally
/// consistent, that the pull path actually fired (anti-vacuity), and that the
/// #1112 storm fence holds: `missing_link_retries` growth stays linear in
/// the update count instead of blowing up with DAG depth.
#[tokio::test]
async fn receiver_pull_paced_convergence_fence() {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .build()
        .await
        .expect("cluster start");

    let timeout = Duration::from_secs(15);
    cluster
        .wait_for_log(0, "p2p_listening", timeout)
        .await
        .expect("node A P2P listener did not start");
    cluster
        .wait_for_log(1, "p2p_listening", timeout)
        .await
        .expect("node B P2P listener did not start");

    let node_a = cluster.client(0);
    let node_b = cluster.client(1);

    node_a.schema_add(SCHEMA).expect("schema add A");
    node_b.schema_add(SCHEMA).expect("schema add B");

    let info_b = node_b.p2p_info().expect("B p2p info");
    let addr_b = info_b
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|v| v.as_str())
        .expect("B has no P2P address")
        .to_string();

    node_a.p2p_connect(&[&addr_b]).expect("A connects to B");
    node_a
        .p2p_collection_add(&["Note"])
        .expect("collection add A");
    node_b
        .p2p_collection_add(&["Note"])
        .expect("collection add B");

    // Register B as A's replicator for the baseline phase only: the direct
    // push gives a deterministic starting point (B holds the v0 DAG) before
    // the storm is forced through the gossip pull path.
    node_a
        .p2p_replicator_set(&["Note"], &addr_b)
        .expect("replicator set A -> B");

    // --- Step 1: baseline replication sanity ---
    let create = node_a
        .query(r#"mutation { add_Note(input: {title: "v0"}) { _docID } }"#)
        .expect("create Note on A");
    let doc_id = extract_doc_id(&create, "add_Note");

    wait_for_title(&node_b, &doc_id, "v0").await;

    // --- Step 2: sync-status fields present and sane ---
    let baseline_status = fetch_sync_status(&cluster, 1).await;
    assert_drained_queue_has_no_next_retry(&baseline_status, "after baseline convergence");

    // --- Step 3: force the paced pull path ---
    // Remove the replicator so A no longer pushes to B. B stays subscribed to
    // the collection topic, so from here on it only learns about A's commits
    // from gossip head announcements. Rapid same-doc updates get folded by
    // A's outbound broadcast coalescer, so B receives heads whose parent
    // chains it does not hold — pending-DAG registration + paced fetch is the
    // only way it can converge.
    node_a
        .p2p_replicator_delete(&["Note"], Some(&addr_b))
        .expect("replicator delete A -> B");

    for i in 1..=UPDATE_COUNT {
        let title = format!("v{i}");
        node_a
            .query(&format!(
                r#"mutation {{ update_Note(docID: "{doc_id}", input: {{title: "{title}"}}) {{ _docID }} }}"#
            ))
            .unwrap_or_else(|e| panic!("update {i} on A failed: {e}"));
    }

    wait_for_title(&node_b, &doc_id, &format!("v{UPDATE_COUNT}")).await;

    let post_storm_status = fetch_sync_status(&cluster, 1).await;
    assert_drained_queue_has_no_next_retry(&post_storm_status, "after storm convergence");

    // Per-manager diagnostic counters only ever increase within one process.
    assert!(
        post_storm_status.missing_link_retries >= baseline_status.missing_link_retries,
        "missing_link_retries must be monotonic"
    );
    assert!(
        post_storm_status.pending_dag_retry_dispatched
            >= baseline_status.pending_dag_retry_dispatched,
        "pending_dag_retry_dispatched must be monotonic"
    );
    assert!(
        post_storm_status.pending_dag_retry_suppressed
            >= baseline_status.pending_dag_retry_suppressed,
        "pending_dag_retry_suppressed must be monotonic"
    );

    let missing_link_growth =
        post_storm_status.missing_link_retries - baseline_status.missing_link_retries;
    let dispatched_growth = post_storm_status.pending_dag_retry_dispatched
        - baseline_status.pending_dag_retry_dispatched;

    // Anti-vacuity (mirrors p2p_admission.rs's "hub never hit pending-DAG
    // capacity" hard-fail): the storm must have positively exercised the
    // paced pull path. A gossip head whose parent chain is missing forces a
    // pending-DAG registration, and every registration's fetch is dispatched
    // through the retry-clock claim gate, bumping
    // `pending_dag_retry_dispatched`. Observed 6/6 local runs: growth is
    // exactly 1 — the first post-delete gossip head registers, its dispatched
    // fetch pulls the missing chain, and later heads then merge directly.
    // `missing_link_retries` is NOT the mover here (0 in all observed runs):
    // in this shape the pending entry is superseded by a later head's direct
    // merge before its frontier empties, so the full verification walk never
    // runs. If dispatched did not grow, B converged without ever pulling —
    // the scenario regressed to the vacuous replicator-push shape and this
    // fence is not testing anything.
    assert!(
        dispatched_growth > 0,
        "storm converged without exercising the paced pull path \
         (pending_dag_retry_dispatched growth = 0, missing_link_retries \
         growth = {missing_link_growth}); the gossip-forcing scenario no \
         longer creates pending-DAG registrations"
    );

    // Bound derivation (#1112 storm fence, #1116 stage 2 incremental
    // frontier): before #1112, a full verification walk ran on EVERY block
    // arrival for every waiting root. In this scenario the dispatched fetch
    // delivers the storm's parent chain — about 2 blocks per update
    // (composite + field block), i.e. ~2 x UPDATE_COUNT arrivals — so a
    // revert of the incremental frontier would push `missing_link_retries`
    // growth to ~2 x UPDATE_COUNT (~40). With the incremental frontier, the
    // walk only runs when a pending root's frontier empties or when the
    // retry clock re-dispatches a stalled root: <= 2 per pending
    // registration, and observed registrations are 1 per storm (growth = 0
    // in 6/6 local runs, since the entry is superseded before completing).
    // UPDATE_COUNT sits comfortably above the legitimate envelope (a few
    // registrations x 2) while staying at half the revert signature, so it
    // fails on the O(arrivals) regression without flaking on scheduler
    // noise.
    let bound = UPDATE_COUNT as u64;
    assert!(
        missing_link_growth <= bound,
        "missing_link_retries grew by {missing_link_growth} over {UPDATE_COUNT} rapid updates \
         (bound {bound}); the incremental frontier should keep growth at O(pending \
         registrations), not O(block arrivals) — see #1112"
    );
}
