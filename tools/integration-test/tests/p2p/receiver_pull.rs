//! #1116 stage 2: paced receiver-pull convergence fence.
//!
//! Exercises the stage-2 receiver-side machinery against a real 2-node
//! cluster: the per-root retry clock (post-increment ladder 4s -> 60s), the
//! incremental pending-DAG frontier (full verification walk only when the
//! frontier empties), and the three new `/api/v0/p2p/sync/status` fields
//! (`pending_dag_retry_dispatched`, `pending_dag_retry_suppressed`,
//! `next_pending_retry_in_ms`).
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
/// stage 2). Read as a typed snapshot so the two points-in-time in this test
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

/// 2-node cluster: B registered as A's replicator, baseline doc convergence,
/// then a ~20-update storm on the same document. Asserts the new sync-status
/// retry-clock fields are present and internally consistent, and that the
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

    // Register B as A's replicator: every Note change on A pushes to B.
    node_a
        .p2p_replicator_set(&["Note"], &addr_b)
        .expect("replicator set A -> B");

    // --- Step 1: baseline replication sanity ---
    let create = node_a
        .query(r#"mutation { add_Note(input: {title: "v0"}) { _docID } }"#)
        .expect("create Note on A");
    let doc_id = extract_doc_id(&create, "add_Note");

    let node_b_ref = &node_b;
    let doc_id_ref = &doc_id;
    poll_until(
        || {
            let result = node_b_ref.query("query { Note { _docID title } }").unwrap();
            result["Note"]
                .as_array()
                .map(|arr| {
                    arr.iter().any(|n| {
                        n["_docID"].as_str() == Some(doc_id_ref.as_str())
                            && n["title"].as_str() == Some("v0")
                    })
                })
                .unwrap_or(false)
        },
        Duration::from_secs(15),
        Duration::from_millis(200),
        "baseline doc did not converge on B",
    )
    .await;

    // --- Step 2: sync-status fields present and sane ---
    let baseline_status = fetch_sync_status(&cluster, 1).await;
    assert_drained_queue_has_no_next_retry(&baseline_status, "after baseline convergence");

    // --- Step 3: storm — ~20 rapid updates to the same document ---
    for i in 1..=UPDATE_COUNT {
        let title = format!("v{i}");
        node_a
            .query(&format!(
                r#"mutation {{ update_Note(docID: "{doc_id}", input: {{title: "{title}"}}) {{ _docID }} }}"#
            ))
            .unwrap_or_else(|e| panic!("update {i} on A failed: {e}"));
    }

    let final_title = format!("v{UPDATE_COUNT}");
    let final_title_ref = &final_title;
    poll_until(
        || {
            let result = node_b_ref.query("query { Note { _docID title } }").unwrap();
            result["Note"]
                .as_array()
                .map(|arr| {
                    arr.iter().any(|n| {
                        n["_docID"].as_str() == Some(doc_id_ref.as_str())
                            && n["title"].as_str() == Some(final_title_ref.as_str())
                    })
                })
                .unwrap_or(false)
        },
        Duration::from_secs(30),
        Duration::from_millis(200),
        "storm updates did not converge on B",
    )
    .await;

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

    // Bound derivation (#1112 storm fence, #1116 stage 2 incremental
    // frontier): before #1112, a naive re-walk ran on every block arrival, so
    // growth scaled with `updates * DAG-depth`. With the incremental
    // frontier, a full verification walk (`record_missing_link_retry`) only
    // fires when (a) a pending root's frontier empties on completion, or (b)
    // the per-root retry clock re-dispatches a stalled root — at most one
    // clock re-dispatch per completion for a healthy 2-node cluster with no
    // induced loss. That is <= 2 full walks per update in the worst case, so
    // growth over `UPDATE_COUNT` rapid same-doc updates should stay within
    // `2 * UPDATE_COUNT` plus a small constant for the baseline create.
    // Observed on this branch (3/3 local runs): growth = 0, with exactly one
    // `pending_dag_retry_dispatched` per run — an unconstrained 2-node run
    // has no capacity pressure, so a pushed root rarely lacks a link locally
    // and pending-DAG registration barely triggers at all. The bound below
    // stays at the full analytical envelope rather than an observed-x2
    // margin: the observed value is 0, and a zero-based margin would make
    // the assertion vacuous instead of load-bearing.
    let bound = 2 * UPDATE_COUNT as u64 + 5;
    assert!(
        missing_link_growth <= bound,
        "missing_link_retries grew by {missing_link_growth} over {UPDATE_COUNT} rapid updates \
         (bound {bound}); the incremental frontier should keep growth O(updates), not \
         O(updates * DAG-depth) — see #1112"
    );
}
