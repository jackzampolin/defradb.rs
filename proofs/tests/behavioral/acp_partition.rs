//! ACP-UNDER-PARTITION (ignored, reporting) — access-control invariants when the
//! grant/revoke happens while the peer is partitioned. The live grant/revoke path
//! is covered by acp.rs and the integration suite; these probe whether the
//! relationship change RECONCILES on heal:
//!
//! * revoke-during-partition: a peer that LOSES access while partitioned must not
//!   retain it (or receive post-revoke writes) once reconnected — a stale grant
//!   surviving a partition is an access leak.
//! * grant-during-partition: a peer GRANTED access while partitioned must gain it
//!   on heal — a grant lost to a partition is an availability bug.
//!
//! Report-only (no assert): each prints what the peer can see, so a divergence
//! from the modeled access semantics is visible.
//!
//! Run: cargo test -p conformance --test tla_conformance acp_partition:: \
//!        -- --ignored --test-threads=1 --nocapture

use crate::support;
use defra_harness::fixtures::{users_schema_with_policy, USER_ACP_POLICY};
use defra_harness::{generate_identity, TestCluster, TestIdentity};
use std::time::{Duration, Instant};

fn node_addr(cluster: &TestCluster, i: usize) -> String {
    let info = cluster.client(i).p2p_info().expect("p2p info");
    info.as_array()
        .and_then(|a| a.first())
        .and_then(|v| v.as_str())
        .expect("p2p address")
        .to_string()
}

/// Count `User` docs visible to `identity` on node `n`.
fn seen_by(cluster: &TestCluster, n: usize, identity: &TestIdentity) -> usize {
    cluster
        .client(n)
        .query_with_identity("query { User { _docID name } }", &identity.private_key_hex)
        .map(|v| v["User"].as_array().map(|a| a.len()).unwrap_or(0))
        .unwrap_or(0)
}

/// The `age` the owner observes for the (single) doc on node `n`, or -1 if none.
/// Control: confirms a write actually reached the peer (so a relationship that
/// did NOT reconcile can't be confused with a write that never replicated).
fn owner_age(cluster: &TestCluster, n: usize, owner: &TestIdentity) -> i64 {
    cluster
        .client(n)
        .query_with_identity("query { User { age } }", &owner.private_key_hex)
        .map(|v| v["User"][0]["age"].as_i64().unwrap_or(-1))
        .unwrap_or(-1)
}

async fn poll_seen(
    cluster: &TestCluster,
    n: usize,
    identity: &TestIdentity,
    want: usize,
    timeout: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if seen_by(cluster, n, identity) == want {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// Common setup: 2 ACP+P2P nodes, same policy+schema on both, bidirectional
/// owner-authenticated replication, one protected doc created on node0 and
/// replicated to node1. Returns (owner, reader, doc_id).
async fn setup(cluster: &TestCluster) -> (TestIdentity, TestIdentity, String) {
    let binary = cluster.client(0).binary_path().to_path_buf();
    let owner = generate_identity(&binary).expect("owner identity");
    let reader = generate_identity(&binary).expect("reader identity");

    // Add the policy on BOTH nodes, but bind BOTH schemas to node0's policy_id
    // (mirrors p2p_iroh/acp/dac.rs). Using each node's own returned id would
    // break gating if the impl assigns non-identical policy ids per node — the
    // replicated doc references node0's policy, so node1's schema must too.
    let policy0 = cluster
        .client(0)
        .acp_policy_add(USER_ACP_POLICY, &owner.private_key_hex)
        .expect("add ACP policy node0");
    let policy_id = policy0["PolicyID"]
        .as_str()
        .or_else(|| policy0["policyID"].as_str())
        .expect("PolicyID")
        .to_string();
    cluster
        .client(1)
        .acp_policy_add(USER_ACP_POLICY, &owner.private_key_hex)
        .expect("add ACP policy node1");
    for n in 0..2 {
        cluster
            .client(n)
            .schema_add_with_identity(
                &users_schema_with_policy(&policy_id),
                &owner.private_key_hex,
            )
            .expect("add @policy schema");
    }

    // Mirror the working integration setup (p2p_iroh/acp/dac.rs): a single
    // one-way replicator node0->node1, no identity. node0 owns and pushes.
    let a1 = node_addr(cluster, 1);
    cluster.client(0).p2p_connect(&[a1.as_str()]).ok();
    cluster.client(0).p2p_collection_add(&["User"]).ok();
    cluster.client(1).p2p_collection_add(&["User"]).ok();
    cluster.client(0).p2p_replicator_set(&["User"], &a1).ok();

    let created = cluster
        .client(0)
        .query_with_identity(
            r#"mutation { add_User(input: {name: "Protected", age: 42}) { _docID } }"#,
            &owner.private_key_hex,
        )
        .expect("owner creates protected doc");
    let doc_id = created["add_User"][0]["_docID"]
        .as_str()
        .expect("_docID")
        .to_string();

    // The protected doc must reach node1 for the owner before we partition.
    poll_seen(cluster, 1, &owner, 1, Duration::from_secs(20)).await;
    (owner, reader, doc_id)
}

async fn reheal(cluster: &TestCluster, _owner: &TestIdentity) {
    // One-way node0->node1 (matches `setup`): owner re-pushes doc + relationship
    // state after the partition.
    let a1b = node_addr(cluster, 1);
    cluster.client(0).p2p_connect(&[a1b.as_str()]).ok();
    cluster.client(0).p2p_collection_add(&["User"]).ok();
    cluster.client(1).p2p_collection_add(&["User"]).ok();
    cluster
        .client(0)
        .p2p_replicator_delete(&["User"], Some(&a1b))
        .ok();
    cluster.client(0).p2p_replicator_set(&["User"], &a1b).ok();
}

/// Which implementation + transport to exercise. Go runs only libp2p (iroh is
/// Rust-specific). Go variants require the harness-compatible Go `defradb` on
/// PATH (`<go-repo>/build`).
#[derive(Clone, Copy)]
enum Backend {
    RustLibp2p,
    RustIroh,
    Go,
}

async fn build_cluster(backend: Backend) -> TestCluster {
    let b = TestCluster::builder()
        .with_acp_local()
        .with_p2p()
        .with_keyring()
        .with_rust_binary(support::release_binary());
    let b = match backend {
        Backend::RustLibp2p => b.rust_nodes(2).with_store("redb"),
        Backend::RustIroh => b.rust_nodes(2).with_store("redb").with_iroh_transport(),
        Backend::Go => b.go_nodes(2).with_store("badger"),
    };
    b.build().await.expect("build ACP+P2P cluster")
}

/// BASELINE ACCESS MATRIX (no grant, no partition) — ground truth for ACP gating
/// of a replicated protected doc. Reports, on BOTH nodes, what the owner, an
/// anonymous client, and an ungranted reader can see. Correct gating: owner sees
/// 1 everywhere; anon and ungranted reader see 0 everywhere. Used to compare Rust
/// vs Go baseline before any partition probe (a divergent baseline confounds the
/// partition result).
async fn run_baseline(backend: Backend, label: &str) {
    let cluster = build_cluster(backend).await;
    let (owner, reader, _doc_id) = setup(&cluster).await;

    let anon = |n: usize| -> usize {
        cluster
            .client(n)
            .query("query { User { _docID } }")
            .map(|v| v["User"].as_array().map(|a| a.len()).unwrap_or(0))
            .unwrap_or(0)
    };
    eprintln!(
        "ACP_PARTITION[baseline/{label}] node0(owner={},anon={},reader={}) node1(owner={},anon={},reader={}) | want owner=1 anon=0 reader=0",
        seen_by(&cluster, 0, &owner), anon(0), seen_by(&cluster, 0, &reader),
        seen_by(&cluster, 1, &owner), anon(1), seen_by(&cluster, 1, &reader),
    );
}

#[ignore = "ACP-partition probe; run with --ignored --nocapture"]
#[tokio::test]
async fn acp_baseline_rust() {
    run_baseline(Backend::RustLibp2p, "rust_libp2p").await;
}

#[ignore = "ACP-partition probe; needs Go binary on PATH; run with --ignored --nocapture"]
#[tokio::test]
async fn acp_baseline_go() {
    run_baseline(Backend::Go, "go").await;
}

/// LIVE GRANT PROPAGATION (no partition) — the control that isolates a transport
/// gap: grant reader over a live link and report whether the relationship reaches
/// node1. A/Bs libp2p vs iroh (and Go) to confirm ACP relationship propagation is
/// transport- and implementation-agnostic on the live path.
async fn run_live_grant(backend: Backend, label: &str) {
    let cluster = build_cluster(backend).await;
    let (owner, reader, doc_id) = setup(&cluster).await;

    let before = seen_by(&cluster, 1, &reader);
    cluster
        .client(0)
        .acp_relationship_add(
            "User",
            &doc_id,
            "reader",
            &reader.did,
            &owner.private_key_hex,
        )
        .expect("grant reader");
    let propagated = poll_seen(&cluster, 1, &reader, 1, Duration::from_secs(20)).await;

    eprintln!(
        "ACP_PARTITION[live_grant/{label}] reader_before={before} owner_on_node1={} reader_after={} | PROPAGATED={propagated}",
        seen_by(&cluster, 1, &owner),
        seen_by(&cluster, 1, &reader),
    );
}

#[ignore = "ACP-partition probe; run with --ignored --nocapture"]
#[tokio::test]
async fn acp_live_grant_libp2p() {
    run_live_grant(Backend::RustLibp2p, "rust_libp2p").await;
}

#[ignore = "ACP-partition probe; run with --ignored --nocapture"]
#[tokio::test]
async fn acp_live_grant_iroh() {
    run_live_grant(Backend::RustIroh, "rust_iroh").await;
}

#[ignore = "ACP-partition probe; needs Go binary on PATH; run with --ignored --nocapture"]
#[tokio::test]
async fn acp_live_grant_go() {
    run_live_grant(Backend::Go, "go").await;
}

/// REVOKE-DURING-PARTITION: grant reader, replicate, partition node1, then revoke
/// AND make a post-revoke owner write while partitioned; heal. The reader must
/// not retain access nor receive the post-revoke write on node1.
async fn run_revoke_partition(backend: Backend, label: &str) {
    let mut cluster = build_cluster(backend).await;
    let (owner, reader, doc_id) = setup(&cluster).await;

    // Grant reader and confirm propagation to node1 (live).
    cluster
        .client(0)
        .acp_relationship_add(
            "User",
            &doc_id,
            "reader",
            &reader.did,
            &owner.private_key_hex,
        )
        .expect("grant reader");
    let granted = poll_seen(&cluster, 1, &reader, 1, Duration::from_secs(20)).await;

    // PARTITION node1.
    cluster
        .restart_node(1, Duration::from_secs(30))
        .await
        .expect("restart node1");

    // While partitioned: revoke reader AND owner writes a new value.
    cluster
        .client(0)
        .acp_relationship_delete(
            "User",
            &doc_id,
            "reader",
            &reader.did,
            &owner.private_key_hex,
        )
        .expect("revoke reader");
    cluster
        .client(0)
        .query_with_identity(
            &format!(
                r#"mutation {{ update_User(docID: "{doc_id}", input: {{age: 100}}) {{ _docID }} }}"#
            ),
            &owner.private_key_hex,
        )
        .ok();

    reheal(&cluster, &owner).await;
    tokio::time::sleep(Duration::from_secs(10)).await;

    let reader_sees = seen_by(&cluster, 1, &reader);
    let owner_sees = seen_by(&cluster, 1, &owner);

    // Diagnostic: a subsequent owner write triggers `republish_document` with the
    // CURRENT (revoked) relationship snapshot. If this reconciles the revoke, the
    // gap is specifically "no relationship re-sync on reconnect" (the backfill
    // re-sends blocks but not the relationship state).
    cluster
        .client(0)
        .query_with_identity(
            &format!(
                r#"mutation {{ update_User(docID: "{doc_id}", input: {{age: 101}}) {{ _docID }} }}"#
            ),
            &owner.private_key_hex,
        )
        .ok();
    tokio::time::sleep(Duration::from_secs(8)).await;
    let reader_after_write = seen_by(&cluster, 1, &reader);
    let owner_age_node1 = owner_age(&cluster, 1, &owner); // control: did the write (age=101) land?

    eprintln!(
        "ACP_PARTITION[revoke/{label}] granted_before={granted} | after-heal: reader_sees={reader_sees} (want 0) owner_sees={owner_sees} | after-owner-write(age=101): owner_age@node1={owner_age_node1} reader_sees={reader_after_write} | LEAK={} FIXED_BY_WRITE={}",
        reader_sees > 0,
        reader_sees > 0 && reader_after_write == 0
    );
}

#[ignore = "ACP-partition probe; run with --ignored --nocapture"]
#[tokio::test]
async fn acp_revoke_during_partition() {
    run_revoke_partition(Backend::RustIroh, "rust_iroh").await;
}

#[ignore = "ACP-partition probe; needs Go binary on PATH; run with --ignored --nocapture"]
#[tokio::test]
async fn acp_revoke_during_partition_go() {
    run_revoke_partition(Backend::Go, "go").await;
}

/// GRANT-DURING-PARTITION: replicate doc (reader has no access), partition node1,
/// grant reader while partitioned, heal. The reader must GAIN access on node1
/// after the grant reconciles.
async fn run_grant_partition(backend: Backend, label: &str) {
    let mut cluster = build_cluster(backend).await;
    let (owner, reader, doc_id) = setup(&cluster).await;

    // Reader has no access yet.
    let before = seen_by(&cluster, 1, &reader);

    // PARTITION node1.
    cluster
        .restart_node(1, Duration::from_secs(30))
        .await
        .expect("restart node1");

    // While partitioned: grant reader.
    cluster
        .client(0)
        .acp_relationship_add(
            "User",
            &doc_id,
            "reader",
            &reader.did,
            &owner.private_key_hex,
        )
        .expect("grant reader");

    reheal(&cluster, &owner).await;
    let gained = poll_seen(&cluster, 1, &reader, 1, Duration::from_secs(20)).await;
    let reader_sees = seen_by(&cluster, 1, &reader);

    // Diagnostic: a subsequent owner write triggers `republish_document` with the
    // CURRENT (granted) relationship snapshot. If this reconciles the grant, the
    // gap is specifically "no relationship re-sync on reconnect".
    cluster
        .client(0)
        .query_with_identity(
            &format!(
                r#"mutation {{ update_User(docID: "{doc_id}", input: {{age: 43}}) {{ _docID }} }}"#
            ),
            &owner.private_key_hex,
        )
        .ok();
    let gained_after_write = poll_seen(&cluster, 1, &reader, 1, Duration::from_secs(15)).await;

    eprintln!(
        "ACP_PARTITION[grant/{label}] reader_before={before} | after-heal: reader_sees={reader_sees} (want 1) | after-owner-write: gained={gained_after_write} | LOST_GRANT={} FIXED_BY_WRITE={}",
        !gained,
        !gained && gained_after_write
    );
}

#[ignore = "ACP-partition probe; run with --ignored --nocapture"]
#[tokio::test]
async fn acp_grant_during_partition() {
    run_grant_partition(Backend::RustIroh, "rust_iroh").await;
}

#[ignore = "ACP-partition probe; needs Go binary on PATH; run with --ignored --nocapture"]
#[tokio::test]
async fn acp_grant_during_partition_go() {
    run_grant_partition(Backend::Go, "go").await;
}
