//! Local (DAC) ACP is NODE-LOCAL — regression guard that matches Go.
//!
//! A document protected by a Local ACP policy is gated on the node that CREATED
//! it (the owner is registered there at creation), but a copy replicated to a
//! PEER is NOT gated on that peer: Local ACP makes no cross-node calls and a
//! document that is unregistered in a node's ACP store is treated as public
//! (`LocalDocumentACP::check_doc_access`). Cross-node access control is
//! SourceHub ACP's role (shared on-chain registry). This matches Go exactly —
//! Go never registers replicated docs on the peer for Local ACP, and gates the
//! "hidden on peer" behaviour to SourceHub only.
//!
//! A Rust-only feature once gated Local-ACP replicas on the peer and propagated
//! grants/revokes over P2P; it was removed (nothing depended on it) in favour of
//! Go parity. This test locks that in: if peer-side Local-ACP gating is
//! reintroduced, the `node1` assertions below fail.
//!
//! The `_go` variant is the parity control (needs the harness Go binary on PATH;
//! run with `--ignored`).

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

/// Count `User` docs visible to an anonymous (no-identity) client on node `n`.
fn seen_anon(cluster: &TestCluster, n: usize) -> usize {
    cluster
        .client(n)
        .query("query { User { _docID } }")
        .map(|v| v["User"].as_array().map(|a| a.len()).unwrap_or(0))
        .unwrap_or(0)
}

async fn poll_owner_replicated(cluster: &TestCluster, owner: &TestIdentity, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while seen_by(cluster, 1, owner) == 0 {
        if Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

/// Backend under test. Go runs only libp2p (iroh is Rust-specific) and needs the
/// harness-compatible Go `defradb` on PATH.
#[derive(Clone, Copy)]
enum Backend {
    Rust,
    Go,
}

async fn build_cluster(backend: Backend) -> TestCluster {
    let b = TestCluster::builder()
        .with_acp_local()
        .with_p2p()
        .with_rust_binary(support::release_binary());
    let b = match backend {
        Backend::Rust => b.rust_nodes(2).with_store("redb"),
        Backend::Go => b.go_nodes(2).with_store("badger"),
    };
    b.build().await.expect("build ACP+P2P cluster")
}

/// Create an ACP-protected doc on node0 and replicate it to node1. Both nodes
/// add the policy; both schemas bind to node0's policy id (so the replicated
/// doc's policy reference matches node1's schema). Returns (owner, reader).
async fn setup(cluster: &TestCluster) -> (TestIdentity, TestIdentity) {
    let binary = cluster.client(0).binary_path().to_path_buf();
    let owner = generate_identity(&binary).expect("owner identity");
    let reader = generate_identity(&binary).expect("reader identity");

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

    // One-way owner-pushed replication node0 -> node1.
    let a1 = node_addr(cluster, 1);
    cluster.client(0).p2p_connect(&[a1.as_str()]).ok();
    cluster.client(0).p2p_collection_add(&["User"]).ok();
    cluster.client(1).p2p_collection_add(&["User"]).ok();
    cluster.client(0).p2p_replicator_set(&["User"], &a1).ok();

    cluster
        .client(0)
        .query_with_identity(
            r#"mutation { add_User(input: {name: "Protected", age: 42}) { _docID } }"#,
            &owner.private_key_hex,
        )
        .expect("owner creates protected doc");
    poll_owner_replicated(cluster, &owner, Duration::from_secs(20)).await;
    (owner, reader)
}

/// Assert the node-local access matrix:
/// - creating node (node0) GATES: owner sees the doc, anon + ungranted reader do not;
/// - peer (node1) does NOT gate: owner, anon, and ungranted reader ALL see it.
async fn assert_node_local(backend: Backend) {
    let cluster = build_cluster(backend).await;
    let (owner, reader) = setup(&cluster).await;

    // The replicated copy must be present on node1 (visible to the owner there).
    assert_eq!(
        seen_by(&cluster, 1, &owner),
        1,
        "owner must see the replicated doc on the peer"
    );

    // node0 (creator) gates the protected doc.
    assert_eq!(seen_by(&cluster, 0, &owner), 1, "node0: owner sees doc");
    assert_eq!(
        seen_anon(&cluster, 0),
        0,
        "node0: anon must NOT see protected doc"
    );
    assert_eq!(
        seen_by(&cluster, 0, &reader),
        0,
        "node0: ungranted reader must NOT see protected doc"
    );

    // node1 (peer) does NOT gate — Local ACP is node-local, so the replicated doc
    // is public there (matches Go). If peer-side Local-ACP gating is reintroduced,
    // these two assertions fail.
    assert_eq!(
        seen_anon(&cluster, 1),
        1,
        "node1: replicated Local-ACP doc is public to anon"
    );
    assert_eq!(
        seen_by(&cluster, 1, &reader),
        1,
        "node1: replicated Local-ACP doc is public to an ungranted reader"
    );
}

#[tokio::test]
async fn local_acp_is_node_local_rust() {
    assert_node_local(Backend::Rust).await;
}

/// Parity control: Go exhibits the same node-local matrix. Needs the
/// harness-compatible Go `defradb` on PATH.
#[ignore = "parity control; needs Go binary on PATH; run with --ignored"]
#[tokio::test]
async fn local_acp_is_node_local_go() {
    assert_node_local(Backend::Go).await;
}
