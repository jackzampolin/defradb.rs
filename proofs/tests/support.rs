//! Shared scaffolding for binary-axis conformance: locate the release artifact
//! under test, plus query helpers shared by the behavioral convergence/parity
//! twins. Included via `#[path]` from the behavioral test binaries.

use defra_harness::{BinarySource, DefraClient, TestCluster};
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::{Duration, Instant};

/// The artifact under test.
///
/// Defaults to `target/debug/defra` — the binary that `cargo test --workspace`
/// rebuilds fresh for the exact revision under test. We deliberately do NOT
/// prefer `target/release/defra`: a self-hosted CI runner caches `target/`, so a
/// release binary left over from an earlier run can be stale and silently
/// validate old behavior (which produced confusing red CI here). `cargo test`
/// never rebuilds the release binary, but always brings the debug one current.
///
/// Override with `DEFRA_CONFORMANCE_BINARY` to validate a specific shipped
/// artifact (e.g. a downloaded tagged release, or a local release build from
/// `proofs/verify-all.sh`).
pub fn release_binary() -> BinarySource {
    if let Some(path) = std::env::var_os("DEFRA_CONFORMANCE_BINARY") {
        return BinarySource::Path(PathBuf::from(path));
    }
    BinarySource::Path(workspace_root().join("target/debug/defra"))
}

pub fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("proofs/ has a parent (the workspace root)")
        .to_path_buf()
}

/// Materialized `age` of the (single) `User` doc on `node`, or -1 if absent.
pub fn indexed_age(node: &DefraClient) -> i64 {
    node.query("query { User { age } }").unwrap_or_default()["User"][0]["age"]
        .as_i64()
        .unwrap_or(-1)
}

/// How many `User` docs `node` returns for `age == value` — an INDEX-filtered
/// count (see the `@explain` honesty checks that this exercises the index, not a
/// full scan).
pub fn count_by_index(node: &DefraClient, age: i64) -> usize {
    node.query(&format!(
        "query {{ User(filter: {{age: {{_eq: {age}}}}}) {{ name }} }}"
    ))
    .unwrap_or_default()["User"]
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0)
}

/// All commit CIDs reachable from a document's heads on `node`. Uses the
/// Go-compatible `_commits` field (Rust also exposes the unprefixed `commits`
/// alias, hence the fallback).
pub fn commit_cids(node: &DefraClient, doc_id: &str) -> BTreeSet<String> {
    let resp = node
        .query(&format!(
            "query {{ _commits(docID: \"{doc_id}\") {{ cid }} }}"
        ))
        .unwrap_or_default();
    resp.get("_commits")
        .or_else(|| resp.get("commits"))
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|c| c["cid"].as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

/// Both replicas hold the identical (content-addressed) commit DAG for `doc_id`
/// — proof each has MERGED the other's deltas, not merely materialized its own
/// local write. This is what makes a winner-takes-all LWW assertion meaningful on
/// the node that locally wrote the winner: without it, that node passes from its
/// own write alone, never exercising the merge. Returns false on timeout.
pub async fn poll_dags_converged(
    a: &DefraClient,
    b: &DefraClient,
    doc_id: &str,
    timeout: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        let (ca, cb) = (commit_cids(a, doc_id), commit_cids(b, doc_id));
        if !ca.is_empty() && ca == cb {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn storm_node_addr(cluster: &TestCluster, i: usize) -> String {
    cluster.client(i).p2p_info().expect("p2p info")[0]
        .as_str()
        .expect("p2p address")
        .to_string()
}

fn storm_hits(node: &DefraClient) -> f64 {
    node.query("query { Tally { hits } }").unwrap_or_default()["Tally"][0]["hits"]
        .as_f64()
        .unwrap_or(f64::NAN)
}

async fn storm_poll_all(cluster: &TestCluster, nodes: usize, want: f64, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if (0..nodes).all(|n| storm_hits(&cluster.client(n)) == want) {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Reusable CONCURRENT same-document counter STORM, cluster-agnostic so the same
/// driver runs Rust-only, Go-only, and mixed Rust/Go meshes (the #1021 counter
/// regression guard + its cross-impl parity twin). `nodes` = `node_deltas.len()`;
/// each node fires `burst` increments of its signed delta per round at ONE doc
/// across a full mesh, and every node must converge to the EXACT running sum each
/// round (below = a delta dropped, above = one double-applied). Parameterized by
/// `crdt_type` ("pcounter"|"pncounter"), `field_type` ("Int"|"Float"), and per-node
/// signed `node_deltas`. The caller builds the cluster (choosing rust/go/mixed).
pub async fn run_counter_storm(
    cluster: &TestCluster,
    crdt_type: &str,
    field_type: &str,
    node_deltas: &[f64],
    rounds: i64,
    burst: i64,
) {
    let nodes = node_deltas.len();
    let lit = |v: f64| -> String {
        if field_type == "Float" {
            format!("{v:?}")
        } else {
            format!("{}", v as i64)
        }
    };

    let schema =
        format!("type Tally {{ name: String  hits: {field_type} @crdt(type: {crdt_type}) }}");
    let addr: Vec<String> = (0..nodes).map(|n| storm_node_addr(cluster, n)).collect();
    for n in 0..nodes {
        cluster.client(n).schema_add(&schema).expect("schema");
        cluster.client(n).p2p_collection_add(&["Tally"]).ok();
    }
    for i in 0..nodes {
        for (j, peer) in addr.iter().enumerate() {
            if i != j {
                cluster.client(i).p2p_connect(&[peer.as_str()]).ok();
                cluster.client(i).p2p_replicator_set(&["Tally"], peer).ok();
            }
        }
    }

    let created = cluster
        .client(0)
        .query(&format!(
            r#"mutation {{ add_Tally(input: {{name: "t", hits: {}}}) {{ _docID }} }}"#,
            lit(0.0)
        ))
        .expect("create");
    let id = created["add_Tally"][0]["_docID"]
        .as_str()
        .expect("_docID")
        .to_string();
    assert!(
        storm_poll_all(cluster, nodes, 0.0, Duration::from_secs(30)).await,
        "seed (hits=0) must reach all nodes before the storm"
    );

    let per_round: f64 = node_deltas.iter().sum::<f64>() * burst as f64;
    let mut expected = 0.0f64;
    for round in 0..rounds {
        for _ in 0..burst {
            for (n, delta) in node_deltas.iter().enumerate() {
                cluster
                    .client(n)
                    .query(&format!(
                        r#"mutation {{ update_Tally(docID: "{id}", input: {{hits: {}}}) {{ _docID }} }}"#,
                        lit(*delta)
                    ))
                    .expect("increment");
            }
        }
        expected += per_round;
        if !storm_poll_all(cluster, nodes, expected, Duration::from_secs(40)).await {
            let got: Vec<f64> = (0..nodes).map(|n| storm_hits(&cluster.client(n))).collect();
            panic!(
                "round {round} ({crdt_type}/{field_type}): nodes did not all reach exactly {expected} (below=loss, above=double-apply); hits = {got:?}"
            );
        }
    }
}
