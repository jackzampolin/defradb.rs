//! Shared scaffolding for binary-axis conformance: locate the release artifact
//! under test, plus query helpers shared by the behavioral convergence/parity
//! twins. Included via `#[path]` from the behavioral test binaries.

use defra_harness::{BinarySource, DefraClient};
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
