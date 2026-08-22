//! `--no-signing` must actually disable commit signing (#1413).
//!
//! The flag was parsed and stored but never consulted: commits were signed
//! whenever a node identity was configured, regardless. A plumbing assertion
//! cannot catch that, so these assert the observable behavior instead --
//! whether `_commits` come back carrying a signature.
//!
//! Run with:
//!   cargo test -p integration-test --test basic -- no_signing

use integration_test::{generate_identity, rust_binary, TestCluster};
use serial_test::serial;

const SCHEMA: &str = "type Users { name: String }";

/// Start a single Rust node with a node identity, write one document, and
/// report the `signature` field of every commit.
async fn commit_signatures(signing: bool) -> Vec<serde_json::Value> {
    let identity = generate_identity(&rust_binary()).expect("generate node identity");

    let builder = TestCluster::builder()
        .rust_nodes(1)
        .with_identity(&identity.private_key_hex);
    let builder = if signing {
        builder.with_signing()
    } else {
        // Opt out of the `signed-docs` multiplier, which would otherwise force
        // signing back on and make the unsigned case unrunnable.
        builder.no_signing_multiplier()
    };

    let cluster = builder.build().await.expect("cluster starts");
    let node = cluster.client(0);

    node.schema_add(SCHEMA).expect("schema add");
    node.query(r#"mutation { add_Users(input: {name: "Alice"}) { _docID } }"#)
        .expect("create Alice");

    let commits = node
        .query(r#"query { _commits { signature { type } } }"#)
        .expect("_commits query");

    let arr = commits["_commits"]
        .as_array()
        .expect("_commits should be an array")
        .clone();
    assert!(!arr.is_empty(), "expected at least one commit");
    arr
}

#[tokio::test]
#[serial]
async fn rust_no_signing_produces_unsigned_commits() {
    let commits = commit_signatures(false).await;

    let signed: Vec<_> = commits
        .iter()
        .filter(|c| !c["signature"].is_null())
        .collect();

    assert!(
        signed.is_empty(),
        "--no-signing was passed but {} of {} commits carry a signature: {:?}",
        signed.len(),
        commits.len(),
        signed
    );
}

/// The other half of the pair: without `--no-signing`, a configured node
/// identity still signs. Without this, the test above would pass just as well
/// against a build where signing never happens at all.
#[tokio::test]
#[serial]
async fn rust_signing_enabled_produces_signed_commits() {
    let commits = commit_signatures(true).await;

    let signed = commits.iter().filter(|c| !c["signature"].is_null()).count();

    assert!(
        signed > 0,
        "expected signed commits with a node identity and signing enabled, got {:?}",
        commits
    );
}
