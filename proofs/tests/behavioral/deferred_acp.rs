//! Deferred-ACP overlay consistency family — `MC_DeferredAcp_Green`.
//!
//! INV_FailClosedActive: a transaction-local ACP projection gates *exactly* as
//! committed state would. Within an uncommitted txn, an ACP-protected document
//! created by the owner is visible to the owner (positive) but denied to an
//! unauthorized identity (the projection gates — no owner-bypass), and once the
//! txn is discarded the write leaves no residue in committed state (rollback is
//! a no-op).
//!
//! Anti-tautology: every negative (denied / empty) is preceded by a positive
//! (owner sees the doc through the *same* txn-scoped path), so a denial can
//! never pass merely because the txn setup silently failed to write anything.

use crate::support;
use defra_harness::fixtures::{users_schema_with_policy, USER_ACP_POLICY};
use defra_harness::{generate_identity, TestCluster};
use serde_json::Value;
use std::path::Path;
use std::process::Command;

fn user_count(v: &Value) -> usize {
    v["User"].as_array().map(|a| a.len()).unwrap_or(0)
}

/// Run a GraphQL query through the CLI inside a transaction *and* under a
/// specific identity. The harness exposes `query_with_tx` (no identity) and
/// `query_with_identity` (no tx) but not the composition; both `--tx` and `-i`
/// are `global = true` flags on the `client` subcommand, so they compose. We
/// build the exact `exec` arg shape directly against the node's binary + URL.
fn query_with_tx_and_identity(
    binary: &Path,
    url: &str,
    gql: &str,
    tx_id: &str,
    hex_key: &str,
) -> Value {
    let output = Command::new(binary)
        .args([
            "--url", url, "client", "-i", hex_key, "--tx", tx_id, "query", gql,
        ])
        .output()
        .expect("spawn tx+identity query");
    assert!(
        output.status.success(),
        "tx+identity query failed (exit {}): stderr={} stdout={}",
        output.status,
        String::from_utf8_lossy(&output.stderr).trim(),
        String::from_utf8_lossy(&output.stdout).trim(),
    );
    let out = String::from_utf8_lossy(&output.stdout);
    let json_str = out.find('{').map(|i| &out[i..]).unwrap_or(&out);
    let val: Value = serde_json::from_str(json_str).expect("parse tx+identity query output");
    val.get("data").cloned().unwrap_or(val)
}

/// Same as above but for a mutation: returns the parsed `data` payload so the
/// caller can pull out the created `_docID`.
fn mutate_with_tx_and_identity(
    binary: &Path,
    url: &str,
    gql: &str,
    tx_id: &str,
    hex_key: &str,
) -> Value {
    query_with_tx_and_identity(binary, url, gql, tx_id, hex_key)
}

#[tokio::test]
async fn deferred_acp_txn_local_gating() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_acp_local()
        .with_rust_binary(support::release_binary())
        .build()
        .await
        .expect("build ACP-enabled single-node cluster");
    let node = cluster.client(0);
    let binary = node.binary_path().to_path_buf();
    // The CLI `--url` flag wants a bare `host:port`; `api_url` carries the
    // `http://` scheme (used for raw HTTP), so strip it for CLI invocation.
    let url = cluster
        .api_url(0)
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .to_string();

    let owner = generate_identity(&binary).expect("owner identity");
    let mallory = generate_identity(&binary).expect("mallory identity");

    // Schema + policy are committed (auto-commit) so the txn projects over a
    // real ACP-protected collection.
    let policy = node
        .acp_policy_add(USER_ACP_POLICY, &owner.private_key_hex)
        .expect("add ACP policy");
    let policy_id = policy["PolicyID"]
        .as_str()
        .or_else(|| policy["policyID"].as_str())
        .expect("PolicyID in policy add result");
    node.schema_add_with_identity(&users_schema_with_policy(policy_id), &owner.private_key_hex)
        .expect("add @policy schema");

    let user_q = "query { User { _docID name } }";

    // ---- Phase 1: txn-local projection gates as committed state would ----

    let tx = node.tx_create().expect("create transaction");

    // Owner creates an ACP-protected document *inside the uncommitted txn*.
    let created = mutate_with_tx_and_identity(
        &binary,
        &url,
        r#"mutation { add_User(input: {name: "TxnSecret", age: 7}) { _docID } }"#,
        &tx,
        &owner.private_key_hex,
    );
    let doc_id = created["add_User"][0]["_docID"]
        .as_str()
        .expect("_docID from txn-scoped create")
        .to_string();
    assert!(!doc_id.is_empty(), "txn-scoped create must yield a _docID");

    // POSITIVE (anti-tautology): the owner sees the doc through the *same*
    // txn-scoped, identity-bearing path. If this were empty, every later denial
    // would be vacuous.
    let owner_in_tx =
        query_with_tx_and_identity(&binary, &url, user_q, &tx, &owner.private_key_hex);
    assert_eq!(
        user_count(&owner_in_tx),
        1,
        "owner must see the txn-local protected doc (projection lets the owner through)"
    );

    // NEGATIVE — no owner-bypass: an unauthorized identity, querying inside the
    // SAME uncommitted txn, is still denied. The deferred-ACP overlay gates the
    // txn-local projection exactly as committed state would.
    let mallory_in_tx =
        query_with_tx_and_identity(&binary, &url, user_q, &tx, &mallory.private_key_hex);
    assert_eq!(
        user_count(&mallory_in_tx),
        0,
        "unauthorized identity must be denied the txn-local doc (no owner-bypass in the overlay)"
    );

    // NEGATIVE — anonymous identity inside the txn is likewise denied.
    let anon_in_tx = {
        let output = Command::new(&binary)
            .args(["--url", &url, "client", "--tx", &tx, "query", user_q])
            .output()
            .expect("spawn anon tx query");
        assert!(output.status.success(), "anon tx query should not error");
        let out = String::from_utf8_lossy(&output.stdout);
        let s = out.find('{').map(|i| &out[i..]).unwrap_or(&out);
        let v: Value = serde_json::from_str(s).expect("parse anon tx query");
        v.get("data").cloned().unwrap_or(v)
    };
    assert_eq!(
        user_count(&anon_in_tx),
        0,
        "anonymous identity must be denied the txn-local doc"
    );

    // ---- Phase 2: rollback is a no-op (no residue) ----

    node.tx_discard(&tx).expect("discard transaction");

    // The owner — now in committed state — must see NOTHING: the discarded
    // write left no residue. Contrast with Phase 1 where the owner saw exactly
    // one doc, proving this emptiness is caused by rollback, not by ACP denial.
    let owner_committed = node
        .query_with_identity(user_q, &owner.private_key_hex)
        .expect("owner committed query after discard");
    assert_eq!(
        user_count(&owner_committed),
        0,
        "discarded txn must leave no residue: owner sees nothing in committed state"
    );

    // ---- Phase 3: committed-state baseline confirms the gate is real ----
    // Commit an equivalent doc outside any txn, then prove the SAME unauthorized
    // identity is denied in committed state. This anchors Phase 1's txn-local
    // denial to the committed-state contract the invariant claims equivalence to.

    let committed = node
        .query_with_identity(
            r#"mutation { add_User(input: {name: "CommittedSecret", age: 9}) { _docID } }"#,
            &owner.private_key_hex,
        )
        .expect("owner commits a protected doc");
    assert!(
        committed["add_User"][0]["_docID"].as_str().is_some(),
        "committed create must yield a _docID"
    );

    // POSITIVE: owner sees the committed doc.
    assert_eq!(
        user_count(
            &node
                .query_with_identity(user_q, &owner.private_key_hex)
                .expect("owner committed query")
        ),
        1,
        "owner must see the committed protected doc"
    );

    // NEGATIVE: the unauthorized identity is denied in committed state too —
    // identical gating to Phase 1's txn-local projection.
    assert_eq!(
        user_count(
            &node
                .query_with_identity(user_q, &mallory.private_key_hex)
                .expect("mallory committed query")
        ),
        0,
        "unauthorized identity must be denied in committed state (matches txn-local gating)"
    );
}
