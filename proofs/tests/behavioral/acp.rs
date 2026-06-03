//! ACP family — both read paths (the `User` collection query AND the `_commits`
//! DAG-scan) are independently gated, and revocation propagates to both.
//! Model: `MC_Acp_Green` / `MC_Commits_Green` (proofs/tla).
//!
//! Anti-tautology: the positive side (owner sees both paths; a granted identity
//! sees both) is asserted before every negative, so an empty result can never
//! pass merely because setup silently failed.

use crate::support;
use defra_harness::fixtures::{users_schema_with_policy, USER_ACP_POLICY};
use defra_harness::{generate_identity, TestCluster};
use serde_json::Value;

fn user_count(v: &Value) -> usize {
    v["User"].as_array().map(|a| a.len()).unwrap_or(0)
}
fn commit_count(v: &Value) -> usize {
    v["_commits"].as_array().map(|a| a.len()).unwrap_or(0)
}

#[tokio::test]
async fn acp_dual_path_gating_and_revocation() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_acp_local()
        .with_rust_binary(support::release_binary())
        .build()
        .await
        .expect("build ACP-enabled single-node cluster");
    let node = cluster.client(0);
    let binary = node.binary_path().to_path_buf();
    let owner = generate_identity(&binary).expect("owner identity");
    let bob = generate_identity(&binary).expect("bob identity");

    let policy = node
        .acp_policy_add(USER_ACP_POLICY, &owner.private_key_hex)
        .expect("add ACP policy");
    let policy_id = policy["PolicyID"]
        .as_str()
        .or_else(|| policy["policyID"].as_str())
        .expect("PolicyID in policy add result");
    node.schema_add_with_identity(&users_schema_with_policy(policy_id), &owner.private_key_hex)
        .expect("add @policy schema");

    let created = node
        .query_with_identity(
            r#"mutation { add_User(input: {name: "Secret", age: 42}) { _docID } }"#,
            &owner.private_key_hex,
        )
        .expect("owner creates protected document");
    let doc_id = created["add_User"][0]["_docID"]
        .as_str()
        .expect("_docID")
        .to_string();

    let user_q = "query { User { _docID name } }";
    let commits_q = format!(r#"query {{ _commits(docID: "{doc_id}") {{ cid height }} }}"#);

    // Positive: the owner sees BOTH paths (so later emptiness is meaningful).
    let o_user = node
        .query_with_identity(user_q, &owner.private_key_hex)
        .expect("owner User query");
    let o_com = node
        .query_with_identity(&commits_q, &owner.private_key_hex)
        .expect("owner commits query");
    assert_eq!(user_count(&o_user), 1, "owner must see the document");
    assert!(
        commit_count(&o_com) > 0,
        "owner must see the document's commits"
    );

    // Anonymous outsider sees NEITHER path.
    assert_eq!(
        user_count(&node.query(user_q).expect("anon User query")),
        0,
        "anonymous must not see the User doc (User path gated)"
    );
    assert_eq!(
        commit_count(&node.query(&commits_q).expect("anon commits query")),
        0,
        "anonymous must not see _commits (commits path gated)"
    );

    // A real but ungranted identity (bob) sees NEITHER path.
    assert_eq!(
        user_count(
            &node
                .query_with_identity(user_q, &bob.private_key_hex)
                .expect("bob User query")
        ),
        0,
        "ungranted bob must not see the User doc"
    );
    assert_eq!(
        commit_count(
            &node
                .query_with_identity(&commits_q, &bob.private_key_hex)
                .expect("bob commits query")
        ),
        0,
        "ungranted bob must not see _commits"
    );

    // Grant bob reader -> he now sees BOTH paths.
    node.acp_relationship_add("User", &doc_id, "reader", &bob.did, &owner.private_key_hex)
        .expect("grant bob reader");
    assert_eq!(
        user_count(
            &node
                .query_with_identity(user_q, &bob.private_key_hex)
                .expect("bob User query (granted)")
        ),
        1,
        "granted bob must see the User doc"
    );
    assert!(
        commit_count(
            &node
                .query_with_identity(&commits_q, &bob.private_key_hex)
                .expect("bob commits query (granted)")
        ) > 0,
        "granted bob must see _commits"
    );

    // Revoke -> bob loses BOTH paths again (revocation reaches both gates).
    node.acp_relationship_delete("User", &doc_id, "reader", &bob.did, &owner.private_key_hex)
        .expect("revoke bob reader");
    assert_eq!(
        user_count(
            &node
                .query_with_identity(user_q, &bob.private_key_hex)
                .expect("bob User query (revoked)")
        ),
        0,
        "revoked bob must not see the User doc"
    );
    assert_eq!(
        commit_count(
            &node
                .query_with_identity(&commits_q, &bob.private_key_hex)
                .expect("bob commits query (revoked)")
        ),
        0,
        "revoked bob must not see _commits"
    );
}
