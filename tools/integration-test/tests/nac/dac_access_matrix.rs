//! NAC × DAC access matrix tests ported from Go DefraDB.
//!
//! Source: `tests/integration/acp/nac/dac_access_by_*_test.go` in
//! https://github.com/sourcenetwork/defradb (develop branch).
//!
//! Covers the interaction between NAC (node access control) and DAC
//! (document access control) — specifically:
//!
//! 1. **Node-owner bypass of DAC** (#739 regression guard). With NAC
//!    enabled, the startup identity — which by default holds the `admin`
//!    NAC relation and therefore the `DacBypass` permission — can access
//!    documents owned by other identities. This is served on the HTTP
//!    query path by [crates/http/src/query_context.rs] via
//!    `should_bypass_dac`, and on the mutation / direct-DB path by the
//!    node-identity shortcut PR #743 added to `check_doc_permission`.
//!    This test locks in the *user-visible* behavior across both paths.
//! 2. **NAC precedes DAC**. An unauthenticated caller or a caller without
//!    a NAC admin grant is rejected before DAC is consulted — even if they
//!    are the DAC owner.
//! 3. **Revoking NAC admin removes access** even if the target still holds
//!    their DAC ownership.
//!
//! These tests complement `nac/document_acp.rs` (which covers the
//! combined NAC-admin-plus-DAC-reader path) by locking in the matrix
//! cells that were behavioral gaps in Phase 1 of the ACP audit.

use integration_test::{for_each_runtime, generate_identity, TestCluster};

// =========================================================================
// Policy + schema fixtures
// =========================================================================

/// DAC policy used for all tests in this file. Owner auto-injected per
/// #744.
fn users_policy() -> &'static str {
    r#"
description: NAC × DAC test policy
name: test
resources:
  - name: users
    permissions:
      - name: read
        expr: reader
      - name: update
        expr: updater
      - name: delete
        expr: deleter
    relations:
      - name: reader
        types:
          - actor
      - name: updater
        types:
          - actor
      - name: deleter
        types:
          - actor
"#
}

fn users_schema(policy_id: &str) -> String {
    format!(
        r#"type Users @policy(id: "{}", resource: "users") {{ name: String  age: Int }}"#,
        policy_id
    )
}

fn extract_policy_id(value: &serde_json::Value) -> Option<String> {
    value["PolicyID"]
        .as_str()
        .or_else(|| value["policyID"].as_str())
        .map(|s| s.to_string())
}

fn user_count_for(node: &integration_test::DefraClient, key: &str) -> usize {
    node.query_with_identity("query { Users { name } }", key)
        .expect("query Users")["Users"]
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0)
}

/// Check a query result tolerantly: returns `true` if the caller either
/// got an error OR got an empty/non-array `Users` field. NAC can reject
/// either at the HTTP layer (Rust) or via the query engine (Go).
fn saw_no_data<E>(result: &Result<serde_json::Value, E>) -> bool {
    match result {
        Err(_) => true,
        Ok(val) => val["Users"]
            .as_array()
            .map(|a| a.is_empty())
            .unwrap_or(true),
    }
}

// =========================================================================
// #1 — Node owner reads a document owned by another identity under NAC
//
// User-visible regression test for #739 / PR #743: the node owner must be
// able to read documents registered to other DIDs while NAC is enabled.
// Today this is served via the HTTP `resolve_dac_bypass` path using NAC's
// `should_bypass_dac`. PR #743 added belt-and-suspenders coverage for the
// direct-db / mutation path by wiring a node-identity shortcut into
// `check_doc_permission`. Both paths must keep this test green.
// =========================================================================

async fn nac_node_owner_reads_foreign_dac_doc(cluster: TestCluster) {
    let node = cluster.client(0);
    let binary = node.binary_path().to_path_buf();

    // Alice is the startup identity = NAC admin + node owner.
    let alice_key = cluster
        .startup_identity()
        .expect("NAC cluster must have a startup identity")
        .to_string();

    // Bob: another identity we promote to NAC admin so he can operate the
    // API while NAC is on, but who will OWN the document in DAC.
    let bob = generate_identity(&binary).expect("bob identity");
    node.acp_node_relationship_add("admin", &bob.did, &alice_key)
        .expect("grant Bob NAC admin");

    // Bob creates the policy, deploys the schema, and writes the doc —
    // Bob is the DAC owner.
    let policy = node
        .acp_policy_add(users_policy(), &bob.private_key_hex)
        .expect("Bob adds policy");
    let policy_id = extract_policy_id(&policy).expect("policy id");

    node.schema_add_with_identity(&users_schema(&policy_id), &bob.private_key_hex)
        .expect("Bob adds schema");

    node.query_with_identity(
        r#"mutation { add_Users(input: {name: "BobDoc", age: 28}) { _docID } }"#,
        &bob.private_key_hex,
    )
    .expect("Bob creates doc");

    // Alice is the node owner. She must see Bob's doc via the node-identity
    // shortcut added in PR #743 (#739 regression).
    assert_eq!(
        user_count_for(&node, &alice_key),
        1,
        "node owner must bypass DAC and see documents owned by other \
         identities — regression test for #739"
    );

    // And Bob, as the DAC owner, still sees his own doc.
    assert_eq!(user_count_for(&node, &bob.private_key_hex), 1);
}

for_each_runtime!(
    nac_node_owner_reads_foreign_dac_doc,
    nac_node_owner_reads_foreign_dac_doc,
    .with_acp_local().with_nac()
);

// =========================================================================
// #2 — Non-admin identity is rejected under NAC (even if DAC would allow)
//
// Port of dac_access_by_wrong_user_nac_on_test.go — NAC precedes DAC.
// =========================================================================

async fn nac_non_admin_identity_rejected(cluster: TestCluster) {
    let node = cluster.client(0);
    let binary = node.binary_path().to_path_buf();

    let alice_key = cluster
        .startup_identity()
        .expect("NAC cluster must have a startup identity")
        .to_string();

    // Bob is promoted to NAC admin temporarily so he can set up the
    // fixture, owns the doc, then is revoked.
    let bob = generate_identity(&binary).expect("bob identity");
    node.acp_node_relationship_add("admin", &bob.did, &alice_key)
        .expect("grant Bob NAC admin");

    let policy = node
        .acp_policy_add(users_policy(), &bob.private_key_hex)
        .expect("Bob adds policy");
    let policy_id = extract_policy_id(&policy).expect("policy id");

    node.schema_add_with_identity(&users_schema(&policy_id), &bob.private_key_hex)
        .expect("Bob adds schema");

    node.query_with_identity(
        r#"mutation { add_Users(input: {name: "BobDoc"}) { _docID } }"#,
        &bob.private_key_hex,
    )
    .expect("Bob creates doc");

    // Revoke Bob's NAC admin. He is still the DAC owner — but NAC must
    // take precedence and reject his queries.
    node.acp_node_relationship_delete("admin", &bob.did, &alice_key)
        .expect("revoke Bob NAC admin");

    let bob_after = node.query_with_identity("query { Users { name } }", &bob.private_key_hex);
    assert!(
        saw_no_data(&bob_after),
        "DAC owner without NAC admin must not see the document \
         (NAC precedes DAC); got: {:?}",
        bob_after
    );
}

for_each_runtime!(
    nac_non_admin_identity_rejected,
    nac_non_admin_identity_rejected,
    .with_acp_local().with_nac()
);

// =========================================================================
// #3 — Anonymous caller rejected under NAC
//
// Port of dac_access_by_empty_user_nac_on_test.go.
// =========================================================================

async fn nac_anonymous_rejected(cluster: TestCluster) {
    let node = cluster.client(0);

    let alice_key = cluster
        .startup_identity()
        .expect("NAC cluster must have a startup identity")
        .to_string();

    let policy = node
        .acp_policy_add(users_policy(), &alice_key)
        .expect("Alice adds policy");
    let policy_id = extract_policy_id(&policy).expect("policy id");

    node.schema_add_with_identity(&users_schema(&policy_id), &alice_key)
        .expect("Alice adds schema");

    node.query_with_identity(
        r#"mutation { add_Users(input: {name: "AliceDoc"}) { _docID } }"#,
        &alice_key,
    )
    .expect("Alice creates doc");

    // Anonymous caller (no identity) must not see anything — NAC blocks
    // unauthenticated queries entirely.
    let anon = node.query("query { Users { name } }");
    assert!(
        saw_no_data(&anon),
        "anonymous caller must not see data under NAC; got: {:?}",
        anon
    );
}

for_each_runtime!(
    nac_anonymous_rejected,
    nac_anonymous_rejected,
    .with_acp_local().with_nac()
);

// =========================================================================
// #4 — NAC-admin-grant-then-revoke removes access even if doc ownership
// is preserved.
//
// Port of the revoke half of dac_access_by_wrong_user_nac_on_test.go,
// verifying the state transition explicitly: Bob grants himself doc
// access under NAC while admin, then loses NAC admin, and can no longer
// see his own doc.
// =========================================================================

async fn nac_revoke_admin_removes_access(cluster: TestCluster) {
    let node = cluster.client(0);
    let binary = node.binary_path().to_path_buf();

    let alice_key = cluster
        .startup_identity()
        .expect("NAC cluster must have a startup identity")
        .to_string();

    let bob = generate_identity(&binary).expect("bob identity");
    node.acp_node_relationship_add("admin", &bob.did, &alice_key)
        .expect("grant Bob NAC admin");

    let policy = node
        .acp_policy_add(users_policy(), &bob.private_key_hex)
        .expect("Bob adds policy");
    let policy_id = extract_policy_id(&policy).expect("policy id");

    node.schema_add_with_identity(&users_schema(&policy_id), &bob.private_key_hex)
        .expect("Bob adds schema");

    node.query_with_identity(
        r#"mutation { add_Users(input: {name: "BobDoc"}) { _docID } }"#,
        &bob.private_key_hex,
    )
    .expect("Bob creates doc");

    // Sanity: Bob, still NAC admin, sees his own doc.
    assert_eq!(
        user_count_for(&node, &bob.private_key_hex),
        1,
        "Bob (NAC admin + DAC owner) must see his own doc before revoke"
    );

    node.acp_node_relationship_delete("admin", &bob.did, &alice_key)
        .expect("revoke Bob NAC admin");

    let bob_after = node.query_with_identity("query { Users { name } }", &bob.private_key_hex);
    assert!(
        saw_no_data(&bob_after),
        "after NAC admin revoke, Bob must not see his own doc \
         (NAC precedes DAC); got: {:?}",
        bob_after
    );

    // Alice (node owner) still sees it via node-identity bypass.
    assert_eq!(
        user_count_for(&node, &alice_key),
        1,
        "node owner must continue to see Bob's doc after revoke"
    );
}

for_each_runtime!(
    nac_revoke_admin_removes_access,
    nac_revoke_admin_removes_access,
    .with_acp_local().with_nac()
);
