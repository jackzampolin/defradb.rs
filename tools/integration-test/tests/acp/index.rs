//! ACP + index tests ported from Go DefraDB.
//!
//! Source: `tests/integration/acp/dac/index/` in
//! https://github.com/sourcenetwork/defradb (develop branch).
//!
//! Verifies that indexes on ACP-protected collections:
//! - can be created both via the `@index` field directive and via a
//!   separate `index_create` call,
//! - do not leak private documents to queries that are unauthenticated or
//!   authenticated as the wrong identity,
//! - do not bypass ACP when a filter on an indexed field triggers the
//!   index-scan execution path.
//!
//! The Go fixture mixes one anonymous document (public — created without
//! identity) with one private document (owned by Alice). Anonymous and
//! wrong-identity queries must see only the public one; the owner sees
//! both.

use integration_test::{for_each_runtime, generate_identity, TestCluster};

// =========================================================================
// Policy + schema fixtures
// =========================================================================

/// Minimal user policy (owner auto-injected per #744).
fn users_policy() -> &'static str {
    r#"
description: a test policy which marks a collection in a database as a resource
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

/// Users schema with `@index` on `name`.
fn users_schema_indexed(policy_id: &str) -> String {
    format!(
        r#"type Users @policy(id: "{}", resource: "users") {{ name: String @index  age: Int }}"#,
        policy_id
    )
}

/// Users schema without `@index` — used for the separate-index-request test.
fn users_schema_plain(policy_id: &str) -> String {
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

fn add_policy(node: &integration_test::DefraClient, key: &str) -> String {
    let result = node
        .acp_policy_add(users_policy(), key)
        .expect("add policy");
    extract_policy_id(&result).expect("policy id")
}

/// Read the `Users` collection with no filter and return the set of `name`
/// values seen by the given key (or `None` for anonymous).
fn names_visible(node: &integration_test::DefraClient, key: Option<&str>) -> Vec<String> {
    let query = "query { Users { name } }";
    let result = match key {
        Some(k) => node.query_with_identity(query, k).expect("query"),
        None => node.query(query).expect("anon query"),
    };
    result["Users"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v["name"].as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

/// Same as `names_visible` but with a filter on the indexed `name` field.
/// Exercises the index-scan execution path.
fn names_visible_filtered(
    node: &integration_test::DefraClient,
    key: Option<&str>,
    name: &str,
) -> Vec<String> {
    let query = format!(
        r#"query {{ Users(filter: {{name: {{_eq: "{}"}}}}) {{ name }} }}"#,
        name
    );
    let result = match key {
        Some(k) => node.query_with_identity(&query, k).expect("query"),
        None => node.query(&query).expect("anon query"),
    };
    result["Users"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v["name"].as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

// =========================================================================
// Creating an index on a protected collection
// =========================================================================

// Port of new_test.go (TestACP_IndexNewWithDirective_OnCollectionWithPolicy_NoError)
async fn acp_index_create_with_field_directive(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice");
    let policy_id = add_policy(&node, &alice.private_key_hex);

    // Deploying a schema with `@index` on an ACP-protected collection must
    // succeed without error.
    node.schema_add_with_identity(&users_schema_indexed(&policy_id), &alice.private_key_hex)
        .expect("schema with @index on ACP collection must deploy");

    // Empty collection → empty anonymous query result.
    assert_eq!(names_visible(&node, None), Vec::<String>::new());
}

for_each_runtime!(
    acp_index_create_with_field_directive,
    acp_index_create_with_field_directive,
    .with_acp_local()
);

// Port of new_test.go (TestACP_IndexNewWithSeparateRequest_OnCollectionWithPolicy_NoError)
async fn acp_index_create_via_separate_request(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice");
    let policy_id = add_policy(&node, &alice.private_key_hex);

    node.schema_add_with_identity(&users_schema_plain(&policy_id), &alice.private_key_hex)
        .expect("schema without @index must deploy");

    // Creating an index after the fact via the index API must also succeed.
    node.index_create("Users", &["name"], Some("some_index"), false)
        .expect("create post-hoc index on protected collection");

    assert_eq!(names_visible(&node, None), Vec::<String>::new());
}

for_each_runtime!(
    acp_index_create_via_separate_request,
    acp_index_create_via_separate_request,
    .with_acp_local()
);

// =========================================================================
// Query with index — ACP filtering parity with Go
// =========================================================================

/// Set up: policy, schema with `@index` on `name`, one public doc
/// ("Shahzad", created anonymously) and one private doc ("Islam", owned by
/// Alice). Returns Alice's private key hex.
async fn bootstrap_indexed_with_mixed_docs(node: &integration_test::DefraClient) -> String {
    let alice = generate_identity(node.binary_path()).expect("alice");
    let policy_id = add_policy(node, &alice.private_key_hex);

    node.schema_add_with_identity(&users_schema_indexed(&policy_id), &alice.private_key_hex)
        .expect("add schema");

    // Public doc — anonymous creation means no DAC registration, so it's
    // readable by anyone.
    node.query(r#"mutation { add_Users(input: {name: "Shahzad"}) { _docID } }"#)
        .expect("create public doc");

    // Private doc — owned by Alice.
    node.query_with_identity(
        r#"mutation { add_Users(input: {name: "Islam"}) { _docID } }"#,
        &alice.private_key_hex,
    )
    .expect("create private doc");

    alice.private_key_hex
}

// Port of query_test.go (TestACPWithIndex_UponQueryingPrivateDocWithoutIdentity_ShouldNotFetch)
async fn acp_index_anon_query_sees_only_public(cluster: TestCluster) {
    let node = cluster.client(0);
    let _alice = bootstrap_indexed_with_mixed_docs(&node).await;

    let visible = names_visible(&node, None);
    assert_eq!(visible, vec!["Shahzad".to_string()]);
}

for_each_runtime!(
    acp_index_anon_query_sees_only_public,
    acp_index_anon_query_sees_only_public,
    .with_acp_local()
);

// Port of query_test.go (TestACPWithIndex_UponQueryingPrivateDocWithIdentity_ShouldFetch)
async fn acp_index_owner_query_sees_both(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice_key = bootstrap_indexed_with_mixed_docs(&node).await;

    let mut visible = names_visible(&node, Some(&alice_key));
    visible.sort();
    assert_eq!(visible, vec!["Islam".to_string(), "Shahzad".to_string()]);
}

for_each_runtime!(
    acp_index_owner_query_sees_both,
    acp_index_owner_query_sees_both,
    .with_acp_local()
);

// Port of query_test.go (TestACPWithIndex_UponQueryingPrivateDocWithWrongIdentity_ShouldNotFetch)
async fn acp_index_wrong_identity_sees_only_public(cluster: TestCluster) {
    let node = cluster.client(0);
    let _alice_key = bootstrap_indexed_with_mixed_docs(&node).await;
    let bob = generate_identity(node.binary_path()).expect("bob");

    let visible = names_visible(&node, Some(&bob.private_key_hex));
    assert_eq!(visible, vec!["Shahzad".to_string()]);
}

for_each_runtime!(
    acp_index_wrong_identity_sees_only_public,
    acp_index_wrong_identity_sees_only_public,
    .with_acp_local()
);

// Added coverage beyond Go: exercise the index-scan execution path by
// filtering on the indexed `name` field. This is the codepath most likely
// to leak private docs if ACP filtering is applied after the index fetch
// instead of within it.
async fn acp_index_filtered_query_respects_acp(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice_key = bootstrap_indexed_with_mixed_docs(&node).await;
    let bob = generate_identity(node.binary_path()).expect("bob");

    // Anonymous caller filtering on "Islam" must see nothing.
    assert_eq!(
        names_visible_filtered(&node, None, "Islam"),
        Vec::<String>::new(),
        "anonymous index-scan on private doc must not leak"
    );

    // Wrong identity filtering on "Islam" must see nothing.
    assert_eq!(
        names_visible_filtered(&node, Some(&bob.private_key_hex), "Islam"),
        Vec::<String>::new(),
        "wrong-identity index-scan on private doc must not leak"
    );

    // Owner filtering on "Islam" sees the private doc via the index.
    assert_eq!(
        names_visible_filtered(&node, Some(&alice_key), "Islam"),
        vec!["Islam".to_string()],
        "owner must find the private doc via the index"
    );

    // Anonymous filtering on "Shahzad" sees the public doc.
    assert_eq!(
        names_visible_filtered(&node, None, "Shahzad"),
        vec!["Shahzad".to_string()],
        "public doc must be visible via the index to anonymous callers"
    );
}

for_each_runtime!(
    acp_index_filtered_query_respects_acp,
    acp_index_filtered_query_respects_acp,
    .with_acp_local()
);
