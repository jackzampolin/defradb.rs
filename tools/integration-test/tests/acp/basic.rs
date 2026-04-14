use integration_test::{
    for_each_runtime, generate_identity, users_schema_with_policy, TestCluster, USER_ACP_POLICY,
};

async fn acp_basic_test(cluster: TestCluster) {
    let node = cluster.client(0);
    let binary_path = node.binary_path().to_path_buf();

    // Generate two identities: Alice (owner) and Bob (outsider)
    let alice = generate_identity(&binary_path).expect("failed to generate Alice identity");
    let bob = generate_identity(&binary_path).expect("failed to generate Bob identity");

    // Add ACP policy as Alice
    let policy_result = node
        .acp_policy_add(USER_ACP_POLICY, &alice.private_key_hex)
        .expect("failed to add ACP policy");

    let policy_id = policy_result["PolicyID"]
        .as_str()
        .or_else(|| policy_result["policyID"].as_str())
        .expect("missing PolicyID in policy add result");

    // Deploy schema with @policy directive as Alice
    let schema = users_schema_with_policy(policy_id);
    node.schema_add_with_identity(&schema, &alice.private_key_hex)
        .expect("failed to add schema with policy");

    // Create a protected document as Alice
    let data = node
        .query_with_identity(
            r#"mutation { add_User(input: {name: "Secret", age: 42}) { _docID name age } }"#,
            &alice.private_key_hex,
        )
        .expect("failed to create protected document");

    let doc_id = data["add_User"][0]["_docID"]
        .as_str()
        .expect("missing _docID");

    // Alice queries -> sees the document
    let alice_result = node
        .query_with_identity("query { User { _docID name age } }", &alice.private_key_hex)
        .expect("Alice query failed");

    let alice_users = alice_result["User"]
        .as_array()
        .expect("Alice result not array");
    assert_eq!(alice_users.len(), 1, "Alice should see 1 document");
    assert_eq!(alice_users[0]["name"], "Secret");

    // Bob queries -> sees 0 documents (ACP blocks)
    let bob_result = node
        .query_with_identity("query { User { _docID name age } }", &bob.private_key_hex)
        .expect("Bob query failed");

    let bob_users = bob_result["User"].as_array().expect("Bob result not array");
    assert_eq!(
        bob_users.len(),
        0,
        "Bob should see 0 documents before grant"
    );

    // Alice grants Bob "reader" relationship on the document
    node.acp_relationship_add("User", doc_id, "reader", &bob.did, &alice.private_key_hex)
        .expect("failed to add reader relationship");

    // Bob queries again -> now sees the document
    let bob_result2 = node
        .query_with_identity("query { User { _docID name age } }", &bob.private_key_hex)
        .expect("Bob query after grant failed");

    let bob_users2 = bob_result2["User"]
        .as_array()
        .expect("Bob result2 not array");
    assert_eq!(bob_users2.len(), 1, "Bob should see 1 document after grant");
    assert_eq!(bob_users2[0]["name"], "Secret");
}

for_each_runtime!(acp_basic, acp_basic_test, .with_acp_local());

/// Regression test for #551 — single-doc reads against an ACP-protected
/// document by a user without permission must surface an explicit
/// permission-denied GraphQL error, not a silent empty result.
///
/// Browse queries (no explicit doc ID) keep the existing filter-only
/// semantics; only explicit `User(docID: "...")` lookups error.
///
/// Rust-only: Go DefraDB still returns empty results in this case, which
/// is the upstream gap Backbone CI hit. Once Go gains the same behavior
/// this test can move to `for_each_runtime!`.
#[tokio::test]
async fn rust_acp_explicit_denial_on_single_doc_read() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_acp_local()
        .build()
        .await
        .expect("build rust cluster with local acp");
    acp_explicit_denial_on_single_doc_read_test(cluster).await;
}

async fn acp_explicit_denial_on_single_doc_read_test(cluster: TestCluster) {
    let node = cluster.client(0);
    let binary_path = node.binary_path().to_path_buf();

    let alice = generate_identity(&binary_path).expect("generate Alice");
    let bob = generate_identity(&binary_path).expect("generate Bob");

    let policy_result = node
        .acp_policy_add(USER_ACP_POLICY, &alice.private_key_hex)
        .expect("add policy");
    let policy_id = policy_result["PolicyID"]
        .as_str()
        .or_else(|| policy_result["policyID"].as_str())
        .expect("missing PolicyID");

    let schema = users_schema_with_policy(policy_id);
    node.schema_add_with_identity(&schema, &alice.private_key_hex)
        .expect("add schema");

    let data = node
        .query_with_identity(
            r#"mutation { add_User(input: {name: "Secret", age: 42}) { _docID name } }"#,
            &alice.private_key_hex,
        )
        .expect("create protected doc");
    let doc_id = data["add_User"][0]["_docID"]
        .as_str()
        .expect("missing _docID")
        .to_string();

    // Browse query: still silently filtered (Bob sees an empty array).
    let browse_result = node
        .query_with_identity("query { User { _docID name } }", &bob.private_key_hex)
        .expect("Bob browse query failed");
    let browse_users = browse_result["User"]
        .as_array()
        .expect("browse result not array");
    assert_eq!(
        browse_users.len(),
        0,
        "browse queries should still silently filter denied docs"
    );

    // Explicit single-doc query: must surface an error mentioning the denied doc ID.
    // Different runtimes wrap denial errors differently — accept either an Err from
    // the client or an `errors[]` field in the JSON response.
    let single_doc_query = format!(r#"query {{ User(docID: "{}") {{ _docID name }} }}"#, doc_id);
    let single_doc_result = node.query_with_identity(&single_doc_query, &bob.private_key_hex);

    let denial_message = match &single_doc_result {
        Err(err) => err.to_string(),
        Ok(value) => {
            // Some HTTP layers surface GraphQL errors as a 200 response with
            // an `errors[]` field; check both shapes.
            let errors = value
                .get("errors")
                .and_then(|v| v.as_array())
                .filter(|arr| !arr.is_empty());
            match errors {
                Some(arr) => arr
                    .iter()
                    .filter_map(|e| e.get("message").and_then(|m| m.as_str()))
                    .collect::<Vec<_>>()
                    .join("; "),
                None => panic!(
                    "expected explicit permission-denied error for single-doc read, got: {}",
                    serde_json::to_string_pretty(value).unwrap()
                ),
            }
        }
    };

    assert!(
        denial_message.to_lowercase().contains("denied")
            || denial_message.to_lowercase().contains("permission"),
        "denial message should mention permission/denied, got: {}",
        denial_message
    );
    assert!(
        denial_message.contains(&doc_id),
        "denial message should reference the requested doc id, got: {}",
        denial_message
    );
}
