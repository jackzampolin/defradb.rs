use integration_test::{
    for_each_runtime, generate_identity, users_schema_with_policy, TestCluster, USER_ACP_POLICY,
};

/// 02-22: Unauthorized user cannot read _commits for an ACP-protected document.
///
/// The _commits path is a separate code path from the User query path.
/// Both must apply ACP filtering independently.
async fn commits_acp_denied_test(cluster: TestCluster) {
    let node = cluster.client(0);
    let binary = node.binary_path().to_path_buf();

    let alice = generate_identity(&binary).expect("Alice identity");
    let bob = generate_identity(&binary).expect("Bob identity");

    let policy = node
        .acp_policy_add(USER_ACP_POLICY, &alice.private_key_hex)
        .expect("add policy");
    let policy_id = policy["PolicyID"]
        .as_str()
        .or_else(|| policy["policyID"].as_str())
        .expect("missing PolicyID");
    let schema = users_schema_with_policy(policy_id);
    node.schema_add_with_identity(&schema, &alice.private_key_hex)
        .expect("add schema");

    let data = node
        .query_with_identity(
            r#"mutation { add_User(input: {name: "Secret", age: 42}) { _docID } }"#,
            &alice.private_key_hex,
        )
        .expect("create doc");
    let doc_id = data["add_User"][0]["_docID"]
        .as_str()
        .expect("_docID")
        .to_string();

    // Alice can read her own commits
    let alice_commits = node
        .query_with_identity(
            &format!(
                r#"query {{ _commits(docID: "{}") {{ cid height }} }}"#,
                doc_id
            ),
            &alice.private_key_hex,
        )
        .expect("Alice commits query");
    let alice_count = alice_commits["_commits"]
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0);
    assert!(
        alice_count > 0,
        "Alice should see commits for her own document"
    );

    // Bob cannot read commits for a document he has no access to
    let bob_commits = node
        .query_with_identity(
            &format!(
                r#"query {{ _commits(docID: "{}") {{ cid height }} }}"#,
                doc_id
            ),
            &bob.private_key_hex,
        )
        .expect("Bob commits query succeeded at transport layer");
    let bob_count = bob_commits["_commits"]
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0);
    assert_eq!(
        bob_count, 0,
        "Bob must not read commits for a document he cannot access (ACP must filter _commits)"
    );
}

#[tokio::test]
async fn rust_commits_acp_denied() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_acp_local()
        .build()
        .await
        .unwrap();
    commits_acp_denied_test(cluster).await;
}

/// Known upstream bug: Go DefraDB does not filter _commits by ACP.
#[tokio::test]
#[ignore]
async fn go_commits_acp_denied() {
    let cluster = TestCluster::builder()
        .go_nodes(1)
        .with_acp_local()
        .build()
        .await
        .unwrap();
    commits_acp_denied_test(cluster).await;
}

/// 02-23: Dump endpoint requires authentication — anonymous dump is denied.
///
/// The dump operation exposes raw block-level data. Without ACP enforcement it
/// would allow any caller to extract all stored content regardless of policy.
async fn dump_requires_auth_test(cluster: TestCluster) {
    let node = cluster.client(0);
    let binary = node.binary_path().to_path_buf();

    let alice = generate_identity(&binary).expect("Alice identity");

    let policy = node
        .acp_policy_add(USER_ACP_POLICY, &alice.private_key_hex)
        .expect("add policy");
    let policy_id = policy["PolicyID"]
        .as_str()
        .or_else(|| policy["policyID"].as_str())
        .expect("missing PolicyID");
    let schema = users_schema_with_policy(policy_id);
    node.schema_add_with_identity(&schema, &alice.private_key_hex)
        .expect("add schema");

    node.query_with_identity(
        r#"mutation { add_User(input: {name: "Confidential", age: 1}) { _docID } }"#,
        &alice.private_key_hex,
    )
    .expect("create doc");

    // Anonymous dump must be denied. An unauthenticated dump would expose
    // all block data regardless of ACP policy.
    let dump_result = node.dump();
    assert!(
        dump_result.is_err(),
        "anonymous dump must be denied when ACP is active — got: {:?}",
        dump_result.ok()
    );
}

for_each_runtime!(dump_requires_auth, dump_requires_auth_test, .with_acp_local());

/// 02-26: Mutation denial assertions are precise — update and delete by unauthorized
/// user return zero affected documents, and the document remains unchanged.
async fn mutation_denial_precise_test(cluster: TestCluster) {
    let node = cluster.client(0);
    let binary = node.binary_path().to_path_buf();

    let alice = generate_identity(&binary).expect("Alice identity");
    let bob = generate_identity(&binary).expect("Bob identity");

    let policy = node
        .acp_policy_add(USER_ACP_POLICY, &alice.private_key_hex)
        .expect("add policy");
    let policy_id = policy["PolicyID"]
        .as_str()
        .or_else(|| policy["policyID"].as_str())
        .expect("missing PolicyID");
    let schema = users_schema_with_policy(policy_id);
    node.schema_add_with_identity(&schema, &alice.private_key_hex)
        .expect("add schema");

    let data = node
        .query_with_identity(
            r#"mutation { add_User(input: {name: "Original", age: 10}) { _docID name age } }"#,
            &alice.private_key_hex,
        )
        .expect("create doc");
    let doc_id = data["add_User"][0]["_docID"]
        .as_str()
        .expect("_docID")
        .to_string();

    // Bob attempts update — must affect 0 rows
    let bob_update = node.query_with_identity(
        &format!(
            r#"mutation {{ update_User(docID: "{}", input: {{name: "Hijacked", age: 99}}) {{ _docID name age }} }}"#,
            doc_id
        ),
        &bob.private_key_hex,
    );
    match bob_update {
        Err(_) => {}
        Ok(val) => {
            let updated = val["update_User"].as_array().map(|a| a.len()).unwrap_or(0);
            assert_eq!(
                updated, 0,
                "unauthorized update must affect 0 documents, got {}: {:?}",
                updated, val
            );
        }
    }

    // Verify document content is unchanged after Bob's attempt
    let alice_read = node
        .query_with_identity("query { User { _docID name age } }", &alice.private_key_hex)
        .expect("Alice read after Bob update attempt");
    let users = alice_read["User"].as_array().expect("User array");
    assert_eq!(users.len(), 1, "document must still exist");
    assert_eq!(
        users[0]["name"], "Original",
        "name must be unchanged after unauthorized update attempt"
    );
    assert_eq!(
        users[0]["age"], 10,
        "age must be unchanged after unauthorized update attempt"
    );

    // Bob attempts delete — must affect 0 rows
    let bob_delete = node.query_with_identity(
        &format!(
            r#"mutation {{ delete_User(docID: "{}") {{ _docID }} }}"#,
            doc_id
        ),
        &bob.private_key_hex,
    );
    match bob_delete {
        Err(_) => {}
        Ok(val) => {
            let deleted = val["delete_User"].as_array().map(|a| a.len()).unwrap_or(0);
            assert_eq!(
                deleted, 0,
                "unauthorized delete must affect 0 documents, got {}: {:?}",
                deleted, val
            );
        }
    }

    // Document must still be readable by Alice after Bob's delete attempt
    let after_delete = node
        .query_with_identity("query { User { _docID name age } }", &alice.private_key_hex)
        .expect("Alice read after Bob delete attempt");
    assert_eq!(
        after_delete["User"]
            .as_array()
            .map(|a| a.len())
            .unwrap_or(0),
        1,
        "document must survive unauthorized delete attempt"
    );
}

for_each_runtime!(mutation_denial_precise, mutation_denial_precise_test, .with_acp_local());

/// Anonymous create on an ACP-protected collection (DAC only, no NAC) succeeds
/// but the document is unregistered with ACP — effectively public.
///
/// Go intentionally allows this: `RegisterDocOnCollectionWithDocumentACP` skips
/// registration when identity is empty.  Only NAC (node-level access control)
/// blocks anonymous operations.
async fn anonymous_create_is_public_test(cluster: TestCluster) {
    let node = cluster.client(0);
    let binary = node.binary_path().to_path_buf();

    let alice = generate_identity(&binary).expect("Alice identity");

    let policy = node
        .acp_policy_add(USER_ACP_POLICY, &alice.private_key_hex)
        .expect("add policy");
    let policy_id = policy["PolicyID"]
        .as_str()
        .or_else(|| policy["policyID"].as_str())
        .expect("missing PolicyID");
    let schema = users_schema_with_policy(policy_id);
    node.schema_add_with_identity(&schema, &alice.private_key_hex)
        .expect("add schema");

    // Anonymous create succeeds — the document is created but unregistered with ACP
    let anon_create = node
        .query(r#"mutation { add_User(input: {name: "Ghost", age: 0}) { _docID } }"#)
        .expect("anonymous create should succeed in DAC-only mode");
    let created = anon_create["add_User"]
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0);
    assert_eq!(
        created, 1,
        "anonymous create produces 1 document (unregistered with ACP)"
    );

    // The unregistered document is publicly readable (even by anonymous queries)
    let anon_read = node
        .query("query { User { _docID name } }")
        .expect("anonymous read");
    let anon_count = anon_read["User"].as_array().map(|a| a.len()).unwrap_or(0);
    assert!(anon_count >= 1, "anonymous doc should be publicly readable");

    // Alice can also create normally — her doc IS registered with ACP
    let alice_create = node
        .query_with_identity(
            r#"mutation { add_User(input: {name: "Legit", age: 1}) { _docID } }"#,
            &alice.private_key_hex,
        )
        .expect("authorized create must succeed");
    let alice_count = alice_create["add_User"]
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0);
    assert_eq!(alice_count, 1, "Alice's create must produce 1 document");
}

for_each_runtime!(anonymous_create_is_public, anonymous_create_is_public_test, .with_acp_local());

/// 02-25: NAC enforcement applies to GraphQL queries — a user without NAC admin
/// access sees an empty result even if they supply a valid identity.
async fn nac_graphql_enforcement_test(cluster: TestCluster) {
    let node = cluster.client(0);
    let binary = node.binary_path().to_path_buf();

    let jack_key = cluster
        .startup_identity()
        .expect("NAC cluster must have a startup identity")
        .to_string();

    let outsider = generate_identity(&binary).expect("outsider identity");

    let policy = node
        .acp_policy_add(USER_ACP_POLICY, &jack_key)
        .expect("add policy");
    let policy_id = policy["PolicyID"]
        .as_str()
        .or_else(|| policy["policyID"].as_str())
        .expect("missing PolicyID");
    let schema = users_schema_with_policy(policy_id);
    node.schema_add_with_identity(&schema, &jack_key)
        .expect("add schema");

    node.query_with_identity(
        r#"mutation { add_User(input: {name: "NACProtected", age: 7}) { _docID } }"#,
        &jack_key,
    )
    .expect("create doc");

    // NAC admin (jack) sees the document
    let jack_result = node
        .query_with_identity("query { User { _docID name age } }", &jack_key)
        .expect("jack query");
    assert_eq!(
        jack_result["User"].as_array().map(|a| a.len()).unwrap_or(0),
        1,
        "NAC admin must see the document"
    );

    // Outsider with a valid identity but no NAC admin grant must be blocked.
    // Go returns 200 with empty results; Rust returns HTTP 401.  Both are
    // valid enforcement — the outsider sees no data either way.
    let outsider_result = node.query_with_identity(
        "query { User { _docID name age } }",
        &outsider.private_key_hex,
    );
    match outsider_result {
        Err(_) => {
            // Rust returns 401 — strict NAC enforcement at HTTP layer
        }
        Ok(val) => {
            // Go returns 200 with empty results — NAC enforcement in query engine
            let outsider_count = val["User"].as_array().map(|a| a.len()).unwrap_or(0);
            assert_eq!(
                outsider_count, 0,
                "non-NAC-admin identity must see 0 documents"
            );
        }
    }
}

for_each_runtime!(nac_graphql_enforcement, nac_graphql_enforcement_test, .with_acp_local().with_nac());
