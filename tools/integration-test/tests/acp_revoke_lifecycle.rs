use integration_test::{
    for_each_runtime, generate_identity, users_schema_with_policy, TestCluster, USER_ACP_POLICY,
};

async fn acp_revoke_lifecycle_test(cluster: TestCluster) {
    let node = cluster.client(0);
    let binary = node.binary_path().to_path_buf();

    let alice = generate_identity(&binary).expect("Alice identity");
    let bob = generate_identity(&binary).expect("Bob identity");
    let carol = generate_identity(&binary).expect("Carol identity");

    // Alice adds policy + schema
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

    let query = "query { User { _docID name age } }";
    let count = |key: &str| -> usize {
        node.query_with_identity(query, key).expect("query")["User"]
            .as_array()
            .map(|a| a.len())
            .unwrap_or(0)
    };

    // Alice creates 1 document
    let d = node
        .query_with_identity(
            r#"mutation { create_User(input: {name: "TestDoc", age: 1}) { _docID } }"#,
            &alice.private_key_hex,
        )
        .expect("create doc");
    let doc_id = d["create_User"][0]["_docID"]
        .as_str()
        .expect("doc_id")
        .to_string();

    // Step 1: Grant Bob "reader" → Bob sees 1
    node.acp_relationship_add("User", &doc_id, "reader", &bob.did, &alice.private_key_hex)
        .expect("grant Bob reader");
    assert_eq!(count(&bob.private_key_hex), 1, "Bob=1 after grant reader");

    // Step 2: Delete Bob "reader" → Bob sees 0
    node.acp_relationship_delete("User", &doc_id, "reader", &bob.did, &alice.private_key_hex)
        .expect("revoke Bob reader");
    assert_eq!(count(&bob.private_key_hex), 0, "Bob=0 after revoke reader");

    // Step 3: Re-grant Bob "reader" → Bob sees 1
    node.acp_relationship_add("User", &doc_id, "reader", &bob.did, &alice.private_key_hex)
        .expect("re-grant Bob reader");
    assert_eq!(count(&bob.private_key_hex), 1, "Bob=1 after re-grant");

    // Step 4: Grant Bob "writer" (additive) → Bob still sees 1 (same doc)
    node.acp_relationship_add("User", &doc_id, "writer", &bob.did, &alice.private_key_hex)
        .expect("grant Bob writer");
    assert_eq!(
        count(&bob.private_key_hex),
        1,
        "Bob=1 with both reader+writer"
    );

    // Step 5: Delete Bob "reader" → Bob still sees 1 (writer expr includes read)
    node.acp_relationship_delete("User", &doc_id, "reader", &bob.did, &alice.private_key_hex)
        .expect("revoke Bob reader (keeping writer)");
    assert_eq!(
        count(&bob.private_key_hex),
        1,
        "Bob=1 with writer only (read expr: writer + reader)"
    );

    // Step 6: Delete Bob "writer" → Bob sees 0
    node.acp_relationship_delete("User", &doc_id, "writer", &bob.did, &alice.private_key_hex)
        .expect("revoke Bob writer");
    assert_eq!(count(&bob.private_key_hex), 0, "Bob=0 after all revoked");

    // Step 7: Grant Carol "reader" → Carol sees 1
    node.acp_relationship_add(
        "User",
        &doc_id,
        "reader",
        &carol.did,
        &alice.private_key_hex,
    )
    .expect("grant Carol reader");
    assert_eq!(count(&carol.private_key_hex), 1, "Carol=1");

    // Step 8: Truncate collection → Alice=0, Carol=0
    node.collection_truncate("User").expect("truncate");
    assert_eq!(count(&alice.private_key_hex), 0, "Alice=0 after truncate");
    assert_eq!(count(&carol.private_key_hex), 0, "Carol=0 after truncate");

    // Step 9: Alice creates new doc → Alice=1, Carol=0 (old grant doesn't apply)
    node.query_with_identity(
        r#"mutation { create_User(input: {name: "NewDoc", age: 2}) { _docID } }"#,
        &alice.private_key_hex,
    )
    .expect("create new doc");
    assert_eq!(count(&alice.private_key_hex), 1, "Alice=1 with new doc");
    assert_eq!(
        count(&carol.private_key_hex),
        0,
        "Carol=0 (old grant on old doc)"
    );
}

for_each_runtime!(acp_revoke_lifecycle, acp_revoke_lifecycle_test, .with_acp_local());
