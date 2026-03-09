use integration_test::{
    documents_schema_with_policy, for_each_runtime, generate_identity, TestCluster,
    MULTI_ROLE_ACP_POLICY,
};

async fn acp_multi_role_test(cluster: TestCluster) {
    let node = cluster.client(0);
    let binary = node.binary_path().to_path_buf();

    let alice = generate_identity(&binary).expect("Alice identity");
    let bob = generate_identity(&binary).expect("Bob identity");
    let carol = generate_identity(&binary).expect("Carol identity");
    let dave = generate_identity(&binary).expect("Dave identity");

    // Alice adds multi-role policy + schema
    let policy = node
        .acp_policy_add(MULTI_ROLE_ACP_POLICY, &alice.private_key_hex)
        .expect("add policy");
    let policy_id = policy["PolicyID"]
        .as_str()
        .or_else(|| policy["policyID"].as_str())
        .expect("missing PolicyID");
    let schema = documents_schema_with_policy(policy_id);
    node.schema_add_with_identity(&schema, &alice.private_key_hex)
        .expect("add schema");

    // Alice creates 2 documents
    let d1 = node
        .query_with_identity(
            r#"mutation { add_Document(input: {title: "Doc1", content: "Content1", classification: "internal"}) { _docID } }"#,
            &alice.private_key_hex,
        )
        .expect("create Doc1");
    let id1 = d1["add_Document"][0]["_docID"]
        .as_str()
        .expect("id1")
        .to_string();

    node.query_with_identity(
        r#"mutation { add_Document(input: {title: "Doc2", content: "Content2", classification: "public"}) { _docID } }"#,
        &alice.private_key_hex,
    )
    .expect("create Doc2");

    let query = "query { Document { _docID title content classification } }";

    // Grant Bob "admin" on doc1, Carol "writer" on doc1, Dave "reader" on doc1
    node.acp_relationship_add("Document", &id1, "admin", &bob.did, &alice.private_key_hex)
        .expect("grant Bob admin");
    node.acp_relationship_add(
        "Document",
        &id1,
        "writer",
        &carol.did,
        &alice.private_key_hex,
    )
    .expect("grant Carol writer");
    node.acp_relationship_add(
        "Document",
        &id1,
        "reader",
        &dave.did,
        &alice.private_key_hex,
    )
    .expect("grant Dave reader");

    let count = |key: &str| -> usize {
        node.query_with_identity(query, key).expect("query")["Document"]
            .as_array()
            .map(|a| a.len())
            .unwrap_or(0)
    };

    // Alice sees 2 (owner of both), Bob=1 (admin on doc1), Carol=1, Dave=1
    assert_eq!(count(&alice.private_key_hex), 2, "Alice=2");
    assert_eq!(count(&bob.private_key_hex), 1, "Bob=1 (admin on doc1)");
    assert_eq!(count(&carol.private_key_hex), 1, "Carol=1 (writer on doc1)");
    assert_eq!(count(&dave.private_key_hex), 1, "Dave=1 (reader on doc1)");

    // Carol updates doc1 content (writer can update)
    node.query_with_identity(
        &format!(
            r#"mutation {{ update_Document(docID: "{}", input: {{content: "Updated by Carol"}}) {{ _docID }} }}"#,
            id1
        ),
        &carol.private_key_hex,
    )
    .expect("Carol update doc1");

    // Verify Alice sees the update
    let alice_result = node
        .query_with_identity(query, &alice.private_key_hex)
        .expect("Alice query");
    let doc1 = alice_result["Document"]
        .as_array()
        .expect("array")
        .iter()
        .find(|d| d["title"] == "Doc1")
        .expect("Doc1");
    assert_eq!(doc1["content"], "Updated by Carol");

    // Dave tries to update doc1 (reader can't update) — expect failure
    let dave_update = node.query_with_identity(
        &format!(
            r#"mutation {{ update_Document(docID: "{}", input: {{content: "Dave attempt"}}) {{ _docID }} }}"#,
            id1
        ),
        &dave.private_key_hex,
    );
    // Reader update should either fail or return empty result
    if let Ok(result) = dave_update {
        let updated = result["update_Document"]
            .as_array()
            .map(|a| a.len())
            .unwrap_or(0);
        assert_eq!(updated, 0, "Dave (reader) should not update doc1");
    }

    // Verify doc1 content unchanged by Dave's attempt
    let alice_result2 = node
        .query_with_identity(query, &alice.private_key_hex)
        .expect("Alice query after Dave attempt");
    let doc1_after = alice_result2["Document"]
        .as_array()
        .expect("array")
        .iter()
        .find(|d| d["title"] == "Doc1")
        .expect("Doc1");
    assert_eq!(
        doc1_after["content"], "Updated by Carol",
        "content should still be Carol's update"
    );

    // Carol tries to delete doc1 (writer can't delete) — expect failure
    let carol_delete = node.query_with_identity(
        &format!(
            r#"mutation {{ delete_Document(docID: "{}") {{ _docID }} }}"#,
            id1
        ),
        &carol.private_key_hex,
    );
    if let Ok(result) = carol_delete {
        let deleted = result["delete_Document"]
            .as_array()
            .map(|a| a.len())
            .unwrap_or(0);
        assert_eq!(deleted, 0, "Carol (writer) should not delete doc1");
    }

    // Verify doc1 still exists
    assert_eq!(
        count(&alice.private_key_hex),
        2,
        "Alice still=2 after Carol delete attempt"
    );
}

for_each_runtime!(acp_multi_role, acp_multi_role_test, .with_acp_local());
