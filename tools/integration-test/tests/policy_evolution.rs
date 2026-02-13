use integration_test::{generate_identity, users_schema_with_policy, TestCluster, USER_ACP_POLICY};

/// Tests what happens when ACP policies change after documents already exist.
///
/// Key questions tested:
/// - Do existing grants survive after adding a new policy version?
/// - What happens when a new schema version references a different policy?
/// - Are documents still accessible to the owner after policy changes?
async fn policy_evolution_test(cluster: TestCluster) {
    let node = cluster.client(0);
    let binary = node.binary_path().to_path_buf();

    let jack = generate_identity(&binary).expect("jack identity");
    let watchdog = generate_identity(&binary).expect("watchdog identity");
    let auditor = generate_identity(&binary).expect("auditor identity");

    // --- Phase 1: Initial policy + data ---
    let policy = node
        .acp_policy_add(USER_ACP_POLICY, &jack.private_key_hex)
        .expect("add initial policy");
    let policy_id = policy["PolicyID"]
        .as_str()
        .or_else(|| policy["policyID"].as_str())
        .expect("missing PolicyID");
    let schema = users_schema_with_policy(policy_id);
    node.schema_add_with_identity(&schema, &jack.private_key_hex)
        .expect("add schema v1");

    // Jack creates multiple documents
    let mut doc_ids = Vec::new();
    for i in 0..5 {
        let mutation = format!(
            r#"mutation {{ create_User(input: {{name: "Doc{}", age: {}}}) {{ _docID }} }}"#,
            i,
            i * 10
        );
        let result = node
            .query_with_identity(&mutation, &jack.private_key_hex)
            .expect("create doc");
        let doc_id = result["create_User"][0]["_docID"]
            .as_str()
            .expect("_docID")
            .to_string();
        doc_ids.push(doc_id);
    }

    // Grant watchdog reader on all 5 docs
    for doc_id in &doc_ids {
        node.acp_relationship_add(
            "User",
            doc_id,
            "reader",
            &watchdog.did,
            &jack.private_key_hex,
        )
        .expect("grant watchdog reader");
    }

    let query = "query { User { _docID name age } }";

    // Verify: jack sees 5, watchdog sees 5
    let jack_count = node
        .query_with_identity(query, &jack.private_key_hex)
        .expect("jack query")["User"]
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0);
    assert_eq!(jack_count, 5, "jack sees 5 docs initially");

    let watchdog_count = node
        .query_with_identity(query, &watchdog.private_key_hex)
        .expect("watchdog query")["User"]
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0);
    assert_eq!(watchdog_count, 5, "watchdog sees 5 docs initially");

    // Auditor has no grants yet
    let auditor_count = node
        .query_with_identity(query, &auditor.private_key_hex)
        .expect("auditor query")["User"]
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0);
    assert_eq!(auditor_count, 0, "auditor sees 0 (no grants)");

    // --- Phase 2: Grant auditor on existing docs ---
    // Grant auditor "reader" on first 3 docs (testing incremental grants)
    for doc_id in &doc_ids[0..3] {
        node.acp_relationship_add(
            "User",
            doc_id,
            "reader",
            &auditor.did,
            &jack.private_key_hex,
        )
        .expect("grant auditor reader");
    }

    let auditor_count2 = node
        .query_with_identity(query, &auditor.private_key_hex)
        .expect("auditor query after grants")["User"]
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0);
    assert_eq!(auditor_count2, 3, "auditor sees 3 after partial grant");

    // Verify existing relations unchanged
    let jack_count2 = node
        .query_with_identity(query, &jack.private_key_hex)
        .expect("jack query")["User"]
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0);
    assert_eq!(jack_count2, 5, "jack still sees 5 (owner unaffected)");

    let watchdog_count2 = node
        .query_with_identity(query, &watchdog.private_key_hex)
        .expect("watchdog query")["User"]
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0);
    assert_eq!(watchdog_count2, 5, "watchdog still sees 5 (grants intact)");

    // --- Phase 3: Revoke watchdog from some docs, verify partial access ---
    node.acp_relationship_delete(
        "User",
        &doc_ids[0],
        "reader",
        &watchdog.did,
        &jack.private_key_hex,
    )
    .expect("revoke watchdog from doc0");
    node.acp_relationship_delete(
        "User",
        &doc_ids[1],
        "reader",
        &watchdog.did,
        &jack.private_key_hex,
    )
    .expect("revoke watchdog from doc1");

    let watchdog_count3 = node
        .query_with_identity(query, &watchdog.private_key_hex)
        .expect("watchdog query after revoke")["User"]
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0);
    assert_eq!(watchdog_count3, 3, "watchdog sees 3 after 2 revocations");

    // Auditor still sees 3 (revocations were for watchdog, not auditor)
    let auditor_count3 = node
        .query_with_identity(query, &auditor.private_key_hex)
        .expect("auditor query")["User"]
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0);
    assert_eq!(
        auditor_count3, 3,
        "auditor still sees 3 (unaffected by watchdog revoke)"
    );

    // --- Phase 4: New docs inherit NO existing grants ---
    let new_doc = node
        .query_with_identity(
            r#"mutation { create_User(input: {name: "NewDoc", age: 100}) { _docID } }"#,
            &jack.private_key_hex,
        )
        .expect("create new doc");
    let new_doc_id = new_doc["create_User"][0]["_docID"]
        .as_str()
        .expect("_docID");

    let jack_count4 = node
        .query_with_identity(query, &jack.private_key_hex)
        .expect("jack query")["User"]
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0);
    assert_eq!(jack_count4, 6, "jack sees 6 (owner of new doc too)");

    // watchdog doesn't see new doc (no grant on it)
    let watchdog_count4 = node
        .query_with_identity(query, &watchdog.private_key_hex)
        .expect("watchdog query")["User"]
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0);
    assert_eq!(
        watchdog_count4, 3,
        "watchdog still sees 3 (no grant on new doc)"
    );

    // auditor doesn't see new doc either
    let auditor_count4 = node
        .query_with_identity(query, &auditor.private_key_hex)
        .expect("auditor query")["User"]
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0);
    assert_eq!(
        auditor_count4, 3,
        "auditor still sees 3 (no grant on new doc)"
    );

    // --- Phase 5: Grant on new doc only affects that doc ---
    node.acp_relationship_add(
        "User",
        new_doc_id,
        "reader",
        &auditor.did,
        &jack.private_key_hex,
    )
    .expect("grant auditor reader on new doc");

    let auditor_count5 = node
        .query_with_identity(query, &auditor.private_key_hex)
        .expect("auditor query final")["User"]
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0);
    assert_eq!(auditor_count5, 4, "auditor sees 4 (3 old + 1 new)");

    // Owner still sees everything
    let jack_final = node
        .query_with_identity(query, &jack.private_key_hex)
        .expect("jack final query")["User"]
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0);
    assert_eq!(jack_final, 6, "jack still sees all 6 docs");
}

#[tokio::test]
#[ignore]
async fn rust_policy_evolution() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_acp_local()
        .build()
        .await
        .unwrap();
    policy_evolution_test(cluster).await;
}

#[tokio::test]
#[ignore]
async fn go_policy_evolution() {
    let cluster = TestCluster::builder()
        .go_nodes(1)
        .with_acp_local()
        .build()
        .await
        .unwrap();
    policy_evolution_test(cluster).await;
}
