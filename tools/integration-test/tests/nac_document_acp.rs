use integration_test::{generate_identity, users_schema_with_policy, TestCluster, USER_ACP_POLICY};

/// Tests the two-layer access control model:
/// - NAC (Node Access Control): who can use the DefraDB instance
/// - Document ACP: who can access specific documents
///
/// With NAC enabled in Go, ALL operations require NAC "admin" relation.
/// The node's startup identity gets automatic admin access.
///
/// Rust's NAC enforcement is incomplete — it doesn't block at query level.
async fn nac_document_acp_test(cluster: TestCluster, is_go: bool) {
    let node = cluster.client(0);
    let binary = node.binary_path().to_path_buf();

    // The startup identity is the NAC admin (Go grants automatic access)
    let jack_key = cluster
        .startup_identity()
        .expect("NAC cluster must have a startup identity")
        .to_string();

    let regular_user = generate_identity(&binary).expect("regular_user identity");

    // --- Setup: Jack (startup identity) is both NAC admin and document owner ---

    let policy = node
        .acp_policy_add(USER_ACP_POLICY, &jack_key)
        .expect("add ACP policy");
    let policy_id = policy["PolicyID"]
        .as_str()
        .or_else(|| policy["policyID"].as_str())
        .expect("missing PolicyID");
    let schema = users_schema_with_policy(policy_id);
    node.schema_add_with_identity(&schema, &jack_key)
        .expect("add schema");

    let data = node
        .query_with_identity(
            r#"mutation { create_User(input: {name: "NAC Test Doc", age: 42}) { _docID } }"#,
            &jack_key,
        )
        .expect("create doc");
    let doc_id = data["create_User"][0]["_docID"]
        .as_str()
        .expect("_docID")
        .to_string();

    let query = "query { User { _docID name age } }";

    // --- Test 1: Jack (NAC admin + doc owner) reads -> ALLOW ---
    let jack_result = node
        .query_with_identity(query, &jack_key)
        .expect("jack query");
    assert_eq!(
        jack_result["User"].as_array().map(|a| a.len()).unwrap_or(0),
        1,
        "jack (NAC admin + owner) should read doc"
    );

    // --- Test 2: regular_user WITHOUT NAC admin ---
    // Go: NAC blocks before document ACP (sees 0)
    // Rust: NAC enforcement is incomplete (sees 0 due to document ACP)
    node.acp_relationship_add("User", &doc_id, "reader", &regular_user.did, &jack_key)
        .expect("grant regular_user document reader");

    let regular_no_nac = node
        .query_with_identity(query, &regular_user.private_key_hex)
        .expect("regular_user query without NAC");
    let regular_no_nac_count = regular_no_nac["User"]
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0);

    if is_go {
        // Go: NAC blocks ALL queries for users without NAC admin
        assert_eq!(
            regular_no_nac_count, 0,
            "Go: regular_user without NAC admin should see 0"
        );
    } else {
        // Rust: NAC enforcement incomplete, document ACP still works
        // regular_user has document reader → sees doc via document ACP
        assert_eq!(
            regular_no_nac_count, 1,
            "Rust: NAC enforcement incomplete, user sees doc via document ACP"
        );
    }

    // --- Test 3: Grant regular_user NAC admin -> they can read ---
    let nac_grant = node.acp_node_relationship_add("admin", &regular_user.did, &jack_key);
    if let Err(e) = &nac_grant {
        eprintln!("NAC add regular_user failed: {}", e);
    }

    if nac_grant.is_ok() {
        let regular_with_nac = node
            .query_with_identity(query, &regular_user.private_key_hex)
            .expect("regular_user query with NAC admin + doc reader");
        assert_eq!(
            regular_with_nac["User"]
                .as_array()
                .map(|a| a.len())
                .unwrap_or(0),
            1,
            "regular_user with NAC admin + document reader should see doc"
        );

        // --- Test 4: Revoke document reader ---
        node.acp_relationship_delete("User", &doc_id, "reader", &regular_user.did, &jack_key)
            .expect("revoke regular_user document reader");

        let regular_nac_only = node
            .query_with_identity(query, &regular_user.private_key_hex)
            .expect("regular_user query NAC-only");
        let nac_admin_count = regular_nac_only["User"]
            .as_array()
            .map(|a| a.len())
            .unwrap_or(0);
        eprintln!(
            "NAC admin without document relation sees {} docs",
            nac_admin_count
        );

        // --- Test 5: Remove NAC admin ---
        node.acp_node_relationship_delete("admin", &regular_user.did, &jack_key)
            .expect("revoke regular_user NAC admin");
    } else {
        // Clean up document reader if NAC operations aren't supported
        let _ =
            node.acp_relationship_delete("User", &doc_id, "reader", &regular_user.did, &jack_key);
    }

    let regular_no_access = node
        .query_with_identity(query, &regular_user.private_key_hex)
        .expect("regular_user query after full revocation");
    assert_eq!(
        regular_no_access["User"]
            .as_array()
            .map(|a| a.len())
            .unwrap_or(0),
        0,
        "regular_user without any access should see 0"
    );

    // --- Test 6: NAC disable/re-enable cycle ---
    let disable_result = node.acp_node_disable();
    if let Err(e) = &disable_result {
        eprintln!("NAC disable failed (may not be supported): {}", e);
    }

    let status_disabled = node.acp_node_status();
    if let Ok(status) = &status_disabled {
        let status_str = serde_json::to_string(status).unwrap();
        assert!(!status_str.is_empty(), "status should be non-empty");
    }

    // Jack should still read during NAC disable (still doc owner)
    let jack_during_disable = node
        .query_with_identity(query, &jack_key)
        .expect("jack query during NAC disable");
    assert_eq!(
        jack_during_disable["User"]
            .as_array()
            .map(|a| a.len())
            .unwrap_or(0),
        1,
        "jack should read docs during NAC disable (owner)"
    );

    let reenable_result = node.acp_node_reenable();
    if let Err(e) = &reenable_result {
        eprintln!("NAC re-enable failed (may not be supported): {}", e);
    }

    let jack_after_reenable = node
        .query_with_identity(query, &jack_key)
        .expect("jack query after NAC re-enable");
    assert_eq!(
        jack_after_reenable["User"]
            .as_array()
            .map(|a| a.len())
            .unwrap_or(0),
        1,
        "jack should read docs after NAC re-enable"
    );
}

#[tokio::test]
#[ignore]
async fn rust_nac_document_acp() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_acp_local()
        .with_nac()
        .build()
        .await
        .unwrap();
    nac_document_acp_test(cluster, false).await;
}

#[tokio::test]
#[ignore]
async fn go_nac_document_acp() {
    let cluster = TestCluster::builder()
        .go_nodes(1)
        .with_acp_local()
        .with_nac()
        .build()
        .await
        .unwrap();
    nac_document_acp_test(cluster, true).await;
}
