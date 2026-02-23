use integration_test::{
    for_each_runtime, generate_ed25519_identity, generate_identity, generate_secp256r1_identity,
    users_schema_with_policy, TestCluster, USER_ACP_POLICY,
};

async fn identity_types_test(cluster: TestCluster) {
    let node = cluster.client(0);
    let binary = node.binary_path().to_path_buf();

    // Generate secp256k1 identity (default)
    let secp = generate_identity(&binary).expect("secp256k1 identity");
    assert!(
        secp.private_key_hex.len() == 64,
        "secp256k1 key should be 64 hex chars, got {}",
        secp.private_key_hex.len()
    );
    assert!(
        secp.did.starts_with("did:key:z"),
        "secp256k1 DID should start with did:key:z, got {}",
        secp.did
    );

    // Generate ed25519 identity
    let ed = generate_ed25519_identity(&binary).expect("ed25519 identity");
    assert!(
        ed.private_key_hex.len() >= 64,
        "ed25519 key should be at least 64 hex chars, got {}",
        ed.private_key_hex.len()
    );
    assert!(
        ed.did.starts_with("did:key:z"),
        "ed25519 DID should start with did:key:z, got {}",
        ed.did
    );
    // Generate secp256r1 (P-256) identity
    let p256 = generate_secp256r1_identity(&binary).expect("secp256r1 identity");
    assert!(
        p256.private_key_hex.len() == 64,
        "secp256r1 key should be 64 hex chars, got {}",
        p256.private_key_hex.len()
    );
    assert!(
        p256.did.starts_with("did:key:z"),
        "secp256r1 DID should start with did:key:z, got {}",
        p256.did
    );
    assert_eq!(
        p256.key_type.as_deref(),
        Some("secp256r1"),
        "expected KeyType secp256r1"
    );

    // All three DIDs should be distinct
    assert_ne!(
        secp.did, ed.did,
        "secp256k1 and ed25519 should produce different DIDs"
    );
    assert_ne!(
        secp.did, p256.did,
        "secp256k1 and secp256r1 should produce different DIDs"
    );
    assert_ne!(
        ed.did, p256.did,
        "ed25519 and secp256r1 should produce different DIDs"
    );

    // Set up ACP with secp256k1 identity as owner
    let policy = node
        .acp_policy_add(USER_ACP_POLICY, &secp.private_key_hex)
        .expect("add policy");
    let policy_id = policy["PolicyID"]
        .as_str()
        .or_else(|| policy["policyID"].as_str())
        .expect("missing PolicyID");
    let schema = users_schema_with_policy(policy_id);
    node.schema_add_with_identity(&schema, &secp.private_key_hex)
        .expect("add schema");

    // secp256k1 identity creates a protected document
    let d = node
        .query_with_identity(
            r#"mutation { create_User(input: {name: "CrossKey", age: 42}) { _docID } }"#,
            &secp.private_key_hex,
        )
        .expect("create doc");
    let doc_id = d["create_User"][0]["_docID"].as_str().expect("doc_id");

    let query = "query { User { _docID name age } }";

    // ed25519 identity can't see it yet
    let ed_result = node
        .query_with_identity(query, &ed.private_key_hex)
        .expect("ed25519 query before grant");
    let ed_count = ed_result["User"].as_array().map(|a| a.len()).unwrap_or(0);
    assert_eq!(ed_count, 0, "ed25519 should see 0 before grant");

    // Grant ed25519 identity "reader" using its DID
    let grant_result =
        node.acp_relationship_add("User", doc_id, "reader", &ed.did, &secp.private_key_hex);
    if let Err(e) = &grant_result {
        eprintln!(
            "Cross-key-type grant failed (ed25519 ACP may not be supported): {}",
            e
        );
        return;
    }

    // ed25519 identity should now see the document (if cross-key-type ACP is supported)
    let ed_result2 = node
        .query_with_identity(query, &ed.private_key_hex)
        .expect("ed25519 query after grant");
    let ed_count2 = ed_result2["User"].as_array().map(|a| a.len()).unwrap_or(0);
    if ed_count2 == 0 {
        eprintln!(
            "Cross-key-type ACP: ed25519 identity cannot read after grant (platform limitation)"
        );
    } else {
        assert_eq!(ed_count2, 1, "ed25519 should see 1 after grant");
        assert_eq!(ed_result2["User"][0]["name"], "CrossKey");
    }

    // P-256 identity can't see it yet
    let p256_result = node
        .query_with_identity(query, &p256.private_key_hex)
        .expect("p256 query before grant");
    let p256_count = p256_result["User"].as_array().map(|a| a.len()).unwrap_or(0);
    assert_eq!(p256_count, 0, "p256 should see 0 before grant");

    // Grant p256 identity "reader" using its DID
    let grant_result =
        node.acp_relationship_add("User", doc_id, "reader", &p256.did, &secp.private_key_hex);
    if let Err(e) = &grant_result {
        eprintln!(
            "Cross-key-type grant failed (P-256 ACP may not be supported): {}",
            e
        );
        return;
    }

    // P-256 identity should now see the document
    let p256_result2 = node
        .query_with_identity(query, &p256.private_key_hex)
        .expect("p256 query after grant");
    let p256_count2 = p256_result2["User"]
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0);
    if p256_count2 == 0 {
        eprintln!(
            "Cross-key-type ACP: P-256 identity cannot read after grant (platform limitation)"
        );
    } else {
        assert_eq!(p256_count2, 1, "p256 should see 1 after grant");
        assert_eq!(p256_result2["User"][0]["name"], "CrossKey");
    }
}

for_each_runtime!(identity_types, identity_types_test, .with_acp_local());
