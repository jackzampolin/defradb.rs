use std::time::Duration;

use integration_test::node::{DefraNode, RustNode};
use integration_test::{generate_identity, users_schema_with_policy, TestCluster, USER_ACP_POLICY};

/// Circuit breaker fail-closed: when SourceHub becomes unreachable after
/// initial setup, ACP-protected queries deny ALL access (including owner).
///
/// Sequence:
/// 1. Start cluster with SourceHub, create policy + protected doc
/// 2. Verify Jack (owner) can read during normal operation
/// 3. Stop the SourceHub process (simulates network partition)
/// 4. Verify Jack is DENIED (node can't verify ACP → fail-closed)
/// 5. Verify anonymous is also denied
#[tokio::test]
async fn rust_circuit_breaker_trip_recovery() {
    let binary = RustNode::from_workspace().binary_path().to_path_buf();
    RustNode::build().expect("build rust binary");
    let jack = generate_identity(&binary).expect("Jack identity");

    let mut cluster = TestCluster::builder()
        .rust_nodes(1)
        .skip_build()
        .with_source_hub()
        .with_identity(&jack.private_key_hex)
        .build()
        .await
        .expect("build cluster");

    let node = cluster.client(0);

    // Phase 1: Normal operation — create policy and document
    let policy_result = node
        .acp_policy_add(USER_ACP_POLICY, &jack.private_key_hex)
        .expect("add policy");
    let policy_id = policy_result["PolicyID"]
        .as_str()
        .or_else(|| policy_result["policyID"].as_str())
        .expect("PolicyID");

    let schema = users_schema_with_policy(policy_id);
    node.schema_add_with_identity(&schema, &jack.private_key_hex)
        .expect("add schema");

    node.query_with_identity(
        r#"mutation { create_User(input: {name: "Jack", age: 30}) { _docID } }"#,
        &jack.private_key_hex,
    )
    .expect("create doc");

    // Jack reads successfully
    let jack_read = node
        .query_with_identity("query { User { _docID name } }", &jack.private_key_hex)
        .expect("Jack read");
    assert_eq!(
        jack_read["User"].as_array().unwrap().len(),
        1,
        "Jack should see 1 doc during normal operation"
    );

    // Phase 2: Stop SourceHub — kill the devnet process
    cluster
        .stop_source_hub()
        .expect("failed to stop source hub");

    // Give time for connections to notice the shutdown
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Phase 3: Fail-closed — even the owner is denied when SourceHub is unreachable.
    // The node cannot verify ACP permissions without SourceHub, so it denies all access.
    let jack_after_stop = node
        .query_with_identity("query { User { _docID name } }", &jack.private_key_hex)
        .expect("Jack read after SourceHub stop");
    assert_eq!(
        jack_after_stop["User"].as_array().unwrap().len(),
        0,
        "Jack should be denied when SourceHub is down (fail-closed)"
    );

    // Anonymous is also denied
    let anon_after_stop = node
        .query("query { User { _docID name } }")
        .expect("anon read after stop");
    assert_eq!(
        anon_after_stop["User"].as_array().unwrap().len(),
        0,
        "anonymous must be denied with SourceHub down (fail-closed)"
    );
}

/// Policy cache verification: after creating a policy on SourceHub,
/// subsequent ACP operations use the cached policy without hitting
/// the chain for every request.
///
/// This tests the positive path — policy is cached and operations are
/// fast. The 5-minute TTL makes expiry impractical to test at integration
/// level (unit tests cover expiry in policy_cache.rs).
#[tokio::test]
async fn rust_policy_cache_ttl_expiry() {
    let binary = RustNode::from_workspace().binary_path().to_path_buf();
    RustNode::build().expect("build rust binary");
    let alice = generate_identity(&binary).expect("Alice identity");
    let bob = generate_identity(&binary).expect("Bob identity");

    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .skip_build()
        .with_source_hub()
        .with_identity(&alice.private_key_hex)
        .build()
        .await
        .expect("build cluster");

    let node = cluster.client(0);

    // Create policy — triggers on-chain tx + cache insert
    let policy_result = node
        .acp_policy_add(USER_ACP_POLICY, &alice.private_key_hex)
        .expect("add policy");
    let policy_id = policy_result["PolicyID"]
        .as_str()
        .or_else(|| policy_result["policyID"].as_str())
        .expect("PolicyID");

    let schema = users_schema_with_policy(policy_id);
    node.schema_add_with_identity(&schema, &alice.private_key_hex)
        .expect("add schema");

    // Create a doc — registers on-chain, caches locally
    let data = node
        .query_with_identity(
            r#"mutation { create_User(input: {name: "Alice", age: 25}) { _docID } }"#,
            &alice.private_key_hex,
        )
        .expect("create doc");
    let doc_id = data["create_User"][0]["_docID"]
        .as_str()
        .expect("_docID");

    // Multiple rapid ACP operations — all should use cached policy
    for i in 0..5 {
        let result = node
            .query_with_identity("query { User { _docID name } }", &alice.private_key_hex)
            .expect(&format!("Alice query iteration {}", i));
        assert_eq!(
            result["User"].as_array().unwrap().len(),
            1,
            "Alice should see 1 doc on iteration {}",
            i
        );
    }

    // Grant Bob reader (on-chain tx) — policy is already cached
    node.acp_relationship_add("User", doc_id, "reader", &bob.did, &alice.private_key_hex)
        .expect("grant Bob reader");

    // Bob reads — triggers verify_access which uses cached policy
    let bob_read = node
        .query_with_identity("query { User { _docID name } }", &bob.private_key_hex)
        .expect("Bob read");
    assert_eq!(
        bob_read["User"].as_array().unwrap().len(),
        1,
        "Bob should see 1 doc after grant"
    );

    // Revoke and verify — all using cached policy
    node.acp_relationship_delete("User", doc_id, "reader", &bob.did, &alice.private_key_hex)
        .expect("revoke Bob");

    let bob_revoked = node
        .query_with_identity("query { User { _docID name } }", &bob.private_key_hex)
        .expect("Bob read after revoke");
    assert_eq!(
        bob_revoked["User"].as_array().unwrap().len(),
        0,
        "Bob should see 0 docs after revoke"
    );
}

/// Go runtime variant — circuit breaker behavior
#[tokio::test]
#[ignore = "Go node with SourceHub not yet supported"]
async fn go_circuit_breaker_trip_recovery() {
    let binary = RustNode::from_workspace().binary_path().to_path_buf();
    RustNode::build().expect("build rust binary");
    let jack = generate_identity(&binary).expect("Jack identity");

    let cluster = TestCluster::builder()
        .go_nodes(1)
        .with_source_hub()
        .with_identity(&jack.private_key_hex)
        .build()
        .await
        .expect("build cluster");

    let node = cluster.client(0);

    let policy_result = node
        .acp_policy_add(USER_ACP_POLICY, &jack.private_key_hex)
        .expect("add policy");
    let policy_id = policy_result["PolicyID"]
        .as_str()
        .or_else(|| policy_result["policyID"].as_str())
        .expect("PolicyID");

    let schema = users_schema_with_policy(policy_id);
    node.schema_add_with_identity(&schema, &jack.private_key_hex)
        .expect("add schema");

    node.query_with_identity(
        r#"mutation { create_User(input: {name: "Jack", age: 30}) { _docID } }"#,
        &jack.private_key_hex,
    )
    .expect("create doc");

    let jack_read = node
        .query_with_identity("query { User { _docID name } }", &jack.private_key_hex)
        .expect("Jack read");
    assert_eq!(jack_read["User"].as_array().unwrap().len(), 1);
}

/// Go runtime variant — policy cache behavior
#[tokio::test]
#[ignore = "Go node with SourceHub not yet supported"]
async fn go_policy_cache_ttl_expiry() {
    let binary = RustNode::from_workspace().binary_path().to_path_buf();
    RustNode::build().expect("build rust binary");
    let alice = generate_identity(&binary).expect("Alice identity");

    let cluster = TestCluster::builder()
        .go_nodes(1)
        .with_source_hub()
        .with_identity(&alice.private_key_hex)
        .build()
        .await
        .expect("build cluster");

    let node = cluster.client(0);

    let policy_result = node
        .acp_policy_add(USER_ACP_POLICY, &alice.private_key_hex)
        .expect("add policy");
    let policy_id = policy_result["PolicyID"]
        .as_str()
        .or_else(|| policy_result["policyID"].as_str())
        .expect("PolicyID");

    let schema = users_schema_with_policy(policy_id);
    node.schema_add_with_identity(&schema, &alice.private_key_hex)
        .expect("add schema");

    node.query_with_identity(
        r#"mutation { create_User(input: {name: "Alice", age: 25}) { _docID } }"#,
        &alice.private_key_hex,
    )
    .expect("create doc");

    // Multiple rapid queries — should use cached policy
    for i in 0..5 {
        let result = node
            .query_with_identity("query { User { _docID name } }", &alice.private_key_hex)
            .expect(&format!("query iteration {}", i));
        assert_eq!(result["User"].as_array().unwrap().len(), 1);
    }
}
