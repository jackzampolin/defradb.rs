use std::time::Duration;

use integration_test::node::{DefraNode, RustNode};
use integration_test::{generate_identity, users_schema_with_policy, TestCluster, USER_ACP_POLICY};

/// Circuit breaker with threshold=1 trips on the very first failure after
/// SourceHub goes down, rather than requiring the default 3 failures.
#[tokio::test]
#[serial_test::serial]
async fn rust_circuit_breaker_threshold_1_trips_immediately() {
    let binary = RustNode::from_workspace().binary_path().to_path_buf();
    RustNode::build().expect("build rust binary");
    let jack = generate_identity(&binary).expect("Jack identity");
    let bob = generate_identity(&binary).expect("Bob identity");

    let mut cluster = TestCluster::builder()
        .rust_nodes(1)
        .skip_build()
        .with_source_hub()
        .with_identity(&jack.private_key_hex)
        .with_acp_circuit_breaker_threshold(1)
        .with_acp_circuit_breaker_reset_timeout(60)
        .build()
        .await
        .expect("build cluster");

    let node = cluster.client(0);

    // Setup: create policy + protected doc while SourceHub is healthy
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

    let data = node
        .query_with_identity(
            r#"mutation { add_User(input: {name: "Jack", age: 30}) { _docID } }"#,
            &jack.private_key_hex,
        )
        .expect("create doc");
    let doc_id = data["add_User"][0]["_docID"].as_str().expect("_docID");

    // Verify reads work during normal operation
    let jack_read = node
        .query_with_identity("query { User { _docID name } }", &jack.private_key_hex)
        .expect("Jack read");
    assert_eq!(
        jack_read["User"].as_array().unwrap().len(),
        1,
        "Jack should see 1 doc during normal operation"
    );

    node.acp_relationship_add("User", doc_id, "reader", &bob.did, &jack.private_key_hex)
        .expect("grant Bob reader");
    let bob_read = node
        .query_with_identity("query { User { _docID name } }", &bob.private_key_hex)
        .expect("Bob read");
    assert_eq!(
        bob_read["User"].as_array().unwrap().len(),
        1,
        "Bob should see 1 doc during normal operation"
    );

    // Kill SourceHub
    cluster
        .stop_source_hub()
        .expect("failed to stop source hub");

    tokio::time::sleep(Duration::from_secs(2)).await;

    // With threshold=1, the very first failed ACP check should trip the breaker.
    // All subsequent requests are denied immediately without even attempting SourceHub.
    let bob_after_stop = node
        .query_with_identity("query { User { _docID name } }", &bob.private_key_hex)
        .expect("Bob read after stop");
    assert_eq!(
        bob_after_stop["User"].as_array().unwrap().len(),
        0,
        "threshold=1: Bob denied after SourceHub down (fail-closed)"
    );
}

/// Short cache TTL (2s) causes cached policy entries to expire quickly.
/// After expiry, the node must re-query SourceHub for the policy.
///
/// This test was previously impractical with the hardcoded 300s TTL.
#[tokio::test]
#[serial_test::serial]
async fn rust_cache_ttl_expiry_with_short_ttl() {
    let binary = RustNode::from_workspace().binary_path().to_path_buf();
    RustNode::build().expect("build rust binary");
    let alice = generate_identity(&binary).expect("Alice identity");
    let bob = generate_identity(&binary).expect("Bob identity");

    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .skip_build()
        .with_source_hub()
        .with_identity(&alice.private_key_hex)
        .with_acp_cache_ttl(2)
        .build()
        .await
        .expect("build cluster");

    let node = cluster.client(0);

    // Create policy + doc
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

    let data = node
        .query_with_identity(
            r#"mutation { add_User(input: {name: "Alice", age: 25}) { _docID } }"#,
            &alice.private_key_hex,
        )
        .expect("create doc");
    let doc_id = data["add_User"][0]["_docID"].as_str().expect("_docID");

    // Immediate reads use cached policy
    let alice_read = node
        .query_with_identity("query { User { _docID name } }", &alice.private_key_hex)
        .expect("Alice read cached");
    assert_eq!(alice_read["User"].as_array().unwrap().len(), 1);

    // Grant Bob reader access
    node.acp_relationship_add("User", doc_id, "reader", &bob.did, &alice.private_key_hex)
        .expect("grant Bob reader");

    let bob_read = node
        .query_with_identity("query { User { _docID name } }", &bob.private_key_hex)
        .expect("Bob read");
    assert_eq!(
        bob_read["User"].as_array().unwrap().len(),
        1,
        "Bob should see 1 doc after grant"
    );

    // Wait for TTL to expire
    tokio::time::sleep(Duration::from_secs(3)).await;

    // After TTL expiry, the node re-queries SourceHub for policy.
    // This should still work because SourceHub is healthy.
    let alice_after_expiry = node
        .query_with_identity("query { User { _docID name } }", &alice.private_key_hex)
        .expect("Alice read after cache expiry");
    assert_eq!(
        alice_after_expiry["User"].as_array().unwrap().len(),
        1,
        "Alice should still see 1 doc after cache expiry (re-fetched from SourceHub)"
    );

    // Revoke Bob and verify — this exercises the full cycle with short TTL
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

/// Short request timeout (1s) causes fast fail-closed behavior when
/// SourceHub is slow or unreachable.
#[tokio::test]
#[serial_test::serial]
async fn rust_short_request_timeout_fail_closed() {
    let binary = RustNode::from_workspace().binary_path().to_path_buf();
    RustNode::build().expect("build rust binary");
    let jack = generate_identity(&binary).expect("Jack identity");
    let bob = generate_identity(&binary).expect("Bob identity");

    let mut cluster = TestCluster::builder()
        .rust_nodes(1)
        .skip_build()
        .with_source_hub()
        .with_identity(&jack.private_key_hex)
        .with_acp_request_timeout(1)
        .with_acp_circuit_breaker_threshold(1)
        .build()
        .await
        .expect("build cluster");

    let node = cluster.client(0);

    // Setup while SourceHub is healthy
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

    let data = node
        .query_with_identity(
            r#"mutation { add_User(input: {name: "Jack", age: 30}) { _docID } }"#,
            &jack.private_key_hex,
        )
        .expect("create doc");
    let doc_id = data["add_User"][0]["_docID"].as_str().expect("_docID");

    node.acp_relationship_add("User", doc_id, "reader", &bob.did, &jack.private_key_hex)
        .expect("grant Bob reader");

    // Kill SourceHub
    cluster
        .stop_source_hub()
        .expect("failed to stop source hub");

    tokio::time::sleep(Duration::from_secs(2)).await;

    // With 1s timeout + threshold=1, the node should fail-closed very quickly
    let start = std::time::Instant::now();
    let bob_read = node
        .query_with_identity("query { User { _docID name } }", &bob.private_key_hex)
        .expect("Bob read after stop");
    let elapsed = start.elapsed();

    assert_eq!(
        bob_read["User"].as_array().unwrap().len(),
        0,
        "Bob denied with short timeout (fail-closed)"
    );

    // The request should complete quickly (within timeout + overhead)
    assert!(
        elapsed < Duration::from_secs(10),
        "request should fail-closed quickly with 1s timeout, took {:?}",
        elapsed
    );
}

/// Access decision cache: repeated queries by the same identity for the
/// same documents are served from cache. Grant/revoke eagerly invalidates
/// the cache so permission changes take effect immediately.
#[tokio::test]
#[serial_test::serial]
async fn rust_access_cache_grant_revoke_invalidation() {
    let binary = RustNode::from_workspace().binary_path().to_path_buf();
    RustNode::build().expect("build rust binary");
    let alice = generate_identity(&binary).expect("Alice identity");
    let bob = generate_identity(&binary).expect("Bob identity");

    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .skip_build()
        .with_source_hub()
        .with_identity(&alice.private_key_hex)
        .with_acp_cache_ttl(300)
        .build()
        .await
        .expect("build cluster");

    let node = cluster.client(0);

    // Create policy + doc
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

    let data = node
        .query_with_identity(
            r#"mutation { add_User(input: {name: "Alice", age: 25}) { _docID } }"#,
            &alice.private_key_hex,
        )
        .expect("create doc");
    let doc_id = data["add_User"][0]["_docID"].as_str().expect("_docID");

    // Bob cannot read (no grant yet)
    let bob_denied = node
        .query_with_identity("query { User { _docID name } }", &bob.private_key_hex)
        .expect("Bob read denied");
    assert_eq!(
        bob_denied["User"].as_array().unwrap().len(),
        0,
        "Bob should see 0 docs before grant"
    );

    // Rapid repeat queries — these should hit the access cache (denial cached)
    for i in 0..5 {
        let result = node
            .query_with_identity("query { User { _docID name } }", &bob.private_key_hex)
            .unwrap_or_else(|_| panic!("Bob cached denial iteration {}", i));
        assert_eq!(
            result["User"].as_array().unwrap().len(),
            0,
            "Bob should still be denied on cached iteration {}",
            i
        );
    }

    // Grant Bob reader — this must invalidate the cached denial
    node.acp_relationship_add("User", doc_id, "reader", &bob.did, &alice.private_key_hex)
        .expect("grant Bob reader");

    // Bob can now read — cache was invalidated by the grant
    let bob_allowed = node
        .query_with_identity("query { User { _docID name } }", &bob.private_key_hex)
        .expect("Bob read after grant");
    assert_eq!(
        bob_allowed["User"].as_array().unwrap().len(),
        1,
        "Bob should see 1 doc after grant (cache invalidated)"
    );

    // Rapid repeat queries — grant result is now cached
    for i in 0..5 {
        let result = node
            .query_with_identity("query { User { _docID name } }", &bob.private_key_hex)
            .unwrap_or_else(|_| panic!("Bob cached grant iteration {}", i));
        assert_eq!(
            result["User"].as_array().unwrap().len(),
            1,
            "Bob should see 1 doc on cached grant iteration {}",
            i
        );
    }

    // Revoke Bob — must invalidate the cached grant
    node.acp_relationship_delete("User", doc_id, "reader", &bob.did, &alice.private_key_hex)
        .expect("revoke Bob");

    // Bob denied again — cache was invalidated by the revoke
    let bob_revoked = node
        .query_with_identity("query { User { _docID name } }", &bob.private_key_hex)
        .expect("Bob read after revoke");
    assert_eq!(
        bob_revoked["User"].as_array().unwrap().len(),
        0,
        "Bob should see 0 docs after revoke (cache invalidated)"
    );

    // Alice always has access throughout (owner)
    let alice_read = node
        .query_with_identity("query { User { _docID name } }", &alice.private_key_hex)
        .expect("Alice final read");
    assert_eq!(
        alice_read["User"].as_array().unwrap().len(),
        1,
        "Alice (owner) should always see 1 doc"
    );
}
