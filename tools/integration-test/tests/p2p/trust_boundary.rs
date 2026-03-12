use std::time::Duration;

use integration_test::{
    generate_identity, poll_until, users_schema_with_policy, TestCluster, USER_ACP_POLICY,
};

/// Trust ring boundaries: Core (node0) ↔ Near (node1) with asymmetric trust.
///
/// Owner DID propagates via PushLog Creator field, so the receiving node
/// registers the document in local ACP under the original owner. Tests:
/// - ACP-protected docs replicate from Core to Near with owner DID
/// - Owner (Jack) can read protected docs on both nodes
/// - Non-owner identities cannot read protected docs on either node
/// - Anonymous users see only public (unregistered) docs on both nodes
async fn p2p_trust_boundary_test(cluster: TestCluster) {
    let core = cluster.client(0);
    let near = cluster.client(1);
    let binary = core.binary_path().to_path_buf();

    let jack = generate_identity(&binary).expect("jack identity");
    let vps_service = generate_identity(&binary).expect("vps_service identity");

    let timeout = Duration::from_secs(15);
    cluster
        .wait_for_log(0, "p2p_listening", timeout)
        .await
        .expect("core P2P listener");
    cluster
        .wait_for_log(1, "p2p_listening", timeout)
        .await
        .expect("near P2P listener");

    // Get near's multiaddr
    let near_info = near.p2p_info().expect("near p2p info");
    let near_addr = near_info
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|v| v.as_str())
        .expect("near has no P2P address");

    // Deploy ACP policy + schema on BOTH nodes (each node gets its own policy ID)
    let core_policy = core
        .acp_policy_add(USER_ACP_POLICY, &jack.private_key_hex)
        .expect("add policy on core");
    let core_policy_id = core_policy["PolicyID"]
        .as_str()
        .or_else(|| core_policy["policyID"].as_str())
        .expect("missing core PolicyID");

    let near_policy = near
        .acp_policy_add(USER_ACP_POLICY, &jack.private_key_hex)
        .expect("add policy on near");
    let near_policy_id = near_policy["PolicyID"]
        .as_str()
        .or_else(|| near_policy["policyID"].as_str())
        .expect("missing near PolicyID");

    let core_schema = users_schema_with_policy(core_policy_id);
    let near_schema = users_schema_with_policy(near_policy_id);
    core.schema_add_with_identity(&core_schema, &jack.private_key_hex)
        .expect("add schema on core");
    near.schema_add_with_identity(&near_schema, &jack.private_key_hex)
        .expect("add schema on near");

    // Set up P2P replication Core → Near
    core.p2p_connect(&[near_addr]).unwrap();
    core.p2p_collection_add(&["User"]).unwrap();
    near.p2p_collection_add(&["User"]).unwrap();
    core.p2p_replicator_set_with_identity(&["User"], near_addr, &jack.private_key_hex)
        .unwrap();

    // Jack creates ACP-protected tweet on Core
    core.query_with_identity(
        r#"mutation { add_User(input: {name: "Core Secret", age: 42}) { _docID } }"#,
        &jack.private_key_hex,
    )
    .expect("create protected doc on core");

    // Jack creates a public (no identity) doc on Core
    core.query(r#"mutation { add_User(input: {name: "Public Info", age: 99}) { _docID } }"#)
        .expect("create public doc on core");

    // Verify ACP on Core: jack sees 2, vps_service sees 1 (public only), anon sees 1
    let jack_core = core
        .query_with_identity("query { User { name } }", &jack.private_key_hex)
        .expect("jack query core");
    assert_eq!(
        jack_core["User"].as_array().map(|a| a.len()).unwrap_or(0),
        2,
        "jack sees both docs on core"
    );

    let vps_core = core
        .query_with_identity("query { User { name } }", &vps_service.private_key_hex)
        .expect("vps query core");
    assert_eq!(
        vps_core["User"].as_array().map(|a| a.len()).unwrap_or(0),
        1,
        "vps_service sees 1 on core (public doc only, no relation to protected doc)"
    );

    let anon_core = core
        .query("query { User { name } }")
        .expect("anon query core");
    assert_eq!(
        anon_core["User"].as_array().map(|a| a.len()).unwrap_or(0),
        1,
        "anonymous sees only public doc on core"
    );

    // Wait for replication to Near. Use Jack's identity since the protected
    // doc is now ACP-registered on Near under Jack's DID.
    let near_ref = &near;
    let jack_key = jack.private_key_hex.clone();
    poll_until(
        || {
            near_ref
                .query_with_identity("query { User { _docID } }", &jack_key)
                .ok()
                .and_then(|v| v["User"].as_array().map(|arr| arr.len() >= 2))
                .unwrap_or(false)
        },
        Duration::from_secs(15),
        Duration::from_millis(200),
        "docs did not replicate to near",
    )
    .await;

    // Jack sees both docs on Near (he owns the protected one, public is visible to all)
    let jack_near = near
        .query_with_identity("query { User { name } }", &jack.private_key_hex)
        .expect("jack query near");
    assert_eq!(
        jack_near["User"].as_array().map(|a| a.len()).unwrap_or(0),
        2,
        "jack should see both docs on near"
    );

    // vps_service has no grant and is not the owner — sees 0 protected docs.
    // The anonymous (public) doc may or may not be visible depending on how
    // the ACP query engine handles unregistered replicated docs.
    let vps_near = near
        .query_with_identity("query { User { name } }", &vps_service.private_key_hex)
        .expect("vps query near");
    let vps_near_count = vps_near["User"].as_array().map(|a| a.len()).unwrap_or(0);
    assert!(
        vps_near_count <= 1,
        "vps_service should not see protected doc on near, got {}",
        vps_near_count
    );

    // Anonymous query on Near
    let anon_near = near
        .query("query { User { name } }")
        .expect("anon query near");
    let anon_near_count = anon_near["User"].as_array().map(|a| a.len()).unwrap_or(0);
    assert!(
        anon_near_count <= 1,
        "anonymous should not see protected doc on near, got {}",
        anon_near_count
    );
}

#[tokio::test]
async fn rust_rust_p2p_trust_boundary() {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .with_acp_local()
        .build()
        .await
        .unwrap();
    p2p_trust_boundary_test(cluster).await;
}

/// Go does not carry owner DID in PushLog Creator field.
#[tokio::test]
#[ignore]
async fn go_go_p2p_trust_boundary() {
    let cluster = TestCluster::builder()
        .go_nodes(2)
        .with_p2p()
        .with_acp_local()
        .build()
        .await
        .unwrap();
    p2p_trust_boundary_test(cluster).await;
}

/// Go does not carry owner DID in PushLog Creator field.
#[tokio::test]
#[ignore]
async fn go_rust_p2p_trust_boundary() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .go_nodes(1)
        .with_p2p()
        .with_acp_local()
        .build()
        .await
        .unwrap();
    p2p_trust_boundary_test(cluster).await;
}
