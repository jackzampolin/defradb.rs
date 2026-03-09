//! Trust boundary tests for iroh P2P transport.
//!
//! Tests asymmetric trust ring: Core (node0) ↔ Near (node1)
//! with ACP policy enforcement across iroh replication.
//!
//! Validates:
//! - ACP-protected docs replicate from Core to Near
//! - Owner sees both public and protected docs on Core
//! - Anonymous sees only public doc on Core
//! - ACP state is node-local (not replicated)
//! - Non-owner identity visibility rules
//!
//! Run with:
//!   cargo test -p integration-test --test p2p_iroh_trust_boundary -- --ignored

use std::time::Duration;

use integration_test::{
    generate_identity, poll_until, users_schema_with_policy, TestCluster, USER_ACP_POLICY,
};
use serial_test::serial;

/// Core ↔ Near trust boundary with asymmetric ACP over iroh.
#[tokio::test]
#[serial]
async fn iroh_trust_boundary() {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_iroh_transport()
        .with_acp_local()
        .build()
        .await
        .unwrap();

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

    let near_info = near.p2p_info().expect("near p2p info");
    let near_addr = near_info
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|v| v.as_str())
        .expect("near has no P2P address");

    // Deploy ACP policy + schema on BOTH nodes
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
    core.p2p_replicator_set(&["User"], near_addr).unwrap();

    // Jack creates ACP-protected doc on Core
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
    let jack_core_names: Vec<&str> = jack_core["User"]
        .as_array()
        .expect("jack core result not array")
        .iter()
        .filter_map(|u| u["name"].as_str())
        .collect();
    assert_eq!(
        jack_core_names.len(),
        2,
        "jack sees both docs on core, got {:?}",
        jack_core_names
    );
    assert!(
        jack_core_names.contains(&"Core Secret"),
        "jack should see Core Secret on core"
    );
    assert!(
        jack_core_names.contains(&"Public Info"),
        "jack should see Public Info on core"
    );

    let vps_core = core
        .query_with_identity("query { User { name } }", &vps_service.private_key_hex)
        .expect("vps query core");
    let vps_core_names: Vec<&str> = vps_core["User"]
        .as_array()
        .expect("vps core result not array")
        .iter()
        .filter_map(|u| u["name"].as_str())
        .collect();
    assert_eq!(
        vps_core_names.len(),
        1,
        "vps_service sees only public doc on core, got {:?}",
        vps_core_names
    );
    assert_eq!(
        vps_core_names[0], "Public Info",
        "vps_service should only see Public Info on core"
    );

    let anon_core = core
        .query("query { User { name } }")
        .expect("anon query core");
    let anon_core_names: Vec<&str> = anon_core["User"]
        .as_array()
        .expect("anon core result not array")
        .iter()
        .filter_map(|u| u["name"].as_str())
        .collect();
    assert_eq!(
        anon_core_names.len(),
        1,
        "anonymous sees only public doc on core, got {:?}",
        anon_core_names
    );
    assert_eq!(
        anon_core_names[0], "Public Info",
        "anonymous should only see Public Info on core"
    );

    // Wait for replication to Near. Poll with Jack's identity since ACP is
    // registered during merge and anonymous can't see protected docs.
    let near_ref = &near;
    let jack_key_clone = jack.private_key_hex.clone();
    poll_until(
        || {
            let result = near_ref
                .query_with_identity("query { User { _docID } }", &jack_key_clone)
                .unwrap_or_default();
            result["User"]
                .as_array()
                .map(|arr| arr.len() == 2)
                .unwrap_or(false)
        },
        Duration::from_secs(15),
        Duration::from_millis(200),
        "docs did not replicate to near",
    )
    .await;

    // On Near: ACP IS registered during merge.
    // Jack (owner) sees both docs: his protected doc + public doc = 2
    let jack_near = near
        .query_with_identity("query { User { name } }", &jack.private_key_hex)
        .expect("jack query near");
    let jack_near_names: Vec<&str> = jack_near["User"]
        .as_array()
        .expect("jack near result not array")
        .iter()
        .filter_map(|u| u["name"].as_str())
        .collect();
    assert_eq!(
        jack_near_names.len(),
        2,
        "jack should see both docs on near, got {:?}",
        jack_near_names
    );
    assert!(
        jack_near_names.contains(&"Core Secret"),
        "jack should see Core Secret on near"
    );
    assert!(
        jack_near_names.contains(&"Public Info"),
        "jack should see Public Info on near"
    );

    // vps_service on Near: not the owner, sees only public doc
    let vps_near = near
        .query_with_identity("query { User { name } }", &vps_service.private_key_hex)
        .expect("vps query near");
    let vps_near_count = vps_near["User"].as_array().map(|a| a.len()).unwrap_or(0);
    assert_eq!(
        vps_near_count, 1,
        "vps on near should see only public doc (ACP registered during merge), got {}",
        vps_near_count
    );

    // Anonymous on Near: sees only public doc
    let anon_near = near
        .query("query { User { name } }")
        .expect("anon query near");
    let anon_near_names: Vec<&str> = anon_near["User"]
        .as_array()
        .expect("anon near result not array")
        .iter()
        .filter_map(|u| u["name"].as_str())
        .collect();
    assert_eq!(
        anon_near_names.len(),
        1,
        "anonymous should see only public doc on near, got {:?}",
        anon_near_names
    );
    assert_eq!(
        anon_near_names[0], "Public Info",
        "anonymous should only see Public Info on near"
    );
}
