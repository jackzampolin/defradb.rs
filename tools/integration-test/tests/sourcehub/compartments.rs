use std::time::Duration;

use integration_test::node::{DefraNode, RustNode};
use integration_test::{
    generate_identity, interaction_schema_with_policy, peak_schema_with_policy,
    secret_schema_with_policy, tweet_schema_with_policy, workout_schema_with_policy, TestCluster,
    HIKING_ACP_POLICY, SECRET_ACP_POLICY, XARCHIVE_ACP_POLICY,
};

/// Helper to add a policy and return the policy ID.
fn add_policy(node: &integration_test::DefraClient, policy: &str, identity: &str) -> String {
    let result = node
        .acp_policy_add(policy, identity)
        .expect("failed to add policy");
    result["PolicyID"]
        .as_str()
        .or_else(|| result["policyID"].as_str())
        .expect("missing PolicyID")
        .to_string()
}

/// Multi-identity compartment test with 5 identities and 2 compartments.
///
/// Compartment 1: x-archive (Tweet, Interaction)
/// Compartment 2: hiking (Workout, Peak)
/// Plus: Secret collection (owner-only)
///
/// Identities:
///   Jack (owner), Agent-XArchive (writer), Agent-Hiking (writer),
///   Vanessa (reader), Outsider (none)
///
/// The node is started with Jack's identity so SourceHub transactions work.
#[tokio::test]
async fn rust_sourcehub_compartments() {
    let binary = RustNode::from_workspace().binary_path().to_path_buf();
    RustNode::build().expect("build rust binary");
    let jack = generate_identity(&binary).expect("Jack identity");

    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .skip_build()
        .with_source_hub()
        .with_identity(&jack.private_key_hex)
        .build()
        .await
        .expect("failed to build source hub cluster");

    let node = cluster.client(0);

    // Generate other identities
    let agent_xarchive = generate_identity(&binary).expect("Agent-XArchive identity");
    let _agent_hiking = generate_identity(&binary).expect("Agent-Hiking identity");
    let vanessa = generate_identity(&binary).expect("Vanessa identity");
    let outsider = generate_identity(&binary).expect("Outsider identity");

    // Create policies on Source Hub (one at a time to avoid account sequence issues)
    let xarchive_policy_id = add_policy(&node, XARCHIVE_ACP_POLICY, &jack.private_key_hex);
    tokio::time::sleep(Duration::from_secs(2)).await;
    let hiking_policy_id = add_policy(&node, HIKING_ACP_POLICY, &jack.private_key_hex);
    tokio::time::sleep(Duration::from_secs(2)).await;
    let secret_policy_id = add_policy(&node, SECRET_ACP_POLICY, &jack.private_key_hex);
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Deploy schemas
    node.schema_add_with_identity(
        &tweet_schema_with_policy(&xarchive_policy_id),
        &jack.private_key_hex,
    )
    .expect("Tweet schema");
    node.schema_add_with_identity(
        &interaction_schema_with_policy(&xarchive_policy_id),
        &jack.private_key_hex,
    )
    .expect("Interaction schema");
    node.schema_add_with_identity(
        &workout_schema_with_policy(&hiking_policy_id),
        &jack.private_key_hex,
    )
    .expect("Workout schema");
    node.schema_add_with_identity(
        &peak_schema_with_policy(&hiking_policy_id),
        &jack.private_key_hex,
    )
    .expect("Peak schema");
    node.schema_add_with_identity(
        &secret_schema_with_policy(&secret_policy_id),
        &jack.private_key_hex,
    )
    .expect("Secret schema");

    // --- Scenario 1: Owner creates and owns ---
    let tweet_data = node
        .query_with_identity(
            r#"mutation { add_Tweet(input: {text: "Hello world", likes: 42, archived: false}) { _docID text } }"#,
            &jack.private_key_hex,
        )
        .expect("Jack create Tweet");
    let tweet_id = tweet_data["add_Tweet"][0]["_docID"]
        .as_str()
        .expect("tweet _docID");

    let jack_tweets = node
        .query_with_identity(
            "query { Tweet { _docID text likes } }",
            &jack.private_key_hex,
        )
        .expect("Jack query tweets");
    assert_eq!(
        jack_tweets["Tweet"].as_array().unwrap().len(),
        1,
        "Jack should see his tweet"
    );

    // --- Scenario 2: Grant Agent-XArchive writer on tweets ---
    node.acp_relationship_add(
        "Tweet",
        tweet_id,
        "writer",
        &agent_xarchive.did,
        &jack.private_key_hex,
    )
    .expect("grant agent-xarchive writer on tweet");

    let agent_xa_tweets = node
        .query_with_identity(
            "query { Tweet { _docID text } }",
            &agent_xarchive.private_key_hex,
        )
        .expect("agent-xarchive query tweets");
    assert_eq!(
        agent_xa_tweets["Tweet"].as_array().unwrap().len(),
        1,
        "Agent-XArchive should read tweets"
    );

    // --- Scenario 3: Cross-compartment isolation ---
    let workout_data = node
        .query_with_identity(
            r#"mutation { add_Workout(input: {activity: "Trail Run", duration_min: 45}) { _docID } }"#,
            &jack.private_key_hex,
        )
        .expect("Jack create Workout");
    let workout_id = workout_data["add_Workout"][0]["_docID"]
        .as_str()
        .expect("workout _docID");

    // Agent-XArchive should NOT see workouts (wrong compartment, no grant)
    let agent_xa_workouts = node
        .query_with_identity(
            "query { Workout { _docID activity } }",
            &agent_xarchive.private_key_hex,
        )
        .expect("agent-xarchive query workouts");
    assert_eq!(
        agent_xa_workouts["Workout"].as_array().unwrap().len(),
        0,
        "Agent-XArchive should NOT see hiking workouts"
    );

    // --- Scenario 4: Reader access grant ---
    node.acp_relationship_add(
        "Workout",
        workout_id,
        "reader",
        &vanessa.did,
        &jack.private_key_hex,
    )
    .expect("grant Vanessa reader on workout");

    let vanessa_workouts = node
        .query_with_identity(
            "query { Workout { _docID activity } }",
            &vanessa.private_key_hex,
        )
        .expect("Vanessa query workouts");
    assert_eq!(
        vanessa_workouts["Workout"].as_array().unwrap().len(),
        1,
        "Vanessa should read workouts after grant"
    );

    // --- Scenario 5: Owner-only secrets ---
    let secret_data = node
        .query_with_identity(
            r#"mutation { add_Secret(input: {content: "Top Secret", classification: "eyes-only"}) { _docID } }"#,
            &jack.private_key_hex,
        )
        .expect("Jack create Secret");
    let _secret_id = secret_data["add_Secret"][0]["_docID"]
        .as_str()
        .expect("secret _docID");

    // Outsider cannot see secrets
    let outsider_secrets = node
        .query_with_identity(
            "query { Secret { _docID content } }",
            &outsider.private_key_hex,
        )
        .expect("outsider query secrets");
    assert_eq!(
        outsider_secrets["Secret"].as_array().unwrap().len(),
        0,
        "Outsider should NOT see secrets"
    );

    // Jack can see secrets (owner)
    let jack_secrets = node
        .query_with_identity("query { Secret { _docID content } }", &jack.private_key_hex)
        .expect("Jack query secrets");
    assert_eq!(
        jack_secrets["Secret"].as_array().unwrap().len(),
        1,
        "Jack should see his secrets"
    );

    // --- Scenario 6: Agent scoping (writer can read+write, NOT delete) ---
    let delete_result = node.query_with_identity(
        &format!(
            r#"mutation {{ delete_Tweet(docID: "{}") {{ _docID }} }}"#,
            tweet_id
        ),
        &agent_xarchive.private_key_hex,
    );
    // Delete should either fail or return empty (no permission)
    if let Ok(ref val) = delete_result {
        let deleted = val["delete_Tweet"].as_array().map(|a| a.len()).unwrap_or(0);
        assert_eq!(
            deleted, 0,
            "Agent-XArchive should NOT be able to delete tweets"
        );
    }
}
