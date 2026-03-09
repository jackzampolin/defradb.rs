use integration_test::{
    for_each_runtime, generate_identity, multi_resource_policy, typed_schema, TestCluster,
    STANDARD_FIELDS,
};

async fn cross_compartment_isolation_test(cluster: TestCluster) {
    let node = cluster.client(0);
    let binary = node.binary_path().to_path_buf();

    // --- 4 identities: jack (owner of both), xbot, hiking_service, anonymous ---
    let jack = generate_identity(&binary).expect("jack identity");
    let xbot = generate_identity(&binary).expect("xbot identity");
    let hiking_svc = generate_identity(&binary).expect("hiking_service identity");

    // --- Policy A: x-archive (tweet, interaction) ---
    let x_policy_yaml = multi_resource_policy(
        "x-archive-policy",
        "X archive compartment",
        &["tweet", "interaction"],
    );
    let x_policy = node
        .acp_policy_add(&x_policy_yaml, &jack.private_key_hex)
        .expect("add x-archive policy");
    let x_policy_id = x_policy["PolicyID"]
        .as_str()
        .or_else(|| x_policy["policyID"].as_str())
        .expect("missing x-archive PolicyID");

    // --- Policy B: hiking (trail, workout, weather) ---
    let h_policy_yaml = multi_resource_policy(
        "hiking-policy",
        "Hiking compartment",
        &["trail", "workout", "weather"],
    );
    let h_policy = node
        .acp_policy_add(&h_policy_yaml, &jack.private_key_hex)
        .expect("add hiking policy");
    let h_policy_id = h_policy["PolicyID"]
        .as_str()
        .or_else(|| h_policy["policyID"].as_str())
        .expect("missing hiking PolicyID");

    // --- Deploy schemas for both compartments ---
    let x_types = [("Tweet", "tweet"), ("Interaction", "interaction")];
    let h_types = [
        ("Trail", "trail"),
        ("Workout", "workout"),
        ("Weather", "weather"),
    ];

    for (type_name, resource) in &x_types {
        let schema = typed_schema(type_name, x_policy_id, resource, STANDARD_FIELDS);
        node.schema_add_with_identity(&schema, &jack.private_key_hex)
            .unwrap_or_else(|e| panic!("add {} schema failed: {}", type_name, e));
    }
    for (type_name, resource) in &h_types {
        let schema = typed_schema(type_name, h_policy_id, resource, STANDARD_FIELDS);
        node.schema_add_with_identity(&schema, &jack.private_key_hex)
            .unwrap_or_else(|e| panic!("add {} schema failed: {}", type_name, e));
    }

    // --- Jack creates documents in both compartments ---
    let tweet_data = node
        .query_with_identity(
            r#"mutation { add_Tweet(input: {title: "My tweet", body: "tweet body", score: 1}) { _docID } }"#,
            &jack.private_key_hex,
        )
        .expect("create tweet");
    let tweet_id = tweet_data["add_Tweet"][0]["_docID"]
        .as_str()
        .expect("tweet _docID")
        .to_string();

    node.query_with_identity(
        r#"mutation { add_Interaction(input: {title: "Like", body: "liked a tweet", score: 1}) { _docID } }"#,
        &jack.private_key_hex,
    )
    .expect("create interaction");

    let trail_data = node
        .query_with_identity(
            r#"mutation { add_Trail(input: {title: "Eagle Peak", body: "mountain trail", score: 8}) { _docID } }"#,
            &jack.private_key_hex,
        )
        .expect("create trail");
    let trail_id = trail_data["add_Trail"][0]["_docID"]
        .as_str()
        .expect("trail _docID")
        .to_string();

    node.query_with_identity(
        r#"mutation { add_Workout(input: {title: "Morning run", body: "5k", score: 7}) { _docID } }"#,
        &jack.private_key_hex,
    )
    .expect("create workout");

    node.query_with_identity(
        r#"mutation { add_Weather(input: {title: "Sunny", body: "clear skies", score: 9}) { _docID } }"#,
        &jack.private_key_hex,
    )
    .expect("create weather");

    // --- Grant xbot writer on x-archive resources ---
    node.acp_relationship_add(
        "Tweet",
        &tweet_id,
        "writer",
        &xbot.did,
        &jack.private_key_hex,
    )
    .expect("grant xbot writer on tweet");

    // --- Grant hiking_service writer on hiking resources ---
    node.acp_relationship_add(
        "Trail",
        &trail_id,
        "writer",
        &hiking_svc.did,
        &jack.private_key_hex,
    )
    .expect("grant hiking_svc writer on trail");

    let read_count = |key: &str, type_name: &str| -> usize {
        let query = format!("query {{ {} {{ _docID }} }}", type_name);
        node.query_with_identity(&query, key)
            .map(|v| v[type_name].as_array().map(|a| a.len()).unwrap_or(0))
            .unwrap_or(0)
    };

    // === Compartment isolation: xbot can't see hiking ===
    assert_eq!(
        read_count(&xbot.private_key_hex, "Tweet"),
        1,
        "xbot reads Tweet -> ALLOW"
    );
    assert_eq!(
        read_count(&xbot.private_key_hex, "Trail"),
        0,
        "xbot reads Trail -> DENY (no relation in hiking policy)"
    );
    assert_eq!(
        read_count(&xbot.private_key_hex, "Workout"),
        0,
        "xbot reads Workout -> DENY"
    );
    assert_eq!(
        read_count(&xbot.private_key_hex, "Weather"),
        0,
        "xbot reads Weather -> DENY"
    );

    // === hiking_service can't see x-archive ===
    assert_eq!(
        read_count(&hiking_svc.private_key_hex, "Trail"),
        1,
        "hiking_svc reads Trail -> ALLOW"
    );
    assert_eq!(
        read_count(&hiking_svc.private_key_hex, "Tweet"),
        0,
        "hiking_svc reads Tweet -> DENY (no relation in x-archive)"
    );
    assert_eq!(
        read_count(&hiking_svc.private_key_hex, "Interaction"),
        0,
        "hiking_svc reads Interaction -> DENY"
    );

    // === Jack (owner) traverses both compartments ===
    assert_eq!(
        read_count(&jack.private_key_hex, "Tweet"),
        1,
        "jack reads Tweet"
    );
    assert_eq!(
        read_count(&jack.private_key_hex, "Interaction"),
        1,
        "jack reads Interaction"
    );
    assert_eq!(
        read_count(&jack.private_key_hex, "Trail"),
        1,
        "jack reads Trail"
    );
    assert_eq!(
        read_count(&jack.private_key_hex, "Workout"),
        1,
        "jack reads Workout"
    );
    assert_eq!(
        read_count(&jack.private_key_hex, "Weather"),
        1,
        "jack reads Weather"
    );

    // === Cross-compartment write attempt ===
    let xbot_write_trail = node.query_with_identity(
        &format!(
            r#"mutation {{ update_Trail(docID: "{}", input: {{score: 999}}) {{ _docID }} }}"#,
            trail_id
        ),
        &xbot.private_key_hex,
    );
    if let Ok(result) = xbot_write_trail {
        let count = result["update_Trail"]
            .as_array()
            .map(|a| a.len())
            .unwrap_or(0);
        assert_eq!(
            count, 0,
            "xbot should NOT write to Trail (cross-compartment)"
        );
    }

    let hiking_write_tweet = node.query_with_identity(
        &format!(
            r#"mutation {{ update_Tweet(docID: "{}", input: {{score: 999}}) {{ _docID }} }}"#,
            tweet_id
        ),
        &hiking_svc.private_key_hex,
    );
    if let Ok(result) = hiking_write_tweet {
        let count = result["update_Tweet"]
            .as_array()
            .map(|a| a.len())
            .unwrap_or(0);
        assert_eq!(
            count, 0,
            "hiking_svc should NOT write to Tweet (cross-compartment)"
        );
    }

    // === Selective cross-compartment grant: xbot gets reader on ONE trail doc ===
    node.acp_relationship_add(
        "Trail",
        &trail_id,
        "reader",
        &xbot.did,
        &jack.private_key_hex,
    )
    .expect("grant xbot reader on specific trail");

    assert_eq!(
        read_count(&xbot.private_key_hex, "Trail"),
        1,
        "xbot reads granted Trail doc -> ALLOW"
    );
    // xbot still can't see other hiking resources
    assert_eq!(
        read_count(&xbot.private_key_hex, "Workout"),
        0,
        "xbot reads Workout -> still DENY (grant was per-document)"
    );
    assert_eq!(
        read_count(&xbot.private_key_hex, "Weather"),
        0,
        "xbot reads Weather -> still DENY"
    );

    // === Blast radius test: even with xbot's DID, can't pivot to hiking writes ===
    // xbot has reader on trail (not writer), so can't update
    let xbot_update_trail = node.query_with_identity(
        &format!(
            r#"mutation {{ update_Trail(docID: "{}", input: {{score: 0}}) {{ _docID }} }}"#,
            trail_id
        ),
        &xbot.private_key_hex,
    );
    if let Ok(result) = xbot_update_trail {
        let count = result["update_Trail"]
            .as_array()
            .map(|a| a.len())
            .unwrap_or(0);
        assert_eq!(
            count, 0,
            "xbot should NOT update Trail (only has reader, not writer)"
        );
    }

    // xbot can't delete tweets either (writer, not admin)
    let xbot_delete_tweet = node.query_with_identity(
        &format!(
            r#"mutation {{ delete_Tweet(docID: "{}") {{ _docID }} }}"#,
            tweet_id
        ),
        &xbot.private_key_hex,
    );
    if let Ok(result) = xbot_delete_tweet {
        let count = result["delete_Tweet"]
            .as_array()
            .map(|a| a.len())
            .unwrap_or(0);
        assert_eq!(count, 0, "xbot should NOT delete Tweet (writer, not admin)");
    }
}

for_each_runtime!(cross_compartment_isolation, cross_compartment_isolation_test, .with_acp_local());
