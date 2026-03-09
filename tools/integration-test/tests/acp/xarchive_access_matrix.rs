use integration_test::{
    for_each_runtime, generate_identity, multi_resource_policy, typed_schema, TestCluster,
    STANDARD_FIELDS,
};

const RESOURCES: &[(&str, &str)] = &[
    ("Tweet", "tweet"),
    ("Interaction", "interaction"),
    ("TimelineSnapshot", "timeline_snapshot"),
    ("Digest", "digest"),
    ("ApiUsage", "api_usage"),
];

/// Resources where xbot gets "writer" relation.
const XBOT_WRITER_RESOURCES: &[&str] = &["TimelineSnapshot", "Digest", "ApiUsage"];

fn is_xbot_writer(type_name: &str) -> bool {
    XBOT_WRITER_RESOURCES.contains(&type_name)
}

async fn xarchive_access_matrix_test(cluster: TestCluster) {
    let node = cluster.client(0);
    let binary = node.binary_path().to_path_buf();

    // --- Setup: 3 identities ---
    let jack = generate_identity(&binary).expect("jack identity");
    let xbot = generate_identity(&binary).expect("xbot identity");
    let watchdog = generate_identity(&binary).expect("watchdog identity");

    // --- Deploy x-archive policy ---
    let resource_names: Vec<&str> = RESOURCES.iter().map(|(_, r)| *r).collect();
    let policy_yaml = multi_resource_policy(
        "x-archive-policy",
        "X archive compartment access control",
        &resource_names,
    );
    let policy = node
        .acp_policy_add(&policy_yaml, &jack.private_key_hex)
        .expect("add x-archive policy");
    let policy_id = policy["PolicyID"]
        .as_str()
        .or_else(|| policy["policyID"].as_str())
        .expect("missing PolicyID");

    // --- Deploy 5 schemas, one per resource ---
    for (type_name, resource) in RESOURCES {
        let schema = typed_schema(type_name, policy_id, resource, STANDARD_FIELDS);
        node.schema_add_with_identity(&schema, &jack.private_key_hex)
            .unwrap_or_else(|e| panic!("add schema for {} failed: {}", type_name, e));
    }

    // --- Jack creates 1 document per collection ---
    let mut doc_ids: Vec<(&str, String)> = Vec::new();
    for (type_name, _) in RESOURCES {
        let mutation = format!(
            r#"mutation {{ create_{}(input: {{title: "{} doc", body: "test body", score: 1}}) {{ _docID }} }}"#,
            type_name, type_name
        );
        let result = node
            .query_with_identity(&mutation, &jack.private_key_hex)
            .unwrap_or_else(|e| panic!("create {} doc failed: {}", type_name, e));
        let key = format!("add_{}", type_name);
        let doc_id = result[&key][0]["_docID"]
            .as_str()
            .unwrap_or_else(|| panic!("missing _docID for {}", type_name))
            .to_string();
        doc_ids.push((type_name, doc_id));
    }

    // --- Assign relations ---
    // xbot gets "writer" on TimelineSnapshot, Digest, ApiUsage
    for (type_name, doc_id) in &doc_ids {
        if is_xbot_writer(type_name) {
            node.acp_relationship_add(
                type_name,
                doc_id,
                "writer",
                &xbot.did,
                &jack.private_key_hex,
            )
            .unwrap_or_else(|e| panic!("grant xbot writer on {} failed: {}", type_name, e));
        }
    }

    // watchdog gets "reader" on all 5
    for (type_name, doc_id) in &doc_ids {
        node.acp_relationship_add(
            type_name,
            doc_id,
            "reader",
            &watchdog.did,
            &jack.private_key_hex,
        )
        .unwrap_or_else(|e| panic!("grant watchdog reader on {} failed: {}", type_name, e));
    }

    // === READ CHECKS (15 identity x resource + 5 anonymous = 20) ===

    let read_count = |key: &str, type_name: &str| -> usize {
        let query = format!("query {{ {} {{ _docID }} }}", type_name);
        node.query_with_identity(&query, key)
            .map(|v| v[type_name].as_array().map(|a| a.len()).unwrap_or(0))
            .unwrap_or(0)
    };

    // Jack (owner) reads all 5 -> ALLOW
    for (type_name, _) in RESOURCES {
        assert_eq!(
            read_count(&jack.private_key_hex, type_name),
            1,
            "jack should read {} (owner)",
            type_name
        );
    }

    // xbot reads: DENY on tweet/interaction, ALLOW on snapshot/digest/api_usage
    for (type_name, _) in RESOURCES {
        let expected = if is_xbot_writer(type_name) { 1 } else { 0 };
        assert_eq!(
            read_count(&xbot.private_key_hex, type_name),
            expected,
            "xbot read {} expected {}",
            type_name,
            expected
        );
    }

    // watchdog reads all 5 -> ALLOW (has reader on all)
    for (type_name, _) in RESOURCES {
        assert_eq!(
            read_count(&watchdog.private_key_hex, type_name),
            1,
            "watchdog should read {} (reader)",
            type_name
        );
    }

    // Anonymous reads -> DENY on all 5
    for (type_name, _) in RESOURCES {
        let query = format!("query {{ {} {{ _docID }} }}", type_name);
        let result = node.query(&query).expect("anon query");
        let count = result[type_name].as_array().map(|a| a.len()).unwrap_or(0);
        assert_eq!(
            count, 0,
            "anonymous should not read {} (ACP-protected)",
            type_name
        );
    }

    // === UPDATE CHECKS (15) ===

    let can_update = |key: &str, type_name: &str, doc_id: &str| -> bool {
        let mutation = format!(
            r#"mutation {{ update_{}(docID: "{}", input: {{score: 999}}) {{ _docID }} }}"#,
            type_name, doc_id
        );
        match node.query_with_identity(&mutation, key) {
            Ok(v) => {
                let update_key = format!("update_{}", type_name);
                v[&update_key]
                    .as_array()
                    .map(|a| !a.is_empty())
                    .unwrap_or(false)
            }
            Err(_) => false,
        }
    };

    // Jack (owner) updates all 5 -> ALLOW
    for (type_name, doc_id) in &doc_ids {
        assert!(
            can_update(&jack.private_key_hex, type_name, doc_id),
            "jack should update {} (owner)",
            type_name
        );
    }

    // xbot updates: ALLOW on writer resources, DENY on tweet/interaction
    for (type_name, doc_id) in &doc_ids {
        let expected = is_xbot_writer(type_name);
        assert_eq!(
            can_update(&xbot.private_key_hex, type_name, doc_id),
            expected,
            "xbot update {} expected {}",
            type_name,
            expected
        );
    }

    // watchdog updates -> DENY on all 5 (reader can't update)
    for (type_name, doc_id) in &doc_ids {
        assert!(
            !can_update(&watchdog.private_key_hex, type_name, doc_id),
            "watchdog should NOT update {} (reader only)",
            type_name
        );
    }

    // === DELETE CHECKS (15) ===

    let can_delete = |key: &str, type_name: &str, doc_id: &str| -> bool {
        let mutation = format!(
            r#"mutation {{ delete_{}(docID: "{}") {{ _docID }} }}"#,
            type_name, doc_id
        );
        match node.query_with_identity(&mutation, key) {
            Ok(v) => {
                let delete_key = format!("delete_{}", type_name);
                v[&delete_key]
                    .as_array()
                    .map(|a| !a.is_empty())
                    .unwrap_or(false)
            }
            Err(_) => false,
        }
    };

    // xbot deletes -> DENY on all 5 (writer can't delete, delete requires admin)
    for (type_name, doc_id) in &doc_ids {
        assert!(
            !can_delete(&xbot.private_key_hex, type_name, doc_id),
            "xbot should NOT delete {} (writer, not admin)",
            type_name
        );
    }

    // watchdog deletes -> DENY on all 5
    for (type_name, doc_id) in &doc_ids {
        assert!(
            !can_delete(&watchdog.private_key_hex, type_name, doc_id),
            "watchdog should NOT delete {} (reader only)",
            type_name
        );
    }

    // Jack (owner) deletes one doc to verify owner can delete
    // Use the last collection (ApiUsage) to avoid breaking earlier checks
    let (last_type, last_id) = doc_ids.last().expect("has docs");
    assert!(
        can_delete(&jack.private_key_hex, last_type, last_id),
        "jack should delete {} (owner)",
        last_type
    );

    // === SPECIAL: Grant xbot reader on tweet -> xbot can now read tweet ===
    let (_, tweet_id) = &doc_ids[0];
    node.acp_relationship_add(
        "Tweet",
        tweet_id,
        "reader",
        &xbot.did,
        &jack.private_key_hex,
    )
    .expect("grant xbot reader on tweet");
    assert_eq!(
        read_count(&xbot.private_key_hex, "Tweet"),
        1,
        "xbot should read Tweet after explicit reader grant"
    );

    // xbot still can't write tweet (reader doesn't grant write)
    assert!(
        !can_update(&xbot.private_key_hex, "Tweet", tweet_id),
        "xbot should NOT update Tweet (reader only, not writer)"
    );
}

for_each_runtime!(xarchive_access_matrix, xarchive_access_matrix_test, .with_acp_local());
