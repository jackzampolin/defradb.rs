use integration_test::{for_each_runtime, generate_identity, TestCluster};

const VIEWER_ACP_POLICY: &str = r#"name: viewer-policy
description: Custom ACP policy with a non-standard viewer relation

resources:
  - name: reports
    permissions:
      - name: read
        expr: viewer
      - name: update
        expr: editor
      - name: delete
        expr: remover
    relations:
      - name: viewer
        types:
          - actor
      - name: editor
        types:
          - actor
      - name: remover
        types:
          - actor"#;

fn reports_schema_with_policy(policy_id: &str) -> String {
    format!(
        r#"type Report @policy(id: "{}", resource: "reports") {{ title: String  body: String }}"#,
        policy_id
    )
}

async fn custom_relation_policy_test(cluster: TestCluster) {
    let node = cluster.client(0);
    let binary = node.binary_path().to_path_buf();

    let owner = generate_identity(&binary).expect("owner identity");
    let viewer = generate_identity(&binary).expect("viewer identity");

    let policy = node
        .acp_policy_add(VIEWER_ACP_POLICY, &owner.private_key_hex)
        .expect("add custom ACP policy");
    let policy_id = policy["PolicyID"]
        .as_str()
        .or_else(|| policy["policyID"].as_str())
        .expect("missing PolicyID");

    let schema = reports_schema_with_policy(policy_id);
    node.schema_add_with_identity(&schema, &owner.private_key_hex)
        .expect("add schema");

    let created = node
        .query_with_identity(
            r#"mutation { add_Report(input: {title: "Quarterly", body: "Q1"}) { _docID title } }"#,
            &owner.private_key_hex,
        )
        .expect("create protected report");
    let doc_id = created["add_Report"][0]["_docID"]
        .as_str()
        .expect("_docID")
        .to_string();

    let before = node
        .query_with_identity("query { Report { _docID title body } }", &viewer.private_key_hex)
        .expect("query before viewer grant");
    assert_eq!(
        before["Report"].as_array().map(|a| a.len()).unwrap_or(0),
        0,
        "custom viewer relation must not be implicit before grant"
    );

    node.acp_relationship_add(
        "Report",
        &doc_id,
        "viewer",
        &viewer.did,
        &owner.private_key_hex,
    )
    .expect("grant viewer relation");

    let after = node
        .query_with_identity("query { Report { _docID title body } }", &viewer.private_key_hex)
        .expect("query after viewer grant");
    let reports = after["Report"].as_array().expect("Report array");
    assert_eq!(reports.len(), 1, "viewer should see exactly one report");
    assert_eq!(reports[0]["title"], "Quarterly");

    let update_attempt = node.query_with_identity(
        &format!(
            r#"mutation {{ update_Report(docID: "{}", input: {{title: "Tampered"}}) {{ _docID title }} }}"#,
            doc_id
        ),
        &viewer.private_key_hex,
    );
    match update_attempt {
        Err(_) => {}
        Ok(val) => {
            let updated = val["update_Report"].as_array().map(|a| a.len()).unwrap_or(0);
            assert_eq!(
                updated, 0,
                "viewer relation must not grant update access: {:?}",
                val
            );
        }
    }

    let owner_read = node
        .query_with_identity("query { Report { _docID title body } }", &owner.private_key_hex)
        .expect("owner read after viewer update attempt");
    let owner_reports = owner_read["Report"].as_array().expect("owner report array");
    assert_eq!(owner_reports.len(), 1);
    assert_eq!(
        owner_reports[0]["title"], "Quarterly",
        "viewer update attempt must not modify the report"
    );
}

for_each_runtime!(
    custom_relation_policy,
    custom_relation_policy_test,
    .with_acp_local()
);
