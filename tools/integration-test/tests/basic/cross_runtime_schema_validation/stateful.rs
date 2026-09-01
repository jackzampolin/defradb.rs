use integration_test::{generate_identity, TestCluster};
use serde_json::Value;

use super::{assert_outcome, purge_both};

const VIEW_POLICY: &str = r#"name: view-policy
description: Policy used by schema validation parity tests
resources:
  - name: records
    permissions:
      - name: read
        expr: reader
      - name: update
      - name: delete
    relations:
      - name: reader
        types:
          - actor"#;

fn active_description(description: &Value) -> &Value {
    description
        .as_array()
        .and_then(|versions| {
            versions.iter().find(|version| {
                version
                    .get("IsActive")
                    .or_else(|| version.get("is_active"))
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
            })
        })
        .unwrap_or(description)
}

fn active_version_id(description: &Value) -> &str {
    let version = active_description(description);
    version
        .get("VersionID")
        .or_else(|| version.get("version_id"))
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("active VersionID missing from {description}"))
}

fn active_collection_id(description: &Value) -> &str {
    let version = active_description(description);
    version
        .get("CollectionID")
        .or_else(|| version.get("collection_id"))
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("active CollectionID missing from {description}"))
}

fn policy_id(policy: &Value) -> &str {
    policy
        .get("PolicyID")
        .or_else(|| policy.get("policyID"))
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("PolicyID missing from {policy}"))
}

#[tokio::test]
async fn go_schema_stateful_validation_parity() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .go_nodes(1)
        .with_development()
        .with_acp_local()
        .build()
        .await
        .unwrap();
    let rust = cluster.client(0);
    let go = cluster.client(1);

    let sdl = "type FirstVersion { name: String } type SecondVersion { name: String }";
    rust.schema_add(sdl).expect("add Rust ID schema");
    go.schema_add(sdl).expect("add Go ID schema");
    let rust_second = rust
        .collection_describe_version("SecondVersion")
        .expect("describe Rust SecondVersion");
    let go_second = go
        .collection_describe_version("SecondVersion")
        .expect("describe Go SecondVersion");
    let rust_second_id = active_version_id(&rust_second);
    let go_second_id = active_version_id(&go_second);
    assert_eq!(rust_second_id, go_second_id);
    let duplicate_id_patch = format!(
        r#"[{{"op":"replace","path":"/FirstVersion/VersionID","value":"{rust_second_id}"}}]"#
    );
    let rust_result = rust.collection_patch(&duplicate_id_patch);
    let go_result = go.collection_patch(&duplicate_id_patch);
    assert_outcome("validateIDUnique", false, &rust_result, &go_result);
    purge_both(&rust, &go);

    let sdl = "type VersionedCollection { name: String }";
    rust.schema_add(sdl).expect("add Rust versioned schema");
    go.schema_add(sdl).expect("add Go versioned schema");
    let rust_original = rust
        .collection_describe_version("VersionedCollection")
        .expect("describe Rust original version");
    let go_original = go
        .collection_describe_version("VersionedCollection")
        .expect("describe Go original version");
    let rust_original_id = active_version_id(&rust_original);
    let go_original_id = active_version_id(&go_original);
    assert_eq!(rust_original_id, go_original_id);
    let add_field = r#"[{"op":"add","path":"/VersionedCollection/Fields/-","value":{"Name":"extra","Kind":"String"}}]"#;
    let rust_result = rust.collection_patch(add_field);
    let go_result = go.collection_patch(add_field);
    assert_outcome("valid version creation", true, &rust_result, &go_result);
    let activate_original =
        format!(r#"[{{"op":"replace","path":"/{rust_original_id}/IsActive","value":true}}]"#);
    let rust_result = rust.collection_patch(&activate_original);
    let go_result = go.collection_patch(&activate_original);
    assert_outcome(
        "validateSingleVersionActive",
        false,
        &rust_result,
        &go_result,
    );
    purge_both(&rust, &go);

    let sdl = "type SelfReference { name: String }";
    rust.schema_add(sdl).expect("add Rust self schema");
    go.schema_add(sdl).expect("add Go self schema");
    let rust_description = rust
        .collection_describe_version("SelfReference")
        .expect("describe Rust self collection");
    let go_description = go
        .collection_describe_version("SelfReference")
        .expect("describe Go self collection");
    let rust_collection_id = active_collection_id(&rust_description);
    let go_collection_id = active_collection_id(&go_description);
    assert_eq!(rust_collection_id, go_collection_id);
    let self_reference = format!(
        r#"[{{"op":"add","path":"/SelfReference/Fields/-","value":{{"Name":"parent","Kind":{{"CollectionID":"{rust_collection_id}","Array":false}},"RelationName":"self_reference","IsPrimary":true}}}}]"#
    );
    let rust_result = rust.collection_patch(&self_reference);
    let go_result = go.collection_patch(&self_reference);
    assert_outcome("validateSelfReferences", false, &rust_result, &go_result);
    purge_both(&rust, &go);

    rust.schema_add("type ViewSource { name: String }")
        .expect("add Rust view source");
    go.schema_add("type ViewSource { name: String }")
        .expect("add Go view source");
    let add_source = r#"mutation { add_ViewSource(input: {name: "value"}) { _docID } }"#;
    rust.query(add_source).expect("add Rust source document");
    go.query(add_source).expect("add Go source document");
    rust.view_add(
        "ViewSource { name }",
        "type MaterializedView { name: String }",
    )
    .expect("add Rust materialized view");
    go.view_add(
        "ViewSource { name }",
        "type MaterializedView { name: String }",
    )
    .expect("add Go materialized view");
    let _ = rust.view_refresh(Some("MaterializedView"));
    let _ = go.view_refresh(Some("MaterializedView"));
    let rust_view = rust
        .query("query { MaterializedView { name } }")
        .expect("query Rust materialized view");
    let go_view = go
        .query("query { MaterializedView { name } }")
        .expect("query Go materialized view");
    assert_eq!(rust_view, go_view);
    assert_eq!(
        rust_view
            .pointer("/MaterializedView/0/name")
            .and_then(Value::as_str),
        Some("value")
    );
    let dematerialize =
        r#"[{"op":"replace","path":"/MaterializedView/IsMaterialized","value":false}]"#;
    let rust_result = rust.collection_patch(dematerialize);
    let go_result = go.collection_patch(dematerialize);
    assert_outcome(
        "validateDematerializedViewHasNoData",
        false,
        &rust_result,
        &go_result,
    );
    purge_both(&rust, &go);

    let owner = generate_identity(rust.binary_path()).expect("generate policy owner");
    let rust_policy = rust
        .acp_policy_add(VIEW_POLICY, &owner.private_key_hex)
        .expect("add Rust policy");
    let go_policy = go
        .acp_policy_add(VIEW_POLICY, &owner.private_key_hex)
        .expect("add Go policy");
    let rust_policy_id = policy_id(&rust_policy);
    let go_policy_id = policy_id(&go_policy);
    assert_eq!(rust_policy_id, go_policy_id);

    let protected_collection = format!(
        r#"type ProtectedCollection @policy(id: "{rust_policy_id}", resource: "records") {{ name: String }}"#
    );
    let rust_result = rust.schema_add_with_identity(&protected_collection, &owner.private_key_hex);
    let go_result = go.schema_add_with_identity(&protected_collection, &owner.private_key_hex);
    assert_outcome(
        "valid materialized collection with policy",
        true,
        &rust_result,
        &go_result,
    );

    let source_sdl = "type PolicyViewSource { name: String }";
    rust.schema_add(source_sdl)
        .expect("add Rust policy view source");
    go.schema_add(source_sdl)
        .expect("add Go policy view source");
    let protected_view = format!(
        r#"type ProtectedView @policy(id: "{rust_policy_id}", resource: "records") @materialized(if: false) {{ name: String }}"#
    );
    let rust_result = rust.view_add("PolicyViewSource { name }", &protected_view);
    let go_result = go.view_add("PolicyViewSource { name }", &protected_view);
    assert_outcome(
        "valid non-materialized view with policy",
        true,
        &rust_result,
        &go_result,
    );
    let materialize = r#"[{"op":"replace","path":"/ProtectedView/IsMaterialized","value":true}]"#;
    let rust_result = rust.collection_patch(materialize);
    let go_result = go.collection_patch(materialize);
    assert_outcome(
        "validateMaterializedHasNoPolicy",
        false,
        &rust_result,
        &go_result,
    );
}
