//! ACP link-collection (DRI) tests ported from Go DefraDB.
//!
//! Source: `tests/integration/acp/dac/link_collection/` in
//! https://github.com/sourcenetwork/defradb (develop branch)
//!
//! These tests verify the binding between a GraphQL collection and an ACP
//! policy via the `@policy(id:, resource:)` directive:
//!
//! - Accept: valid DRI (Resource Interface) — collection is registered.
//! - Reject: invalid `@policy` args (missing id, missing resource, wrong type,
//!   empty strings).
//! - Reject: DRI not found (nonexistent policy ID or resource name).
//! - Reject: DPI rule violation (resource missing required read/update/delete
//!   owner permission on the DRI).
//!
//! ## Policy YAML format note
//!
//! Rust DefraDB requires that `owner` is NOT declared explicitly in the
//! `relations` block (it is auto-injected). See issue #744. Policies here
//! therefore use the Rust-compatible format (no explicit `owner`).
//!
//! ## Error-string matching
//!
//! Rust's error strings differ from Go's. Assertions use lenient substring
//! matching with multiple alternatives and always include `"bad_input"` as a
//! generic fallback — matching the Phase 3a `policy_validation.rs` style.
//!
//! ## DRI existence + DPI rule checks
//!
//! Rust now enforces the same DRI → DPI chain Go does at schema-add time
//! (#746 fixed). When a `@policy(id:, resource:)` directive references:
//! - a policy that doesn't exist → `"policyID specified does not exist with acp"`
//! - a resource name not on the policy → `"resource does not exist on the specified policy"`
//! - a resource missing `read`/`update`/`delete` permission → `"resource is missing required permission on policy"`
//! Error strings are Go-compatible so mixed Rust/Go deployments produce
//! identical user-facing output.

use integration_test::{for_each_runtime, generate_identity, TestCluster};

// =========================================================================
// Helpers
// =========================================================================

/// Base valid policy (users resource with read/update/delete perms).
fn users_policy() -> &'static str {
    r#"
description: a policy
name: test
resources:
  - name: users
    permissions:
      - name: read
        expr: reader
      - name: update
        expr: updater
      - name: delete
        expr: deleter
    relations:
      - name: reader
        types:
          - actor
      - name: updater
        types:
          - actor
      - name: deleter
        types:
          - actor
"#
}

/// Policy that has an extra permission ("magic") beyond the DPI minimum.
/// DPI-required read/update/delete must still be present.
fn users_policy_with_extra_perm() -> &'static str {
    r#"
description: a policy
name: test
resources:
  - name: users
    permissions:
      - name: read
        expr: reader
      - name: update
        expr: updater
      - name: delete
        expr: deleter
      - name: magic
        expr: reader
    relations:
      - name: reader
        types:
          - actor
      - name: updater
        types:
          - actor
      - name: deleter
        types:
          - actor
"#
}

/// Policy with an admin relation that manages reader.
fn users_policy_with_managed_relation() -> &'static str {
    r#"
description: a policy
name: test
resources:
  - name: users
    permissions:
      - name: delete
      - name: read
        expr: reader
      - name: update
    relations:
      - name: admin
        manages:
          - reader
        types:
          - actor
      - name: reader
        types:
          - actor
"#
}

/// Policy with multiple resources where only `users` is linked by the schema.
fn users_policy_with_multiple_resources() -> &'static str {
    r#"
description: a policy
name: test
resources:
  - name: books
    permissions:
      - name: delete
      - name: read
      - name: update
  - name: users
    permissions:
      - name: delete
      - name: read
        expr: reader
      - name: update
    relations:
      - name: reader
        types:
          - actor
"#
}

/// Policy with one invalid and one valid resource; the valid one should still link.
fn users_policy_with_partial_dri() -> &'static str {
    r#"
name: test
description: A Partially DRI Compliant Policy
resources:
  - name: usersInvalid
    permissions:
      - name: delete
        expr: reader
      - name: update
        expr: reader
    relations:
      - name: reader
        types:
          - actor
  - name: usersValid
    permissions:
      - name: delete
      - name: read
        expr: reader
      - name: update
    relations:
      - name: reader
        types:
          - actor
"#
}

/// Policy where owner authority is omitted from the DRI permissions.
fn users_policy_with_owner_bad_on_update() -> &'static str {
    r#"
description: a policy
name: test
resources:
  - name: users
    permissions:
      - name: delete
      - name: read
      - name: update
        expr: ownerBad
    relations:
      - name: ownerBad
        types:
          - actor
"#
}

fn users_policy_with_owner_bad_on_read() -> &'static str {
    r#"
description: a policy
name: test
resources:
  - name: users
    permissions:
      - name: delete
      - name: read
        expr: ownerBad
      - name: update
    relations:
      - name: ownerBad
        types:
          - actor
"#
}

fn users_policy_with_owner_bad_on_delete() -> &'static str {
    r#"
description: a policy
name: test
resources:
  - name: users
    permissions:
      - name: delete
        expr: ownerBad
      - name: read
      - name: update
    relations:
      - name: ownerBad
        types:
          - actor
"#
}

/// Policy missing the `read` permission entirely — DPI violation.
fn users_policy_missing_read() -> &'static str {
    r#"
description: a policy
name: test
resources:
  - name: users
    permissions:
      - name: update
        expr: updater
      - name: delete
        expr: deleter
    relations:
      - name: updater
        types:
          - actor
      - name: deleter
        types:
          - actor
"#
}

/// Policy missing the `update` permission entirely — DPI violation.
fn users_policy_missing_update() -> &'static str {
    r#"
description: a policy
name: test
resources:
  - name: users
    permissions:
      - name: read
        expr: reader
      - name: delete
        expr: deleter
    relations:
      - name: reader
        types:
          - actor
      - name: deleter
        types:
          - actor
"#
}

/// Policy missing the `delete` permission entirely — DPI violation.
fn users_policy_missing_delete() -> &'static str {
    r#"
description: a policy
name: test
resources:
  - name: users
    permissions:
      - name: read
        expr: reader
      - name: update
        expr: updater
    relations:
      - name: reader
        types:
          - actor
      - name: updater
        types:
          - actor
"#
}

/// Extract a policy ID from the JSON returned by `acp_policy_add`.
fn extract_policy_id(value: &serde_json::Value) -> Option<String> {
    value["PolicyID"]
        .as_str()
        .or_else(|| value["policyID"].as_str())
        .map(|s| s.to_string())
}

/// Try to add a schema via the authenticated schema endpoint and return the
/// error message, if any. `None` = success.
fn try_add_schema(
    node: &integration_test::DefraClient,
    sdl: &str,
    hex_key: &str,
) -> Option<String> {
    match node.schema_add_with_identity(sdl, hex_key) {
        Ok(_) => None,
        Err(e) => Some(format!("{:#}", e)),
    }
}

/// Add a policy and return its ID. Panics on failure.
fn add_users_policy(
    node: &integration_test::DefraClient,
    policy_yaml: &str,
    hex_key: &str,
) -> String {
    let result = node
        .acp_policy_add(policy_yaml, hex_key)
        .expect("policy add must succeed");
    extract_policy_id(&result).expect("policy add must return a PolicyID")
}

/// Introspect a type and return whether it exists.
fn type_exists(node: &integration_test::DefraClient, type_name: &str) -> bool {
    let query = format!(r#"query {{ __type(name: "{}") {{ name }} }}"#, type_name);
    match node.query(&query) {
        Ok(v) => v["__type"]["name"].as_str().is_some(),
        Err(_) => false,
    }
}

// =========================================================================
// Accept tests
// =========================================================================

// Port of accept_basic_dri_fmts_test.go
async fn acp_link_collection_basic_accept(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice identity");

    let policy_id = add_users_policy(&node, users_policy(), &alice.private_key_hex);

    let sdl = format!(
        r#"type Users @policy(id: "{}", resource: "users") {{ name: String  age: Int }}"#,
        policy_id
    );
    node.schema_add_with_identity(&sdl, &alice.private_key_hex)
        .expect("schema with valid DRI should be accepted");

    assert!(
        type_exists(&node, "Users"),
        "Users type must be registered after accepting a valid DRI"
    );
}

for_each_runtime!(
    acp_link_collection_basic_accept,
    acp_link_collection_basic_accept,
    .with_acp_local()
);

// Port of accept_extra_permissions_on_dri_test.go
async fn acp_link_collection_extra_permissions_accept(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice identity");

    let policy_id = add_users_policy(
        &node,
        users_policy_with_extra_perm(),
        &alice.private_key_hex,
    );

    let sdl = format!(
        r#"type Users @policy(id: "{}", resource: "users") {{ name: String  age: Int }}"#,
        policy_id
    );
    node.schema_add_with_identity(&sdl, &alice.private_key_hex)
        .expect("extra permissions on the DRI must still be accepted");

    assert!(
        type_exists(&node, "Users"),
        "Users type must be registered when the DRI has extra permissions"
    );
}

for_each_runtime!(
    acp_link_collection_extra_permissions_accept,
    acp_link_collection_extra_permissions_accept,
    .with_acp_local()
);

// Port of accept_managed_relation_on_dri_test.go
async fn acp_link_collection_managed_relation_accept(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice identity");

    let policy_id = add_users_policy(
        &node,
        users_policy_with_managed_relation(),
        &alice.private_key_hex,
    );

    let sdl = format!(
        r#"type Users @policy(id: "{}", resource: "users") {{ name: String  age: Int }}"#,
        policy_id
    );
    node.schema_add_with_identity(&sdl, &alice.private_key_hex)
        .expect("managed relation on the DRI must still be accepted");

    assert!(
        type_exists(&node, "Users"),
        "Users type must be registered when the DRI includes a managed relation"
    );
}

for_each_runtime!(
    acp_link_collection_managed_relation_accept,
    acp_link_collection_managed_relation_accept,
    .with_acp_local()
);

// Port of accept_mixed_resources_on_partial_dri_test.go
async fn acp_link_collection_partial_dri_valid_resource_accept(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice identity");

    let policy_id = add_users_policy(
        &node,
        users_policy_with_partial_dri(),
        &alice.private_key_hex,
    );

    let sdl = format!(
        r#"type Users @policy(id: "{}", resource: "usersValid") {{ name: String  age: Int }}"#,
        policy_id
    );
    node.schema_add_with_identity(&sdl, &alice.private_key_hex)
        .expect("valid resource on a partially DRI-compliant policy must be accepted");

    assert!(
        type_exists(&node, "Users"),
        "Users type must be registered when linking to the valid resource on a mixed policy"
    );
}

for_each_runtime!(
    acp_link_collection_partial_dri_valid_resource_accept,
    acp_link_collection_partial_dri_valid_resource_accept,
    .with_acp_local()
);

// Port of accept_multi_resources_on_dri_test.go (first case)
async fn acp_link_collection_multiple_resources_accept(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice identity");

    let policy_id = add_users_policy(
        &node,
        users_policy_with_multiple_resources(),
        &alice.private_key_hex,
    );

    let sdl = format!(
        r#"type Users @policy(id: "{}", resource: "users") {{ name: String  age: Int }}"#,
        policy_id
    );
    node.schema_add_with_identity(&sdl, &alice.private_key_hex)
        .expect("users resource on a multi-resource policy must be accepted");

    assert!(
        type_exists(&node, "Users"),
        "Users type must be registered when linking to one resource on a multi-resource policy"
    );
}

for_each_runtime!(
    acp_link_collection_multiple_resources_accept,
    acp_link_collection_multiple_resources_accept,
    .with_acp_local()
);

// Port of accept_multi_resources_on_dri_test.go (both resources used)
async fn acp_link_collection_multiple_resources_both_used_accept(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice identity");

    let policy_id = add_users_policy(
        &node,
        users_policy_with_multiple_resources(),
        &alice.private_key_hex,
    );

    let users_sdl = format!(
        r#"type Users @policy(id: "{}", resource: "users") {{ name: String  age: Int }}"#,
        policy_id
    );
    node.schema_add_with_identity(&users_sdl, &alice.private_key_hex)
        .expect("Users schema should be accepted");

    let books_sdl = format!(
        r#"type Books @policy(id: "{}", resource: "books") {{ name: String }}"#,
        policy_id
    );
    node.schema_add_with_identity(&books_sdl, &alice.private_key_hex)
        .expect("Books schema should also be accepted with the same multi-resource policy");

    assert!(type_exists(&node, "Users"));
    assert!(type_exists(&node, "Books"));
}

for_each_runtime!(
    acp_link_collection_multiple_resources_both_used_accept,
    acp_link_collection_multiple_resources_both_used_accept,
    .with_acp_local()
);

// Port of accept_permission_with_omitted_owner_authority_test.go (update case)
async fn acp_link_collection_owner_bad_update_accept(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice identity");

    let policy_id = add_users_policy(
        &node,
        users_policy_with_owner_bad_on_update(),
        &alice.private_key_hex,
    );

    let sdl = format!(
        r#"type Users @policy(id: "{}", resource: "users") {{ name: String  age: Int }}"#,
        policy_id
    );
    node.schema_add_with_identity(&sdl, &alice.private_key_hex)
        .expect("ownerBad update expression on the DRI must still be accepted");

    assert!(type_exists(&node, "Users"));
}

for_each_runtime!(
    acp_link_collection_owner_bad_update_accept,
    acp_link_collection_owner_bad_update_accept,
    .with_acp_local()
);

// Port of accept_permission_with_omitted_owner_authority_test.go (read case)
async fn acp_link_collection_owner_bad_read_accept(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice identity");

    let policy_id = add_users_policy(
        &node,
        users_policy_with_owner_bad_on_read(),
        &alice.private_key_hex,
    );

    let sdl = format!(
        r#"type Users @policy(id: "{}", resource: "users") {{ name: String  age: Int }}"#,
        policy_id
    );
    node.schema_add_with_identity(&sdl, &alice.private_key_hex)
        .expect("ownerBad read expression on the DRI must still be accepted");

    assert!(type_exists(&node, "Users"));
}

for_each_runtime!(
    acp_link_collection_owner_bad_read_accept,
    acp_link_collection_owner_bad_read_accept,
    .with_acp_local()
);

// Port of accept_permission_with_omitted_owner_authority_test.go (delete case)
async fn acp_link_collection_owner_bad_delete_accept(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice identity");

    let policy_id = add_users_policy(
        &node,
        users_policy_with_owner_bad_on_delete(),
        &alice.private_key_hex,
    );

    let sdl = format!(
        r#"type Users @policy(id: "{}", resource: "users") {{ name: String  age: Int }}"#,
        policy_id
    );
    node.schema_add_with_identity(&sdl, &alice.private_key_hex)
        .expect("ownerBad delete expression on the DRI must still be accepted");

    assert!(type_exists(&node, "Users"));
}

for_each_runtime!(
    acp_link_collection_owner_bad_delete_accept,
    acp_link_collection_owner_bad_delete_accept,
    .with_acp_local()
);

// Port of accept_multi_dris_test.go
async fn acp_link_collection_multi_dris_accept(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice identity");
    let bob = generate_identity(node.binary_path()).expect("bob identity");

    // Two distinct policies (different owners) with the same resource name.
    let policy_id_1 = add_users_policy(&node, users_policy(), &alice.private_key_hex);
    let policy_id_2 = add_users_policy(&node, users_policy(), &bob.private_key_hex);
    assert_ne!(
        policy_id_1, policy_id_2,
        "two distinct policies must have distinct IDs"
    );

    let sdl_old = format!(
        r#"type OldUsers @policy(id: "{}", resource: "users") {{ name: String  age: Int }}"#,
        policy_id_1
    );
    node.schema_add_with_identity(&sdl_old, &alice.private_key_hex)
        .expect("OldUsers schema with policy 1 DRI should be accepted");

    assert!(type_exists(&node, "OldUsers"));

    let sdl_new = format!(
        r#"type NewUsers @policy(id: "{}", resource: "users") {{ name: String  age: Int }}"#,
        policy_id_2
    );
    node.schema_add_with_identity(&sdl_new, &bob.private_key_hex)
        .expect("NewUsers schema with policy 2 DRI should be accepted");

    assert!(type_exists(&node, "NewUsers"));
}

for_each_runtime!(
    acp_link_collection_multi_dris_accept,
    acp_link_collection_multi_dris_accept,
    .with_acp_local()
);

// Port of accept_same_resource_on_diff_collections_test.go
async fn acp_link_collection_same_resource_diff_collections_accept(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice identity");

    let policy_id = add_users_policy(&node, users_policy(), &alice.private_key_hex);

    let sdl_old = format!(
        r#"type OldUsers @policy(id: "{}", resource: "users") {{ name: String  age: Int }}"#,
        policy_id
    );
    node.schema_add_with_identity(&sdl_old, &alice.private_key_hex)
        .expect("OldUsers schema should be accepted");

    let sdl_new = format!(
        r#"type NewUsers @policy(id: "{}", resource: "users") {{ name: String  age: Int }}"#,
        policy_id
    );
    node.schema_add_with_identity(&sdl_new, &alice.private_key_hex)
        .expect("NewUsers schema reusing the same DRI should also be accepted");

    assert!(type_exists(&node, "OldUsers"));
    assert!(type_exists(&node, "NewUsers"));
}

for_each_runtime!(
    acp_link_collection_same_resource_diff_collections_accept,
    acp_link_collection_same_resource_diff_collections_accept,
    .with_acp_local()
);

// =========================================================================
// Reject: invalid `@policy` directive arguments
// =========================================================================

// Port of reject_empty_arg_on_collection_test.go (both cases)
async fn acp_link_collection_empty_args_reject(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice identity");
    let _ = add_users_policy(&node, users_policy(), &alice.private_key_hex);

    // Case 1: `@policy` directive with no arguments.
    let sdl_no_args = r#"type Users @policy { name: String age: Int }"#;
    let err = try_add_schema(&node, sdl_no_args, &alice.private_key_hex)
        .expect("@policy with no args must be rejected");
    let el = err.to_lowercase();
    assert!(
        el.contains("missing policy arguments")
            || el.contains("policy arguments")
            || el.contains("both id and resource")
            || el.contains("id")
            || el.contains("bad_input"),
        "expected missing-policy-args error, got: {}",
        err
    );

    // Case 2: `@policy(id: "", resource: "")` — both args empty strings.
    let sdl_empty_args = r#"type Users @policy(resource: "", id: "") { name: String age: Int }"#;
    let err = try_add_schema(&node, sdl_empty_args, &alice.private_key_hex)
        .expect("@policy with empty-string args must be rejected");
    let el = err.to_lowercase();
    assert!(
        el.contains("must not be empty")
            || el.contains("missing policy arguments")
            || el.contains("policy arguments")
            || el.contains("empty")
            || el.contains("bad_input"),
        "expected empty-policy-args error, got: {}",
        err
    );

    assert!(
        !type_exists(&node, "Users"),
        "Users type must not exist after both rejections"
    );
}

for_each_runtime!(
    acp_link_collection_empty_args_reject,
    acp_link_collection_empty_args_reject,
    .with_acp_local()
);

// Port of reject_missing_id_arg_on_collection_test.go
async fn acp_link_collection_missing_id_arg_reject(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice identity");
    let _ = add_users_policy(&node, users_policy(), &alice.private_key_hex);

    let sdl = r#"type Users @policy(resource: "users") { name: String age: Int }"#;
    let err = try_add_schema(&node, sdl, &alice.private_key_hex)
        .expect("@policy without id must be rejected");
    let el = err.to_lowercase();
    assert!(
        el.contains("policyid must not be empty")
            || el.contains("policy id")
            || el.contains("id must not be empty")
            || el.contains("missing policy arguments")
            || el.contains("id")
            || el.contains("bad_input"),
        "expected missing-id error, got: {}",
        err
    );
    assert!(!type_exists(&node, "Users"));
}

for_each_runtime!(
    acp_link_collection_missing_id_arg_reject,
    acp_link_collection_missing_id_arg_reject,
    .with_acp_local()
);

// Port of reject_missing_resource_arg_on_collection_test.go
async fn acp_link_collection_missing_resource_arg_reject(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice identity");
    let policy_id = add_users_policy(&node, users_policy(), &alice.private_key_hex);

    let sdl = format!(
        r#"type Users @policy(id: "{}") {{ name: String age: Int }}"#,
        policy_id
    );
    let err = try_add_schema(&node, &sdl, &alice.private_key_hex)
        .expect("@policy without resource must be rejected");
    let el = err.to_lowercase();
    assert!(
        el.contains("resource name must not be empty")
            || el.contains("resource")
            || el.contains("missing policy arguments")
            || el.contains("bad_input"),
        "expected missing-resource error, got: {}",
        err
    );
    assert!(!type_exists(&node, "Users"));
}

for_each_runtime!(
    acp_link_collection_missing_resource_arg_reject,
    acp_link_collection_missing_resource_arg_reject,
    .with_acp_local()
);

// Port of reject_invalid_arg_type_on_collection_test.go (both cases)
async fn acp_link_collection_invalid_arg_type_reject(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice identity");
    let policy_id = add_users_policy(&node, users_policy(), &alice.private_key_hex);

    // Case 1: id is a numeric literal (not a String).
    let sdl_bad_id = r#"type Users @policy(id: 123, resource: "users") { name: String age: Int }"#;
    let err = try_add_schema(&node, sdl_bad_id, &alice.private_key_hex)
        .expect("@policy with numeric id must be rejected");
    let el = err.to_lowercase();
    assert!(
        el.contains("invalid value")
            || el.contains("type")
            || el.contains("argument")
            || el.contains("expected")
            || el.contains("string")
            || el.contains("bad_input"),
        "expected invalid-id-type error, got: {}",
        err
    );

    // Case 2: resource is a numeric literal.
    let sdl_bad_resource = format!(
        r#"type Users @policy(id: "{}", resource: 123) {{ name: String age: Int }}"#,
        policy_id
    );
    let err = try_add_schema(&node, &sdl_bad_resource, &alice.private_key_hex)
        .expect("@policy with numeric resource must be rejected");
    let el = err.to_lowercase();
    assert!(
        el.contains("invalid value")
            || el.contains("type")
            || el.contains("argument")
            || el.contains("expected")
            || el.contains("string")
            || el.contains("bad_input"),
        "expected invalid-resource-type error, got: {}",
        err
    );

    assert!(!type_exists(&node, "Users"));
}

for_each_runtime!(
    acp_link_collection_invalid_arg_type_reject,
    acp_link_collection_invalid_arg_type_reject,
    .with_acp_local()
);

// =========================================================================
// Reject: DRI not found
// =========================================================================

// Port of reject_missing_dri_test.go (both cases).
//
// Before #746 was fixed Rust silently accepted a schema with a
// nonexistent policy id. After the fix, Rust rejects with the same
// Go-compatible error string as Go does: "policyID specified does not
// exist with acp".
async fn acp_link_collection_nonexistent_policy_reject(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice identity");

    let nonexistent_policy_id = "1239a04400966b311339f62db50044b1bde70cece2ce9897d69c1bafa5cfab81";

    // Case 1: no policy has been added at all.
    let sdl = format!(
        r#"type UsersA @policy(id: "{}", resource: "users") {{ name: String age: Int }}"#,
        nonexistent_policy_id
    );
    let err = try_add_schema(&node, &sdl, &alice.private_key_hex)
        .expect("schema with nonexistent policy id must be rejected");
    assert!(
        err.to_lowercase().contains("does not exist with acp"),
        "expected Go-compatible nonexistent-policy error, got: {}",
        err
    );
    assert!(!type_exists(&node, "UsersA"));

    // Case 2: a different policy exists but the referenced one does not.
    let _ = add_users_policy(&node, users_policy(), &alice.private_key_hex);
    let sdl_b = format!(
        r#"type UsersB @policy(id: "{}", resource: "users") {{ name: String age: Int }}"#,
        nonexistent_policy_id
    );
    let err = try_add_schema(&node, &sdl_b, &alice.private_key_hex).expect(
        "schema with nonexistent policy id must still be rejected (unrelated policy present)",
    );
    assert!(
        err.to_lowercase().contains("does not exist with acp"),
        "expected Go-compatible nonexistent-policy error (with unrelated policy present), got: {}",
        err
    );
    assert!(!type_exists(&node, "UsersB"));
}

for_each_runtime!(
    acp_link_collection_nonexistent_policy_reject,
    acp_link_collection_nonexistent_policy_reject,
    .with_acp_local()
);

// Port of reject_missing_resource_on_dri_test.go.
//
// After #746 fix: Rust rejects with "resource does not exist on the
// specified policy" — Go-compatible error string.
async fn acp_link_collection_nonexistent_resource_reject(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice identity");
    let policy_id = add_users_policy(&node, users_policy(), &alice.private_key_hex);

    let sdl = format!(
        r#"type Users @policy(id: "{}", resource: "doesNotExist") {{ name: String age: Int }}"#,
        policy_id
    );
    let err = try_add_schema(&node, &sdl, &alice.private_key_hex)
        .expect("schema with nonexistent resource must be rejected");
    assert!(
        err.to_lowercase()
            .contains("resource does not exist on the specified policy"),
        "expected Go-compatible nonexistent-resource error, got: {}",
        err
    );
    assert!(!type_exists(&node, "Users"));
}

for_each_runtime!(
    acp_link_collection_nonexistent_resource_reject,
    acp_link_collection_nonexistent_resource_reject,
    .with_acp_local()
);

// =========================================================================
// Reject: DPI rule violations (resource missing required perm)
// =========================================================================

// Port of reject_invalid_owner_read_perm_on_dri_test.go.
//
// After #746 fix: Rust rejects with "resource is missing required
// permission on policy. ... Permission: read". The DPI rule requires
// documents to declare read/update/delete on the linked resource.
async fn acp_link_collection_missing_read_perm_reject(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice identity");

    let policy_result = node.acp_policy_add(users_policy_missing_read(), &alice.private_key_hex);
    // The policy itself doesn't enforce DPI rules at add time — the
    // check happens at schema-link time. If the store pre-validates the
    // policy and rejects here, also accept that (Go parity).
    let policy_id = match policy_result {
        Ok(v) => extract_policy_id(&v).expect("policy id"),
        Err(e) => {
            let el = format!("{:#}", e).to_lowercase();
            assert!(
                el.contains("read") || el.contains("permission") || el.contains("required"),
                "policy-add-time DPI rejection must mention read/permission, got: {}",
                e
            );
            return;
        }
    };
    let sdl = format!(
        r#"type Users @policy(id: "{}", resource: "users") {{ name: String age: Int }}"#,
        policy_id
    );
    let err = try_add_schema(&node, &sdl, &alice.private_key_hex)
        .expect("schema with missing-read DRI must be rejected");
    let el = err.to_lowercase();
    assert!(
        el.contains("missing required permission") && el.contains("read"),
        "expected Go-compatible missing-read-perm error, got: {}",
        err
    );
    assert!(!type_exists(&node, "Users"));
}

for_each_runtime!(
    acp_link_collection_missing_read_perm_reject,
    acp_link_collection_missing_read_perm_reject,
    .with_acp_local()
);

// Port of reject_invalid_owner_update_perm_on_dri_test.go.
// After #746 fix: Rust rejects with the Go-compatible error string.
async fn acp_link_collection_missing_update_perm_reject(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice identity");

    let policy_result = node.acp_policy_add(users_policy_missing_update(), &alice.private_key_hex);
    let policy_id = match policy_result {
        Ok(v) => extract_policy_id(&v).expect("policy id"),
        Err(e) => {
            let el = format!("{:#}", e).to_lowercase();
            assert!(
                el.contains("update") || el.contains("permission") || el.contains("required"),
                "policy-add-time DPI rejection must mention update/permission, got: {}",
                e
            );
            return;
        }
    };
    let sdl = format!(
        r#"type Users @policy(id: "{}", resource: "users") {{ name: String age: Int }}"#,
        policy_id
    );
    let err = try_add_schema(&node, &sdl, &alice.private_key_hex)
        .expect("schema with missing-update DRI must be rejected");
    let el = err.to_lowercase();
    assert!(
        el.contains("missing required permission") && el.contains("update"),
        "expected Go-compatible missing-update-perm error, got: {}",
        err
    );
    assert!(!type_exists(&node, "Users"));
}

for_each_runtime!(
    acp_link_collection_missing_update_perm_reject,
    acp_link_collection_missing_update_perm_reject,
    .with_acp_local()
);

// Port of reject_invalid_owner_delete_perm_on_dri_test.go.
// After #746 fix: Rust rejects with the Go-compatible error string.
async fn acp_link_collection_missing_delete_perm_reject(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice identity");

    let policy_result = node.acp_policy_add(users_policy_missing_delete(), &alice.private_key_hex);
    let policy_id = match policy_result {
        Ok(v) => extract_policy_id(&v).expect("policy id"),
        Err(e) => {
            let el = format!("{:#}", e).to_lowercase();
            assert!(
                el.contains("delete") || el.contains("permission") || el.contains("required"),
                "policy-add-time DPI rejection must mention delete/permission, got: {}",
                e
            );
            return;
        }
    };
    let sdl = format!(
        r#"type Users @policy(id: "{}", resource: "users") {{ name: String age: Int }}"#,
        policy_id
    );
    let err = try_add_schema(&node, &sdl, &alice.private_key_hex)
        .expect("schema with missing-delete DRI must be rejected");
    let el = err.to_lowercase();
    assert!(
        el.contains("missing required permission") && el.contains("delete"),
        "expected Go-compatible missing-delete-perm error, got: {}",
        err
    );
    assert!(!type_exists(&node, "Users"));
}

for_each_runtime!(
    acp_link_collection_missing_delete_perm_reject,
    acp_link_collection_missing_delete_perm_reject,
    .with_acp_local()
);
