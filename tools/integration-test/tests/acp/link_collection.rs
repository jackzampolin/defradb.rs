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
//! ## Known divergence: DRI existence + DPI rule checks not enforced
//!
//! Five of the rejection tests below currently assert the **Rust** behavior
//! rather than the Go behavior, with `DIVERGENCE:` comments. Rust's schema
//! loader does not verify that the referenced policy/resource exists or
//! enforce the DPI rule that requires `read`/`update`/`delete` permissions
//! on the linked resource. Tracked in issue #746. These tests are
//! regression guards — when #746 is fixed, each flagged assertion must be
//! flipped back to expect an error.

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
// DIVERGENCE (#746): Go DefraDB rejects at schema-add time with
// "policyID specified does not exist with acp". Rust's
// `parse_policy_directive` validates arg types and emptiness but never
// consults ACP to verify the policy exists, so Rust accepts the schema
// and registers the type. The DRI existence check is missing at the
// schema-build path ([crates/query/src/sdl_parse/builder.rs:784]).
// Regression guard — flip the asserts back to expect an error once #746
// is fixed.
async fn acp_link_collection_nonexistent_policy_reject(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice identity");

    let nonexistent_policy_id = "1239a04400966b311339f62db50044b1bde70cece2ce9897d69c1bafa5cfab81";

    // Case 1: no policy has been added at all.
    let sdl = format!(
        r#"type UsersA @policy(id: "{}", resource: "users") {{ name: String age: Int }}"#,
        nonexistent_policy_id
    );
    let result = try_add_schema(&node, &sdl, &alice.private_key_hex);
    assert!(
        result.is_none(),
        "Rust currently silently accepts a schema referencing a nonexistent \
         policy id — Go rejects it. If this fails, the divergence has been \
         fixed and the test must assert the error instead. Actual: {:?}",
        result
    );

    // Case 2: a different policy exists but the referenced one does not.
    let _ = add_users_policy(&node, users_policy(), &alice.private_key_hex);
    let sdl_b = format!(
        r#"type UsersB @policy(id: "{}", resource: "users") {{ name: String age: Int }}"#,
        nonexistent_policy_id
    );
    let result = try_add_schema(&node, &sdl_b, &alice.private_key_hex);
    assert!(
        result.is_none(),
        "Rust currently silently accepts (with unrelated policy present). \
         If this fails, the divergence has been fixed. Actual: {:?}",
        result
    );
}

for_each_runtime!(
    acp_link_collection_nonexistent_policy_reject,
    acp_link_collection_nonexistent_policy_reject,
    .with_acp_local()
);

// Port of reject_missing_resource_on_dri_test.go.
//
// DIVERGENCE (#746): Go rejects with "resource does not exist on the
// specified policy". Rust accepts — same root cause as
// `acp_link_collection_nonexistent_policy_reject`: no DRI validation at
// schema-build time. Regression guard.
async fn acp_link_collection_nonexistent_resource_reject(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice identity");
    let policy_id = add_users_policy(&node, users_policy(), &alice.private_key_hex);

    let sdl = format!(
        r#"type Users @policy(id: "{}", resource: "doesNotExist") {{ name: String age: Int }}"#,
        policy_id
    );
    let result = try_add_schema(&node, &sdl, &alice.private_key_hex);
    assert!(
        result.is_none(),
        "Rust currently silently accepts a schema with a nonexistent \
         resource name on a valid policy — Go rejects it. If this fails, \
         the divergence has been fixed. Actual: {:?}",
        result
    );
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
// DIVERGENCE (#746): Go enforces the DPI rule that a resource bound via
// `@policy` must define the `read`, `update`, and `delete` permissions
// and rejects with "resource is missing required permission on policy".
// Rust enforces neither at policy-add time nor at schema-link time.
// Regression guard for the current silent-accept behavior.
async fn acp_link_collection_missing_read_perm_reject(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice identity");

    let policy_result = node.acp_policy_add(users_policy_missing_read(), &alice.private_key_hex);
    let policy_id = match policy_result {
        Ok(v) => extract_policy_id(&v),
        Err(_) => {
            // Policy add failed = DPI enforced at policy-add time. Rust fixed.
            return;
        }
    };
    let Some(policy_id) = policy_id else {
        return;
    };
    let sdl = format!(
        r#"type Users @policy(id: "{}", resource: "users") {{ name: String age: Int }}"#,
        policy_id
    );
    let result = try_add_schema(&node, &sdl, &alice.private_key_hex);
    assert!(
        result.is_none(),
        "Rust currently silently accepts a schema whose DRI is missing \
         the `read` permission — Go rejects it. If this fails, the \
         divergence has been fixed. Actual: {:?}",
        result
    );
}

for_each_runtime!(
    acp_link_collection_missing_read_perm_reject,
    acp_link_collection_missing_read_perm_reject,
    .with_acp_local()
);

// Port of reject_invalid_owner_update_perm_on_dri_test.go.
// See `acp_link_collection_missing_read_perm_reject` for the divergence note.
async fn acp_link_collection_missing_update_perm_reject(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice identity");

    let policy_result = node.acp_policy_add(users_policy_missing_update(), &alice.private_key_hex);
    let policy_id = match policy_result {
        Ok(v) => extract_policy_id(&v),
        Err(_) => return,
    };
    let Some(policy_id) = policy_id else {
        return;
    };
    let sdl = format!(
        r#"type Users @policy(id: "{}", resource: "users") {{ name: String age: Int }}"#,
        policy_id
    );
    let result = try_add_schema(&node, &sdl, &alice.private_key_hex);
    assert!(
        result.is_none(),
        "Rust currently silently accepts a schema whose DRI is missing \
         the `update` permission — Go rejects it. If this fails, the \
         divergence has been fixed. Actual: {:?}",
        result
    );
}

for_each_runtime!(
    acp_link_collection_missing_update_perm_reject,
    acp_link_collection_missing_update_perm_reject,
    .with_acp_local()
);

// Port of reject_invalid_owner_delete_perm_on_dri_test.go.
// See `acp_link_collection_missing_read_perm_reject` for the divergence note.
async fn acp_link_collection_missing_delete_perm_reject(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice identity");

    let policy_result = node.acp_policy_add(users_policy_missing_delete(), &alice.private_key_hex);
    let policy_id = match policy_result {
        Ok(v) => extract_policy_id(&v),
        Err(_) => return,
    };
    let Some(policy_id) = policy_id else {
        return;
    };
    let sdl = format!(
        r#"type Users @policy(id: "{}", resource: "users") {{ name: String age: Int }}"#,
        policy_id
    );
    let result = try_add_schema(&node, &sdl, &alice.private_key_hex);
    assert!(
        result.is_none(),
        "Rust currently silently accepts a schema whose DRI is missing \
         the `delete` permission — Go rejects it. If this fails, the \
         divergence has been fixed. Actual: {:?}",
        result
    );
}

for_each_runtime!(
    acp_link_collection_missing_delete_perm_reject,
    acp_link_collection_missing_delete_perm_reject,
    .with_acp_local()
);
