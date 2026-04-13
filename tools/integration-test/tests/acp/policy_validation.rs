//! ACP policy validation tests ported from Go DefraDB.
//!
//! Source: `tests/integration/acp/dac/add_policy/` in
//! https://github.com/sourcenetwork/defradb (develop branch)
//!
//! These tests verify policy YAML validation rules:
//! - Empty/missing args
//! - Empty/missing resources, relations, permissions
//! - Permission expression validation
//! - DPI (DefraDB Policy Interface) rule enforcement
//! - Multiple resources and multi-policy scenarios
//!
//! ## Policy format note
//!
//! Rust DefraDB requires that `owner` is NOT declared explicitly in the
//! `relations` block (it is auto-injected). Go DefraDB accepts both formats.
//! This divergence is tracked in #744. Until it is resolved, the policies in
//! these tests use the Rust-compatible format (no explicit `owner`).
//!
//! Each test runs against the Rust implementation. Once the Go binary is
//! present and the YAML format divergence is resolved, the same tests can be
//! exercised against Go for cross-implementation parity.

use integration_test::{for_each_runtime, generate_identity, TestCluster};

// =========================================================================
// Helpers
// =========================================================================

/// Build a minimal valid policy in the Rust YAML format.
/// Owner is auto-injected, so we declare reader/updater/deleter only.
fn minimal_valid_policy() -> &'static str {
    r#"
description: minimal valid policy
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

/// Try to add a policy and return the error message (if any).
fn try_add_policy(
    node: &integration_test::DefraClient,
    policy: &str,
    hex_key: &str,
) -> Option<String> {
    match node.acp_policy_add(policy, hex_key) {
        Ok(_) => None,
        Err(e) => Some(format!("{:#}", e)),
    }
}

/// Extract a policy ID from the JSON returned by `acp_policy_add`.
fn extract_policy_id(value: &serde_json::Value) -> Option<String> {
    value["PolicyID"]
        .as_str()
        .or_else(|| value["policyID"].as_str())
        .map(|s| s.to_string())
}

// =========================================================================
// Basic policy add (port of basic_test.go)
// =========================================================================

async fn acp_add_policy_basic_yaml_valid(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice identity");

    let result = node
        .acp_policy_add(minimal_valid_policy(), &alice.private_key_hex)
        .expect("minimal valid policy should add");

    let policy_id = extract_policy_id(&result);
    assert!(policy_id.is_some(), "policy add should return PolicyID");
}

for_each_runtime!(acp_add_policy_basic_yaml_valid, acp_add_policy_basic_yaml_valid, .with_acp_local());

// =========================================================================
// Empty / missing args (port of with_empty_args_test.go)
// =========================================================================

async fn acp_add_policy_empty_data_errors(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice identity");

    let err = try_add_policy(&node, "", &alice.private_key_hex).expect("empty policy must fail");
    assert!(
        err.to_lowercase().contains("policy data") || err.to_lowercase().contains("empty"),
        "expected empty-policy error, got: {}",
        err
    );
}

for_each_runtime!(acp_add_policy_empty_data_errors, acp_add_policy_empty_data_errors, .with_acp_local());

// =========================================================================
// Empty resources (port of with_empty_resource_test.go and with_no_resources_test.go)
// =========================================================================

async fn acp_add_policy_no_resources_accepted(cluster: TestCluster) {
    // Both Rust and Go accept a policy with no `resources:` block as a
    // zero-resources no-op. This test locks in that shared behavior so
    // neither side can regress into rejecting it without a deliberate
    // review. Verified on Go via `go_*` variant of this test with
    // defradb binary commit d5a5a879 (2026-04-10).
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice identity");

    let policy = r#"
name: test
description: a policy with no resources
"#;
    let result = node.acp_policy_add(policy, &alice.private_key_hex);
    assert!(
        result.is_ok(),
        "no-resources policy must be accepted (shared Rust/Go behavior); got: {:?}",
        result.err()
    );
}

for_each_runtime!(acp_add_policy_no_resources_accepted, acp_add_policy_no_resources_accepted, .with_acp_local());

// =========================================================================
// Empty relations (port of with_empty_relations_test.go)
// =========================================================================

async fn acp_add_policy_undeclared_relation_errors(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice identity");

    // Resource with permissions referencing relations but no relations declared.
    let policy = r#"
name: test
description: a policy referencing undeclared relations
resources:
  - name: users
    permissions:
      - name: read
        expr: reader
      - name: update
        expr: updater
      - name: delete
        expr: deleter
"#;
    let err = try_add_policy(&node, policy, &alice.private_key_hex)
        .expect("policy with undeclared relations must fail");
    assert!(
        err.to_lowercase().contains("relation")
            || err.to_lowercase().contains("undeclared")
            || err.to_lowercase().contains("bad_input"),
        "expected undeclared-relation error, got: {}",
        err
    );
}

for_each_runtime!(acp_add_policy_undeclared_relation_errors, acp_add_policy_undeclared_relation_errors, .with_acp_local());

// =========================================================================
// Permission expression validation (port of with_perm_invalid_expr_test.go)
// =========================================================================

async fn acp_add_policy_expr_invalid_symbol_errors(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice identity");

    // `^` is not a valid relation expression operator.
    let policy = r#"
name: test
description: a policy with invalid expression operator
resources:
  - name: users
    permissions:
      - name: read
        expr: reader ^ updater
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
"#;
    let err = try_add_policy(&node, policy, &alice.private_key_hex)
        .expect("invalid expression operator must fail");
    assert!(
        err.to_lowercase().contains("token")
            || err.to_lowercase().contains("invalid")
            || err.to_lowercase().contains("expression")
            || err.to_lowercase().contains("symbol"),
        "expected expression parse error, got: {}",
        err
    );
}

for_each_runtime!(acp_add_policy_expr_invalid_symbol_errors, acp_add_policy_expr_invalid_symbol_errors, .with_acp_local());

async fn acp_add_policy_expr_references_owner_errors(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice identity");

    // DPI rule: permission expressions must NOT reference `owner` directly.
    // Owner is auto-injected; explicit references are rejected.
    let policy = r#"
name: test
description: a policy that references owner in expression
resources:
  - name: users
    permissions:
      - name: read
        expr: reader + owner
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
"#;
    let err = try_add_policy(&node, policy, &alice.private_key_hex)
        .expect("expression referencing owner must fail");
    assert!(
        err.to_lowercase().contains("owner"),
        "expected owner-reference error, got: {}",
        err
    );
}

for_each_runtime!(acp_add_policy_expr_references_owner_errors, acp_add_policy_expr_references_owner_errors, .with_acp_local());

async fn acp_add_policy_expr_undeclared_relation_errors(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice identity");

    // Expression references `manager` but no relation `manager` is declared.
    let policy = r#"
name: test
description: a policy referencing an undeclared relation
resources:
  - name: users
    permissions:
      - name: read
        expr: reader + manager
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
"#;
    let err = try_add_policy(&node, policy, &alice.private_key_hex)
        .expect("undeclared relation must fail");
    assert!(
        err.to_lowercase().contains("undeclared")
            || err.to_lowercase().contains("not found")
            || err.to_lowercase().contains("manager")
            || err.to_lowercase().contains("bad_input"),
        "expected undeclared-relation error, got: {}",
        err
    );
}

for_each_runtime!(acp_add_policy_expr_undeclared_relation_errors, acp_add_policy_expr_undeclared_relation_errors, .with_acp_local());

// =========================================================================
// Multiple resources (port of with_multiple_resources_test.go)
// =========================================================================

async fn acp_add_policy_multiple_resources_succeeds(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice identity");

    let policy = r#"
name: test
description: a policy with multiple resources
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
  - name: posts
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
"#;
    let result = node.acp_policy_add(policy, &alice.private_key_hex);
    assert!(
        result.is_ok(),
        "policy with multiple resources should add: {:?}",
        result.err()
    );
}

for_each_runtime!(acp_add_policy_multiple_resources_succeeds, acp_add_policy_multiple_resources_succeeds, .with_acp_local());

// =========================================================================
// Multiple policies (port of with_multi_policies_test.go)
// =========================================================================

async fn acp_add_policy_multiple_policies_succeed(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice identity");

    let policy1 = r#"
name: policy1
description: first policy
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
"#;
    let policy2 = r#"
name: policy2
description: second policy
resources:
  - name: posts
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
"#;

    let result1 = node
        .acp_policy_add(policy1, &alice.private_key_hex)
        .expect("policy1 should add");
    let result2 = node
        .acp_policy_add(policy2, &alice.private_key_hex)
        .expect("policy2 should add");

    let id1 = extract_policy_id(&result1).expect("policy1 id");
    let id2 = extract_policy_id(&result2).expect("policy2 id");
    assert_ne!(id1, id2, "distinct policies should have distinct IDs");
}

for_each_runtime!(acp_add_policy_multiple_policies_succeed, acp_add_policy_multiple_policies_succeed, .with_acp_local());

// =========================================================================
// Extra perms / extra relations (port of with_extra_perms_test.go and friends)
// =========================================================================

async fn acp_add_policy_extra_relations_allowed(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice identity");

    // Declaring extra relations beyond the four DPI minimum is allowed,
    // as long as the required ones are present and the DPI rules are met.
    let policy = r#"
name: test
description: a policy with extra relations
resources:
  - name: users
    permissions:
      - name: read
        expr: reader + manager
      - name: update
        expr: updater + manager
      - name: delete
        expr: deleter + manager
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
      - name: manager
        types:
          - actor
"#;
    let result = node.acp_policy_add(policy, &alice.private_key_hex);
    assert!(
        result.is_ok(),
        "policy with extra relations should add: {:?}",
        result.err()
    );
}

for_each_runtime!(acp_add_policy_extra_relations_allowed, acp_add_policy_extra_relations_allowed, .with_acp_local());

// =========================================================================
// Explicit owner relation — shared Rust/Go behavior (closes #744)
// =========================================================================

async fn acp_add_policy_explicit_owner_relation_rejected(cluster: TestCluster) {
    // #744 originally claimed Go accepts explicit `owner` declarations
    // while Rust rejects. Verified on defradb commit d5a5a879
    // (2026-04-10): **both backends reject** with the same error
    // ("owner is a reserved relation name"), because both delegate
    // policy transformation to the same `sourcenetwork/acp_core`
    // library. The original audit finding was incorrect — this is a
    // shared constraint enforced by ACPCore's transformer, not a
    // Rust-only behavior.
    //
    // This test locks in the shared rejection so neither side can
    // diverge without a deliberate review.
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice identity");

    let policy = r#"
name: test
description: a policy with explicit owner declaration
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
      - name: owner
        types:
          - actor
      - name: reader
        types:
          - actor
      - name: updater
        types:
          - actor
      - name: deleter
        types:
          - actor
"#;
    let err = try_add_policy(&node, policy, &alice.private_key_hex)
        .expect("explicit owner declaration must be rejected (shared Rust/Go behavior)");
    let el = err.to_lowercase();
    assert!(
        el.contains("reserved relation name") || el.contains("bad_input"),
        "expected reserved-owner error, got: {}",
        err
    );
}

for_each_runtime!(acp_add_policy_explicit_owner_relation_rejected, acp_add_policy_explicit_owner_relation_rejected, .with_acp_local());
