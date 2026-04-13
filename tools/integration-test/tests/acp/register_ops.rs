//! ACP document registration lifecycle tests ported from Go DefraDB.
//!
//! Source:
//! - `tests/integration/acp/dac/register_and_read_test.go`
//! - `tests/integration/acp/dac/register_and_update_test.go`
//! - `tests/integration/acp/dac/register_and_delete_test.go`
//!
//! These tests cover the core DAC registration behavior:
//! - documents created without identity remain public
//! - documents created with identity are ACP-registered to that actor
//! - only the registering identity can read/update/delete the protected doc

use integration_test::{for_each_runtime, generate_identity, TestCluster};

fn register_ops_policy() -> &'static str {
    r#"
description: a test policy which marks a collection in a database as a resource
name: test
resources:
  - name: users
    permissions:
      - name: read
        expr: writer + reader
      - name: update
        expr: writer
      - name: delete
        expr: writer
    relations:
      - name: writer
        types:
          - actor
      - name: reader
        types:
          - actor
"#
}

fn users_schema(policy_id: &str) -> String {
    format!(
        r#"type Users @policy(id: "{}", resource: "users") {{ name: String age: Int }}"#,
        policy_id
    )
}

fn extract_policy_id(value: &serde_json::Value) -> Option<String> {
    value["PolicyID"]
        .as_str()
        .or_else(|| value["policyID"].as_str())
        .map(|s| s.to_string())
}

async fn setup_users_collection(node: &integration_test::DefraClient, owner_key: &str) -> String {
    let policy = node
        .acp_policy_add(register_ops_policy(), owner_key)
        .expect("add register-ops policy");
    let policy_id = extract_policy_id(&policy).expect("policy id");

    node.schema_add_with_identity(&users_schema(&policy_id), owner_key)
        .expect("add Users schema");

    policy_id
}

fn create_user(
    node: &integration_test::DefraClient,
    key: Option<&str>,
    name: &str,
    age: i64,
) -> String {
    let mutation = format!(
        r#"mutation {{ add_Users(input: {{name: "{}", age: {}}}) {{ _docID name age }} }}"#,
        name, age
    );

    let value = match key {
        Some(key) => node
            .query_with_identity(&mutation, key)
            .expect("create user with identity"),
        None => node.query(&mutation).expect("create user anonymously"),
    };

    value["add_Users"][0]["_docID"]
        .as_str()
        .expect("_docID")
        .to_string()
}

fn users_visible(
    node: &integration_test::DefraClient,
    key: Option<&str>,
) -> Vec<serde_json::Value> {
    let query = r#"query { Users { _docID name age } }"#;
    let value = match key {
        Some(key) => node
            .query_with_identity(query, key)
            .expect("query Users with identity"),
        None => node.query(query).expect("query Users anonymously"),
    };

    value["Users"].as_array().cloned().unwrap_or_default()
}

fn assert_single_user(
    node: &integration_test::DefraClient,
    key: Option<&str>,
    expected_name: &str,
    expected_age: i64,
) {
    let users = users_visible(node, key);
    assert_eq!(
        users.len(),
        1,
        "expected exactly one visible Users document"
    );
    assert_eq!(users[0]["name"], expected_name);
    assert_eq!(users[0]["age"], expected_age);
}

fn assert_no_users(node: &integration_test::DefraClient, key: Option<&str>) {
    assert!(
        users_visible(node, key).is_empty(),
        "expected no visible Users documents"
    );
}

fn update_user(node: &integration_test::DefraClient, key: Option<&str>, doc_id: &str, name: &str) {
    let mutation = format!(
        r#"mutation {{ update_Users(docID: "{}", input: {{name: "{}"}}) {{ _docID name age }} }}"#,
        doc_id, name
    );

    let result = match key {
        Some(key) => node.query_with_identity(&mutation, key),
        None => node.query(&mutation),
    };

    match result {
        Err(_) => {}
        Ok(value) => {
            let updated = value["update_Users"]
                .as_array()
                .map(|a| a.len())
                .unwrap_or(0);
            assert!(
                updated <= 1,
                "update_Users should affect at most one document: {:?}",
                value
            );
        }
    }
}

fn delete_user(node: &integration_test::DefraClient, key: Option<&str>, doc_id: &str) {
    let mutation = format!(
        r#"mutation {{ delete_Users(docID: "{}") {{ _docID }} }}"#,
        doc_id
    );

    let result = match key {
        Some(key) => node.query_with_identity(&mutation, key),
        None => node.query(&mutation),
    };

    match result {
        Err(_) => {}
        Ok(value) => {
            let deleted = value["delete_Users"]
                .as_array()
                .map(|a| a.len())
                .unwrap_or(0);
            assert!(
                deleted <= 1,
                "delete_Users should affect at most one document: {:?}",
                value
            );
        }
    }
}

// Port of TestACP_AddWithoutIdentityAndReadWithoutIdentity_CanRead
async fn acp_register_anon_read_anon_can_read(cluster: TestCluster) {
    let node = cluster.client(0);
    let owner = generate_identity(node.binary_path()).expect("owner");
    let _ = setup_users_collection(&node, &owner.private_key_hex).await;

    create_user(&node, None, "Shahzad", 28);
    assert_single_user(&node, None, "Shahzad", 28);
}

for_each_runtime!(
    acp_register_anon_read_anon_can_read,
    acp_register_anon_read_anon_can_read,
    .with_acp_local()
);

// Port of TestACP_AddWithoutIdentityAndReadWithIdentity_CanRead
async fn acp_register_anon_read_owner_can_read(cluster: TestCluster) {
    let node = cluster.client(0);
    let owner = generate_identity(node.binary_path()).expect("owner");
    let _ = setup_users_collection(&node, &owner.private_key_hex).await;

    create_user(&node, None, "Shahzad", 28);
    assert_single_user(&node, Some(&owner.private_key_hex), "Shahzad", 28);
}

for_each_runtime!(
    acp_register_anon_read_owner_can_read,
    acp_register_anon_read_owner_can_read,
    .with_acp_local()
);

// Port of TestACP_AddWithIdentityAndReadWithIdentity_CanRead
async fn acp_register_owner_read_owner_can_read(cluster: TestCluster) {
    let node = cluster.client(0);
    let owner = generate_identity(node.binary_path()).expect("owner");
    let _ = setup_users_collection(&node, &owner.private_key_hex).await;

    create_user(&node, Some(&owner.private_key_hex), "Shahzad", 28);
    assert_single_user(&node, Some(&owner.private_key_hex), "Shahzad", 28);
}

for_each_runtime!(
    acp_register_owner_read_owner_can_read,
    acp_register_owner_read_owner_can_read,
    .with_acp_local()
);

// Port of TestACP_AddWithIdentityAndReadWithoutIdentity_CanNotRead
async fn acp_register_owner_read_anon_cannot_read(cluster: TestCluster) {
    let node = cluster.client(0);
    let owner = generate_identity(node.binary_path()).expect("owner");
    let _ = setup_users_collection(&node, &owner.private_key_hex).await;

    create_user(&node, Some(&owner.private_key_hex), "Shahzad", 28);
    assert_no_users(&node, None);
}

for_each_runtime!(
    acp_register_owner_read_anon_cannot_read,
    acp_register_owner_read_anon_cannot_read,
    .with_acp_local()
);

// Port of TestACP_AddWithIdentityAndReadWithWrongIdentity_CanNotRead
async fn acp_register_owner_read_wrong_identity_cannot_read(cluster: TestCluster) {
    let node = cluster.client(0);
    let owner = generate_identity(node.binary_path()).expect("owner");
    let wrong = generate_identity(node.binary_path()).expect("wrong");
    let _ = setup_users_collection(&node, &owner.private_key_hex).await;

    create_user(&node, Some(&owner.private_key_hex), "Shahzad", 28);
    assert_no_users(&node, Some(&wrong.private_key_hex));
}

for_each_runtime!(
    acp_register_owner_read_wrong_identity_cannot_read,
    acp_register_owner_read_wrong_identity_cannot_read,
    .with_acp_local()
);

// Port of TestACP_AddWithoutIdentityAndUpdateWithoutIdentity_CanUpdate
async fn acp_register_anon_update_anon_can_update(cluster: TestCluster) {
    let node = cluster.client(0);
    let owner = generate_identity(node.binary_path()).expect("owner");
    let _ = setup_users_collection(&node, &owner.private_key_hex).await;

    let doc_id = create_user(&node, None, "Shahzad", 28);
    update_user(&node, None, &doc_id, "Shahzad Lone");

    assert_single_user(&node, None, "Shahzad Lone", 28);
}

for_each_runtime!(
    acp_register_anon_update_anon_can_update,
    acp_register_anon_update_anon_can_update,
    .with_acp_local()
);

// Port of TestACP_AddWithoutIdentityAndUpdateWithIdentity_CanUpdate
async fn acp_register_anon_update_owner_can_update(cluster: TestCluster) {
    let node = cluster.client(0);
    let owner = generate_identity(node.binary_path()).expect("owner");
    let _ = setup_users_collection(&node, &owner.private_key_hex).await;

    let doc_id = create_user(&node, None, "Shahzad", 28);
    update_user(&node, Some(&owner.private_key_hex), &doc_id, "Shahzad Lone");

    assert_single_user(&node, None, "Shahzad Lone", 28);
}

for_each_runtime!(
    acp_register_anon_update_owner_can_update,
    acp_register_anon_update_owner_can_update,
    .with_acp_local()
);

// Port of TestACP_AddWithIdentityAndUpdateWithIdentity_CanUpdate
async fn acp_register_owner_update_owner_can_update(cluster: TestCluster) {
    let node = cluster.client(0);
    let owner = generate_identity(node.binary_path()).expect("owner");
    let _ = setup_users_collection(&node, &owner.private_key_hex).await;

    let doc_id = create_user(&node, Some(&owner.private_key_hex), "Shahzad", 28);
    update_user(&node, Some(&owner.private_key_hex), &doc_id, "Shahzad Lone");

    assert_single_user(&node, Some(&owner.private_key_hex), "Shahzad Lone", 28);
}

for_each_runtime!(
    acp_register_owner_update_owner_can_update,
    acp_register_owner_update_owner_can_update,
    .with_acp_local()
);

// Port of TestACP_AddWithIdentityAndUpdateWithoutIdentity_CanNotUpdate
async fn acp_register_owner_update_anon_cannot_update(cluster: TestCluster) {
    let node = cluster.client(0);
    let owner = generate_identity(node.binary_path()).expect("owner");
    let _ = setup_users_collection(&node, &owner.private_key_hex).await;

    let doc_id = create_user(&node, Some(&owner.private_key_hex), "Shahzad", 28);
    update_user(&node, None, &doc_id, "Shahzad Lone");

    assert_single_user(&node, Some(&owner.private_key_hex), "Shahzad", 28);
}

for_each_runtime!(
    acp_register_owner_update_anon_cannot_update,
    acp_register_owner_update_anon_cannot_update,
    .with_acp_local()
);

// Port of TestACP_AddWithIdentityAndUpdateWithWrongIdentity_CanNotUpdate
async fn acp_register_owner_update_wrong_identity_cannot_update(cluster: TestCluster) {
    let node = cluster.client(0);
    let owner = generate_identity(node.binary_path()).expect("owner");
    let wrong = generate_identity(node.binary_path()).expect("wrong");
    let _ = setup_users_collection(&node, &owner.private_key_hex).await;

    let doc_id = create_user(&node, Some(&owner.private_key_hex), "Shahzad", 28);
    update_user(&node, Some(&wrong.private_key_hex), &doc_id, "Shahzad Lone");

    assert_single_user(&node, Some(&owner.private_key_hex), "Shahzad", 28);
}

for_each_runtime!(
    acp_register_owner_update_wrong_identity_cannot_update,
    acp_register_owner_update_wrong_identity_cannot_update,
    .with_acp_local()
);

// Port of TestACP_AddWithoutIdentityAndDeleteWithoutIdentity_CanDelete
async fn acp_register_anon_delete_anon_can_delete(cluster: TestCluster) {
    let node = cluster.client(0);
    let owner = generate_identity(node.binary_path()).expect("owner");
    let _ = setup_users_collection(&node, &owner.private_key_hex).await;

    let doc_id = create_user(&node, None, "Shahzad", 28);
    delete_user(&node, None, &doc_id);

    assert_no_users(&node, None);
}

for_each_runtime!(
    acp_register_anon_delete_anon_can_delete,
    acp_register_anon_delete_anon_can_delete,
    .with_acp_local()
);

// Port of TestACP_AddWithoutIdentityAndDeleteWithIdentity_CanDelete
async fn acp_register_anon_delete_owner_can_delete(cluster: TestCluster) {
    let node = cluster.client(0);
    let owner = generate_identity(node.binary_path()).expect("owner");
    let _ = setup_users_collection(&node, &owner.private_key_hex).await;

    let doc_id = create_user(&node, None, "Shahzad", 28);
    delete_user(&node, Some(&owner.private_key_hex), &doc_id);

    assert_no_users(&node, None);
}

for_each_runtime!(
    acp_register_anon_delete_owner_can_delete,
    acp_register_anon_delete_owner_can_delete,
    .with_acp_local()
);

// Port of TestACP_AddWithIdentityAndDeleteWithIdentity_CanDelete
async fn acp_register_owner_delete_owner_can_delete(cluster: TestCluster) {
    let node = cluster.client(0);
    let owner = generate_identity(node.binary_path()).expect("owner");
    let _ = setup_users_collection(&node, &owner.private_key_hex).await;

    let doc_id = create_user(&node, Some(&owner.private_key_hex), "Shahzad", 28);
    delete_user(&node, Some(&owner.private_key_hex), &doc_id);

    assert_no_users(&node, Some(&owner.private_key_hex));
}

for_each_runtime!(
    acp_register_owner_delete_owner_can_delete,
    acp_register_owner_delete_owner_can_delete,
    .with_acp_local()
);

// Port of TestACP_AddWithIdentityAndDeleteWithoutIdentity_CanNotDelete
async fn acp_register_owner_delete_anon_cannot_delete(cluster: TestCluster) {
    let node = cluster.client(0);
    let owner = generate_identity(node.binary_path()).expect("owner");
    let _ = setup_users_collection(&node, &owner.private_key_hex).await;

    let doc_id = create_user(&node, Some(&owner.private_key_hex), "Shahzad", 28);
    delete_user(&node, None, &doc_id);

    assert_single_user(&node, Some(&owner.private_key_hex), "Shahzad", 28);
}

for_each_runtime!(
    acp_register_owner_delete_anon_cannot_delete,
    acp_register_owner_delete_anon_cannot_delete,
    .with_acp_local()
);

// Port of TestACP_AddWithIdentityAndDeleteWithWrongIdentity_CanNotDelete
async fn acp_register_owner_delete_wrong_identity_cannot_delete(cluster: TestCluster) {
    let node = cluster.client(0);
    let owner = generate_identity(node.binary_path()).expect("owner");
    let wrong = generate_identity(node.binary_path()).expect("wrong");
    let _ = setup_users_collection(&node, &owner.private_key_hex).await;

    let doc_id = create_user(&node, Some(&owner.private_key_hex), "Shahzad", 28);
    delete_user(&node, Some(&wrong.private_key_hex), &doc_id);

    assert_single_user(&node, Some(&owner.private_key_hex), "Shahzad", 28);
}

for_each_runtime!(
    acp_register_owner_delete_wrong_identity_cannot_delete,
    acp_register_owner_delete_wrong_identity_cannot_delete,
    .with_acp_local()
);
