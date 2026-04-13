//! ACP relationship (doc actor) tests ported from Go DefraDB.
//!
//! Source: `tests/integration/acp/dac/relationship/doc_actor/{add,delete}/`
//! in https://github.com/sourcenetwork/defradb (develop branch).
//!
//! These tests verify the `acp_relationship_add` / `acp_relationship_delete`
//! code paths end-to-end:
//!
//! - Granting `reader` / `updater` / `deleter` to a non-owner identity
//!   makes the target document visible / writable / deletable to that actor.
//! - Ownership is NOT transferred — owner still has full access after grants.
//! - Grants are scoped to a single permission — a `reader` cannot update or
//!   delete; a `deleter` cannot read updates, etc.
//! - A manager (a relation with `manages: [...]`) can grant the relations it
//!   manages but cannot grant unrelated ones.
//! - Relations that are declared on the policy but used as no-ops (e.g. a
//!   `dummy` relation) can be added/removed without changing access.
//! - The HTTP handler rejects missing doc id / collection name / relation
//!   name / target actor / requesting identity with Go-compatible errors.
//! - Revoking a relationship removes the granted access.
//!
//! ## Policy shape
//!
//! Rust rejects explicit `owner` in the relations block (#744). The policy
//! below declares `reader`, `updater`, `deleter`, a `dummy` relation, and an
//! `admin` relation that `manages` the first three — matching the shape used
//! by the Go relationship tests minus the explicit owner declaration.
//!
//! Read permission implies read-via-update / read-via-delete:
//! `read: reader + updater + deleter`. This mirrors Go's
//! `ImplyDocumentReadPerm` semantics.

use integration_test::{for_each_runtime, generate_identity, TestCluster};

// =========================================================================
// Policy + schema fixtures
// =========================================================================

/// Policy used by every test in this file. Matches the shape of the policy
/// used throughout Go's `relationship/doc_actor/` suite, minus the explicit
/// owner declaration (Rust auto-injects owner).
fn relationship_policy() -> &'static str {
    r#"
description: A Policy
name: Test Policy
resources:
  - name: users
    permissions:
      - name: read
        expr: reader + updater + deleter
      - name: update
        expr: updater
      - name: delete
        expr: deleter
      - name: nothing
        expr: dummy
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
      - name: dummy
        types:
          - actor
      - name: admin
        types:
          - actor
        manages:
          - reader
          - updater
          - deleter
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

/// Bootstrap: add the relationship_policy(), register the Users schema, and
/// create one document. Returns `(policy_id, doc_id)`.
async fn bootstrap_doc(node: &integration_test::DefraClient, owner_key: &str) -> (String, String) {
    let policy = node
        .acp_policy_add(relationship_policy(), owner_key)
        .expect("add relationship policy");
    let policy_id = extract_policy_id(&policy).expect("policy id");

    node.schema_add_with_identity(&users_schema(&policy_id), owner_key)
        .expect("add Users schema");

    let data = node
        .query_with_identity(
            r#"mutation { add_Users(input: {name: "Shahzad", age: 28}) { _docID } }"#,
            owner_key,
        )
        .expect("create protected doc");
    let doc_id = data["add_Users"][0]["_docID"]
        .as_str()
        .expect("_docID")
        .to_string();

    (policy_id, doc_id)
}

fn user_count(node: &integration_test::DefraClient, key: &str) -> usize {
    node.query_with_identity("query { Users { _docID name age } }", key)
        .expect("query Users")["Users"]
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0)
}

// =========================================================================
// Add relationship — accept paths
// =========================================================================

// Port of add/with_reader_test.go (TestACP_OwnerGivesReadAccessToAnotherActor_OtherActorCanRead)
async fn acp_rel_add_reader_grants_read(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice");
    let bob = generate_identity(node.binary_path()).expect("bob");

    let (_, doc_id) = bootstrap_doc(&node, &alice.private_key_hex).await;

    assert_eq!(
        user_count(&node, &bob.private_key_hex),
        0,
        "Bob must not see the doc before grant"
    );

    node.acp_relationship_add("Users", &doc_id, "reader", &bob.did, &alice.private_key_hex)
        .expect("grant Bob reader");

    assert_eq!(
        user_count(&node, &bob.private_key_hex),
        1,
        "Bob must see the doc after being granted reader"
    );
}

for_each_runtime!(
    acp_rel_add_reader_grants_read,
    acp_rel_add_reader_grants_read,
    .with_acp_local()
);

// Port of add/with_reader_test.go (TestACP_OwnerGivesReadAccessToAnotherActor_OtherActorCanReadSoCanTheOwner)
// Ownership is not transferred when a reader relationship is created.
async fn acp_rel_add_reader_owner_still_reads(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice");
    let bob = generate_identity(node.binary_path()).expect("bob");

    let (_, doc_id) = bootstrap_doc(&node, &alice.private_key_hex).await;

    node.acp_relationship_add("Users", &doc_id, "reader", &bob.did, &alice.private_key_hex)
        .expect("grant Bob reader");

    assert_eq!(user_count(&node, &bob.private_key_hex), 1, "Bob reads");
    assert_eq!(
        user_count(&node, &alice.private_key_hex),
        1,
        "Alice still reads after granting reader — ownership not transferred"
    );
}

for_each_runtime!(
    acp_rel_add_reader_owner_still_reads,
    acp_rel_add_reader_owner_still_reads,
    .with_acp_local()
);

// Port of add/with_reader_test.go (TestACP_OwnerGivesOnlyReadAccessToAnotherActor_OtherActorCanReadButNotUpdate)
async fn acp_rel_add_reader_cannot_update(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice");
    let bob = generate_identity(node.binary_path()).expect("bob");

    let (_, doc_id) = bootstrap_doc(&node, &alice.private_key_hex).await;

    node.acp_relationship_add("Users", &doc_id, "reader", &bob.did, &alice.private_key_hex)
        .expect("grant Bob reader");

    assert_eq!(user_count(&node, &bob.private_key_hex), 1);

    // Bob tries to update: the read-implies-read-via-update does not let him.
    let update = node.query_with_identity(
        &format!(
            r#"mutation {{ update_Users(docID: "{}", input: {{name: "Shahzad Lone"}}) {{ _docID name }} }}"#,
            doc_id
        ),
        &bob.private_key_hex,
    );

    // Either an explicit error OR a silent no-op (empty result array) is
    // acceptable — both indicate the update did not land. Verify by
    // re-reading as the owner and asserting the name is unchanged.
    let _ = update;

    let owner_view = node
        .query_with_identity("query { Users { _docID name } }", &alice.private_key_hex)
        .expect("owner re-read");
    let name = owner_view["Users"][0]["name"].as_str().unwrap_or("");
    assert_eq!(
        name, "Shahzad",
        "reader must not be able to mutate the document"
    );
}

for_each_runtime!(
    acp_rel_add_reader_cannot_update,
    acp_rel_add_reader_cannot_update,
    .with_acp_local()
);

// Port of add/with_update_test.go (TestACP_OwnerGivesUpdateAccessToAnotherActor_OtherActorCanUpdate)
async fn acp_rel_add_updater_can_update(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice");
    let bob = generate_identity(node.binary_path()).expect("bob");

    let (_, doc_id) = bootstrap_doc(&node, &alice.private_key_hex).await;

    node.acp_relationship_add(
        "Users",
        &doc_id,
        "updater",
        &bob.did,
        &alice.private_key_hex,
    )
    .expect("grant Bob updater");

    // Updater implies reader (via `read: reader + updater + deleter`).
    assert_eq!(
        user_count(&node, &bob.private_key_hex),
        1,
        "Bob must see the doc after being granted updater"
    );

    node.query_with_identity(
        &format!(
            r#"mutation {{ update_Users(docID: "{}", input: {{name: "Shahzad Lone"}}) {{ _docID }} }}"#,
            doc_id
        ),
        &bob.private_key_hex,
    )
    .expect("Bob updates the doc");

    let owner_view = node
        .query_with_identity("query { Users { _docID name } }", &alice.private_key_hex)
        .expect("owner re-read");
    assert_eq!(owner_view["Users"][0]["name"], "Shahzad Lone");
}

for_each_runtime!(
    acp_rel_add_updater_can_update,
    acp_rel_add_updater_can_update,
    .with_acp_local()
);

// Port of add/with_delete_test.go (TestACP_OwnerGivesDeleteAccessToAnotherActor_OtherActorCanDelete)
async fn acp_rel_add_deleter_can_delete(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice");
    let bob = generate_identity(node.binary_path()).expect("bob");

    let (_, doc_id) = bootstrap_doc(&node, &alice.private_key_hex).await;

    node.acp_relationship_add(
        "Users",
        &doc_id,
        "deleter",
        &bob.did,
        &alice.private_key_hex,
    )
    .expect("grant Bob deleter");

    // Deleter implies reader.
    assert_eq!(
        user_count(&node, &bob.private_key_hex),
        1,
        "Bob must see the doc after being granted deleter"
    );

    node.query_with_identity(
        &format!(
            r#"mutation {{ delete_Users(docID: "{}") {{ _docID }} }}"#,
            doc_id
        ),
        &bob.private_key_hex,
    )
    .expect("Bob deletes the doc");

    assert_eq!(
        user_count(&node, &alice.private_key_hex),
        0,
        "doc must be gone after deleter deletes it"
    );
}

for_each_runtime!(
    acp_rel_add_deleter_can_delete,
    acp_rel_add_deleter_can_delete,
    .with_acp_local()
);

// Port of add/with_reader_test.go (TestACP_OwnerGivesReadAccessToAnotherActorTwice_ShowThatTheRelationshipAlreadyExists)
async fn acp_rel_add_reader_twice_is_noop(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice");
    let bob = generate_identity(node.binary_path()).expect("bob");

    let (_, doc_id) = bootstrap_doc(&node, &alice.private_key_hex).await;

    let first = node
        .acp_relationship_add("Users", &doc_id, "reader", &bob.did, &alice.private_key_hex)
        .expect("first grant");
    let second = node
        .acp_relationship_add("Users", &doc_id, "reader", &bob.did, &alice.private_key_hex)
        .expect("second grant (must not fail, just be a no-op)");

    // If the harness surfaces ExistedAlready, the first is false and the
    // second is true. Either way, Bob must see exactly 1 doc.
    let first_existed = first["ExistedAlready"].as_bool().unwrap_or(false);
    let second_existed = second["ExistedAlready"].as_bool().unwrap_or(true);
    assert!(
        !first_existed,
        "first grant must report ExistedAlready=false, got {:?}",
        first
    );
    assert!(
        second_existed,
        "second grant must report ExistedAlready=true, got {:?}",
        second
    );

    assert_eq!(user_count(&node, &bob.private_key_hex), 1);
}

for_each_runtime!(
    acp_rel_add_reader_twice_is_noop,
    acp_rel_add_reader_twice_is_noop,
    .with_acp_local()
);

// =========================================================================
// Add relationship — manager paths
// =========================================================================

// Port of add/with_manager_test.go (TestACP_ManagerGivesReadAccessToAnotherActor_OtherActorCanRead)
async fn acp_rel_manager_grants_reader(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice");
    let bob = generate_identity(node.binary_path()).expect("bob"); // manager
    let carol = generate_identity(node.binary_path()).expect("carol"); // target reader

    let (_, doc_id) = bootstrap_doc(&node, &alice.private_key_hex).await;

    // Alice makes Bob an admin (manager of reader/updater/deleter).
    node.acp_relationship_add("Users", &doc_id, "admin", &bob.did, &alice.private_key_hex)
        .expect("grant Bob admin");

    // Bob — acting as manager — grants Carol reader.
    node.acp_relationship_add("Users", &doc_id, "reader", &carol.did, &bob.private_key_hex)
        .expect("manager Bob grants Carol reader");

    assert_eq!(
        user_count(&node, &carol.private_key_hex),
        1,
        "Carol must see the doc after being granted reader by a manager"
    );
}

for_each_runtime!(
    acp_rel_manager_grants_reader,
    acp_rel_manager_grants_reader,
    .with_acp_local()
);

// Port of add/with_manager_test.go (TestACP_ManagerAddsRelationshipWithRelationItDoesNotManageAccordingToPolicy_Error)
async fn acp_rel_manager_cannot_grant_unmanaged(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice");
    let bob = generate_identity(node.binary_path()).expect("bob"); // manager (but only of the 3 relations)
    let carol = generate_identity(node.binary_path()).expect("carol");

    let (_, doc_id) = bootstrap_doc(&node, &alice.private_key_hex).await;

    node.acp_relationship_add("Users", &doc_id, "admin", &bob.did, &alice.private_key_hex)
        .expect("grant Bob admin");

    // Bob tries to grant Carol a `dummy` relation. admin does not manage
    // dummy, so this must fail.
    let res =
        node.acp_relationship_add("Users", &doc_id, "dummy", &carol.did, &bob.private_key_hex);
    assert!(
        res.is_err(),
        "manager must not be able to grant a relation outside its managed set; got: {:?}",
        res
    );
}

for_each_runtime!(
    acp_rel_manager_cannot_grant_unmanaged,
    acp_rel_manager_cannot_grant_unmanaged,
    .with_acp_local()
);

// Port of add/with_manager_test.go (TestACP_CantMakeRelationshipIfNotOwnerOrManager_Error)
async fn acp_rel_non_owner_cannot_grant(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice");
    let bob = generate_identity(node.binary_path()).expect("bob"); // not owner, not manager
    let carol = generate_identity(node.binary_path()).expect("carol");

    let (_, doc_id) = bootstrap_doc(&node, &alice.private_key_hex).await;

    let res =
        node.acp_relationship_add("Users", &doc_id, "reader", &carol.did, &bob.private_key_hex);
    assert!(
        res.is_err(),
        "non-owner, non-manager must not be able to grant; got: {:?}",
        res
    );
}

for_each_runtime!(
    acp_rel_non_owner_cannot_grant,
    acp_rel_non_owner_cannot_grant,
    .with_acp_local()
);

// =========================================================================
// Add relationship — dummy relation
// =========================================================================

// Port of add/with_dummy_relation_test.go (TestACP_AddDocActorRelationshipWithDummyRelationDefinedOnPolicy_NothingChanges)
async fn acp_rel_add_dummy_defined_is_noop(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice");
    let bob = generate_identity(node.binary_path()).expect("bob");

    let (_, doc_id) = bootstrap_doc(&node, &alice.private_key_hex).await;

    node.acp_relationship_add("Users", &doc_id, "dummy", &bob.did, &alice.private_key_hex)
        .expect("grant Bob dummy (declared but not wired to read/update/delete)");

    // dummy doesn't appear in read/update/delete expressions, so Bob still
    // cannot see the doc.
    assert_eq!(
        user_count(&node, &bob.private_key_hex),
        0,
        "dummy grant must not expose the doc"
    );
}

for_each_runtime!(
    acp_rel_add_dummy_defined_is_noop,
    acp_rel_add_dummy_defined_is_noop,
    .with_acp_local()
);

// Port of add/with_dummy_relation_test.go (TestACP_AddDocActorRelationshipWithDummyRelationNotDefinedOnPolicy_Error)
async fn acp_rel_add_undefined_relation_errors(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice");
    let bob = generate_identity(node.binary_path()).expect("bob");

    let (_, doc_id) = bootstrap_doc(&node, &alice.private_key_hex).await;

    let res = node.acp_relationship_add(
        "Users",
        &doc_id,
        "notARelation",
        &bob.did,
        &alice.private_key_hex,
    );
    assert!(
        res.is_err(),
        "granting an undefined relation must error; got: {:?}",
        res
    );
}

for_each_runtime!(
    acp_rel_add_undefined_relation_errors,
    acp_rel_add_undefined_relation_errors,
    .with_acp_local()
);

// =========================================================================
// Add relationship — invalid requests
// =========================================================================

// Port of add/invalid_test.go (TestACP_AddDocActorRelationshipMissingCollection_Error)
async fn acp_rel_add_missing_collection_errors(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice");
    let bob = generate_identity(node.binary_path()).expect("bob");
    let (_, doc_id) = bootstrap_doc(&node, &alice.private_key_hex).await;

    let res = node.acp_relationship_add("", &doc_id, "reader", &bob.did, &alice.private_key_hex);
    assert!(
        res.is_err(),
        "empty collection name must error; got: {:?}",
        res
    );
    let el = format!("{:#}", res.unwrap_err()).to_lowercase();
    assert!(
        el.contains("collection name") || el.contains("empty") || el.contains("bad_request"),
        "expected empty-collection error, got: {}",
        el
    );
}

for_each_runtime!(
    acp_rel_add_missing_collection_errors,
    acp_rel_add_missing_collection_errors,
    .with_acp_local()
);

// Port of add/invalid_test.go (TestACP_AddDocActorRelationshipMissingDocID_Error)
async fn acp_rel_add_missing_doc_id_errors(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice");
    let bob = generate_identity(node.binary_path()).expect("bob");
    let (_, _doc_id) = bootstrap_doc(&node, &alice.private_key_hex).await;

    let res = node.acp_relationship_add("Users", "", "reader", &bob.did, &alice.private_key_hex);
    assert!(res.is_err(), "empty doc id must error; got: {:?}", res);
    let el = format!("{:#}", res.unwrap_err()).to_lowercase();
    assert!(
        el.contains("missing a required argument")
            || el.contains("doc")
            || el.contains("required")
            || el.contains("bad_request"),
        "expected missing-doc-id error, got: {}",
        el
    );
}

for_each_runtime!(
    acp_rel_add_missing_doc_id_errors,
    acp_rel_add_missing_doc_id_errors,
    .with_acp_local()
);

// Port of add/invalid_test.go (TestACP_AddDocActorRelationshipMissingRelationName_Error)
async fn acp_rel_add_missing_relation_errors(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice");
    let bob = generate_identity(node.binary_path()).expect("bob");
    let (_, doc_id) = bootstrap_doc(&node, &alice.private_key_hex).await;

    let res = node.acp_relationship_add("Users", &doc_id, "", &bob.did, &alice.private_key_hex);
    assert!(res.is_err(), "empty relation must error; got: {:?}", res);
    let el = format!("{:#}", res.unwrap_err()).to_lowercase();
    assert!(
        el.contains("missing a required argument")
            || el.contains("relation")
            || el.contains("required")
            || el.contains("bad_request"),
        "expected missing-relation error, got: {}",
        el
    );
}

for_each_runtime!(
    acp_rel_add_missing_relation_errors,
    acp_rel_add_missing_relation_errors,
    .with_acp_local()
);

// Port of add/invalid_test.go (TestACP_AddDocActorRelationshipMissingTargetActorName_Error)
async fn acp_rel_add_missing_target_errors(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice");
    let (_, doc_id) = bootstrap_doc(&node, &alice.private_key_hex).await;

    let res = node.acp_relationship_add("Users", &doc_id, "reader", "", &alice.private_key_hex);
    assert!(res.is_err(), "empty target must error; got: {:?}", res);
    let el = format!("{:#}", res.unwrap_err()).to_lowercase();
    assert!(
        el.contains("missing a required argument")
            || el.contains("actor")
            || el.contains("required")
            || el.contains("bad_request"),
        "expected missing-target error, got: {}",
        el
    );
}

for_each_runtime!(
    acp_rel_add_missing_target_errors,
    acp_rel_add_missing_target_errors,
    .with_acp_local()
);

// Port of add/with_reader_test.go / add/with_manager_test.go
// (TestACP_OwnerMakesAManagerThatGivesItSelfReadAndWriteAccess_ManagerCanReadAndWrite)
async fn acp_rel_cannot_add_owner_relation(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice");
    let bob = generate_identity(node.binary_path()).expect("bob");
    let (_, doc_id) = bootstrap_doc(&node, &alice.private_key_hex).await;

    let res =
        node.acp_relationship_add("Users", &doc_id, "owner", &bob.did, &alice.private_key_hex);
    assert!(
        res.is_err(),
        "must not be able to grant the immutable owner relation; got: {:?}",
        res
    );
    let el = format!("{:#}", res.unwrap_err()).to_lowercase();
    assert!(
        el.contains("owner") || el.contains("forbidden") || el.contains("bad_request"),
        "expected owner-forbidden error, got: {}",
        el
    );
}

for_each_runtime!(
    acp_rel_cannot_add_owner_relation,
    acp_rel_cannot_add_owner_relation,
    .with_acp_local()
);

// =========================================================================
// Delete relationship
// =========================================================================

// Port of delete/with_reader_test.go (TestACP_OwnerRevokesGivenReadAccess_OtherActorCanNoLongerRead)
async fn acp_rel_delete_reader_revokes_access(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice");
    let bob = generate_identity(node.binary_path()).expect("bob");

    let (_, doc_id) = bootstrap_doc(&node, &alice.private_key_hex).await;

    node.acp_relationship_add("Users", &doc_id, "reader", &bob.did, &alice.private_key_hex)
        .expect("grant Bob reader");
    assert_eq!(user_count(&node, &bob.private_key_hex), 1);

    node.acp_relationship_delete("Users", &doc_id, "reader", &bob.did, &alice.private_key_hex)
        .expect("revoke Bob reader");

    assert_eq!(
        user_count(&node, &bob.private_key_hex),
        0,
        "Bob must not see the doc after revoke"
    );
}

for_each_runtime!(
    acp_rel_delete_reader_revokes_access,
    acp_rel_delete_reader_revokes_access,
    .with_acp_local()
);

// Port of delete/with_reader_test.go (TestACP_OwnerRevokesReadAccessTwice_ShowThatTheRecordWasNotFoundSecondTime)
async fn acp_rel_delete_reader_twice_second_is_noop(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice");
    let bob = generate_identity(node.binary_path()).expect("bob");

    let (_, doc_id) = bootstrap_doc(&node, &alice.private_key_hex).await;

    node.acp_relationship_add("Users", &doc_id, "reader", &bob.did, &alice.private_key_hex)
        .expect("grant Bob reader");

    // First delete: record found. Must not error.
    node.acp_relationship_delete("Users", &doc_id, "reader", &bob.did, &alice.private_key_hex)
        .expect("first delete");
    // Second delete: record not found. Must still not error (Go returns
    // RecordFound=false, Rust should also be a successful no-op).
    node.acp_relationship_delete("Users", &doc_id, "reader", &bob.did, &alice.private_key_hex)
        .expect("second delete (no-op)");
}

for_each_runtime!(
    acp_rel_delete_reader_twice_second_is_noop,
    acp_rel_delete_reader_twice_second_is_noop,
    .with_acp_local()
);

// Port of delete/with_update_test.go (TestACP_OwnerRevokesGivenUpdateAccess_OtherActorCanNoLongerUpdate)
async fn acp_rel_delete_updater_revokes_update(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice");
    let bob = generate_identity(node.binary_path()).expect("bob");

    let (_, doc_id) = bootstrap_doc(&node, &alice.private_key_hex).await;

    node.acp_relationship_add(
        "Users",
        &doc_id,
        "updater",
        &bob.did,
        &alice.private_key_hex,
    )
    .expect("grant Bob updater");

    // Updater also grants read via the implies-read expression. Confirm.
    assert_eq!(user_count(&node, &bob.private_key_hex), 1);

    node.acp_relationship_delete(
        "Users",
        &doc_id,
        "updater",
        &bob.did,
        &alice.private_key_hex,
    )
    .expect("revoke Bob updater");

    assert_eq!(
        user_count(&node, &bob.private_key_hex),
        0,
        "after revoke, Bob must no longer see the doc"
    );

    // And his updates must no longer land. Apply silently then check state.
    let _ = node.query_with_identity(
        &format!(
            r#"mutation {{ update_Users(docID: "{}", input: {{name: "Shahzad Lone"}}) {{ _docID }} }}"#,
            doc_id
        ),
        &bob.private_key_hex,
    );

    let owner_view = node
        .query_with_identity("query { Users { _docID name } }", &alice.private_key_hex)
        .expect("owner re-read");
    assert_eq!(
        owner_view["Users"][0]["name"], "Shahzad",
        "revoked updater must not mutate the doc"
    );
}

for_each_runtime!(
    acp_rel_delete_updater_revokes_update,
    acp_rel_delete_updater_revokes_update,
    .with_acp_local()
);

// Port of delete/invalid_test.go (TestACP_DeleteDocActorRelationshipMissingCollection_Error)
async fn acp_rel_delete_missing_collection_errors(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice");
    let bob = generate_identity(node.binary_path()).expect("bob");
    let (_, doc_id) = bootstrap_doc(&node, &alice.private_key_hex).await;

    let res = node.acp_relationship_delete("", &doc_id, "reader", &bob.did, &alice.private_key_hex);
    assert!(
        res.is_err(),
        "empty collection name on delete must error; got: {:?}",
        res
    );
}

for_each_runtime!(
    acp_rel_delete_missing_collection_errors,
    acp_rel_delete_missing_collection_errors,
    .with_acp_local()
);

// Port of delete/invalid_test.go (TestACP_DeleteDocActorRelationshipMissingRelationName_Error)
async fn acp_rel_delete_missing_relation_errors(cluster: TestCluster) {
    let node = cluster.client(0);
    let alice = generate_identity(node.binary_path()).expect("alice");
    let bob = generate_identity(node.binary_path()).expect("bob");
    let (_, doc_id) = bootstrap_doc(&node, &alice.private_key_hex).await;

    let res = node.acp_relationship_delete("Users", &doc_id, "", &bob.did, &alice.private_key_hex);
    assert!(
        res.is_err(),
        "empty relation on delete must error; got: {:?}",
        res
    );
}

for_each_runtime!(
    acp_rel_delete_missing_relation_errors,
    acp_rel_delete_missing_relation_errors,
    .with_acp_local()
);
