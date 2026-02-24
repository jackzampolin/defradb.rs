//! Iroh P2P schema migration tests.
//!
//! Ported from Go: tests/integration/net/simple/replicator/ and
//!                  tests/integration/collection_version/migrations/query/
//!
//! These tests verify that documents replicated between nodes at different
//! schema versions are handled correctly, including migration/transform
//! scenarios.
//!
//! Run with:
//!   cargo test -p integration-test --test p2p_iroh -- schema::schema_migration

use std::time::Duration;

use integration_test::{
    extract_p2p_addr, open_merge_events_sse, wait_for_merge_events, TestCluster,
};
use serde_json::Value;
use serial_test::serial;

const SCHEMA: &str = "type Users { Name: String }";
const MIGRATION_SCHEMA: &str = "type Users { name: String  verified: Boolean }";
const P2P_TIMEOUT: Duration = Duration::from_secs(15);
const MERGE_TIMEOUT: Duration = Duration::from_secs(15);

/// JSON Patch to add an Email field (Kind 11 = String) to the Users collection.
const ADD_EMAIL_PATCH: &str =
    r#"[{"op":"add","path":"/Users/Fields/-","value":{"Name":"Email","Kind":11}}]"#;

/// JSON Patch to add a verified field (Kind 6 = Boolean) to the Users collection.
const ADD_VERIFIED_PATCH: &str =
    r#"[{"op":"add","path":"/Users/Fields/-","value":{"Name":"verified","Kind":6}}]"#;

/// JSON Patch to add an email field (Kind 11 = String) to the Users collection (lowercase).
const ADD_EMAIL_PATCH_LC: &str =
    r#"[{"op":"add","path":"/Users/Fields/-","value":{"Name":"email","Kind":11}}]"#;

/// JSON Patch to add an address field (Kind 11 = String) to the Users collection.
const ADD_ADDRESS_PATCH: &str =
    r#"[{"op":"add","path":"/Users/Fields/-","value":{"Name":"address","Kind":11}}]"#;

/// JSON Patch to add a phone field (Kind 11 = String) to the Users collection.
const ADD_PHONE_PATCH: &str =
    r#"[{"op":"add","path":"/Users/Fields/-","value":{"Name":"phone","Kind":11}}]"#;

/// Set up a 2-node iroh cluster with schema deployed on both, connected with replicator.
async fn setup_migration_cluster(schema: &str) -> (TestCluster, String) {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_iroh_transport()
        .build()
        .await
        .unwrap();

    for i in 0..2 {
        cluster
            .wait_for_log(i, "p2p_listening", P2P_TIMEOUT)
            .await
            .unwrap_or_else(|_| panic!("node{} P2P listener", i));
        cluster
            .client(i)
            .schema_add(schema)
            .unwrap_or_else(|_| panic!("schema node{}", i));
    }

    let addr1 = extract_p2p_addr(&cluster, 1);
    cluster.client(0).p2p_connect(&[&addr1]).expect("connect");

    (cluster, addr1)
}

/// Extract the VersionID from a collection describe response.
fn extract_version_id(client: &integration_test::DefraClient, collection: &str) -> String {
    let desc = client
        .collection_describe_version(collection)
        .expect("describe collection");
    desc["VersionID"]
        .as_str()
        .expect("missing VersionID")
        .to_string()
}

// ===========================================================================
// Phase 2: Schema migration WITHOUT lens (field additions via collection_patch)
// ===========================================================================

/// Port: TestP2POneToOneReplicatorCreateWithNewFieldSyncsDocsToOlderSchemaVersion
///
/// Node 0 patches to add Email, node 1 stays at old version.
/// Create doc with Email on node 0 → node 1 sees only Name.
#[tokio::test]
#[serial]
async fn replicator_create_new_field_older_version() {
    let (cluster, addr1) = setup_migration_cluster(SCHEMA).await;
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    // Patch only node 0: add Email field
    node0.collection_patch(ADD_EMAIL_PATCH).expect("patch node0");

    // Set up replication
    node0.p2p_collection_add(&["Users"]).expect("col add n0");
    node1.p2p_collection_add(&["Users"]).expect("col add n1");

    let (sse, merges) = open_merge_events_sse(cluster.api_url(1)).await;
    node0
        .p2p_replicator_set(&["Users"], &addr1)
        .expect("replicator");

    // Create doc with new field on node 0
    node0
        .query(
            r#"mutation { create_Users(input: {Name: "John", Email: "imnotyourbuddyguy@source.ca"}) { _docID } }"#,
        )
        .expect("create doc");

    // Wait for merge on node 1
    wait_for_merge_events(&merges, 1, MERGE_TIMEOUT).await;
    sse.abort();

    // Node 0: has both fields
    let r0 = node0
        .query("query { Users { Name } }")
        .expect("query node0");
    assert_eq!(r0["Users"][0]["Name"].as_str(), Some("John"));

    // Node 1: only sees Name (no Email in its schema)
    let r1 = node1
        .query("query { Users { Name } }")
        .expect("query node1");
    assert_eq!(r1["Users"][0]["Name"].as_str(), Some("John"));
}

/// Port: TestP2POneToOneReplicatorCreateWithNewFieldSyncsDocsToNewerSchemaVersion
///
/// Node 1 patches to add Email, node 0 stays at old version.
/// Create doc without Email on node 0 → node 1 sees only Name.
#[tokio::test]
#[serial]
async fn replicator_create_new_field_newer_version() {
    let (cluster, addr1) = setup_migration_cluster(SCHEMA).await;
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    // Patch only node 1: add Email field
    node1.collection_patch(ADD_EMAIL_PATCH).expect("patch node1");

    // Set up replication
    node0.p2p_collection_add(&["Users"]).expect("col add n0");
    node1.p2p_collection_add(&["Users"]).expect("col add n1");

    let (sse, merges) = open_merge_events_sse(cluster.api_url(1)).await;
    node0
        .p2p_replicator_set(&["Users"], &addr1)
        .expect("replicator");

    // Create doc on node 0 (no Email field in this schema version)
    node0
        .query(r#"mutation { create_Users(input: {Name: "John"}) { _docID } }"#)
        .expect("create doc");

    // Wait for merge on node 1
    wait_for_merge_events(&merges, 1, MERGE_TIMEOUT).await;
    sse.abort();

    // Both nodes see Name="John"
    let r0 = node0
        .query("query { Users { Name } }")
        .expect("query node0");
    assert_eq!(r0["Users"][0]["Name"].as_str(), Some("John"));

    let r1 = node1
        .query("query { Users { Name } }")
        .expect("query node1");
    assert_eq!(r1["Users"][0]["Name"].as_str(), Some("John"));
}

/// Port: TestP2POneToOneReplicatorCreateWithNewFieldSyncsDocsToUpdatedSchemaVersion
///
/// Both nodes patch to add Email. Create doc with Email on node 0.
/// Both see the full document.
#[tokio::test]
#[serial]
async fn replicator_create_new_field_updated_version() {
    let (cluster, addr1) = setup_migration_cluster(SCHEMA).await;
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    // Patch BOTH nodes: add Email field
    node0.collection_patch(ADD_EMAIL_PATCH).expect("patch node0");
    node1.collection_patch(ADD_EMAIL_PATCH).expect("patch node1");

    // Set up replication
    node0.p2p_collection_add(&["Users"]).expect("col add n0");
    node1.p2p_collection_add(&["Users"]).expect("col add n1");

    let (sse, merges) = open_merge_events_sse(cluster.api_url(1)).await;
    node0
        .p2p_replicator_set(&["Users"], &addr1)
        .expect("replicator");

    // Create doc with Email on node 0
    node0
        .query(
            r#"mutation { create_Users(input: {Name: "John", Email: "imnotyourbuddyguy@source.ca"}) { _docID } }"#,
        )
        .expect("create doc");

    // Wait for merge on node 1
    wait_for_merge_events(&merges, 1, MERGE_TIMEOUT).await;
    sse.abort();

    // Both nodes see both fields
    let r0 = node0
        .query("query { Users { Name Email } }")
        .expect("query node0");
    assert_eq!(r0["Users"][0]["Name"].as_str(), Some("John"));
    assert_eq!(
        r0["Users"][0]["Email"].as_str(),
        Some("imnotyourbuddyguy@source.ca")
    );

    let r1 = node1
        .query("query { Users { Name Email } }")
        .expect("query node1");
    assert_eq!(r1["Users"][0]["Name"].as_str(), Some("John"));
    assert_eq!(
        r1["Users"][0]["Email"].as_str(),
        Some("imnotyourbuddyguy@source.ca")
    );
}

/// Port: TestP2PReplicatorUpdateWithNewFieldSyncsDocsToOlderSchemaVersion
///
/// Create doc, set replicator, patch node 0, update both Name+Email.
/// Node 1 gets the Name update but ignores Email.
#[tokio::test]
#[serial]
async fn replicator_update_new_field_older_version() {
    let (cluster, addr1) = setup_migration_cluster(SCHEMA).await;
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    // Create doc on node 0
    let result = node0
        .query(r#"mutation { create_Users(input: {Name: "John"}) { _docID } }"#)
        .expect("create doc");
    let doc_id = integration_test::extract_doc_id(&result, "create_Users");

    // Set up replication
    node0.p2p_collection_add(&["Users"]).expect("col add n0");
    node1.p2p_collection_add(&["Users"]).expect("col add n1");

    let (sse, merges) = open_merge_events_sse(cluster.api_url(1)).await;
    node0
        .p2p_replicator_set(&["Users"], &addr1)
        .expect("replicator");

    // Wait for initial replication (replicator pushes existing doc)
    wait_for_merge_events(&merges, 1, MERGE_TIMEOUT).await;

    // Patch only node 0: add Email field
    node0.collection_patch(ADD_EMAIL_PATCH).expect("patch node0");

    // Update both fields on node 0
    node0
        .query(&format!(
            r#"mutation {{ update_Users(docID: "{}", input: {{Name: "Shahzad", Email: "imnotyourbuddyguy@source.ca"}}) {{ _docID }} }}"#,
            doc_id
        ))
        .expect("update doc");

    // Wait for update to merge on node 1
    wait_for_merge_events(&merges, 2, MERGE_TIMEOUT).await;
    sse.abort();

    // Node 0: sees both fields
    let r0 = node0
        .query("query { Users { Name Email } }")
        .expect("query node0");
    assert_eq!(r0["Users"][0]["Name"].as_str(), Some("Shahzad"));
    assert_eq!(
        r0["Users"][0]["Email"].as_str(),
        Some("imnotyourbuddyguy@source.ca")
    );

    // Node 1: sees only Name (older schema)
    let r1 = node1
        .query("query { Users { Name } }")
        .expect("query node1");
    assert_eq!(r1["Users"][0]["Name"].as_str(), Some("Shahzad"));
}

/// Port: TestP2PReplicatorUpdateWithNewFieldSyncsDocsToOlderSchemaVersionMultistep
///
/// Like the above but updates are done in two steps: first Email, then Name.
#[tokio::test]
#[serial]
async fn replicator_update_new_field_older_version_multistep() {
    let (cluster, addr1) = setup_migration_cluster(SCHEMA).await;
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    // Create doc on node 0
    let result = node0
        .query(r#"mutation { create_Users(input: {Name: "John"}) { _docID } }"#)
        .expect("create doc");
    let doc_id = integration_test::extract_doc_id(&result, "create_Users");

    // Set up replication
    node0.p2p_collection_add(&["Users"]).expect("col add n0");
    node1.p2p_collection_add(&["Users"]).expect("col add n1");

    let (sse, merges) = open_merge_events_sse(cluster.api_url(1)).await;
    node0
        .p2p_replicator_set(&["Users"], &addr1)
        .expect("replicator");

    // Wait for initial replication
    wait_for_merge_events(&merges, 1, MERGE_TIMEOUT).await;

    // Patch only node 0: add Email field
    node0.collection_patch(ADD_EMAIL_PATCH).expect("patch node0");

    // Step 1: Update Email on node 0
    node0
        .query(&format!(
            r#"mutation {{ update_Users(docID: "{}", input: {{Email: "imnotyourbuddyguy@source.ca"}}) {{ _docID }} }}"#,
            doc_id
        ))
        .expect("update email");

    // Step 2: Update Name on node 0
    node0
        .query(&format!(
            r#"mutation {{ update_Users(docID: "{}", input: {{Name: "Shahzad"}}) {{ _docID }} }}"#,
            doc_id
        ))
        .expect("update name");

    // Wait for both updates to merge on node 1
    wait_for_merge_events(&merges, 3, MERGE_TIMEOUT).await;
    sse.abort();

    // Node 0: sees both fields
    let r0 = node0
        .query("query { Users { Name Email } }")
        .expect("query node0");
    assert_eq!(r0["Users"][0]["Name"].as_str(), Some("Shahzad"));
    assert_eq!(
        r0["Users"][0]["Email"].as_str(),
        Some("imnotyourbuddyguy@source.ca")
    );

    // Node 1: sees only Name
    let r1 = node1
        .query("query { Users { Name } }")
        .expect("query node1");
    assert_eq!(r1["Users"][0]["Name"].as_str(), Some("Shahzad"));
}

// ===========================================================================
// Phase 3: Schema migration WITH lens transforms
// ===========================================================================

/// Build the set_default WASM lens module and return its file:// path.
fn wasm_lens_path() -> String {
    integration_test::wasm_lens::WasmLens::build().expect("build set_default WASM lens");
    integration_test::wasm_lens::WasmLens::module_path()
}

/// Build a lens config JSON for the set_default module.
fn set_default_lens_config(dst: &str, value: &Value) -> String {
    let path = wasm_lens_path();
    serde_json::json!({
        "Lenses": [{
            "Path": path,
            "Arguments": {"dst": dst, "value": value}
        }]
    })
    .to_string()
}

/// Port: TestSchemaMigrationQueryWithP2PReplicatedDocAtOlderSchemaVersion
///
/// Node 1 patches to add a field; lens migrates v1→v2 SetDefault verified=true.
/// Node 0 creates doc at v1 → node 1 sees verified=true via forward migration.
#[tokio::test]
#[serial]
async fn replicated_doc_older_schema_version() {
    let (cluster, addr1) = setup_migration_cluster(MIGRATION_SCHEMA).await;
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    // Get initial version ID (same on both nodes)
    let v1 = extract_version_id(&node0, "Users");

    // Patch node 1 only: add email field
    node1.collection_patch(ADD_EMAIL_PATCH_LC).expect("patch node1");
    let v2 = extract_version_id(&node1, "Users");

    // Configure migration on both nodes: v1 → v2
    let lens_config = set_default_lens_config("verified", &Value::Bool(true));
    node0
        .lens_set(&v1, &v2, &lens_config)
        .expect("lens_set node0");
    node1
        .lens_set(&v1, &v2, &lens_config)
        .expect("lens_set node1");

    // Set up replication
    node0.p2p_collection_add(&["Users"]).expect("col add n0");
    node1.p2p_collection_add(&["Users"]).expect("col add n1");

    let (sse, merges) = open_merge_events_sse(cluster.api_url(1)).await;
    node0
        .p2p_replicator_set(&["Users"], &addr1)
        .expect("replicator");

    // Create doc at v1 on node 0
    node0
        .query(r#"mutation { create_Users(input: {name: "John"}) { _docID } }"#)
        .expect("create doc");

    // Wait for merge on node 1
    wait_for_merge_events(&merges, 1, MERGE_TIMEOUT).await;
    sse.abort();

    // Node 0: v1 schema, sees name only
    let r0 = node0
        .query("query { Users { name } }")
        .expect("query node0");
    assert_eq!(r0["Users"][0]["name"].as_str(), Some("John"));

    // Node 1: v2 schema, lens sets verified=true
    let r1 = node1
        .query("query { Users { name verified } }")
        .expect("query node1");
    assert_eq!(r1["Users"][0]["name"].as_str(), Some("John"));
    assert_eq!(r1["Users"][0]["verified"], Value::Bool(true));
}

/// Port: TestSchemaMigrationQueryWithP2PReplicatedDocAtMuchOlderSchemaVersion
///
/// Node 1 patches twice (2 version hops); two lens migrations chain together.
#[tokio::test]
#[serial]
async fn replicated_doc_much_older_schema_version() {
    let (cluster, addr1) = setup_migration_cluster(MIGRATION_SCHEMA).await;
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    let v1 = extract_version_id(&node0, "Users");

    // Patch node 1 twice: add email, then address
    node1.collection_patch(ADD_EMAIL_PATCH_LC).expect("patch1 node1");
    let v2 = extract_version_id(&node1, "Users");
    node1.collection_patch(ADD_ADDRESS_PATCH).expect("patch2 node1");
    let v3 = extract_version_id(&node1, "Users");

    // Migration 1: v1→v2 sets verified=true
    let lens1 = set_default_lens_config("verified", &Value::Bool(true));
    node0.lens_set(&v1, &v2, &lens1).expect("lens_set 1 node0");
    node1.lens_set(&v1, &v2, &lens1).expect("lens_set 1 node1");

    // Migration 2: v2→v3 sets name="Fred"
    let lens2 = set_default_lens_config("name", &Value::String("Fred".into()));
    node0.lens_set(&v2, &v3, &lens2).expect("lens_set 2 node0");
    node1.lens_set(&v2, &v3, &lens2).expect("lens_set 2 node1");

    // Set up replication
    node0.p2p_collection_add(&["Users"]).expect("col add n0");
    node1.p2p_collection_add(&["Users"]).expect("col add n1");

    let (sse, merges) = open_merge_events_sse(cluster.api_url(1)).await;
    node0
        .p2p_replicator_set(&["Users"], &addr1)
        .expect("replicator");

    // Create doc at v1 on node 0
    node0
        .query(r#"mutation { create_Users(input: {name: "John"}) { _docID } }"#)
        .expect("create doc");

    // Wait for merge on node 1
    wait_for_merge_events(&merges, 1, MERGE_TIMEOUT).await;
    sse.abort();

    // Node 0: v1, sees original name
    let r0 = node0
        .query("query { Users { name } }")
        .expect("query node0");
    assert_eq!(r0["Users"][0]["name"].as_str(), Some("John"));

    // Node 1: v3, lens chain: name overwritten to "Fred", verified=true
    let r1 = node1
        .query("query { Users { name verified } }")
        .expect("query node1");
    assert_eq!(r1["Users"][0]["name"].as_str(), Some("Fred"));
    assert_eq!(r1["Users"][0]["verified"], Value::Bool(true));
}

/// Port: TestSchemaMigrationQueryWithP2PReplicatedDocAtNewerSchemaVersion
///
/// Node 0 patches; lens v1→v2 sets verified=true.
/// Node 0 creates doc at v2 → node 1 sees verified=nil (inverse clears it).
#[tokio::test]
#[serial]
async fn replicated_doc_newer_schema_version() {
    let (cluster, addr1) = setup_migration_cluster(MIGRATION_SCHEMA).await;
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    let v1 = extract_version_id(&node0, "Users");

    // Patch node 0 only: add email field
    node0.collection_patch(ADD_EMAIL_PATCH_LC).expect("patch node0");
    let v2 = extract_version_id(&node0, "Users");

    // Configure migration on both nodes: v1 → v2
    let lens_config = set_default_lens_config("verified", &Value::Bool(true));
    node0
        .lens_set(&v1, &v2, &lens_config)
        .expect("lens_set node0");
    node1
        .lens_set(&v1, &v2, &lens_config)
        .expect("lens_set node1");

    // Set up replication
    node0.p2p_collection_add(&["Users"]).expect("col add n0");
    node1.p2p_collection_add(&["Users"]).expect("col add n1");

    let (sse, merges) = open_merge_events_sse(cluster.api_url(1)).await;
    node0
        .p2p_replicator_set(&["Users"], &addr1)
        .expect("replicator");

    // Create doc at v2 on node 0 (with verified=true)
    node0
        .query(r#"mutation { create_Users(input: {name: "John", verified: true}) { _docID } }"#)
        .expect("create doc");

    // Wait for merge on node 1
    wait_for_merge_events(&merges, 1, MERGE_TIMEOUT).await;
    sse.abort();

    // Node 0: v2, sees both
    let r0 = node0
        .query("query { Users { name verified } }")
        .expect("query node0");
    assert_eq!(r0["Users"][0]["name"].as_str(), Some("John"));
    assert_eq!(r0["Users"][0]["verified"], Value::Bool(true));

    // Node 1: v1, inverse lens clears verified
    let r1 = node1
        .query("query { Users { name verified } }")
        .expect("query node1");
    assert_eq!(r1["Users"][0]["name"].as_str(), Some("John"));
    assert!(
        r1["Users"][0]["verified"].is_null(),
        "verified should be null on node1, got {:?}",
        r1["Users"][0]["verified"]
    );
}

/// Port: TestSchemaMigrationQueryWithP2PReplicatedDocAtMuchNewerSchemaVersionWithSchemaHistoryGap
///
/// Node 0 patches twice; only v2→v3 migration registered (no v1→v2).
/// Missing migration means no transform applied; doc still replicates.
#[tokio::test]
#[serial]
async fn replicated_doc_much_newer_with_history_gap() {
    let schema = "type Users { name: String }";
    let (cluster, addr1) = setup_migration_cluster(schema).await;
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    let _v1 = extract_version_id(&node0, "Users");

    // Patch node 0 twice: add verified, then email
    node0.collection_patch(ADD_VERIFIED_PATCH).expect("patch1 node0");
    let v2 = extract_version_id(&node0, "Users");
    node0.collection_patch(ADD_EMAIL_PATCH_LC).expect("patch2 node0");
    let v3 = extract_version_id(&node0, "Users");

    // Only v2→v3 migration registered (gap: no v1→v2)
    let lens_config = set_default_lens_config("verified", &Value::Bool(true));
    node0
        .lens_set(&v2, &v3, &lens_config)
        .expect("lens_set node0");
    node1
        .lens_set(&v2, &v3, &lens_config)
        .expect("lens_set node1");

    // Set up replication
    node0.p2p_collection_add(&["Users"]).expect("col add n0");
    node1.p2p_collection_add(&["Users"]).expect("col add n1");

    let (sse, merges) = open_merge_events_sse(cluster.api_url(1)).await;
    node0
        .p2p_replicator_set(&["Users"], &addr1)
        .expect("replicator");

    // Create doc at v1 on node 0 (before patches)
    node0
        .query(r#"mutation { create_Users(input: {name: "John"}) { _docID } }"#)
        .expect("create doc");

    // Wait for merge on node 1
    wait_for_merge_events(&merges, 1, MERGE_TIMEOUT).await;
    sse.abort();

    // Node 1 can still receive doc despite missing migration
    let r1 = node1
        .query("query { Users { name } }")
        .expect("query node1");
    assert_eq!(r1["Users"][0]["name"].as_str(), Some("John"));
}

/// Port: TestSchemaMigrationQueryWithP2PReplicatedDocOnOtherSchemaBranch
///
/// Different patches on each node create divergent schema branches.
/// Node 0 patches with email; node 1 patches with phone.
/// Lens on node 0: v1→v2 sets name="Fred".
/// Inline lens on node 1: v1→v3 sets phone="1234567890".
#[tokio::test]
#[serial]
async fn replicated_doc_other_schema_branch() {
    let (cluster, addr1) = setup_migration_cluster(MIGRATION_SCHEMA).await;
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    let v1 = extract_version_id(&node0, "Users");

    // Patch node 0: add email
    node0.collection_patch(ADD_EMAIL_PATCH_LC).expect("patch node0");
    let v2 = extract_version_id(&node0, "Users");

    // Lens on both: v1→v2 sets name="Fred"
    let lens1 = set_default_lens_config("name", &Value::String("Fred".into()));
    node0.lens_set(&v1, &v2, &lens1).expect("lens_set node0");
    node1.lens_set(&v1, &v2, &lens1).expect("lens_set node1");

    // Patch node 1: add phone (creates different branch v1→v3)
    node1.collection_patch(ADD_PHONE_PATCH).expect("patch node1");
    let v3 = extract_version_id(&node1, "Users");

    // Lens on both: v1→v3 sets phone="1234567890"
    let lens2 = set_default_lens_config("phone", &Value::String("1234567890".into()));
    node0.lens_set(&v1, &v3, &lens2).expect("lens2 node0");
    node1.lens_set(&v1, &v3, &lens2).expect("lens2 node1");

    // Set up replication
    node0.p2p_collection_add(&["Users"]).expect("col add n0");
    node1.p2p_collection_add(&["Users"]).expect("col add n1");

    let (sse, merges) = open_merge_events_sse(cluster.api_url(1)).await;
    node0
        .p2p_replicator_set(&["Users"], &addr1)
        .expect("replicator");

    // Create doc at v2 on node 0
    node0
        .query(r#"mutation { create_Users(input: {name: "John", verified: true}) { _docID } }"#)
        .expect("create doc");

    // Wait for merge on node 1
    wait_for_merge_events(&merges, 1, MERGE_TIMEOUT).await;
    sse.abort();

    // Node 0: v2, sees original values
    let r0 = node0
        .query("query { Users { name verified } }")
        .expect("query node0");
    assert_eq!(r0["Users"][0]["name"].as_str(), Some("John"));
    assert_eq!(r0["Users"][0]["verified"], Value::Bool(true));

    // Node 1: v3, inverse of v1→v2 clears name, v1→v3 sets phone
    let r1 = node1
        .query("query { Users { name phone verified } }")
        .expect("query node1");
    assert!(
        r1["Users"][0]["name"].is_null(),
        "name should be null on node1 (inverse clears it), got {:?}",
        r1["Users"][0]["name"]
    );
    assert_eq!(
        r1["Users"][0]["phone"].as_str(),
        Some("1234567890"),
        "phone should be set by lens"
    );
    assert_eq!(r1["Users"][0]["verified"], Value::Bool(true));
}
