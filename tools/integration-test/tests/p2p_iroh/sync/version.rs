//! Iroh P2P collection version sync tests.
//!
//! Ported from Go: tests/integration/net/sync/collection_version/
//!
//! Collection version sync transfers schema definitions (collection versions)
//! between nodes. Synced versions arrive inactive and must be explicitly
//! activated before use.
//!
//! Run with:
//!   cargo test --test p2p_iroh -- sync::version::

use std::path::Path;
use std::time::Duration;

use integration_test::{extract_p2p_addr, poll_until, TestCluster};
use serial_test::serial;

const SCHEMA: &str = "type Users { name: String  age: Int }";
const P2P_TIMEOUT: Duration = Duration::from_secs(15);

/// Helper: set up 2 iroh nodes, deploy schema on node0, connect them.
/// Returns (cluster, version_id_from_node0).
async fn setup_version_sync_cluster(schema: &str) -> (TestCluster, String) {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_iroh_transport()
        .build()
        .await
        .unwrap();

    cluster
        .wait_for_log(0, "p2p_listening", P2P_TIMEOUT)
        .await
        .expect("node0 P2P listener");
    cluster
        .wait_for_log(1, "p2p_listening", P2P_TIMEOUT)
        .await
        .expect("node1 P2P listener");

    let node0 = cluster.client(0);

    node0.schema_add(schema).expect("schema add node0");

    // Get the version ID from node0
    let desc = node0
        .collection_describe_version("Users")
        .expect("describe Users on node0");
    let version_id = desc["VersionID"]
        .as_str()
        .expect("missing VersionID")
        .to_string();

    // Connect peers
    let addr0 = extract_p2p_addr(&cluster, 0);
    let addr1 = extract_p2p_addr(&cluster, 1);
    node0.p2p_connect(&[&addr1]).expect("connect 0→1");
    cluster
        .client(1)
        .p2p_connect(&[&addr0])
        .expect("connect 1→0");

    (cluster, version_id)
}

/// Port: TestSyncColVersion_WithInitialColVersion
/// Sync initial collection version to peer — arrives inactive.
#[tokio::test]
#[serial]
async fn initial_col_version() {
    let (cluster, version_id) = setup_version_sync_cluster(SCHEMA).await;
    let node1 = cluster.client(1);

    // Node1 syncs the version from node0 (returns immediately, sync happens in background)
    node1
        .p2p_collection_sync_versions(&[&version_id])
        .expect("p2p_collection_sync_versions");

    // Poll until node1 has the synced version
    let node1_ref = &node1;
    let expected_version = version_id.clone();
    poll_until(
        || {
            node1_ref
                .collection_describe_version("Users")
                .ok()
                .and_then(|desc| desc["VersionID"].as_str().map(|v| v == expected_version))
                .unwrap_or(false)
        },
        P2P_TIMEOUT,
        Duration::from_millis(300),
        "version sync did not arrive on node1",
    )
    .await;
}

/// Port: TestSyncColVersion_WithInitialColVersion_CanBeActivatedAndQueried
/// Synced initial version can be activated and queried.
#[tokio::test]
#[serial]
async fn initial_col_version_activated_and_queried() {
    let (cluster, version_id) = setup_version_sync_cluster(SCHEMA).await;
    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    // Sync version to node1 (returns immediately)
    node1
        .p2p_collection_sync_versions(&[&version_id])
        .expect("p2p_collection_sync_versions");

    // Wait for version to arrive, then activate it
    let node1_ref = &node1;
    let expected_version = version_id.clone();
    poll_until(
        || {
            node1_ref
                .collection_describe_version("Users")
                .ok()
                .and_then(|desc| desc["VersionID"].as_str().map(|v| v == expected_version))
                .unwrap_or(false)
        },
        P2P_TIMEOUT,
        Duration::from_millis(300),
        "version sync did not arrive on node1",
    )
    .await;

    node1
        .collection_set_active(&version_id)
        .expect("collection set-active");

    // Create a doc on node0
    let r1 = node0
        .query(r#"mutation { create_Users(input: {name: "John", age: 4}) { _docID } }"#)
        .expect("create John");
    let doc_id = r1["create_Users"][0]["_docID"]
        .as_str()
        .expect("missing _docID")
        .to_string();

    // Sync the doc to node1
    node1
        .p2p_document_sync("Users", &[&doc_id])
        .expect("p2p_document_sync");

    // Verify node1 can query the doc
    let node1_ref = &node1;
    poll_until(
        || {
            let r = node1_ref
                .query("query { Users { name age } }")
                .unwrap_or_default();
            r["Users"]
                .as_array()
                .map(|arr| {
                    arr.iter()
                        .any(|u| u["name"].as_str() == Some("John") && u["age"].as_i64() == Some(4))
                })
                .unwrap_or(false)
        },
        Duration::from_secs(15),
        Duration::from_millis(300),
        "synced and activated version did not allow querying doc",
    )
    .await;
}

/// Port: TestSyncColVersion_WithPatchVersionOfKnownCollection
/// Sync patch version of a collection that already exists locally.
#[tokio::test]
#[serial]
async fn patch_version_of_known_collection() {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_iroh_transport()
        .build()
        .await
        .unwrap();

    cluster
        .wait_for_log(0, "p2p_listening", P2P_TIMEOUT)
        .await
        .expect("node0");
    cluster
        .wait_for_log(1, "p2p_listening", P2P_TIMEOUT)
        .await
        .expect("node1");

    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    // Both nodes start with same base schema
    let base_schema = "type Users { name: String }";
    node0.schema_add(base_schema).expect("schema node0");
    node1.schema_add(base_schema).expect("schema node1");

    // Node0 patches to add age field
    node0
        .collection_patch(
            r#"[{"op": "add", "path": "/Users/Fields/-", "value": {"Name": "age", "Kind": "Int"}}]"#,
        )
        .expect("collection patch");

    // Get the patched version ID
    let desc = node0
        .collection_describe_version("Users")
        .expect("describe patched Users");
    let patched_version_id = desc["VersionID"]
        .as_str()
        .expect("missing VersionID")
        .to_string();

    // Connect peers
    let addr0 = extract_p2p_addr(&cluster, 0);
    let addr1 = extract_p2p_addr(&cluster, 1);
    node0.p2p_connect(&[&addr1]).expect("connect 0→1");
    node1.p2p_connect(&[&addr0]).expect("connect 1→0");

    // Node1 syncs the patched version (returns immediately)
    node1
        .p2p_collection_sync_versions(&[&patched_version_id])
        .expect("p2p_collection_sync_versions");

    // Poll until node1 has the patched version
    let node1_ref = &node1;
    let expected = patched_version_id.clone();
    poll_until(
        || {
            node1_ref
                .collection_describe_version("Users")
                .ok()
                .and_then(|desc| desc["VersionID"].as_str().map(|v| v == expected))
                .unwrap_or(false)
        },
        P2P_TIMEOUT,
        Duration::from_millis(300),
        "patched version sync did not arrive on node1",
    )
    .await;
}

/// Port: TestSyncColVersion_WithPatchVersionOfUnknownCollection
/// Sync patch version of a collection the node doesn't know about.
#[tokio::test]
#[serial]
async fn patch_version_of_unknown_collection() {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_iroh_transport()
        .build()
        .await
        .unwrap();

    cluster
        .wait_for_log(0, "p2p_listening", P2P_TIMEOUT)
        .await
        .expect("node0");
    cluster
        .wait_for_log(1, "p2p_listening", P2P_TIMEOUT)
        .await
        .expect("node1");

    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    // Only node0 has the schema
    node0
        .schema_add("type Users { name: String }")
        .expect("schema node0");

    // Node0 patches to add age field
    node0
        .collection_patch(
            r#"[{"op": "add", "path": "/Users/Fields/-", "value": {"Name": "age", "Kind": "Int"}}]"#,
        )
        .expect("collection patch");

    // Get the patched version ID
    let desc = node0
        .collection_describe_version("Users")
        .expect("describe patched");
    let patched_version_id = desc["VersionID"]
        .as_str()
        .expect("missing VersionID")
        .to_string();

    // Connect peers
    let addr0 = extract_p2p_addr(&cluster, 0);
    let addr1 = extract_p2p_addr(&cluster, 1);
    node0.p2p_connect(&[&addr1]).expect("connect 0→1");
    node1.p2p_connect(&[&addr0]).expect("connect 1→0");

    // Node1 (has NO schema) syncs the patched version
    // Should recursively sync the base version too
    node1
        .p2p_collection_sync_versions(&[&patched_version_id])
        .expect("p2p_collection_sync_versions");

    // Poll until node1 has the collection
    let node1_ref = &node1;
    poll_until(
        || {
            node1_ref
                .collection_describe_version("Users")
                .ok()
                .and_then(|desc| desc["VersionID"].as_str().map(|_| true))
                .unwrap_or(false)
        },
        P2P_TIMEOUT,
        Duration::from_millis(300),
        "unknown collection version sync did not arrive on node1",
    )
    .await;
}

/// Path to the copy lens WASM module (from the Go repo).
/// This module copies the value from one field to another (src→dst).
const COPY_WASM_PATH: &str = concat!(
    env!("HOME"),
    "/go/src/github.com/sourcenetwork/defradb",
    "/tests/lenses/rust_wasm32_copy/target/wasm32-unknown-unknown/debug/rust_wasm32_copy.wasm"
);

fn require_lens_wasm() {
    assert!(
        Path::new(COPY_WASM_PATH).exists(),
        "Lens WASM binary not found at: {}\n\
         Build it first:\n\
         \n\
         cd ~/go/src/github.com/sourcenetwork/defradb/tests/lenses && make build\n",
        COPY_WASM_PATH
    );
}

/// Port: TestSyncColVersion_WithView
/// Sync a view (derived collection) version to a peer — arrives inactive.
#[tokio::test]
#[serial]
async fn with_view() {
    require_lens_wasm();

    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_iroh_transport()
        .build()
        .await
        .unwrap();

    cluster
        .wait_for_log(0, "p2p_listening", P2P_TIMEOUT)
        .await
        .expect("node0 P2P listener");
    cluster
        .wait_for_log(1, "p2p_listening", P2P_TIMEOUT)
        .await
        .expect("node1 P2P listener");

    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    // Add base schema on node0
    node0
        .schema_add("type Users { name: String }")
        .expect("schema add node0");

    // Add copy lens on node0 (copies name → fullName)
    let lens_config = format!(
        r#"{{
            "Lenses": [{{
                "Path": "{}",
                "Arguments": {{"src": "name", "dst": "fullName"}}
            }}]
        }}"#,
        COPY_WASM_PATH
    );
    let lens_result = node0.lens_add(&lens_config).expect("lens_add");
    let lens_cid = lens_result["lensId"]
        .as_str()
        .expect("missing lensId from lens_add");

    // Create a view using the lens
    node0
        .view_add_with_lens(
            "Users { name }",
            "type UserView @materialized(if: false) { fullName: String }",
            lens_cid,
        )
        .expect("view_add_with_lens");

    // Get the view's collection version ID
    let view_desc = node0
        .collection_describe_version("UserView")
        .expect("describe UserView");
    let view_version_id = view_desc["VersionID"]
        .as_str()
        .expect("missing VersionID for UserView");

    // Connect peers
    let addr0 = extract_p2p_addr(&cluster, 0);
    let addr1 = extract_p2p_addr(&cluster, 1);
    node0.p2p_connect(&[&addr1]).expect("connect 0→1");
    node1.p2p_connect(&[&addr0]).expect("connect 1→0");

    // Sync the view's collection version to node1 (returns immediately)
    node1
        .p2p_collection_sync_versions(&[view_version_id])
        .expect("sync view version");

    // Poll until node1 has the view
    let node1_ref = &node1;
    poll_until(
        || {
            node1_ref
                .collection_describe_version("UserView")
                .ok()
                .and_then(|desc| desc["VersionID"].as_str().map(|_| true))
                .unwrap_or(false)
        },
        Duration::from_secs(30),
        Duration::from_millis(300),
        "view version sync did not arrive on node1",
    )
    .await;
}

/// Port: TestSyncColVersion_WithView_CanBeActivatedAndQueried
/// Synced view can be activated and queried through the lens.
#[tokio::test]
#[serial]
async fn with_view_activated_and_queried() {
    require_lens_wasm();

    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_iroh_transport()
        .build()
        .await
        .unwrap();

    cluster
        .wait_for_log(0, "p2p_listening", P2P_TIMEOUT)
        .await
        .expect("node0 P2P listener");
    cluster
        .wait_for_log(1, "p2p_listening", P2P_TIMEOUT)
        .await
        .expect("node1 P2P listener");

    let node0 = cluster.client(0);
    let node1 = cluster.client(1);

    // Both nodes get the base schema
    node0
        .schema_add("type Users { name: String }")
        .expect("schema add node0");
    node1
        .schema_add("type Users { name: String }")
        .expect("schema add node1");

    // Add copy lens on node0
    let lens_config = format!(
        r#"{{
            "Lenses": [{{
                "Path": "{}",
                "Arguments": {{"src": "name", "dst": "fullName"}}
            }}]
        }}"#,
        COPY_WASM_PATH
    );
    let lens_result = node0.lens_add(&lens_config).expect("lens_add");
    let lens_cid = lens_result["lensId"]
        .as_str()
        .expect("missing lensId from lens_add");

    // Create view on node0
    node0
        .view_add_with_lens(
            "Users { name }",
            "type UserView @materialized(if: false) { fullName: String }",
            lens_cid,
        )
        .expect("view_add_with_lens");

    // Get view version ID
    let view_desc = node0
        .collection_describe_version("UserView")
        .expect("describe UserView");
    let view_version_id = view_desc["VersionID"]
        .as_str()
        .expect("missing VersionID for UserView");

    // Connect peers
    let addr0 = extract_p2p_addr(&cluster, 0);
    let addr1 = extract_p2p_addr(&cluster, 1);
    node0.p2p_connect(&[&addr1]).expect("connect 0→1");
    node1.p2p_connect(&[&addr0]).expect("connect 1→0");

    // Sync view version to node1 (returns immediately)
    node1
        .p2p_collection_sync_versions(&[view_version_id])
        .expect("sync view version");

    // Wait for view version to arrive, then activate it
    let node1_ref = &node1;
    let vv = view_version_id.to_string();
    poll_until(
        || {
            node1_ref
                .collection_describe_version("UserView")
                .ok()
                .and_then(|desc| desc["VersionID"].as_str().map(|_| true))
                .unwrap_or(false)
        },
        Duration::from_secs(30),
        Duration::from_millis(300),
        "view version sync did not arrive on node1",
    )
    .await;

    node1
        .collection_set_active(&vv)
        .expect("activate synced view on node1");

    // Create a doc on node1 (in the Users collection)
    node1
        .query(r#"mutation { create_Users(input: {name: "John"}) { _docID } }"#)
        .expect("create John on node1");

    // Query the view — the lens should copy name→fullName
    let node1_ref = &node1;
    poll_until(
        || {
            let r = node1_ref
                .query("query { UserView { fullName } }")
                .unwrap_or_default();
            r["UserView"]
                .as_array()
                .and_then(|arr| arr.first())
                .and_then(|u| u["fullName"].as_str())
                .map(|name| name == "John")
                .unwrap_or(false)
        },
        Duration::from_secs(15),
        Duration::from_millis(300),
        "view query did not return expected fullName after activating synced view",
    )
    .await;
}
