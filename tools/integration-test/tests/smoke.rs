use integration_test::TestCluster;

#[tokio::test]
#[ignore]
async fn version_json_has_all_fields() {
    let root = integration_test::workspace_root();
    let binary = root.join("target/debug/defra");

    let output = std::process::Command::new(&binary)
        .args(["version", "--format", "json"])
        .output()
        .expect("defra binary not found — run `cargo build -p cli` first");

    assert!(output.status.success(), "defra version failed");

    let json: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("invalid JSON from defra version");

    assert!(json["version"].is_string());
    assert!(json["commit"].is_string());
    assert!(json["commitDate"].is_string());
    assert_eq!(json["httpAPI"], "v0");
    assert_eq!(json["docIdVersions"], "1");
    assert_eq!(json["netProtocol"], "/defra/0.0.1");
    assert!(json["rust"].is_string());
    assert!(json["goCompat"]["commit"].is_string());
    assert!(!json["goCompat"]["commit"].as_str().unwrap().is_empty());
    assert!(json["goCompat"]["branch"].is_string());
}

#[tokio::test]
#[ignore] // Run with: cargo test -p integration-test -- --ignored
async fn smoke_single_rust_node() {
    // 1. Start cluster with 1 Rust node (no P2P)
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    let client = cluster.client(0);

    // 2. Deploy schema: type User { name: String, age: Int }
    client
        .schema_add("type User { name: String  age: Int }")
        .unwrap();

    // 3. Create document via mutation
    let data = client
        .query(r#"mutation { create_User(input: {name: "Alice", age: 30}) { _docID name age } }"#)
        .unwrap();
    assert_eq!(data["create_User"][0]["name"], "Alice");

    // 4. Query document back
    let data = client.query("query { User { _docID name age } }").unwrap();
    assert_eq!(data["User"][0]["name"], "Alice");
    assert_eq!(data["User"][0]["age"], 30);

    // 5. Drop: processes killed, dirs cleaned
}
