use integration_test::TestCluster;

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
