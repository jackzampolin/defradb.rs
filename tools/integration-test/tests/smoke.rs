use integration_test::{GraphQLClient, TestCluster};

#[tokio::test]
#[ignore] // Run with: cargo test -p integration-test -- --ignored
async fn smoke_single_rust_node() {
    // 1. Start cluster with 1 Rust node (no P2P)
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();

    // 2. Deploy schema: type User { name: String, age: Int }
    let gql = GraphQLClient::new(cluster.client.clone(), cluster.api_url(0));
    gql.deploy_schema("type User { name: String  age: Int }")
        .await
        .unwrap();

    // 3. Create document via mutation
    let data = gql
        .query_ok(
            r#"mutation { create_User(input: {name: "Alice", age: 30}) { _docID name age } }"#,
        )
        .await
        .unwrap();
    assert_eq!(data["create_User"]["name"], "Alice");

    // 4. Query document back
    let data = gql
        .query_ok("query { User { _docID name age } }")
        .await
        .unwrap();
    assert_eq!(data["User"][0]["name"], "Alice");
    assert_eq!(data["User"][0]["age"], 30);

    // 5. Drop: processes killed, dirs cleaned
}
