use integration_test::{for_each_runtime, TestCluster};

async fn view_test(cluster: TestCluster) {
    let client = cluster.client(0);

    // Deploy a base schema
    client
        .schema_add("type User { name: String  age: Int }")
        .unwrap();

    // Create some documents
    client
        .query(r#"mutation { create_User(input: {name: "Alice", age: 30}) { _docID } }"#)
        .unwrap();
    client
        .query(r#"mutation { create_User(input: {name: "Bob", age: 25}) { _docID } }"#)
        .unwrap();

    // Add a view — uses --query and --sdl flags (Go-compatible).
    // Note: the query must not include the "query" keyword prefix.
    let result = client.view_add(
        "User { name age }",
        "type UserView { name: String  age: Int }",
    );
    assert!(result.is_ok(), "view add failed: {:?}", result.err());

    // Refresh views — exercises the HTTP endpoint.
    let _result = client.view_refresh(None);
}

for_each_runtime!(view, view_test);
