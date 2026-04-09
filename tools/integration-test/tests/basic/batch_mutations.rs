use integration_test::{for_each_runtime, TestCluster};
use std::time::Duration;

async fn batched_mutation_aliases_test(cluster: TestCluster) {
    let client = cluster.client(0);

    client
        .schema_add("type User { name: String  email: String }")
        .expect("failed to add schema");
    client
        .index_create("User", &["email"], Some("idx_email_unique"), true)
        .expect("failed to add unique email index");

    let data = client
        .query(
            r#"mutation {
                alice: add_User(input: {name: "Alice", email: "alice@example.com"}) {
                    _docID
                    name
                    email
                }
                bob: add_User(input: {name: "Bob", email: "bob@example.com"}) {
                    _docID
                    name
                    email
                }
            }"#,
        )
        .expect("batched aliased mutation should succeed");

    assert_eq!(data["alice"][0]["name"], "Alice");
    assert_eq!(data["alice"][0]["email"], "alice@example.com");
    assert_eq!(data["bob"][0]["name"], "Bob");
    assert_eq!(data["bob"][0]["email"], "bob@example.com");

    let query = client
        .query("query { User { name email } }")
        .expect("query users after batched create");
    let users = query["User"].as_array().expect("users result not array");
    assert_eq!(
        users.len(),
        2,
        "expected 2 committed users, got {:?}",
        query
    );

    let names: Vec<&str> = users
        .iter()
        .filter_map(|user| user["name"].as_str())
        .collect();
    assert!(
        names.contains(&"Alice"),
        "Alice missing after batched create"
    );
    assert!(names.contains(&"Bob"), "Bob missing after batched create");
}

async fn batched_mutation_rollback_test(cluster: TestCluster) {
    let client = cluster.client(0);

    client
        .schema_add("type User { name: String  email: String }")
        .expect("failed to add schema");

    let result = client.query(
        r#"mutation {
            first: add_User(input: {name: "Alice", email: "alice@example.com"}) {
                _docID
            }
            second: add_Missing(input: {name: "Bob"}) {
                _docID
            }
        }"#,
    );

    match result {
        Ok(data) => {
            assert!(
                data.is_null(),
                "failed batched mutation should not return successful data, got {:?}",
                data
            );
        }
        Err(err) => {
            let err_msg = err.to_string();
            assert!(
                err_msg.contains("collection")
                    || err_msg.contains("not found")
                    || err_msg.contains("Cannot query field \"add_Missing\" on type \"Mutation\"."),
                "expected missing-collection mutation error, got: {}",
                err_msg
            );
        }
    }

    let query = client
        .query("query { User { name email } }")
        .expect("query users after failed batch");
    let users = query["User"].as_array().expect("users result not array");
    assert!(
        users.is_empty(),
        "failed batched mutation should leave no committed users, got {:?}",
        query
    );
}

async fn batched_mutation_aliases_p2p_regression_test(cluster: TestCluster) {
    let client = cluster.client(0);

    cluster
        .wait_for_log(0, "p2p_listening", Duration::from_secs(15))
        .await
        .expect("P2P listener did not start");

    client
        .schema_add("type TestDoc @branchable { name: String }")
        .expect("failed to add schema");
    client
        .p2p_collection_add(&["TestDoc"])
        .expect("failed to subscribe collection to P2P");

    let data = client
        .query(
            r#"mutation {
                first: add_TestDoc(input: {name: "first"}) { _docID name }
                second: add_TestDoc(input: {name: "second"}) { _docID name }
                third: add_TestDoc(input: {name: "third"}) { _docID name }
            }"#,
        )
        .expect("batched aliased P2P mutation should succeed");

    assert_eq!(data["first"][0]["name"], "first");
    assert_eq!(data["second"][0]["name"], "second");
    assert_eq!(data["third"][0]["name"], "third");

    let query = client
        .query("query { TestDoc { name } }")
        .expect("query documents after batched P2P create");
    let docs = query["TestDoc"]
        .as_array()
        .expect("documents result not array");
    assert_eq!(
        docs.len(),
        3,
        "expected 3 committed documents, got {:?}",
        query
    );

    let names: Vec<&str> = docs.iter().filter_map(|doc| doc["name"].as_str()).collect();
    assert!(
        names.contains(&"first"),
        "first missing after batched create"
    );
    assert!(
        names.contains(&"second"),
        "second missing after batched create"
    );
    assert!(
        names.contains(&"third"),
        "third missing after batched create"
    );
}

for_each_runtime!(batched_mutation_aliases, batched_mutation_aliases_test);
for_each_runtime!(batched_mutation_rollback, batched_mutation_rollback_test);
for_each_runtime!(
    batched_mutation_aliases_p2p_regression,
    batched_mutation_aliases_p2p_regression_test,
    .with_p2p()
);
