use integration_test::{for_each_runtime, TestCluster};

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
                err_msg.contains("collection") || err_msg.contains("not found"),
                "expected collection-not-found error, got: {}",
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

for_each_runtime!(batched_mutation_aliases, batched_mutation_aliases_test);
for_each_runtime!(batched_mutation_rollback, batched_mutation_rollback_test);
