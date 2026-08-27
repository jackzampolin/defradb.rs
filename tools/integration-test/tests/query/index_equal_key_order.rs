use integration_test::{DefraClient, TestCluster};

fn add_user(node: &DefraClient, name: &str) -> String {
    let result = node
        .query(&format!(
            r#"mutation {{ add_User(input: {{name: "{name}", age: 21}}) {{ _docID }} }}"#
        ))
        .unwrap_or_else(|e| panic!("add {name}: {e}"));
    result["add_User"][0]["_docID"]
        .as_str()
        .unwrap_or_else(|| panic!("missing _docID for {name}: {result}"))
        .to_string()
}

fn delete_users(node: &DefraClient, ids: &[String]) {
    let quoted = ids
        .iter()
        .map(|id| format!("\"{id}\""))
        .collect::<Vec<_>>()
        .join(", ");
    node.query(&format!(
        r#"mutation {{ delete_User(docIDs: [{quoted}]) {{ _docID }} }}"#
    ))
    .unwrap_or_else(|e| panic!("delete {ids:?}: {e}"));
}

/// Two documents that share an indexed value, queried with no `order` clause,
/// must come back in public DocID order (#1602).
///
/// Index keys already suffix node-local short IDs, so KV order follows
/// insert/persist order and diverges across replicas. Public DocIDs are
/// content-addressed and identical everywhere. This inserts the
/// lexicographically larger DocID first so a short-ID scan would return
/// the reverse of the asserted order.
#[tokio::test]
async fn rust_equal_index_keys_are_ordered_by_doc_id() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    let node = cluster.client(0);

    node.schema_add(
        r#"
        type User {
            name: String
            age: Int @index
        }
        "#,
    )
    .expect("add schema");

    // Prefer the names from the Go oracle test, then fall back until the
    // second insert has the smaller public DocID.
    let candidates = [
        ("John", "Andy"),
        ("zeta", "alpha"),
        ("zz", "aa"),
        ("UserZ", "UserA"),
        ("m", "a"),
    ];

    let mut chosen: Option<(String, String, String, String)> = None;
    for (first_name, second_name) in candidates {
        let first_id = add_user(&node, first_name);
        let second_id = add_user(&node, second_name);
        if second_id.as_str() < first_id.as_str() {
            chosen = Some((
                first_name.to_string(),
                first_id,
                second_name.to_string(),
                second_id,
            ));
            break;
        }
        delete_users(&node, &[first_id, second_id]);
    }
    let (first_name, first_id, second_name, second_id) =
        chosen.expect("could not find a pair whose public DocID order opposes insert order");

    let result = node
        .query(r#"query { User(filter: {age: {_eq: 21}}) { name _docID } }"#)
        .expect("query equal indexed keys");
    let users = result["User"].as_array().unwrap_or_else(|| {
        panic!("User array missing from {result}");
    });
    assert_eq!(users.len(), 2, "expected both age=21 users, got {result}");

    let got_names: Vec<&str> = users
        .iter()
        .map(|row| row["name"].as_str().expect("name"))
        .collect();
    let got_ids: Vec<&str> = users
        .iter()
        .map(|row| row["_docID"].as_str().expect("_docID"))
        .collect();

    assert_eq!(
        got_ids,
        vec![second_id.as_str(), first_id.as_str()],
        "equal index keys must come back in public DocID order \
         (inserted {first_name} then {second_name}); got {got_names:?}"
    );
    assert_eq!(got_names, vec![second_name.as_str(), first_name.as_str()]);
}
