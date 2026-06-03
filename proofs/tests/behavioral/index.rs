//! Index-maintenance — after create/update/delete, an indexed-field lookup
//! returns exactly the live docs: no stale entry, none missing.
//! Model: `proofs/lean/IndexMaintenance` (`onDocumentUpdate_correct`).

use crate::support;
use defra_harness::TestCluster;
use serde_json::Value;

fn count(v: &Value) -> usize {
    v["User"].as_array().map(|a| a.len()).unwrap_or(0)
}

#[tokio::test]
async fn index_no_stale_no_missing() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_rust_binary(support::release_binary())
        .build()
        .await
        .expect("build single-node cluster");
    let node = cluster.client(0);
    node.schema_add("type User { name: String @index  age: Int }")
        .expect("schema with @index");

    let created = node
        .query(r#"mutation { add_User(input: {name: "Alice", age: 30}) { _docID } }"#)
        .expect("create");
    let doc_id = created["add_User"][0]["_docID"]
        .as_str()
        .expect("_docID")
        .to_string();

    let by = |n: &str| {
        format!(r#"query {{ User(filter: {{name: {{_eq: "{n}"}}}}) {{ _docID name }} }}"#)
    };

    // Present after create.
    assert_eq!(
        count(&node.query(&by("Alice")).expect("lookup Alice")),
        1,
        "indexed lookup must find the document after create"
    );

    // Update the indexed field: old key must have NO stale entry; new key present.
    node.query(&format!(
        r#"mutation {{ update_User(docID: "{doc_id}", input: {{name: "Alicia"}}) {{ _docID }} }}"#
    ))
    .expect("update");
    assert_eq!(
        count(&node.query(&by("Alice")).expect("lookup old key")),
        0,
        "old indexed key must have no stale entry after update"
    );
    assert_eq!(
        count(&node.query(&by("Alicia")).expect("lookup new key")),
        1,
        "new indexed key must be present after update (none missing)"
    );

    // Delete: the index entry must be gone.
    node.query(&format!(
        r#"mutation {{ delete_User(docID: "{doc_id}") {{ _docID }} }}"#
    ))
    .expect("delete");
    assert_eq!(
        count(&node.query(&by("Alicia")).expect("lookup after delete")),
        0,
        "deleted document must leave no index entry"
    );
}
