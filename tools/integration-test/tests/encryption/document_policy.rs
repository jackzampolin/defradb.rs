use base64::Engine;
use integration_test::{for_each_runtime, TestCluster};
use serde_json::Value;

const SECRET: &str = "ssn 123-45-6789";

fn doc_id_of(data: &Value, mutation: &str) -> String {
    data[mutation]
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|v| v["_docID"].as_str())
        .or_else(|| data[mutation]["_docID"].as_str())
        .expect("missing _docID")
        .to_string()
}

/// The raw delta bytes of the newest commit for `field`.
///
/// `fieldName` is a commit field rather than a `_commits` argument, so the
/// filtering happens here.
fn newest_delta(node: &integration_test::DefraClient, doc_id: &str, field: &str) -> Vec<u8> {
    let query = format!(
        r#"query {{ _commits(docID: "{}") {{ height fieldName delta }} }}"#,
        doc_id
    );
    let result = node.query(&query).expect("commits query");
    let commits = result
        .get("_commits")
        .or_else(|| result.get("commits"))
        .and_then(|v| v.as_array())
        .unwrap_or_else(|| panic!("no commits array in {}", result));

    let newest = commits
        .iter()
        .filter(|c| c["fieldName"].as_str() == Some(field))
        .max_by_key(|c| c["height"].as_i64().unwrap_or(0))
        .unwrap_or_else(|| panic!("no commits for field `{}` in {}", field, result));

    let delta = newest["delta"].as_str().expect("commit delta");
    base64::engine::general_purpose::STANDARD
        .decode(delta)
        .expect("delta is base64")
}

/// A field introduced by an update on a document encrypted as a whole must be
/// stored as ciphertext, across separate requests — the create and the update
/// run in different transactions, so nothing in-process carries the policy
/// between them.
///
/// This is the end-to-end form of `db-blocks`'
/// `field_added_by_update_inherits_document_encryption`, and Rust-only on
/// purpose: run against a Go node this same body fails, because Go derives
/// encryption from the field's own heads (`internal/core/block/store.go`) and a
/// field introduced by an update has none. Asserting Go's behaviour here would
/// pin an upstream defect, so the divergence is documented rather than tested.
async fn document_policy_covers_new_field_test(cluster: TestCluster) {
    let node = cluster.client(0);

    node.schema_add("type Vault { name: String, notes: String }")
        .expect("add Vault schema");

    let created = node
        .query(r#"mutation { add_Vault(input: {name: "Alice"}, encrypt: true) { _docID } }"#)
        .expect("create encrypted document");
    let doc_id = doc_id_of(&created, "add_Vault");

    node.query(&format!(
        r#"mutation {{ update_Vault(docID: "{}", input: {{notes: "{}"}}) {{ _docID }} }}"#,
        doc_id, SECRET
    ))
    .expect("update adding a new field");

    let delta = newest_delta(&node, &doc_id, "notes");
    assert!(
        !delta.windows(SECRET.len()).any(|w| w == SECRET.as_bytes()),
        "`notes` was introduced by an update on an `encrypt: true` document and \
         must be ciphertext, but its delta contains the plaintext value"
    );

    // The value is still readable through the normal query path.
    let read = node
        .query(&format!(
            r#"query {{ Vault(docID: "{}") {{ notes }} }}"#,
            doc_id
        ))
        .expect("read back the document");
    assert_eq!(
        read["Vault"][0]["notes"].as_str(),
        Some(SECRET),
        "the encrypted field must still decrypt for an authorized reader"
    );
}

/// The mirror case: an unencrypted document must not acquire encryption when a
/// field is added to it.
async fn unencrypted_document_new_field_stays_plaintext_test(cluster: TestCluster) {
    let node = cluster.client(0);

    node.schema_add("type Ledger { name: String, notes: String }")
        .expect("add Ledger schema");

    let created = node
        .query(r#"mutation { add_Ledger(input: {name: "Alice"}) { _docID } }"#)
        .expect("create plaintext document");
    let doc_id = doc_id_of(&created, "add_Ledger");

    node.query(&format!(
        r#"mutation {{ update_Ledger(docID: "{}", input: {{notes: "public note"}}) {{ _docID }} }}"#,
        doc_id
    ))
    .expect("update adding a new field");

    let delta = newest_delta(&node, &doc_id, "notes");
    assert!(
        delta
            .windows("public note".len())
            .any(|w| w == b"public note"),
        "an unencrypted document must not acquire encryption from a new field"
    );
}

#[tokio::test]
async fn rust_document_policy_covers_new_field() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .build()
        .await
        .expect("build rust cluster");
    document_policy_covers_new_field_test(cluster).await;
}

for_each_runtime!(
    unencrypted_document_new_field_stays_plaintext,
    unencrypted_document_new_field_stays_plaintext_test
);
