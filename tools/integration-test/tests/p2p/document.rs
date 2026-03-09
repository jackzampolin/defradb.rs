use integration_test::{for_each_runtime, TestCluster};
use serde_json::Value;
use std::time::Duration;

/// Extract document IDs from list output, handling both flat array `["id1", ...]`
/// and wrapped object formats.
fn extract_doc_ids(val: &Value) -> Vec<String> {
    if let Some(arr) = val.as_array() {
        return arr
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect();
    }
    if let Some(obj) = val.as_object() {
        for v in obj.values() {
            if let Some(arr) = v.as_array() {
                return arr
                    .iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect();
            }
        }
    }
    vec![]
}

async fn p2p_document_test(cluster: TestCluster) {
    let node = cluster.client(0);

    // Wait for P2P listener to be ready
    cluster
        .wait_for_log(0, "p2p_listening", Duration::from_secs(15))
        .await
        .expect("P2P listener did not start");

    // Deploy schema
    node.schema_add("type Note { text: String }")
        .expect("add Note schema");

    // 1. List documents on fresh node — should be empty
    let list_empty = node.p2p_document_list().expect("p2p_document_list empty");
    let ids_empty = extract_doc_ids(&list_empty);
    assert!(
        ids_empty.is_empty(),
        "expected 0 P2P documents initially, got {:?}",
        ids_empty
    );

    // 2. Create a real document to get a valid doc ID
    let result = node
        .query(r#"mutation { add_Note(input: {text: "hello world"}) { _docID } }"#)
        .expect("create note");
    let doc_id = result["add_Note"]
        .as_array()
        .and_then(|arr| arr.first())
        .or_else(|| result["add_Note"].as_object().map(|_| &result["add_Note"]))
        .and_then(|v| v.get("_docID"))
        .and_then(|v| v.as_str())
        .expect("could not extract _docID from mutation result");

    // 3. Add document to P2P subscription
    node.p2p_document_add(&[doc_id]).expect("p2p_document_add");

    // 4. Verify document appears in list
    let list_after_add = node
        .p2p_document_list()
        .expect("p2p_document_list after add");
    let ids_after_add = extract_doc_ids(&list_after_add);
    assert!(
        ids_after_add.iter().any(|id| id == doc_id),
        "expected doc {} in P2P document list, got {:?}",
        doc_id,
        ids_after_add
    );

    // 5. Remove document from P2P subscription
    node.p2p_document_delete(&[doc_id])
        .expect("p2p_document_delete");

    // 6. List after delete — should be empty
    let list_after_del = node
        .p2p_document_list()
        .expect("p2p_document_list after delete");
    let ids_after_del = extract_doc_ids(&list_after_del);
    assert!(
        ids_after_del.is_empty(),
        "expected 0 P2P documents after delete, got {:?}",
        ids_after_del
    );
}

for_each_runtime!(p2p_document, p2p_document_test, .with_p2p());
