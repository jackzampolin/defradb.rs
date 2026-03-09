//! Iroh P2P document subscription management tests.
//!
//! Tests add/list/delete of P2P document subscriptions using the iroh transport.
//! Run with:
//!   cargo test -p integration-test --test p2p_iroh_document -- --ignored

use std::time::Duration;

use integration_test::TestCluster;
use serde_json::Value;
use serial_test::serial;

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

/// P2P document subscription lifecycle: add, list, delete.
#[tokio::test]
#[serial]
async fn iroh_document_subscription() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_iroh_transport()
        .build()
        .await
        .unwrap();

    cluster
        .wait_for_log(0, "p2p_listening", Duration::from_secs(15))
        .await
        .expect("P2P listener did not start");

    let node = cluster.client(0);

    node.schema_add("type Note { text: String }")
        .expect("add schema");

    // Document list starts empty
    let list_empty = node.p2p_document_list().expect("p2p_document_list empty");
    let ids_empty = extract_doc_ids(&list_empty);
    assert!(ids_empty.is_empty(), "expected 0 P2P documents initially");

    // Create a document to get a valid doc ID
    let result = node
        .query(r#"mutation { add_Note(input: {text: "hello world"}) { _docID } }"#)
        .expect("create note");
    let doc_id = result["add_Note"]
        .as_array()
        .and_then(|arr| arr.first())
        .or_else(|| result["add_Note"].as_object().map(|_| &result["add_Note"]))
        .and_then(|v| v.get("_docID"))
        .and_then(|v| v.as_str())
        .expect("could not extract _docID");

    // Add document to P2P subscription
    node.p2p_document_add(&[doc_id]).expect("p2p_document_add");

    // Verify it appears in the list
    let list_after = node
        .p2p_document_list()
        .expect("p2p_document_list after add");
    let ids_after = extract_doc_ids(&list_after);
    assert!(
        ids_after.iter().any(|id| id == doc_id),
        "expected doc {} in list, got {:?}",
        doc_id,
        ids_after
    );

    // Remove document from P2P subscription
    node.p2p_document_delete(&[doc_id])
        .expect("p2p_document_delete");

    // Verify it's gone
    let list_final = node
        .p2p_document_list()
        .expect("p2p_document_list after delete");
    let ids_final = extract_doc_ids(&list_final);
    assert!(
        ids_final.is_empty(),
        "expected 0 P2P documents after delete, got {:?}",
        ids_final
    );
}

/// Multiple document subscriptions can be managed concurrently.
#[tokio::test]
#[serial]
async fn iroh_multi_document_subscription() {
    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_iroh_transport()
        .build()
        .await
        .unwrap();

    cluster
        .wait_for_log(0, "p2p_listening", Duration::from_secs(15))
        .await
        .expect("P2P listener did not start");

    let node = cluster.client(0);
    node.schema_add("type Task { title: String }")
        .expect("add schema");

    // Create 3 documents
    let mut doc_ids = Vec::new();
    for title in &["alpha", "beta", "gamma"] {
        let result = node
            .query(&format!(
                r#"mutation {{ add_Task(input: {{title: "{}"}}) {{ _docID }} }}"#,
                title
            ))
            .expect("create task");
        let doc_id = result["add_Task"]
            .as_array()
            .and_then(|arr| arr.first())
            .or_else(|| result["add_Task"].as_object().map(|_| &result["add_Task"]))
            .and_then(|v| v.get("_docID"))
            .and_then(|v| v.as_str())
            .expect("could not extract _docID")
            .to_string();
        doc_ids.push(doc_id);
    }

    // Add all 3
    let refs: Vec<&str> = doc_ids.iter().map(|s| s.as_str()).collect();
    node.p2p_document_add(&refs)
        .expect("p2p_document_add batch");

    // Verify all 3 appear
    let list = node.p2p_document_list().expect("p2p_document_list");
    let ids = extract_doc_ids(&list);
    assert_eq!(ids.len(), 3, "expected exactly 3 documents, got {:?}", ids);
    for doc_id in &doc_ids {
        assert!(
            ids.contains(doc_id),
            "expected doc {} in list, got {:?}",
            doc_id,
            ids
        );
    }

    // Remove just the middle one
    node.p2p_document_delete(&[&doc_ids[1]])
        .expect("p2p_document_delete middle");

    let list_after = node
        .p2p_document_list()
        .expect("p2p_document_list after partial delete");
    let ids_after = extract_doc_ids(&list_after);
    assert_eq!(
        ids_after.len(),
        2,
        "expected exactly 2 remaining documents, got {:?}",
        ids_after
    );
    assert!(
        !ids_after.contains(&doc_ids[1]),
        "deleted doc {} should not be in list, got {:?}",
        doc_ids[1],
        ids_after
    );
    assert!(
        ids_after.contains(&doc_ids[0]),
        "first doc {} should still be in list, got {:?}",
        doc_ids[0],
        ids_after
    );
    assert!(
        ids_after.contains(&doc_ids[2]),
        "third doc {} should still be in list, got {:?}",
        doc_ids[2],
        ids_after
    );
}
