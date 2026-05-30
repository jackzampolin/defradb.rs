use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use integration_test::{for_each_runtime, TestCluster};
use serde_json::Value;
use tokio::task::JoinHandle;

/// Open an SSE subscription against the node's GraphQL endpoint.
///
/// Returns a background task handle and a shared vec that collects SSE `next` event payloads.
fn open_subscription(api_url: &str, query: &str) -> (JoinHandle<()>, Arc<Mutex<Vec<Value>>>) {
    let url = format!("{}/api/v0/graphql", api_url);
    let body = serde_json::json!({ "query": query });
    let events: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let events_clone = events.clone();

    let handle = tokio::spawn(async move {
        let client = reqwest::Client::new();
        let resp = client
            .post(&url)
            .header("Accept", "text/event-stream")
            .json(&body)
            .send()
            .await
            .expect("SSE request failed");

        let mut buf = String::new();
        let mut stream = resp.bytes_stream();
        use futures::StreamExt;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.expect("SSE chunk error");
            buf.push_str(&String::from_utf8_lossy(&chunk));

            while let Some(pos) = buf.find("\n\n") {
                let block = buf[..pos].to_string();
                buf = buf[pos + 2..].to_string();

                let mut event_type = String::new();
                let mut data = String::new();
                for line in block.lines() {
                    if let Some(rest) = line.strip_prefix("event:") {
                        event_type = rest.trim().to_string();
                    } else if let Some(rest) = line.strip_prefix("data:") {
                        data = rest.trim().to_string();
                    }
                }
                if event_type == "next" {
                    if let Ok(val) = serde_json::from_str::<Value>(&data) {
                        events_clone.lock().unwrap().push(val);
                    }
                }
            }
        }
    });

    (handle, events)
}

/// Extract the docID from a subscription event payload.
/// Handles both `data.Collection[0]._docID` and `Collection[0]._docID` layouts.
fn extract_doc_id(event: &Value, collection: &str) -> Option<String> {
    let paths = [
        format!("/data/{}/0/_docID", collection),
        format!("/{}/0/_docID", collection),
    ];
    for path in &paths {
        if let Some(id) = event.pointer(path).and_then(|v| v.as_str()) {
            return Some(id.to_string());
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Test: collection subscription filtered by docID
// ---------------------------------------------------------------------------

async fn subscription_docid_filter_test(cluster: TestCluster) {
    let client = cluster.client(0);
    let api_url = cluster.api_url(0);

    client
        .schema_add("type User { name: String  age: Int }")
        .expect("schema deploy");

    let alice = client
        .query(r#"mutation { add_User(input: {name: "Alice", age: 30}) { _docID } }"#)
        .expect("create Alice");
    let alice_id = alice["add_User"][0]["_docID"]
        .as_str()
        .expect("Alice _docID")
        .to_string();

    let bob = client
        .query(r#"mutation { add_User(input: {name: "Bob", age: 25}) { _docID } }"#)
        .expect("create Bob");
    let bob_id = bob["add_User"][0]["_docID"]
        .as_str()
        .expect("Bob _docID")
        .to_string();

    let sub_query = format!(
        r#"subscription {{ User(docID: "{}") {{ _docID name }} }}"#,
        alice_id
    );
    let (handle, events) = open_subscription(api_url, &sub_query);

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Mutate Bob (should be filtered out)
    client
        .query(r#"mutation { update_User(filter: {name: {_eq: "Bob"}}, input: {age: 26}) { _docID } }"#)
        .expect("update Bob");

    // Mutate Alice (should trigger subscription)
    client
        .query(r#"mutation { update_User(filter: {name: {_eq: "Alice"}}, input: {age: 31}) { _docID } }"#)
        .expect("update Alice");

    // Mutate Bob again (should be filtered out)
    client
        .query(r#"mutation { update_User(filter: {name: {_eq: "Bob"}}, input: {age: 27}) { _docID } }"#)
        .expect("update Bob again");

    tokio::time::sleep(Duration::from_secs(2)).await;
    handle.abort();

    let collected = events.lock().unwrap();
    assert!(
        !collected.is_empty(),
        "expected at least 1 subscription event for Alice, got 0"
    );

    for (i, event) in collected.iter().enumerate() {
        let doc_id = extract_doc_id(event, "User")
            .unwrap_or_else(|| panic!("event {} missing _docID: {:?}", i, event));
        assert_eq!(
            doc_id, alice_id,
            "event {} has wrong docID: got {}, want {} (Alice). Bob {} should be filtered.",
            i, doc_id, alice_id, bob_id
        );
    }
}

for_each_runtime!(subscription_docid_filter, subscription_docid_filter_test);

// ---------------------------------------------------------------------------
// Test: _commits subscription filtered by docID
// ---------------------------------------------------------------------------

async fn commit_subscription_docid_filter_test(cluster: TestCluster) {
    let client = cluster.client(0);
    let api_url = cluster.api_url(0);

    client
        .schema_add("type User { name: String  age: Int }")
        .expect("schema deploy");

    let target = client
        .query(r#"mutation { add_User(input: {name: "Target", age: 40}) { _docID } }"#)
        .expect("create target");
    let target_id = target["add_User"][0]["_docID"]
        .as_str()
        .expect("target _docID")
        .to_string();

    let other = client
        .query(r#"mutation { add_User(input: {name: "Other", age: 50}) { _docID } }"#)
        .expect("create other");
    let other_id = other["add_User"][0]["_docID"]
        .as_str()
        .expect("other _docID")
        .to_string();

    let sub_query = format!(
        r#"subscription {{ _commits(docID: "{}") {{ cid docID }} }}"#,
        target_id
    );
    let (handle, events) = open_subscription(api_url, &sub_query);

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Mutate the non-target (should be filtered)
    client
        .query(r#"mutation { update_User(filter: {name: {_eq: "Other"}}, input: {age: 51}) { _docID } }"#)
        .expect("update other");

    // Mutate the target (should trigger)
    client
        .query(r#"mutation { update_User(filter: {name: {_eq: "Target"}}, input: {age: 41}) { _docID } }"#)
        .expect("update target");

    tokio::time::sleep(Duration::from_secs(2)).await;
    handle.abort();

    let collected = events.lock().unwrap();
    assert!(
        !collected.is_empty(),
        "expected at least 1 commit event for target, got 0"
    );

    for (i, event) in collected.iter().enumerate() {
        let doc_id = event
            .pointer("/data/_commits/0/docID")
            .or_else(|| event.pointer("/_commits/0/docID"))
            .and_then(|v| v.as_str())
            .unwrap_or_else(|| panic!("commit event {} missing docID: {:?}", i, event));
        assert_eq!(
            doc_id, target_id,
            "commit event {} has wrong docID: got {}, want {} (target). Other {} should be filtered.",
            i, doc_id, target_id, other_id
        );
    }
}

#[tokio::test]
async fn rust_commit_subscription_docid_filter() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    commit_subscription_docid_filter_test(cluster).await;
}

#[tokio::test]
async fn go_commit_subscription_docid_filter() {
    let cluster = TestCluster::builder().go_nodes(1).build().await.unwrap();
    commit_subscription_docid_filter_test(cluster).await;
}

// ---------------------------------------------------------------------------
// Test: unfiltered subscription receives events for ALL documents
// ---------------------------------------------------------------------------

async fn subscription_no_filter_test(cluster: TestCluster) {
    let client = cluster.client(0);
    let api_url = cluster.api_url(0);

    client
        .schema_add("type User { name: String  age: Int }")
        .expect("schema deploy");

    let alice = client
        .query(r#"mutation { add_User(input: {name: "Alice", age: 30}) { _docID } }"#)
        .expect("create Alice");
    let alice_id = alice["add_User"][0]["_docID"]
        .as_str()
        .expect("Alice _docID")
        .to_string();

    let bob = client
        .query(r#"mutation { add_User(input: {name: "Bob", age: 25}) { _docID } }"#)
        .expect("create Bob");
    let bob_id = bob["add_User"][0]["_docID"]
        .as_str()
        .expect("Bob _docID")
        .to_string();

    let (handle, events) = open_subscription(api_url, "subscription { User { _docID name } }");

    tokio::time::sleep(Duration::from_millis(500)).await;

    client
        .query(r#"mutation { update_User(filter: {name: {_eq: "Alice"}}, input: {age: 31}) { _docID } }"#)
        .expect("update Alice");

    client
        .query(r#"mutation { update_User(filter: {name: {_eq: "Bob"}}, input: {age: 26}) { _docID } }"#)
        .expect("update Bob");

    tokio::time::sleep(Duration::from_secs(2)).await;
    handle.abort();

    let collected = events.lock().unwrap();
    assert!(
        collected.len() >= 2,
        "expected at least 2 events (one per document), got {}: {:?}",
        collected.len(),
        &*collected
    );

    let seen_ids: HashSet<String> = collected
        .iter()
        .filter_map(|e| extract_doc_id(e, "User"))
        .collect();
    assert!(
        seen_ids.contains(&alice_id),
        "unfiltered subscription should receive Alice events"
    );
    assert!(
        seen_ids.contains(&bob_id),
        "unfiltered subscription should receive Bob events"
    );
}

for_each_runtime!(subscription_no_filter, subscription_no_filter_test);

// ---------------------------------------------------------------------------
// Test: delete subscriptions are scoped by event CID
// ---------------------------------------------------------------------------

async fn subscription_delete_event_scoped_by_cid_test(cluster: TestCluster) {
    let client = cluster.client(0);
    let api_url = cluster.api_url(0);

    client
        .schema_add("type User { name: String  age: Int }")
        .expect("schema deploy");

    let created = client
        .query(r#"mutation { add_User(input: {name: "DeleteMe", age: 30}) { _docID } }"#)
        .expect("create user");
    let doc_id = created["add_User"][0]["_docID"]
        .as_str()
        .expect("user _docID")
        .to_string();

    let (handle, events) =
        open_subscription(api_url, "subscription { User { _docID _deleted name } }");

    tokio::time::sleep(Duration::from_millis(500)).await;

    client
        .query(&format!(
            r#"mutation {{ delete_User(docID: "{doc_id}") {{ _docID }} }}"#
        ))
        .expect("delete user");

    tokio::time::sleep(Duration::from_secs(2)).await;
    handle.abort();

    let collected = events.lock().unwrap();
    assert!(
        collected.iter().any(|event| {
            extract_doc_id(event, "User").as_deref() == Some(doc_id.as_str())
                && event
                    .pointer("/data/User/0/_deleted")
                    .or_else(|| event.pointer("/User/0/_deleted"))
                    .and_then(Value::as_bool)
                    == Some(true)
        }),
        "expected delete subscription event with _deleted=true for {doc_id}, got: {:?}",
        &*collected
    );
}

#[tokio::test]
async fn rust_subscription_delete_event_scoped_by_cid() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    subscription_delete_event_scoped_by_cid_test(cluster).await;
}
