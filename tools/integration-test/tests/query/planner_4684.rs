use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::StreamExt;
use integration_test::TestCluster;
use serde_json::Value;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

fn subscription_timeout() -> Duration {
    std::env::var("DEFRADB_TEST_SUBSCRIPTION_TIMEOUT_MS")
        .ok()
        .and_then(|value| value.parse().ok())
        .map(Duration::from_millis)
        .unwrap_or(Duration::from_secs(5))
}

fn subscription_settle_timeout() -> Duration {
    std::env::var("DEFRADB_TEST_SUBSCRIPTION_SETTLE_MS")
        .ok()
        .and_then(|value| value.parse().ok())
        .map(Duration::from_millis)
        .unwrap_or(Duration::from_millis(250))
}

fn open_subscription(
    api_url: &str,
    query: &str,
) -> (
    JoinHandle<()>,
    oneshot::Receiver<()>,
    Arc<Mutex<Vec<Value>>>,
) {
    let url = format!("{}/api/v0/graphql", api_url);
    let body = serde_json::json!({ "query": query });
    let events: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let events_clone = events.clone();
    let (ready_tx, ready_rx) = oneshot::channel();

    let handle = tokio::spawn(async move {
        let client = reqwest::Client::new();
        let response = client
            .post(&url)
            .header("Accept", "text/event-stream")
            .json(&body)
            .send()
            .await
            .expect("SSE request failed")
            .error_for_status()
            .expect("SSE request returned error status");
        let _ = ready_tx.send(());

        let mut buf = String::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.expect("SSE chunk error");
            buf.push_str(&String::from_utf8_lossy(&chunk));

            while let Some(pos) = buf.find("\n\n") {
                let block = buf[..pos].to_string();
                buf = buf[pos + 2..].to_string();

                let mut event_type = "";
                let mut data = "";
                for line in block.lines() {
                    if let Some(rest) = line.strip_prefix("event:") {
                        event_type = rest.trim();
                    } else if let Some(rest) = line.strip_prefix("data:") {
                        data = rest.trim();
                    }
                }

                if event_type == "next" {
                    if let Ok(value) = serde_json::from_str::<Value>(data) {
                        events_clone.lock().unwrap().push(value);
                    }
                }
            }
        }
    });

    (handle, ready_rx, events)
}

async fn wait_for_exact_subscription_events(events: &Arc<Mutex<Vec<Value>>>, expected_len: usize) {
    let timeout = subscription_timeout();
    let deadline = Instant::now() + timeout;
    let settle_timeout = subscription_settle_timeout();
    let mut expected_since = None;

    loop {
        let current_len = events.lock().unwrap().len();
        if current_len == expected_len {
            let since = expected_since.get_or_insert_with(Instant::now);
            if since.elapsed() >= settle_timeout {
                return;
            }
        } else {
            expected_since = None;
            if current_len > expected_len {
                let collected = events.lock().unwrap();
                panic!("expected {expected_len} subscription events, got {collected:?}");
            }
        }
        if Instant::now() >= deadline {
            let collected = events.lock().unwrap();
            panic!(
                "timed out after {timeout:?} waiting for {expected_len} subscription events, got {collected:?}"
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn subscription_with_indexed_filter_test(cluster: TestCluster) {
    let node = cluster.client(0);
    let api_url = cluster.api_url(0);

    node.schema_add(
        r#"
        type User {
            name: String
            age: Int @index
        }
        "#,
    )
    .expect("add schema");

    let sub_query = r#"
        subscription {
            User(filter: {age: {_lt: 30}}) {
                _docID
                name
                age
            }
        }
    "#;
    let (handle, ready, events) = open_subscription(api_url, sub_query);
    tokio::time::timeout(subscription_timeout(), ready)
        .await
        .expect("subscription did not open before timeout")
        .expect("subscription task ended before opening");

    node.query(r#"mutation { add_User(input: {name: "John", age: 27}) { _docID } }"#)
        .expect("add matching user");
    node.query(r#"mutation { add_User(input: {name: "Addo", age: 31}) { _docID } }"#)
        .expect("add nonmatching user");

    wait_for_exact_subscription_events(&events, 1).await;
    handle.abort();

    let collected = events.lock().unwrap();
    assert_eq!(
        collected.len(),
        1,
        "expected one indexed-filter subscription event, got {collected:?}"
    );
    let user = collected[0]
        .pointer("/data/User/0")
        .or_else(|| collected[0].pointer("/User/0"))
        .unwrap_or_else(|| {
            panic!(
                "subscription event missing User payload: {:?}",
                collected[0]
            )
        });
    assert_eq!(user["name"], "John");
    assert_eq!(user["age"], 27);
}

async fn inverted_one_to_many_parent_filter_with_child_order_test(cluster: TestCluster) {
    let node = cluster.client(0);

    node.schema_add(
        r#"
        type Author {
            name: String
            age: Int
            published: [Book]
        }

        type Book {
            title: String
            rating: Float @index
            author: Author
        }
        "#,
    )
    .expect("add schema");

    let alice = node
        .query(r#"mutation { add_Author(input: {name: "Alice", age: 41}) { _docID } }"#)
        .expect("add Alice");
    let alice_id = alice["add_Author"][0]["_docID"].as_str().unwrap();
    let bob = node
        .query(r#"mutation { add_Author(input: {name: "Bob", age: 42}) { _docID } }"#)
        .expect("add Bob");
    let bob_id = bob["add_Author"][0]["_docID"].as_str().unwrap();

    node.query(&format!(
        r#"mutation {{ add_Book(input: {{title: "Book A1", rating: 4.8, author: "{}"}}) {{ _docID }} }}"#,
        alice_id
    ))
    .expect("add Alice book 1");
    node.query(&format!(
        r#"mutation {{ add_Book(input: {{title: "Book A2", rating: 3.5, author: "{}"}}) {{ _docID }} }}"#,
        alice_id
    ))
    .expect("add Alice book 2");
    node.query(&format!(
        r#"mutation {{ add_Book(input: {{title: "Book B1", rating: 4.0, author: "{}"}}) {{ _docID }} }}"#,
        bob_id
    ))
    .expect("add Bob book 1");
    node.query(&format!(
        r#"mutation {{ add_Book(input: {{title: "Book B2", rating: 2.5, author: "{}"}}) {{ _docID }} }}"#,
        bob_id
    ))
    .expect("add Bob book 2");

    let result = node
        .query(
            r#"
            query {
                Author(
                    filter: {published: {rating: {_geq: 4.0}}}
                    order: {age: ASC}
                ) {
                    name
                    published(order: {rating: DESC}) {
                        title
                        rating
                    }
                }
            }
            "#,
        )
        .expect("query authors");

    assert_eq!(
        result["Author"],
        serde_json::json!([
            {
                "name": "Alice",
                "published": [
                    {"title": "Book A1", "rating": 4.8},
                    {"title": "Book A2", "rating": 3.5}
                ]
            },
            {
                "name": "Bob",
                "published": [
                    {"title": "Book B1", "rating": 4},
                    {"title": "Book B2", "rating": 2.5}
                ]
            }
        ])
    );
}

#[tokio::test]
async fn rust_subscription_with_indexed_filter_receives_matching_update() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    subscription_with_indexed_filter_test(cluster).await;
}

#[tokio::test]
async fn rust_inverted_one_to_many_parent_filter_with_child_order() {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    inverted_one_to_many_parent_filter_with_child_order_test(cluster).await;
}
