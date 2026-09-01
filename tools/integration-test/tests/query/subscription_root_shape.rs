//! What a subscription's root selection may be, and what each shape does.
//!
//! The spec allows a subscription exactly one root field (5.2.3.1), and the
//! rule is about the selection *after* fragments are expanded. A root reached
//! through a fragment -- named or inline -- therefore selects one field, is
//! legal GraphQL, and has to deliver like any other subscription.
//!
//! A root resolving to no field or to several is not a subscription and has
//! to be refused. Where it is refused is the point of these tests: refusal
//! must happen before the stream opens, because a document rejected per event
//! from inside an already-open stream has nowhere to report it but the log,
//! leaving the caller holding a healthy `text/event-stream` that never fires.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use integration_test::TestCluster;

const SCHEMA: &str = "type User { name: String }  type Device { label: String }";

/// Legal GraphQL: one root field, reached through a named fragment.
const NAMED_FRAGMENT_ROOT: &str =
    "subscription { ...F } fragment F on Subscription { User { name } }";

/// The same, through an inline fragment.
const INLINE_FRAGMENT_ROOT: &str = "subscription { ... on Subscription { User { name } } }";

/// Two root fields once the fragment is expanded: still not a subscription.
const FRAGMENT_TO_TWO_FIELDS: &str =
    "subscription { ...F } fragment F on Subscription { User { name } Device { label } }";

/// Rejected by the parser, long before a stream is opened.
const MULTI_ROOT: &str = "subscription { User { name } Device { label } }";

const SINGLE_ROOT: &str = "subscription { User { name } }";

/// How long to wait for an event that should have arrived promptly.
const DELIVERY_WINDOW: Duration = Duration::from_secs(5);

async fn node() -> TestCluster {
    let cluster = TestCluster::builder().rust_nodes(1).build().await.unwrap();
    cluster.client(0).schema_add(SCHEMA).unwrap();
    cluster
}

async fn subscribe(cluster: &TestCluster, query: &str) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("{}/api/v0/graphql", cluster.api_url(0)))
        .header("Accept", "text/event-stream")
        .json(&serde_json::json!({ "query": query }))
        .send()
        .await
        .expect("subscription request")
}

#[tokio::test]
async fn a_named_fragment_root_delivers_like_a_plain_one() {
    assert_delivers(NAMED_FRAGMENT_ROOT).await;
}

#[tokio::test]
async fn an_inline_fragment_root_delivers_like_a_plain_one() {
    assert_delivers(INLINE_FRAGMENT_ROOT).await;
}

async fn assert_delivers(query: &str) {
    let cluster = node().await;
    let response = subscribe(&cluster, query).await;

    let status = response.status();
    assert!(
        status.is_success(),
        "a legal subscription was refused with {status}: {query}"
    );

    let delivered = deliveries_after_a_matching_write(&cluster, response).await;
    assert!(
        delivered > 0,
        "the node opened a stream and then delivered {delivered} events in \
         {DELIVERY_WINDOW:?} after a matching write: {query}"
    );
}

/// A root that resolves to two fields is not a subscription. The parser
/// catches it -- expanding fragments as it goes, so both spellings land in the
/// same place -- and reports it as a GraphQL error, which is a `200` with a
/// JSON body rather than a stream. Held here so this path stays distinct from
/// the roots that legitimately open one.
#[tokio::test]
async fn a_root_resolving_to_two_fields_is_reported() {
    for query in [MULTI_ROOT, FRAGMENT_TO_TWO_FIELDS] {
        let cluster = node().await;
        let response = subscribe(&cluster, query).await;

        let status = response.status();
        let body: serde_json::Value = response.json().await.expect("a JSON error body");
        let rendered = body.to_string();

        assert!(
            !status.is_server_error(),
            "expected the caller to be told, got {status}: {rendered} for {query}"
        );
        assert!(
            rendered.contains("exactly one root field"),
            "expected the reason to name the problem, got {rendered} for {query}"
        );
    }
}

/// The control: the same apparatus, a subscription the node accepts, events
/// arrive. Without it, "0 events" above could as easily be a broken test.
#[tokio::test]
async fn single_root_subscription_delivers_as_a_control() {
    let cluster = node().await;
    let response = subscribe(&cluster, SINGLE_ROOT).await;
    assert!(response.status().is_success());

    let delivered = deliveries_after_a_matching_write(&cluster, response).await;
    assert!(
        delivered > 0,
        "an accepted subscription delivered nothing either, so the measurement \
         is at fault rather than the node"
    );
}

/// Count `next` frames arriving on `response` after writing a document the
/// subscription covers.
async fn deliveries_after_a_matching_write(
    cluster: &TestCluster,
    response: reqwest::Response,
) -> usize {
    let delivered = Arc::new(AtomicUsize::new(0));
    let counter = delivered.clone();

    let reader = tokio::spawn(async move {
        use futures::StreamExt;
        let mut stream = response.bytes_stream();
        let mut buf = String::new();
        while let Some(Ok(chunk)) = stream.next().await {
            buf.push_str(&String::from_utf8_lossy(&chunk));
            while let Some(end) = buf.find("\n\n") {
                let frame: String = buf.drain(..end + 2).collect();
                if frame.contains("event: next") {
                    counter.fetch_add(1, Ordering::SeqCst);
                }
            }
        }
    });

    // The node registers the subscription before returning its headers, but
    // give the reader a moment to be polled before there is anything to read.
    tokio::time::sleep(Duration::from_millis(500)).await;
    cluster
        .client(0)
        .query(r#"mutation { add_User(input: {name: "Alice"}) { _docID } }"#)
        .expect("write a document the subscription covers");
    tokio::time::sleep(DELIVERY_WINDOW).await;

    reader.abort();
    delivered.load(Ordering::SeqCst)
}
