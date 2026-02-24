//! Iroh P2P peer event tests.
//!
//! Ported from Go: tests/integration/net/peer_events/
//!
//! These tests verify that peer join/leave events are emitted correctly
//! when peers connect, subscribe to collections, subscribe to documents,
//! and disconnect.
//!
//! Run with:
//!   cargo test -p integration-test --test p2p_iroh -- peer::events::

use std::time::Duration;

use integration_test::{extract_p2p_addr, open_peer_events_sse, wait_for_peer_events, TestCluster};
use serial_test::serial;

const SCHEMA: &str = "type Users { name: String  age: Int }";
const P2P_TIMEOUT: Duration = Duration::from_secs(15);
const EVENT_TIMEOUT: Duration = Duration::from_secs(10);

/// Helper: build a 2-node iroh cluster with schema deployed and P2P listening.
async fn setup_two_nodes() -> TestCluster {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_iroh_transport()
        .build()
        .await
        .unwrap();

    for i in 0..2 {
        cluster
            .wait_for_log(i, "p2p_listening", P2P_TIMEOUT)
            .await
            .unwrap_or_else(|_| panic!("node{} P2P listener did not start", i));
        cluster
            .client(i)
            .schema_add(SCHEMA)
            .unwrap_or_else(|_| panic!("add schema node{}", i));
    }
    cluster
}

/// Helper: build a 3-node iroh cluster with schema deployed and P2P listening.
async fn setup_three_nodes() -> TestCluster {
    let cluster = TestCluster::builder()
        .rust_nodes(3)
        .with_iroh_transport()
        .build()
        .await
        .unwrap();

    for i in 0..3 {
        cluster
            .wait_for_log(i, "p2p_listening", P2P_TIMEOUT)
            .await
            .unwrap_or_else(|_| panic!("node{} P2P listener did not start", i));
        cluster
            .client(i)
            .schema_add(SCHEMA)
            .unwrap_or_else(|_| panic!("add schema node{}", i));
    }
    cluster
}

/// Helper: connect node0 to node1 and return node1's peer_id.
fn connect_peers(cluster: &TestCluster) -> String {
    let addr1 = extract_p2p_addr(cluster, 1);
    cluster
        .client(0)
        .p2p_connect(&[&addr1])
        .expect("connect peers");
    // Extract peer ID from the address
    if let Some(pos) = addr1.rfind("/p2p/") {
        addr1[pos + 5..].to_string()
    } else {
        addr1
    }
}

/// Helper: extract a docID from a create mutation result.
fn extract_doc_id_from_create(result: &serde_json::Value, collection: &str) -> String {
    let key = format!("create_{}", collection);
    result[&key][0]["_docID"]
        .as_str()
        .unwrap_or_else(|| panic!("expected _docID in create result: {:?}", result))
        .to_string()
}

/// Helper: filter events by event_type.
fn events_with_type(events: &[serde_json::Value], event_type: &str) -> Vec<serde_json::Value> {
    events
        .iter()
        .filter(|e| e["data"]["event_type"].as_str() == Some(event_type))
        .cloned()
        .collect()
}

/// Port: TestPeerEvents_OnConnect_ShouldReceiveJoinEventOnDocSyncTopic
///
/// In iroh, peer events come from gossip topic subscriptions, not raw connections.
/// We connect peers and subscribe both to a collection to trigger the gossip join event.
#[tokio::test]
#[serial]
async fn on_connect_join_event_doc_sync_topic() {
    let cluster = setup_two_nodes().await;

    // Open SSE event stream on node0 before connecting
    let (handle, events) = open_peer_events_sse(cluster.api_url(0)).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Connect peers and subscribe to collection to trigger gossip topic join
    connect_peers(&cluster);
    cluster
        .client(0)
        .p2p_collection_add(&["Users"])
        .expect("collection add node0");
    cluster
        .client(1)
        .p2p_collection_add(&["Users"])
        .expect("collection add node1");

    // Wait for at least 1 JOINED event
    let collected = wait_for_peer_events(&events, 1, EVENT_TIMEOUT).await;
    handle.abort();

    let joined = events_with_type(&collected, "JOINED");
    assert!(
        !joined.is_empty(),
        "expected at least 1 JOINED event, got: {:?}",
        collected
    );
}

/// Port: TestPeerEvents_OnConnectBidirectional_BothNodesShouldReceiveJoinEvents
///
/// Both nodes see JOINED events when they share a gossip topic subscription.
#[tokio::test]
#[serial]
async fn on_connect_bidirectional_join_events() {
    let cluster = setup_two_nodes().await;

    // Open SSE event streams on both nodes
    let (handle0, events0) = open_peer_events_sse(cluster.api_url(0)).await;
    let (handle1, events1) = open_peer_events_sse(cluster.api_url(1)).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    connect_peers(&cluster);

    // Both nodes subscribe to collection to trigger gossip topic join
    cluster
        .client(0)
        .p2p_collection_add(&["Users"])
        .expect("collection add node0");
    cluster
        .client(1)
        .p2p_collection_add(&["Users"])
        .expect("collection add node1");

    // Both nodes should see JOINED events
    let collected0 = wait_for_peer_events(&events0, 1, EVENT_TIMEOUT).await;
    let collected1 = wait_for_peer_events(&events1, 1, EVENT_TIMEOUT).await;
    handle0.abort();
    handle1.abort();

    let joined0 = events_with_type(&collected0, "JOINED");
    let joined1 = events_with_type(&collected1, "JOINED");
    assert!(
        !joined0.is_empty(),
        "node0 should see JOINED events, got: {:?}",
        collected0
    );
    assert!(
        !joined1.is_empty(),
        "node1 should see JOINED events, got: {:?}",
        collected1
    );
}

/// Port: TestPeerEvents_OnConnectMultiplePeers_ShouldReceiveAllJoinEvents
///
/// All 3 peers subscribe to the same collection, node0 sees JOINED from both peers.
#[tokio::test]
#[serial]
async fn on_connect_multiple_peers_all_join_events() {
    let cluster = setup_three_nodes().await;

    // Open SSE on node0 (observer)
    let (handle, events) = open_peer_events_sse(cluster.api_url(0)).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Connect node0 to both node1 and node2
    let addr1 = extract_p2p_addr(&cluster, 1);
    let addr2 = extract_p2p_addr(&cluster, 2);
    cluster
        .client(0)
        .p2p_connect(&[&addr1])
        .expect("connect 0→1");
    cluster
        .client(0)
        .p2p_connect(&[&addr2])
        .expect("connect 0→2");

    // All nodes subscribe to collection to trigger gossip topic joins
    for i in 0..3 {
        cluster
            .client(i)
            .p2p_collection_add(&["Users"])
            .unwrap_or_else(|_| panic!("collection add node{}", i));
    }

    // Wait for at least 2 JOINED events (one per peer)
    let collected = wait_for_peer_events(&events, 2, EVENT_TIMEOUT).await;
    handle.abort();

    let joined = events_with_type(&collected, "JOINED");
    assert!(
        joined.len() >= 2,
        "expected at least 2 JOINED events from 2 peers, got {}: {:?}",
        joined.len(),
        collected
    );
}

/// Port: TestPeerEvents_OnSubscribeToCollection_ShouldReceiveJoinEventOnCollectionTopic
#[tokio::test]
#[serial]
async fn subscribe_collection_join_event() {
    let cluster = setup_two_nodes().await;

    // Connect peers first
    connect_peers(&cluster);
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Open SSE on node0
    let (handle, events) = open_peer_events_sse(cluster.api_url(0)).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Both nodes add collection subscription
    cluster
        .client(0)
        .p2p_collection_add(&["Users"])
        .expect("collection add node0");
    cluster
        .client(1)
        .p2p_collection_add(&["Users"])
        .expect("collection add node1");

    // Wait for JOINED event on collection topic
    let collected = wait_for_peer_events(&events, 1, EVENT_TIMEOUT).await;
    handle.abort();

    let joined = events_with_type(&collected, "JOINED");
    assert!(
        !joined.is_empty(),
        "expected JOINED event for collection subscription, got: {:?}",
        collected
    );
}

/// Port: TestPeerEvents_OnSubscribeToMultipleCollections_ShouldReceiveJoinEventsOnAllTopics
#[tokio::test]
#[serial]
async fn subscribe_multiple_collections_join_events() {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_iroh_transport()
        .build()
        .await
        .unwrap();

    for i in 0..2 {
        cluster
            .wait_for_log(i, "p2p_listening", P2P_TIMEOUT)
            .await
            .unwrap_or_else(|_| panic!("node{} P2P listener did not start", i));
        cluster
            .client(i)
            .schema_add("type Users { name: String  age: Int }  type Books { title: String }")
            .unwrap_or_else(|_| panic!("add schema node{}", i));
    }

    connect_peers(&cluster);
    tokio::time::sleep(Duration::from_millis(500)).await;

    let (handle, events) = open_peer_events_sse(cluster.api_url(0)).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Both nodes add both collections
    cluster
        .client(0)
        .p2p_collection_add(&["Users", "Books"])
        .expect("collection add node0");
    cluster
        .client(1)
        .p2p_collection_add(&["Users", "Books"])
        .expect("collection add node1");

    // Wait for at least 2 JOINED events (one per collection)
    let collected = wait_for_peer_events(&events, 2, EVENT_TIMEOUT).await;
    handle.abort();

    let joined = events_with_type(&collected, "JOINED");
    assert!(
        joined.len() >= 2,
        "expected at least 2 JOINED events for 2 collections, got {}: {:?}",
        joined.len(),
        collected
    );
}

/// Port: TestPeerEvents_MultipleNodesSubscribedToCollection_ShouldReceiveAllJoinEvents
#[tokio::test]
#[serial]
async fn multiple_nodes_subscribed_collection_join_events() {
    let cluster = setup_three_nodes().await;

    // Connect all to node0
    let addr1 = extract_p2p_addr(&cluster, 1);
    let addr2 = extract_p2p_addr(&cluster, 2);
    cluster
        .client(0)
        .p2p_connect(&[&addr1])
        .expect("connect 0→1");
    cluster
        .client(0)
        .p2p_connect(&[&addr2])
        .expect("connect 0→2");
    tokio::time::sleep(Duration::from_millis(500)).await;

    let (handle, events) = open_peer_events_sse(cluster.api_url(0)).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // All nodes subscribe to collection
    for i in 0..3 {
        cluster
            .client(i)
            .p2p_collection_add(&["Users"])
            .unwrap_or_else(|_| panic!("collection add node{}", i));
    }

    // Node0 should see JOINED from node1 and node2
    let collected = wait_for_peer_events(&events, 2, EVENT_TIMEOUT).await;
    handle.abort();

    let joined = events_with_type(&collected, "JOINED");
    assert!(
        joined.len() >= 2,
        "expected at least 2 JOINED events from 2 peers, got {}: {:?}",
        joined.len(),
        collected
    );
}

/// Port: TestPeerEvents_OnUnsubscribeFromCollection_ShouldReceiveLeftEvent
#[tokio::test]
#[serial]
async fn unsubscribe_collection_left_event() {
    let cluster = setup_two_nodes().await;

    connect_peers(&cluster);
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Both nodes add collection
    cluster
        .client(0)
        .p2p_collection_add(&["Users"])
        .expect("collection add node0");
    cluster
        .client(1)
        .p2p_collection_add(&["Users"])
        .expect("collection add node1");
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Open SSE on node0 to catch LEFT events
    let (handle, events) = open_peer_events_sse(cluster.api_url(0)).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // node1 removes collection subscription
    cluster
        .client(1)
        .p2p_collection_delete(&["Users"])
        .expect("collection delete node1");

    let collected = wait_for_peer_events(&events, 1, EVENT_TIMEOUT).await;
    handle.abort();

    let left = events_with_type(&collected, "LEFT");
    assert!(
        !left.is_empty(),
        "expected LEFT event for collection unsubscribe, got: {:?}",
        collected
    );
}

/// Port: TestPeerEvents_OnUnsubscribeFromMultipleCollections_ShouldReceiveLeftEvents
#[tokio::test]
#[serial]
async fn unsubscribe_multiple_collections_left_events() {
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_iroh_transport()
        .build()
        .await
        .unwrap();

    for i in 0..2 {
        cluster
            .wait_for_log(i, "p2p_listening", P2P_TIMEOUT)
            .await
            .unwrap_or_else(|_| panic!("node{} P2P listener did not start", i));
        cluster
            .client(i)
            .schema_add("type Users { name: String  age: Int }  type Books { title: String }")
            .unwrap_or_else(|_| panic!("add schema node{}", i));
    }

    connect_peers(&cluster);
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Both nodes subscribe to both collections
    cluster
        .client(0)
        .p2p_collection_add(&["Users", "Books"])
        .expect("collection add node0");
    cluster
        .client(1)
        .p2p_collection_add(&["Users", "Books"])
        .expect("collection add node1");
    tokio::time::sleep(Duration::from_millis(500)).await;

    let (handle, events) = open_peer_events_sse(cluster.api_url(0)).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // node1 removes both collections
    cluster
        .client(1)
        .p2p_collection_delete(&["Users", "Books"])
        .expect("collection delete node1");

    let collected = wait_for_peer_events(&events, 2, EVENT_TIMEOUT).await;
    handle.abort();

    let left = events_with_type(&collected, "LEFT");
    assert!(
        left.len() >= 2,
        "expected at least 2 LEFT events for 2 collection unsubscribes, got {}: {:?}",
        left.len(),
        collected
    );
}

/// Port: TestPeerEvents_OnSubscribeToDocument_ShouldReceiveJoinEventOnDocumentTopic
#[tokio::test]
#[serial]
async fn subscribe_document_join_event() {
    let cluster = setup_two_nodes().await;

    // Create a document to get a docID
    let result = cluster
        .client(0)
        .query(r#"mutation { create_Users(input: {name: "John", age: 21}) { _docID } }"#)
        .expect("create user");
    let doc_id = extract_doc_id_from_create(&result, "Users");

    connect_peers(&cluster);
    tokio::time::sleep(Duration::from_millis(500)).await;

    let (handle, events) = open_peer_events_sse(cluster.api_url(0)).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Both nodes subscribe to the document
    cluster
        .client(0)
        .p2p_document_add(&[&doc_id])
        .expect("document add node0");
    cluster
        .client(1)
        .p2p_document_add(&[&doc_id])
        .expect("document add node1");

    let collected = wait_for_peer_events(&events, 1, EVENT_TIMEOUT).await;
    handle.abort();

    let joined = events_with_type(&collected, "JOINED");
    assert!(
        !joined.is_empty(),
        "expected JOINED event for document subscription, got: {:?}",
        collected
    );
}

/// Port: TestPeerEvents_OnSubscribeToMultipleDocuments_ShouldReceiveJoinEventsOnAllTopics
#[tokio::test]
#[serial]
async fn subscribe_multiple_documents_join_events() {
    let cluster = setup_two_nodes().await;

    let result1 = cluster
        .client(0)
        .query(r#"mutation { create_Users(input: {name: "John", age: 21}) { _docID } }"#)
        .expect("create user1");
    let doc_id1 = extract_doc_id_from_create(&result1, "Users");

    let result2 = cluster
        .client(0)
        .query(r#"mutation { create_Users(input: {name: "Jane", age: 25}) { _docID } }"#)
        .expect("create user2");
    let doc_id2 = extract_doc_id_from_create(&result2, "Users");

    connect_peers(&cluster);
    tokio::time::sleep(Duration::from_millis(500)).await;

    let (handle, events) = open_peer_events_sse(cluster.api_url(0)).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Both nodes subscribe to both documents
    cluster
        .client(0)
        .p2p_document_add(&[&doc_id1, &doc_id2])
        .expect("document add node0");
    cluster
        .client(1)
        .p2p_document_add(&[&doc_id1, &doc_id2])
        .expect("document add node1");

    let collected = wait_for_peer_events(&events, 2, EVENT_TIMEOUT).await;
    handle.abort();

    let joined = events_with_type(&collected, "JOINED");
    assert!(
        joined.len() >= 2,
        "expected at least 2 JOINED events for 2 document subscriptions, got {}: {:?}",
        joined.len(),
        collected
    );
}

/// Port: TestPeerEvents_OnUnsubscribeFromDocument_ShouldReceiveLeftEvent
#[tokio::test]
#[serial]
async fn unsubscribe_document_left_event() {
    let cluster = setup_two_nodes().await;

    let result = cluster
        .client(0)
        .query(r#"mutation { create_Users(input: {name: "John", age: 21}) { _docID } }"#)
        .expect("create user");
    let doc_id = extract_doc_id_from_create(&result, "Users");

    connect_peers(&cluster);
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Both nodes subscribe to the document
    cluster
        .client(0)
        .p2p_document_add(&[&doc_id])
        .expect("document add node0");
    cluster
        .client(1)
        .p2p_document_add(&[&doc_id])
        .expect("document add node1");
    tokio::time::sleep(Duration::from_millis(500)).await;

    let (handle, events) = open_peer_events_sse(cluster.api_url(0)).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // node1 removes document subscription
    cluster
        .client(1)
        .p2p_document_delete(&[&doc_id])
        .expect("document delete node1");

    let collected = wait_for_peer_events(&events, 1, EVENT_TIMEOUT).await;
    handle.abort();

    let left = events_with_type(&collected, "LEFT");
    assert!(
        !left.is_empty(),
        "expected LEFT event for document unsubscribe, got: {:?}",
        collected
    );
}

/// Port: TestPeerEvents_OnUnsubscribeFromMultipleDocuments_ShouldReceiveLeftEvents
#[tokio::test]
#[serial]
async fn unsubscribe_multiple_documents_left_events() {
    let cluster = setup_two_nodes().await;

    let result1 = cluster
        .client(0)
        .query(r#"mutation { create_Users(input: {name: "John", age: 21}) { _docID } }"#)
        .expect("create user1");
    let doc_id1 = extract_doc_id_from_create(&result1, "Users");

    let result2 = cluster
        .client(0)
        .query(r#"mutation { create_Users(input: {name: "Jane", age: 25}) { _docID } }"#)
        .expect("create user2");
    let doc_id2 = extract_doc_id_from_create(&result2, "Users");

    connect_peers(&cluster);
    tokio::time::sleep(Duration::from_millis(500)).await;

    cluster
        .client(0)
        .p2p_document_add(&[&doc_id1, &doc_id2])
        .expect("document add node0");
    cluster
        .client(1)
        .p2p_document_add(&[&doc_id1, &doc_id2])
        .expect("document add node1");
    tokio::time::sleep(Duration::from_millis(500)).await;

    let (handle, events) = open_peer_events_sse(cluster.api_url(0)).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // node1 removes both document subscriptions
    cluster
        .client(1)
        .p2p_document_delete(&[&doc_id1, &doc_id2])
        .expect("document delete node1");

    let collected = wait_for_peer_events(&events, 2, EVENT_TIMEOUT).await;
    handle.abort();

    let left = events_with_type(&collected, "LEFT");
    assert!(
        left.len() >= 2,
        "expected at least 2 LEFT events for 2 document unsubscribes, got {}: {:?}",
        left.len(),
        collected
    );
}

/// Port: TestPeerEvents_DocumentAndDocSyncTopics_ShouldReceiveJoinEventsOnBoth
///
/// In iroh, there's no connection-level gossip topic — only per-topic subscriptions.
/// This test verifies both document subscription and collection subscription produce events.
#[tokio::test]
#[serial]
async fn document_and_doc_sync_topics_join_events() {
    let cluster = setup_two_nodes().await;

    let result = cluster
        .client(0)
        .query(r#"mutation { create_Users(input: {name: "John", age: 21}) { _docID } }"#)
        .expect("create user");
    let doc_id = extract_doc_id_from_create(&result, "Users");

    connect_peers(&cluster);
    tokio::time::sleep(Duration::from_millis(500)).await;

    let (handle, events) = open_peer_events_sse(cluster.api_url(0)).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Both nodes subscribe to collection AND document
    cluster
        .client(0)
        .p2p_collection_add(&["Users"])
        .expect("collection add node0");
    cluster
        .client(1)
        .p2p_collection_add(&["Users"])
        .expect("collection add node1");
    cluster
        .client(0)
        .p2p_document_add(&[&doc_id])
        .expect("document add node0");
    cluster
        .client(1)
        .p2p_document_add(&[&doc_id])
        .expect("document add node1");

    // Wait for events from both collection and document subscription
    let collected = wait_for_peer_events(&events, 2, EVENT_TIMEOUT).await;
    handle.abort();

    let joined = events_with_type(&collected, "JOINED");
    assert!(
        joined.len() >= 2,
        "expected at least 2 JOINED events (collection + document), got {}: {:?}",
        joined.len(),
        collected
    );
}

/// Port: TestPeerEvents_AllTopicTypes_ShouldReceiveJoinEventsOnAll
///
/// In iroh, there's no connection-level gossip topic. This test verifies
/// collection subscription + document subscription produce 2 JOINED events.
#[tokio::test]
#[serial]
async fn all_topic_types_join_events() {
    let cluster = setup_two_nodes().await;

    let result = cluster
        .client(0)
        .query(r#"mutation { create_Users(input: {name: "John", age: 21}) { _docID } }"#)
        .expect("create user");
    let doc_id = extract_doc_id_from_create(&result, "Users");

    connect_peers(&cluster);
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Open SSE before topic subscriptions
    let (handle, events) = open_peer_events_sse(cluster.api_url(0)).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Add collection subscription
    cluster
        .client(0)
        .p2p_collection_add(&["Users"])
        .expect("collection add node0");
    cluster
        .client(1)
        .p2p_collection_add(&["Users"])
        .expect("collection add node1");

    // Add document subscription
    cluster
        .client(0)
        .p2p_document_add(&[&doc_id])
        .expect("document add node0");
    cluster
        .client(1)
        .p2p_document_add(&[&doc_id])
        .expect("document add node1");

    // Wait for events from both topic types (collection + document)
    let collected = wait_for_peer_events(&events, 2, EVENT_TIMEOUT).await;
    handle.abort();

    let joined = events_with_type(&collected, "JOINED");
    assert!(
        joined.len() >= 2,
        "expected at least 2 JOINED events (collection + document), got {}: {:?}",
        joined.len(),
        collected
    );
}
