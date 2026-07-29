use super::*;
use crate::event::Update;

#[tokio::test]
async fn test_channel_bus_publish_subscribe() {
    let bus = ChannelBus::new();

    // Subscribe to Update events
    let mut sub = bus.subscribe(&[EventName::Update]);
    assert_eq!(bus.subscriber_count(), 1);

    // Publish an Update
    let update = Update::new(
        "doc-1".to_string(),
        cid::Cid::default(),
        "col-1".to_string(),
        vec![],
        false,
        false,
    );
    bus.publish(Message::update(update));

    // Receive the message
    let msg = sub.recv().await.unwrap();
    assert_eq!(msg.name, EventName::Update);
    assert!(msg.as_update().is_some());
}

#[tokio::test]
async fn test_channel_bus_wildcard() {
    let bus = ChannelBus::new();

    // Subscribe to all events
    let mut sub = bus.subscribe(&[EventName::WildCard]);

    // Publish different events
    bus.publish(Message::merge());
    bus.publish(Message::merge_complete(crate::MergeCompleteData {
        doc_id: "test-doc".to_string(),
        subject_doc_id: None,
        cid: cid::Cid::default(),
        collection_id: "test-col".to_string(),
        by_peer: "test-peer".to_string(),
    }));

    // Should receive both
    let msg1 = sub.recv().await.unwrap();
    assert_eq!(msg1.name, EventName::Merge);

    let msg2 = sub.recv().await.unwrap();
    assert_eq!(msg2.name, EventName::MergeComplete);
}

#[tokio::test]
async fn test_channel_bus_filter() {
    let bus = ChannelBus::new();

    // Subscribe only to Merge events
    let mut sub = bus.subscribe(&[EventName::Merge]);

    // Publish different events
    let update = Update::new(
        "doc-1".to_string(),
        cid::Cid::default(),
        "col-1".to_string(),
        vec![],
        false,
        false,
    );
    bus.publish(Message::update(update));
    bus.publish(Message::merge());

    // Should only receive Merge
    let msg = sub.recv().await.unwrap();
    assert_eq!(msg.name, EventName::Merge);

    // No more messages should be immediately available
    assert!(sub.try_recv().is_err());
}

#[tokio::test]
async fn test_channel_bus_unsubscribe() {
    let bus = ChannelBus::new();

    let mut sub = bus.subscribe(&[EventName::Update]);
    let sub_id = sub.id();
    assert_eq!(bus.subscriber_count(), 1);

    bus.unsubscribe(sub_id);
    assert_eq!(bus.subscriber_count(), 0);

    // Receiver should be closed
    let msg = sub.recv().await;
    assert!(msg.is_none());
}

#[tokio::test]
async fn test_channel_bus_close() {
    let bus = ChannelBus::new();

    let mut sub1 = bus.subscribe(&[EventName::Update]);
    let mut sub2 = bus.subscribe(&[EventName::Merge]);
    assert_eq!(bus.subscriber_count(), 2);

    bus.close();
    assert!(bus.is_closed());
    assert_eq!(bus.subscriber_count(), 0);

    // Both receivers should be closed
    assert!(sub1.recv().await.is_none());
    assert!(sub2.recv().await.is_none());

    // New subscriptions should get closed channels
    let mut sub3 = bus.subscribe(&[EventName::Update]);
    assert!(sub3.recv().await.is_none());
}

#[tokio::test]
async fn test_channel_bus_multiple_subscribers() {
    let bus = ChannelBus::new();

    let mut sub1 = bus.subscribe(&[EventName::Update]);
    let mut sub2 = bus.subscribe(&[EventName::Update]);
    assert_eq!(bus.subscriber_count(), 2);

    // Publish one message
    let update = Update::new(
        "doc-1".to_string(),
        cid::Cid::default(),
        "col-1".to_string(),
        vec![],
        false,
        false,
    );
    bus.publish(Message::update(update));

    // Both should receive it
    let msg1 = sub1.recv().await.unwrap();
    let msg2 = sub2.recv().await.unwrap();
    assert_eq!(msg1.name, EventName::Update);
    assert_eq!(msg2.name, EventName::Update);
}

#[tokio::test]
async fn test_channel_bus_buffer_overflow() {
    // Create bus with small buffer for testing
    let config = ChannelBusConfig::new().with_event_buffer_size(2);
    let bus = ChannelBus::with_config(config);

    // Subscribe but don't consume
    let sub = bus.subscribe(&[EventName::Merge]);
    assert_eq!(bus.subscriber_count(), 1);

    // Fill the buffer
    bus.publish(Message::merge());
    bus.publish(Message::merge());

    // This should be dropped (buffer full) - non-blocking
    bus.publish(Message::merge());
    bus.publish(Message::merge());

    // Subscriber should still be active
    assert_eq!(bus.subscriber_count(), 1);

    // Drop subscription
    drop(sub);
}

#[test]
fn test_channel_bus_config() {
    let config = ChannelBusConfig::new().with_event_buffer_size(500);

    assert_eq!(config.event_buffer_size, 500);

    let bus = ChannelBus::with_config(config);
    assert_eq!(bus.config().event_buffer_size, 500);
}
