//! Channel-based event bus implementation.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use parking_lot::RwLock;
use tokio::sync::mpsc;

use crate::bus::Bus;
use crate::event::{EventName, Message};
use crate::subscription::Subscription;

/// Subscriber entry with channel and event filter.
struct Subscriber {
    /// Sender channel for messages.
    sender: mpsc::UnboundedSender<Message>,
    /// Events this subscriber is interested in.
    events: Vec<EventName>,
}

/// Channel-based event bus using tokio broadcast channels.
///
/// This implementation uses unbounded mpsc channels per subscriber.
/// Messages are fan-out to all matching subscribers.
pub struct ChannelBus {
    /// Counter for generating unique subscription IDs.
    next_id: AtomicU64,
    /// Active subscribers indexed by ID.
    subscribers: RwLock<HashMap<u64, Subscriber>>,
    /// Whether the bus is closed.
    closed: AtomicBool,
}

impl ChannelBus {
    /// Create a new channel-based event bus.
    pub fn new() -> Self {
        Self {
            next_id: AtomicU64::new(1),
            subscribers: RwLock::new(HashMap::new()),
            closed: AtomicBool::new(false),
        }
    }

    /// Get the number of active subscribers.
    pub fn subscriber_count(&self) -> usize {
        self.subscribers.read().len()
    }
}

impl Default for ChannelBus {
    fn default() -> Self {
        Self::new()
    }
}

impl Bus for ChannelBus {
    fn publish(&self, msg: Message) {
        if self.closed.load(Ordering::Acquire) {
            tracing::debug!(event = %msg.name, "Bus closed, dropping message");
            return;
        }

        let subscribers = self.subscribers.read();
        let mut delivered = 0;
        let mut dropped = 0;

        for (id, subscriber) in subscribers.iter() {
            // Check if subscriber is interested in this event
            let interested = subscriber.events.iter().any(|e| e.matches(&msg.name));
            if !interested {
                continue;
            }

            // Try to send (non-blocking)
            match subscriber.sender.send(msg.clone()) {
                Ok(()) => delivered += 1,
                Err(_) => {
                    // Subscriber channel closed, will be cleaned up later
                    tracing::debug!(sub_id = *id, "Subscriber channel closed");
                    dropped += 1;
                }
            }
        }

        tracing::trace!(
            event = %msg.name,
            delivered = delivered,
            dropped = dropped,
            "Published event"
        );
    }

    fn subscribe(&self, events: &[EventName]) -> Subscription {
        if self.closed.load(Ordering::Acquire) {
            // Return a subscription with a closed channel
            let (_tx, rx) = mpsc::unbounded_channel();
            return Subscription::new(0, rx);
        }

        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = mpsc::unbounded_channel();

        let subscriber = Subscriber {
            sender: tx,
            events: events.to_vec(),
        };

        self.subscribers.write().insert(id, subscriber);

        tracing::debug!(
            sub_id = id,
            events = ?events,
            "New subscription"
        );

        Subscription::new(id, rx)
    }

    fn unsubscribe(&self, sub_id: u64) {
        if let Some(subscriber) = self.subscribers.write().remove(&sub_id) {
            // Drop the sender to close the receiver
            drop(subscriber);
            tracing::debug!(sub_id = sub_id, "Unsubscribed");
        }
    }

    fn close(&self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            // Already closed
            return;
        }

        // Clear all subscribers (this will close all channels)
        let mut subscribers = self.subscribers.write();
        let count = subscribers.len();
        subscribers.clear();

        tracing::info!(subscribers_closed = count, "Event bus closed");
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
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
        bus.publish(Message::merge_complete());

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
}
