//! Channel-based event bus implementation.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use parking_lot::RwLock;
use tokio::sync::mpsc;

use crate::bus::Bus;
use crate::event::{EventName, Message};
use crate::subscription::Subscription;

/// Configuration for the channel-based event bus.
#[derive(Debug, Clone)]
pub struct ChannelBusConfig {
    /// Buffer size for subscriber event channels.
    /// When the buffer is full, new messages are dropped with a warning.
    /// Default: 100
    pub event_buffer_size: usize,
    /// Whether to send a resync signal when messages are dropped due to buffer overflow.
    /// When enabled, a special "resync_needed" flag is tracked per subscriber.
    /// Default: true
    pub signal_resync_on_overflow: bool,
}

impl Default for ChannelBusConfig {
    fn default() -> Self {
        Self {
            event_buffer_size: 100,
            signal_resync_on_overflow: true,
        }
    }
}

impl ChannelBusConfig {
    /// Create a new configuration with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the event buffer size.
    pub fn with_event_buffer_size(mut self, size: usize) -> Self {
        self.event_buffer_size = size;
        self
    }
}

/// Subscriber entry with channel and event filter.
struct Subscriber {
    /// Sender channel for messages.
    sender: mpsc::Sender<Message>,
    /// Events this subscriber is interested in.
    events: Vec<EventName>,
    /// Shared count of messages dropped due to buffer overflow.
    /// Used to signal clients that they may need to resync.
    dropped_count: std::sync::Arc<AtomicU64>,
}

/// Channel-based event bus using tokio mpsc channels.
///
/// This implementation uses bounded mpsc channels per subscriber.
/// Messages are fan-out to all matching subscribers.
/// When a subscriber's buffer is full, messages are dropped (non-blocking).
pub struct ChannelBus {
    /// Counter for generating unique subscription IDs.
    next_id: AtomicU64,
    /// Active subscribers indexed by ID.
    subscribers: RwLock<HashMap<u64, Subscriber>>,
    /// Whether the bus is closed.
    closed: AtomicBool,
    /// Configuration for the bus.
    config: ChannelBusConfig,
}

impl ChannelBus {
    /// Create a new channel-based event bus with default configuration.
    pub fn new() -> Self {
        Self::with_config(ChannelBusConfig::default())
    }

    /// Create a new channel-based event bus with custom configuration.
    pub fn with_config(config: ChannelBusConfig) -> Self {
        Self {
            next_id: AtomicU64::new(1),
            subscribers: RwLock::new(HashMap::new()),
            closed: AtomicBool::new(false),
            config,
        }
    }

    /// Get the number of active subscribers.
    pub fn subscriber_count(&self) -> usize {
        self.subscribers.read().len()
    }

    /// Get the current configuration.
    pub fn config(&self) -> &ChannelBusConfig {
        &self.config
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

        // Collect dead subscriber IDs for lazy cleanup
        let mut dead_subs: Vec<u64> = Vec::new();

        let subscribers = self.subscribers.read();
        let sub_count = subscribers.len();
        let mut delivered = 0;
        let mut dropped = 0;
        let mut buffer_full = 0;

        for (id, subscriber) in subscribers.iter() {
            // Check if subscriber is interested in this event
            let interested = subscriber.events.iter().any(|e| e.matches(&msg.name));
            if !interested {
                continue;
            }

            // Try to send (non-blocking) - use try_send to avoid blocking
            match subscriber.sender.try_send(msg.clone()) {
                Ok(()) => delivered += 1,
                Err(mpsc::error::TrySendError::Full(_)) => {
                    // Buffer full - track dropped count for resync signaling
                    let prev_dropped = subscriber.dropped_count.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        sub_id = *id,
                        event = %msg.name,
                        total_dropped = prev_dropped + 1,
                        "Subscriber buffer full, dropping message (client may need to resync)"
                    );
                    buffer_full += 1;
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    // Subscriber channel closed, mark for cleanup
                    tracing::debug!(
                        sub_id = *id,
                        "Subscriber channel closed, marking for cleanup"
                    );
                    dead_subs.push(*id);
                    dropped += 1;
                }
            }
        }

        // Release read lock before acquiring write lock for cleanup
        drop(subscribers);

        // Lazy cleanup: remove dead subscribers
        if !dead_subs.is_empty() {
            let mut subscribers_mut = self.subscribers.write();
            for id in &dead_subs {
                if subscribers_mut.remove(id).is_some() {
                    tracing::debug!(sub_id = *id, "Cleaned up dead subscriber");
                }
            }
            tracing::info!(
                cleaned_up = dead_subs.len(),
                remaining = subscribers_mut.len(),
                "Cleaned up dead subscribers"
            );
        }

        if matches!(
            msg.name,
            EventName::MergeComplete | EventName::ReplicatorCompleted
        ) {
            eprintln!("[EVENT-BUS] Published event={} sub_count={} delivered={} dropped={} buffer_full={}", msg.name, sub_count, delivered, dropped, buffer_full);
        }
    }

    fn subscribe(&self, events: &[EventName]) -> Subscription {
        if self.closed.load(Ordering::Acquire) {
            // Return a subscription with a closed channel
            let (_tx, rx) = mpsc::channel(1);
            return Subscription::new(0, rx);
        }

        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = mpsc::channel(self.config.event_buffer_size);

        // Create shared dropped counter for both Subscriber and Subscription
        let dropped_count = std::sync::Arc::new(AtomicU64::new(0));

        let subscriber = Subscriber {
            sender: tx,
            events: events.to_vec(),
            dropped_count: dropped_count.clone(),
        };

        self.subscribers.write().insert(id, subscriber);

        tracing::debug!(
            sub_id = id,
            events = ?events,
            buffer_size = self.config.event_buffer_size,
            "New subscription"
        );

        Subscription::with_dropped_counter(id, rx, dropped_count)
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
        bus.publish(Message::merge_complete(crate::MergeCompleteData {
            doc_id: "test-doc".to_string(),
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
}
