//! Channel-based event bus implementation.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use async_channel::{Sender, TrySendError};
use parking_lot::RwLock;

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
            event_buffer_size: 4096,
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
    sender: Sender<Message>,
    /// Events this subscriber is interested in.
    events: Vec<EventName>,
    /// Shared count of messages dropped due to buffer overflow.
    /// Used to signal clients that they may need to resync.
    dropped_count: std::sync::Arc<AtomicU64>,
}

/// Channel-based event bus using runtime-neutral async channels.
///
/// This implementation uses bounded channels per subscriber.
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
                Err(TrySendError::Full(_)) => {
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
                Err(TrySendError::Closed(_)) => {
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
            tracing::debug!(
                event = %msg.name,
                sub_count,
                delivered,
                dropped,
                buffer_full,
                "Published event"
            );
        }
    }

    fn subscribe(&self, events: &[EventName]) -> Subscription {
        if self.closed.load(Ordering::Acquire) {
            // Return a subscription with a closed channel
            let (_tx, rx) = async_channel::bounded(1);
            return Subscription::new(0, rx);
        }

        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = async_channel::bounded(self.config.event_buffer_size);

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
#[path = "channel_bus_tests.rs"]
mod tests;
