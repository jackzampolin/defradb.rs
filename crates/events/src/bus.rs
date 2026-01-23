//! Event bus trait definition.

use crate::event::{EventName, Message};
use crate::subscription::Subscription;

/// Event bus for pub/sub messaging.
///
/// The bus allows publishers to emit events and subscribers to receive them.
/// Subscribers can filter events by name.
pub trait Bus: Send + Sync {
    /// Publish a message to all subscribers.
    ///
    /// Messages are delivered asynchronously to all subscribers
    /// whose event filter matches the message's event name.
    fn publish(&self, msg: Message);

    /// Subscribe to events matching the given event names.
    ///
    /// Returns a `Subscription` that can be used to receive events.
    /// The subscription will receive messages for any event name in the filter.
    ///
    /// Use `EventName::WildCard` to receive all events.
    fn subscribe(&self, events: &[EventName]) -> Subscription;

    /// Unsubscribe by subscription ID.
    ///
    /// After unsubscribing, the subscription will no longer receive events
    /// and the receiver channel will be closed.
    fn unsubscribe(&self, sub_id: u64);

    /// Close the event bus.
    ///
    /// Closes all subscriptions and stops accepting new subscriptions.
    fn close(&self);

    /// Check if the bus is closed.
    fn is_closed(&self) -> bool;
}
