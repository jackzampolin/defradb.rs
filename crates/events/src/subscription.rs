//! Subscription handle for receiving events.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[cfg(feature = "channel")]
use async_channel::Receiver;

#[cfg(feature = "channel")]
use crate::event::Message;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryRecvError {
    Empty,
    Disconnected,
}

/// Subscription to events from the event bus.
///
/// Provides access to a channel of messages and the subscription ID
/// for unsubscribing. Uses bounded channels to prevent memory exhaustion.
///
/// When the `channel` feature is disabled, this is a stub that never receives messages.
pub struct Subscription {
    /// Unique subscription identifier.
    id: u64,
    /// Receiver channel for messages (bounded).
    #[cfg(feature = "channel")]
    receiver: Receiver<Message>,
    /// Shared counter tracking messages dropped due to buffer overflow.
    /// When non-zero, the client may need to resync to get consistent state.
    dropped_count: Arc<AtomicU64>,
}

impl Subscription {
    /// Create a new subscription with the given ID and receiver.
    #[cfg(feature = "channel")]
    pub(crate) fn new(id: u64, receiver: Receiver<Message>) -> Self {
        Self {
            id,
            receiver,
            dropped_count: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Create a closed subscription for builds without channel support.
    #[cfg(not(feature = "channel"))]
    pub(crate) fn closed() -> Self {
        Self {
            id: 0,
            dropped_count: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Create a new subscription with a shared dropped counter.
    #[cfg(feature = "channel")]
    pub(crate) fn with_dropped_counter(
        id: u64,
        receiver: Receiver<Message>,
        dropped_count: Arc<AtomicU64>,
    ) -> Self {
        Self {
            id,
            receiver,
            dropped_count,
        }
    }

    /// Get the subscription ID.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Receive the next message.
    ///
    /// Returns `None` if the subscription is closed.
    #[cfg(feature = "channel")]
    pub async fn recv(&mut self) -> Option<Message> {
        self.receiver.recv().await.ok()
    }

    /// Try to receive a message without blocking.
    ///
    /// Returns `Ok(msg)` if a message is available,
    /// `Err(TryRecvError::Empty)` if no message is available,
    /// or `Err(TryRecvError::Disconnected)` if the subscription is closed.
    #[cfg(feature = "channel")]
    pub fn try_recv(&mut self) -> Result<Message, TryRecvError> {
        self.receiver.try_recv().map_err(|error| match error {
            async_channel::TryRecvError::Empty => TryRecvError::Empty,
            async_channel::TryRecvError::Closed => TryRecvError::Disconnected,
        })
    }

    /// Convert into the underlying receiver for use with streams.
    #[cfg(feature = "channel")]
    pub fn into_receiver(self) -> Receiver<Message> {
        self.receiver
    }

    /// Check if any messages have been dropped due to buffer overflow.
    ///
    /// Returns the number of messages dropped since the last check.
    /// Resets the counter to zero after reading.
    ///
    /// When this returns a non-zero value, the client should consider
    /// re-fetching the full state to ensure consistency.
    pub fn check_and_reset_dropped(&self) -> u64 {
        self.dropped_count.swap(0, Ordering::Relaxed)
    }

    /// Get the current dropped count without resetting.
    pub fn dropped_count(&self) -> u64 {
        self.dropped_count.load(Ordering::Relaxed)
    }
}

impl std::fmt::Debug for Subscription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Subscription")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_subscription_recv() {
        let (tx, rx) = async_channel::bounded(10);
        let mut sub = Subscription::new(1, rx);

        // Send a message
        let msg = Message::merge();
        tx.send(msg).await.unwrap();

        // Receive it
        let received = sub.recv().await;
        assert!(received.is_some());
        assert_eq!(received.unwrap().name, crate::event::EventName::Merge);

        // Close the channel
        drop(tx);

        // Receive should return None
        let received = sub.recv().await;
        assert!(received.is_none());
    }

    #[test]
    fn test_subscription_try_recv() {
        let (tx, rx) = async_channel::bounded(10);
        let mut sub = Subscription::new(1, rx);

        // No message yet
        assert!(sub.try_recv().is_err());

        // Send a message (blocking send in sync context)
        let msg = Message::merge();
        tx.send_blocking(msg).unwrap();

        // Now we can receive
        let received = sub.try_recv();
        assert!(received.is_ok());
    }
}
