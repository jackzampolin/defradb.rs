//! Subscription handle for receiving events.

use tokio::sync::mpsc;

use crate::event::Message;

/// Subscription to events from the event bus.
///
/// Provides access to a channel of messages and the subscription ID
/// for unsubscribing. Uses bounded channels to prevent memory exhaustion.
pub struct Subscription {
    /// Unique subscription identifier.
    id: u64,
    /// Receiver channel for messages (bounded).
    receiver: mpsc::Receiver<Message>,
}

impl Subscription {
    /// Create a new subscription with the given ID and receiver.
    pub(crate) fn new(id: u64, receiver: mpsc::Receiver<Message>) -> Self {
        Self { id, receiver }
    }

    /// Get the subscription ID.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Receive the next message.
    ///
    /// Returns `None` if the subscription is closed.
    pub async fn recv(&mut self) -> Option<Message> {
        self.receiver.recv().await
    }

    /// Try to receive a message without blocking.
    ///
    /// Returns `Ok(msg)` if a message is available,
    /// `Err(TryRecvError::Empty)` if no message is available,
    /// or `Err(TryRecvError::Disconnected)` if the subscription is closed.
    pub fn try_recv(&mut self) -> Result<Message, mpsc::error::TryRecvError> {
        self.receiver.try_recv()
    }

    /// Convert into the underlying receiver for use with streams.
    pub fn into_receiver(self) -> mpsc::Receiver<Message> {
        self.receiver
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
        let (tx, rx) = mpsc::channel(10);
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
        let (tx, rx) = mpsc::channel(10);
        let mut sub = Subscription::new(1, rx);

        // No message yet
        assert!(sub.try_recv().is_err());

        // Send a message (blocking send in sync context)
        let msg = Message::merge();
        tx.blocking_send(msg).unwrap();

        // Now we can receive
        let received = sub.try_recv();
        assert!(received.is_ok());
    }
}
