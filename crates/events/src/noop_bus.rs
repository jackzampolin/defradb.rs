//! No-op event bus for environments without channel support (e.g., wasm32).

use crate::bus::Bus;
use crate::event::{EventName, Message};
use crate::subscription::Subscription;

/// No-op event bus that silently discards all published messages.
///
/// Used in wasm32 builds where tokio channels are not available.
pub struct NoOpBus;

impl NoOpBus {
    /// Create a new no-op event bus.
    pub fn new() -> Self {
        Self
    }
}

impl Default for NoOpBus {
    fn default() -> Self {
        Self::new()
    }
}

impl Bus for NoOpBus {
    fn publish(&self, _msg: Message) {}

    fn subscribe(&self, _events: &[EventName]) -> Subscription {
        #[cfg(feature = "channel")]
        {
            let (_tx, rx) = tokio::sync::mpsc::channel(1);
            Subscription::new(0, rx)
        }
        #[cfg(not(feature = "channel"))]
        {
            Subscription::closed()
        }
    }

    fn unsubscribe(&self, _sub_id: u64) {}

    fn close(&self) {}

    fn is_closed(&self) -> bool {
        true
    }
}
