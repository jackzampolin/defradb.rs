//! Event bus for DefraDB subscriptions.
//!
//! This crate provides a pub/sub event system for database changes,
//! enabling GraphQL subscriptions to receive real-time updates.

mod bus;
mod channel_bus;
mod event;
mod subscription;

pub use bus::Bus;
pub use channel_bus::{ChannelBus, ChannelBusConfig};
pub use event::{EventName, Message, Update};
pub use subscription::Subscription;
