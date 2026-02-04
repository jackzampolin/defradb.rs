//! Event bus for DefraDB subscriptions.
//!
//! This crate provides a pub/sub event system for database changes,
//! enabling GraphQL subscriptions to receive real-time updates.

mod bus;
#[cfg(feature = "channel")]
mod channel_bus;
mod event;
mod noop_bus;
mod subscription;

pub use bus::Bus;
#[cfg(feature = "channel")]
pub use channel_bus::{ChannelBus, ChannelBusConfig};
pub use event::{
    EventName, MergeCompleteData, Message, SEArtifactReceivedData, TopicPeerEventData, Update,
};
pub use noop_bus::NoOpBus;
pub use subscription::Subscription;
