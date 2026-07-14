//! Event bus for DefraDB subscriptions.
//!
//! This crate provides a pub/sub event system for database changes,
//! enabling GraphQL subscriptions to receive real-time updates.
//!
//! # Go compatibility
//!
//! | Surface | Go DefraDB | Rust DefraDB |
//! | --- | --- | --- |
//! | Raw event bus | Live-only `Publish`/`Subscribe`/`Unsubscribe` | Live-only `publish`/`subscribe`/`unsubscribe` |
//! | HTTP GraphQL SSE | Re-runs a scoped query per live update | Re-runs a scoped query per live update |
//! | FFI GraphQL subscriptions | Polls buffered scoped query results | Polls buffered scoped query results |
//! | `_commits` subscriptions | Scopes by event CID and preserves docID filters | Scopes by event CID and preserves docID filters |
//! | Cursor/replay | Not exposed | Not exposed |
//! | Backpressure | Publisher blocks when channels fill | Messages may be dropped and counted |
//!
//! The raw event bus has no cursor, replay API, global sequence, or durable
//! change log. HTTP and FFI GraphQL subscriptions build on this stream by
//! re-running a scoped query for each live update.
//!
//! Unlike Go's blocking channel bus, the Rust channel bus uses bounded channels
//! and records dropped messages per subscription. Consumers that observe a
//! non-zero dropped count must resync from the database before trusting further
//! incremental observations.

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
    AcpCacheInvalidatedData, AcpHeightAdvancedData, EventName, MergeCompleteData, Message,
    PendingDagQuarantinedData, SEArtifactReceivedData, TopicPeerEventData, Update,
};
pub use noop_bus::NoOpBus;
pub use subscription::Subscription;
