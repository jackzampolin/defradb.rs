//! P2P Host implementation for DefraDB.
//!
//! This module provides the main P2P host that manages the libp2p swarm,
//! handles peer connections, and coordinates CRDT synchronization.

mod command;
mod command_handler;
mod event;
mod handle;
mod p2p_host;

// Re-export public types
pub use command::HostCommand;
pub use event::HostEvent;
pub use handle::P2PHostHandle;
pub use p2p_host::P2PHost;

use libp2p::request_response;

use crate::message::PushLogReply;

/// Opaque response channel for sending PushLog responses.
#[derive(Debug)]
pub struct ResponseChannel(request_response::ResponseChannel<PushLogReply>);

impl ResponseChannel {
    /// Create a new response channel (internal use).
    pub(crate) fn new(inner: request_response::ResponseChannel<PushLogReply>) -> Self {
        Self(inner)
    }

    /// Get the inner response channel.
    pub(crate) fn into_inner(self) -> request_response::ResponseChannel<PushLogReply> {
        self.0
    }
}
