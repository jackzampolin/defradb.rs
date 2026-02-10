//! DefraDB P2P protocol constants.
//!
//! This module defines the protocol identifiers and version information
//! for DefraDB's P2P networking layer.
//!
//! # Wire Compatibility with Go
//!
//! The Go implementation uses a CommChannel pattern with separate request/response
//! protocol IDs. This Rust implementation matches that pattern:
//! - Request protocol: `/defradb/<name>_req/<version>`
//! - Response protocol: `/defradb/<name>_resp/<version>`

use libp2p::StreamProtocol;

/// The protocol name/slug representing DefraDB.
pub const NAME: &str = "defra";

/// DefraDB's multicodec code (arbitrary).
pub const CODE: u64 = 961;

/// Current protocol version.
pub const VERSION: &str = "0.0.1";

/// Base protocol ID for libp2p identification.
/// Format: /{name}/{version}
pub const BASE_PROTOCOL_ID: &str = "/defra/0.0.1";

/// Protocol base for communication channels.
/// Matches Go's `protocolBase = "/defradb/"`.
pub const PROTOCOL_BASE: &str = "/defradb/";

/// Message version string used in wire messages.
/// Matches Go's `messageVersion = "/defradb/0.0.1"`.
pub const MESSAGE_VERSION: &str = "/defradb/0.0.1";

/// Replicator request protocol ID.
/// Go uses "rep" as the channel name: `/defradb/rep_req/0.0.1`
pub const REP_REQUEST_PROTOCOL: &str = "/defradb/rep_req/0.0.1";

/// Replicator response protocol ID.
/// Go uses "rep" as the channel name: `/defradb/rep_resp/0.0.1`
pub const REP_RESPONSE_PROTOCOL: &str = "/defradb/rep_resp/0.0.1";

/// SE (Searchable Encryption) request protocol ID.
/// Go uses "rep_se" as the channel name: `/defradb/rep_se_req/0.0.1`
pub const SE_REQUEST_PROTOCOL: &str = "/defradb/rep_se_req/0.0.1";

/// SE (Searchable Encryption) response protocol ID.
/// Go uses "rep_se" as the channel name: `/defradb/rep_se_resp/0.0.1`
pub const SE_RESPONSE_PROTOCOL: &str = "/defradb/rep_se_resp/0.0.1";

/// StreamProtocol for the SE request protocol.
pub fn se_request_protocol() -> StreamProtocol {
    StreamProtocol::new(SE_REQUEST_PROTOCOL)
}

/// StreamProtocol for the SE response protocol.
pub fn se_response_protocol() -> StreamProtocol {
    StreamProtocol::new(SE_RESPONSE_PROTOCOL)
}

// Legacy aliases for backwards compatibility with existing code
#[deprecated(note = "Use REP_REQUEST_PROTOCOL instead")]
pub const PUSHLOG_REQUEST_PROTOCOL: &str = REP_REQUEST_PROTOCOL;
#[deprecated(note = "Use REP_RESPONSE_PROTOCOL instead")]
pub const PUSHLOG_RESPONSE_PROTOCOL: &str = REP_RESPONSE_PROTOCOL;

/// StreamProtocol for the replicator request protocol.
pub fn rep_request_protocol() -> StreamProtocol {
    StreamProtocol::new(REP_REQUEST_PROTOCOL)
}

/// StreamProtocol for the replicator response protocol.
pub fn rep_response_protocol() -> StreamProtocol {
    StreamProtocol::new(REP_RESPONSE_PROTOCOL)
}

// Legacy aliases
#[deprecated(note = "Use rep_request_protocol instead")]
pub fn pushlog_request_protocol() -> StreamProtocol {
    rep_request_protocol()
}

#[deprecated(note = "Use rep_response_protocol instead")]
pub fn pushlog_response_protocol() -> StreamProtocol {
    rep_response_protocol()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_base_protocol_id_format() {
        assert_eq!(BASE_PROTOCOL_ID, format!("/{}/{}", NAME, VERSION));
    }

    #[test]
    fn test_code_value() {
        assert_eq!(CODE, 961);
    }

    #[test]
    fn test_rep_protocols() {
        assert_eq!(rep_request_protocol().as_ref(), "/defradb/rep_req/0.0.1");
        assert_eq!(rep_response_protocol().as_ref(), "/defradb/rep_resp/0.0.1");
    }

    #[test]
    fn test_message_version() {
        assert_eq!(MESSAGE_VERSION, "/defradb/0.0.1");
    }
}
