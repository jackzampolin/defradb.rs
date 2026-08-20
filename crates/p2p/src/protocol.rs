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

#[cfg(feature = "libp2p-transport")]
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

/// SE query request protocol ID.
/// Go uses "se_query" as the channel name: `/defradb/se_query_req/0.0.1`
pub const SE_QUERY_REQUEST_PROTOCOL: &str = "/defradb/se_query_req/0.0.1";

/// SE query response protocol ID.
/// Go uses "se_query" as the channel name: `/defradb/se_query_resp/0.0.1`
pub const SE_QUERY_RESPONSE_PROTOCOL: &str = "/defradb/se_query_resp/0.0.1";

/// Management mutate request protocol ID.
pub const MANAGE_REQUEST_PROTOCOL: &str = "/defradb/manage_req/0.0.1";

/// Management mutate response protocol ID.
pub const MANAGE_RESPONSE_PROTOCOL: &str = "/defradb/manage_resp/0.0.1";

/// Management query request protocol ID.
pub const MANAGE_QUERY_REQUEST_PROTOCOL: &str = "/defradb/manage_query_req/0.0.1";

/// Management query response protocol ID.
pub const MANAGE_QUERY_RESPONSE_PROTOCOL: &str = "/defradb/manage_query_resp/0.0.1";

/// CAR (Content ARchive) request protocol ID.
pub const CAR_REQUEST_PROTOCOL: &str = "/defradb/car_req/0.0.1";

/// CAR (Content ARchive) response protocol ID.
pub const CAR_RESPONSE_PROTOCOL: &str = "/defradb/car_resp/0.0.1";

/// Identity request protocol ID.
pub const IDENTITY_REQUEST_PROTOCOL: &str = "/defradb/ident_req/0.0.1";

/// Identity response protocol ID.
pub const IDENTITY_RESPONSE_PROTOCOL: &str = "/defradb/ident_resp/0.0.1";

/// StreamProtocol for the SE request protocol.
#[cfg(feature = "libp2p-transport")]
pub fn se_request_protocol() -> StreamProtocol {
    StreamProtocol::new(SE_REQUEST_PROTOCOL)
}

/// StreamProtocol for the SE response protocol.
#[cfg(feature = "libp2p-transport")]
pub fn se_response_protocol() -> StreamProtocol {
    StreamProtocol::new(SE_RESPONSE_PROTOCOL)
}

/// StreamProtocol for the SE query request protocol.
#[cfg(feature = "libp2p-transport")]
pub fn se_query_request_protocol() -> StreamProtocol {
    StreamProtocol::new(SE_QUERY_REQUEST_PROTOCOL)
}

/// StreamProtocol for the SE query response protocol.
#[cfg(feature = "libp2p-transport")]
pub fn se_query_response_protocol() -> StreamProtocol {
    StreamProtocol::new(SE_QUERY_RESPONSE_PROTOCOL)
}

/// StreamProtocol for the CAR request protocol.
#[cfg(feature = "libp2p-transport")]
pub fn car_request_protocol() -> StreamProtocol {
    StreamProtocol::new(CAR_REQUEST_PROTOCOL)
}

/// StreamProtocol for the CAR response protocol.
#[cfg(feature = "libp2p-transport")]
pub fn car_response_protocol() -> StreamProtocol {
    StreamProtocol::new(CAR_RESPONSE_PROTOCOL)
}

/// StreamProtocol for the identity request protocol.
#[cfg(feature = "libp2p-transport")]
pub fn identity_request_protocol() -> StreamProtocol {
    StreamProtocol::new(IDENTITY_REQUEST_PROTOCOL)
}

/// StreamProtocol for the identity response protocol.
#[cfg(feature = "libp2p-transport")]
pub fn identity_response_protocol() -> StreamProtocol {
    StreamProtocol::new(IDENTITY_RESPONSE_PROTOCOL)
}

// Legacy aliases for backwards compatibility with existing code
#[deprecated(note = "Use REP_REQUEST_PROTOCOL instead")]
pub const PUSHLOG_REQUEST_PROTOCOL: &str = REP_REQUEST_PROTOCOL;
#[deprecated(note = "Use REP_RESPONSE_PROTOCOL instead")]
pub const PUSHLOG_RESPONSE_PROTOCOL: &str = REP_RESPONSE_PROTOCOL;

/// StreamProtocol for the replicator request protocol.
#[cfg(feature = "libp2p-transport")]
pub fn rep_request_protocol() -> StreamProtocol {
    StreamProtocol::new(REP_REQUEST_PROTOCOL)
}

/// StreamProtocol for the replicator response protocol.
#[cfg(feature = "libp2p-transport")]
pub fn rep_response_protocol() -> StreamProtocol {
    StreamProtocol::new(REP_RESPONSE_PROTOCOL)
}

// Legacy aliases
#[deprecated(note = "Use rep_request_protocol instead")]
#[cfg(feature = "libp2p-transport")]
pub fn pushlog_request_protocol() -> StreamProtocol {
    rep_request_protocol()
}

#[deprecated(note = "Use rep_response_protocol instead")]
#[cfg(feature = "libp2p-transport")]
pub fn pushlog_response_protocol() -> StreamProtocol {
    rep_response_protocol()
}
