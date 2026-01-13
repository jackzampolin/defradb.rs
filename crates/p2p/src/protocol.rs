// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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

/// Protocol request suffix format.
/// Matches Go's `protocolRequestSuffix = "_req/" + protocolVersion`.
pub const PROTOCOL_REQUEST_SUFFIX: &str = "_req/0.0.1";

/// Protocol response suffix format.
/// Matches Go's `protocolResponseSuffix = "_resp/" + protocolVersion`.
pub const PROTOCOL_RESPONSE_SUFFIX: &str = "_resp/0.0.1";

/// Message version string used in wire messages.
/// Matches Go's `messageVersion = "/defradb/0.0.1"`.
pub const MESSAGE_VERSION: &str = "/defradb/0.0.1";

/// PushLog request protocol ID.
/// Go equivalent: `/defradb/pushlog_req/0.0.1`
pub const PUSHLOG_REQUEST_PROTOCOL: &str = "/defradb/pushlog_req/0.0.1";

/// PushLog response protocol ID.
/// Go equivalent: `/defradb/pushlog_resp/0.0.1`
pub const PUSHLOG_RESPONSE_PROTOCOL: &str = "/defradb/pushlog_resp/0.0.1";

/// StreamProtocol for the pushlog request protocol.
pub fn pushlog_request_protocol() -> StreamProtocol {
    StreamProtocol::new(PUSHLOG_REQUEST_PROTOCOL)
}

/// StreamProtocol for the pushlog response protocol.
pub fn pushlog_response_protocol() -> StreamProtocol {
    StreamProtocol::new(PUSHLOG_RESPONSE_PROTOCOL)
}

/// StreamProtocol for identity exchange.
pub fn identity_protocol() -> StreamProtocol {
    StreamProtocol::new("/defra/identify/0.0.1")
}

/// Build a request protocol ID for a given channel name.
pub fn request_protocol(name: &str) -> String {
    format!("{}{}{}", PROTOCOL_BASE, name, PROTOCOL_REQUEST_SUFFIX)
}

/// Build a response protocol ID for a given channel name.
pub fn response_protocol(name: &str) -> String {
    format!("{}{}{}", PROTOCOL_BASE, name, PROTOCOL_RESPONSE_SUFFIX)
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
    fn test_pushlog_protocols() {
        assert_eq!(
            pushlog_request_protocol().as_ref(),
            "/defradb/pushlog_req/0.0.1"
        );
        assert_eq!(
            pushlog_response_protocol().as_ref(),
            "/defradb/pushlog_resp/0.0.1"
        );
    }

    #[test]
    fn test_protocol_builders() {
        assert_eq!(request_protocol("pushlog"), "/defradb/pushlog_req/0.0.1");
        assert_eq!(response_protocol("pushlog"), "/defradb/pushlog_resp/0.0.1");
        assert_eq!(request_protocol("rep"), "/defradb/rep_req/0.0.1");
        assert_eq!(response_protocol("rep"), "/defradb/rep_resp/0.0.1");
    }

    #[test]
    fn test_message_version() {
        assert_eq!(MESSAGE_VERSION, "/defradb/0.0.1");
    }
}
