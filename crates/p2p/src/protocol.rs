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

use libp2p::StreamProtocol;

/// The protocol name/slug representing DefraDB.
pub const NAME: &str = "defra";

/// DefraDB's multicodec code (arbitrary).
pub const CODE: u64 = 961;

/// Current protocol version.
pub const VERSION: &str = "0.0.1";

/// The complete libp2p protocol identifier.
/// Format: /{name}/{version}
pub const PROTOCOL_ID: &str = "/defra/0.0.1";

/// Message version string used in wire messages.
pub const MESSAGE_VERSION: &str = "/defradb/0.0.1";

/// StreamProtocol for the pushlog request-response protocol.
pub fn pushlog_protocol() -> StreamProtocol {
    StreamProtocol::new(PROTOCOL_ID)
}

/// StreamProtocol for the replicator communication channel.
pub fn replicator_protocol() -> StreamProtocol {
    StreamProtocol::new("/defra/rep/0.0.1")
}

/// StreamProtocol for identity exchange.
pub fn identity_protocol() -> StreamProtocol {
    StreamProtocol::new("/defra/identity/0.0.1")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_protocol_id_format() {
        assert_eq!(PROTOCOL_ID, format!("/{}/{}", NAME, VERSION));
    }

    #[test]
    fn test_code_value() {
        assert_eq!(CODE, 961);
    }

    #[test]
    fn test_stream_protocols() {
        assert_eq!(pushlog_protocol().as_ref(), PROTOCOL_ID);
        assert_eq!(replicator_protocol().as_ref(), "/defra/rep/0.0.1");
        assert_eq!(identity_protocol().as_ref(), "/defra/identity/0.0.1");
    }
}
