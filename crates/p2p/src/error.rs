// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! P2P error types for DefraDB networking.

use std::io;
use thiserror::Error;

/// Result type for P2P operations.
pub type Result<T> = std::result::Result<T, Error>;

/// P2P error types.
#[derive(Debug, Error)]
pub enum Error {
    /// Transport error during network communication.
    #[error("transport error: {0}")]
    Transport(String),

    /// Failed to dial a peer.
    #[error("dial error: {0}")]
    Dial(String),

    /// Connection to peer was closed.
    #[error("connection closed")]
    ConnectionClosed,

    /// Protocol negotiation failed.
    #[error("protocol negotiation failed: {0}")]
    ProtocolNegotiation(String),

    /// Message encoding/decoding error.
    #[error("codec error: {0}")]
    Codec(String),

    /// Invalid message signature.
    #[error("invalid signature")]
    InvalidSignature,

    /// Public key does not match peer ID.
    #[error("public key does not match peer ID")]
    PubkeyPeerIdMismatch,

    /// Failed to generate message ID.
    #[error("failed to generate message ID")]
    MessageIdGeneration,

    /// Failed to encode public key.
    #[error("failed to encode public key: {0}")]
    PublicKeyEncode(String),

    /// Failed to decode public key.
    #[error("failed to decode public key: {0}")]
    PublicKeyDecode(String),

    /// Signing operation failed.
    #[error("signing failed: {0}")]
    SigningFailed(String),

    /// Message has no signature.
    #[error("message has no signature")]
    MissingSignature,

    /// Response timeout.
    #[error("response timeout")]
    ResponseTimeout,

    /// Unexpected response type.
    #[error("unexpected response type: expected {expected}, got {actual}")]
    UnexpectedResponseType { expected: String, actual: String },

    /// Peer not found.
    #[error("peer not found: {0}")]
    PeerNotFound(String),

    /// Invalid multiaddress.
    #[error("invalid multiaddress: {0}")]
    InvalidMultiaddr(String),

    /// Swarm error.
    #[error("swarm error: {0}")]
    Swarm(String),

    /// I/O error.
    #[error("io error: {0}")]
    Io(#[from] io::Error),

    /// CBOR serialization error.
    #[error("cbor serialization error: {0}")]
    CborSerialization(String),

    /// CBOR deserialization error.
    #[error("cbor deserialization error: {0}")]
    CborDeserialization(String),

    /// Noise protocol error.
    #[error("noise protocol error: {0}")]
    Noise(String),

    /// Behaviour error.
    #[error("behaviour error: {0}")]
    Behaviour(String),

    /// Already listening on address.
    #[error("already listening on {0}")]
    AlreadyListening(String),

    /// Not listening.
    #[error("not listening")]
    NotListening,

    /// Invalid peer ID.
    #[error("invalid peer ID: {0}")]
    InvalidPeerId(String),

    /// Channel send error.
    #[error("channel send error")]
    ChannelSend,

    /// Channel receive error.
    #[error("channel receive error")]
    ChannelReceive,

    /// GossipSub subscription error.
    #[error("gossipsub subscription error: {0}")]
    GossipSubSubscription(String),

    /// GossipSub publish error.
    #[error("gossipsub publish error: {0}")]
    GossipSubPublish(String),

    /// GossipSub unsubscribe error.
    #[error("gossipsub unsubscribe error: {0}")]
    GossipSubUnsubscribe(String),

    /// Invalid topic.
    #[error("invalid topic: {0}")]
    InvalidTopic(String),

    /// Invalid CID.
    #[error("invalid CID: {0}")]
    InvalidCid(String),

    /// Blockstore error.
    #[error("blockstore error: {0}")]
    BlockstoreError(String),
}

impl From<serde_cbor::Error> for Error {
    fn from(e: serde_cbor::Error) -> Self {
        if e.is_io() || e.is_eof() || e.is_syntax() {
            Error::CborDeserialization(e.to_string())
        } else {
            Error::CborSerialization(e.to_string())
        }
    }
}

impl From<libp2p::TransportError<io::Error>> for Error {
    fn from(e: libp2p::TransportError<io::Error>) -> Self {
        Error::Transport(e.to_string())
    }
}

impl From<libp2p::multiaddr::Error> for Error {
    fn from(e: libp2p::multiaddr::Error) -> Self {
        Error::InvalidMultiaddr(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        // Test that all error variants have proper display messages
        let transport = Error::Transport("connection refused".to_string());
        assert!(transport.to_string().contains("transport error"));
        assert!(transport.to_string().contains("connection refused"));

        let dial = Error::Dial("no addresses".to_string());
        assert!(dial.to_string().contains("dial error"));

        let closed = Error::ConnectionClosed;
        assert_eq!(closed.to_string(), "connection closed");

        let protocol = Error::ProtocolNegotiation("mismatch".to_string());
        assert!(protocol.to_string().contains("protocol negotiation"));

        let codec = Error::Codec("invalid data".to_string());
        assert!(codec.to_string().contains("codec error"));

        let invalid_sig = Error::InvalidSignature;
        assert_eq!(invalid_sig.to_string(), "invalid signature");

        let pubkey_mismatch = Error::PubkeyPeerIdMismatch;
        assert!(pubkey_mismatch.to_string().contains("public key"));

        let msg_id_gen = Error::MessageIdGeneration;
        assert!(msg_id_gen.to_string().contains("message ID"));

        let pubkey_encode = Error::PublicKeyEncode("encoding failed".to_string());
        assert!(pubkey_encode.to_string().contains("encode public key"));

        let pubkey_decode = Error::PublicKeyDecode("decoding failed".to_string());
        assert!(pubkey_decode.to_string().contains("decode public key"));

        let signing_failed = Error::SigningFailed("key unavailable".to_string());
        assert!(signing_failed.to_string().contains("signing failed"));

        let missing_sig = Error::MissingSignature;
        assert!(missing_sig.to_string().contains("no signature"));

        let timeout = Error::ResponseTimeout;
        assert_eq!(timeout.to_string(), "response timeout");

        let unexpected = Error::UnexpectedResponseType {
            expected: "PushLogReply".to_string(),
            actual: "Unknown".to_string(),
        };
        assert!(unexpected.to_string().contains("PushLogReply"));
        assert!(unexpected.to_string().contains("Unknown"));

        let peer_not_found = Error::PeerNotFound("12D3...".to_string());
        assert!(peer_not_found.to_string().contains("peer not found"));

        let invalid_addr = Error::InvalidMultiaddr("/invalid".to_string());
        assert!(invalid_addr.to_string().contains("invalid multiaddress"));

        let swarm = Error::Swarm("behaviour error".to_string());
        assert!(swarm.to_string().contains("swarm error"));

        let io_err = Error::Io(io::Error::new(io::ErrorKind::NotFound, "not found"));
        assert!(io_err.to_string().contains("io error"));

        let cbor_ser = Error::CborSerialization("failed to serialize".to_string());
        assert!(cbor_ser.to_string().contains("cbor serialization"));

        let cbor_de = Error::CborDeserialization("failed to deserialize".to_string());
        assert!(cbor_de.to_string().contains("cbor deserialization"));

        let noise = Error::Noise("handshake failed".to_string());
        assert!(noise.to_string().contains("noise protocol"));

        let behaviour = Error::Behaviour("init failed".to_string());
        assert!(behaviour.to_string().contains("behaviour error"));

        let already_listening = Error::AlreadyListening("0.0.0.0:9000".to_string());
        assert!(already_listening.to_string().contains("already listening"));

        let not_listening = Error::NotListening;
        assert_eq!(not_listening.to_string(), "not listening");

        let invalid_peer = Error::InvalidPeerId("not-a-peer-id".to_string());
        assert!(invalid_peer.to_string().contains("invalid peer ID"));

        let channel_send = Error::ChannelSend;
        assert_eq!(channel_send.to_string(), "channel send error");

        let channel_recv = Error::ChannelReceive;
        assert_eq!(channel_recv.to_string(), "channel receive error");
    }

    #[test]
    fn test_error_from_io() {
        let io_err = io::Error::new(io::ErrorKind::ConnectionRefused, "refused");
        let err: Error = io_err.into();

        match err {
            Error::Io(e) => assert_eq!(e.kind(), io::ErrorKind::ConnectionRefused),
            _ => panic!("Expected Io error variant"),
        }
    }

    #[test]
    fn test_error_from_multiaddr() {
        // Create an invalid multiaddr to test the conversion
        let result: std::result::Result<libp2p::Multiaddr, _> = "/invalid/addr".parse();
        if let Err(e) = result {
            let err: Error = e.into();
            match err {
                Error::InvalidMultiaddr(msg) => {
                    assert!(!msg.is_empty());
                }
                _ => panic!("Expected InvalidMultiaddr error"),
            }
        }
    }

    #[test]
    fn test_error_debug() {
        // Test Debug implementation
        let err = Error::Transport("test".to_string());
        let debug_str = format!("{:?}", err);
        assert!(debug_str.contains("Transport"));
        assert!(debug_str.contains("test"));
    }

    #[test]
    fn test_result_type() {
        fn returns_result() -> Result<i32> {
            Ok(42)
        }

        fn returns_error() -> Result<i32> {
            Err(Error::ConnectionClosed)
        }

        assert_eq!(returns_result().unwrap(), 42);
        assert!(returns_error().is_err());
    }

    #[test]
    fn test_gossipsub_errors() {
        let sub_err = Error::GossipSubSubscription("topic not found".to_string());
        assert!(sub_err.to_string().contains("gossipsub subscription"));
        assert!(sub_err.to_string().contains("topic not found"));

        let pub_err = Error::GossipSubPublish("no peers".to_string());
        assert!(pub_err.to_string().contains("gossipsub publish"));
        assert!(pub_err.to_string().contains("no peers"));

        let unsub_err = Error::GossipSubUnsubscribe("not subscribed".to_string());
        assert!(unsub_err.to_string().contains("gossipsub unsubscribe"));
        assert!(unsub_err.to_string().contains("not subscribed"));

        let topic_err = Error::InvalidTopic("empty topic".to_string());
        assert!(topic_err.to_string().contains("invalid topic"));
        assert!(topic_err.to_string().contains("empty topic"));
    }
}
