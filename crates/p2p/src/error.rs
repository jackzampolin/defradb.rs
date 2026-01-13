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
