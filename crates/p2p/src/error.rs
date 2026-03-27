//! P2P error types for DefraDB networking.

use std::io;
use thiserror::Error;

/// Result type for P2P operations.
pub type Result<T> = std::result::Result<T, Error>;

/// P2P error types.
#[derive(Debug, Error)]
#[non_exhaustive]
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

    /// Block CID verification failed — peer sent data that does not match the claimed CID.
    #[error("block CID verification failed for {cid}: content hash does not match")]
    BlockCidMismatch { cid: String },

    /// Unsupported hash algorithm in block CID — only SHA2-256 is accepted from peers.
    #[error("unsupported hash algorithm 0x{code:x} in block CID {cid}: only SHA2-256 is accepted")]
    UnsupportedBlockHashAlgorithm { code: u64, cid: String },

    /// Blockstore error.
    #[error("blockstore error: {0}")]
    BlockstoreError(String),

    /// Failed to send response.
    #[error("failed to send response: {0}")]
    ResponseSend(String),

    /// DAG sync failed with reason.
    #[error("DAG sync failed for CID {cid}: {reason}")]
    DagSyncFailed {
        /// The CID that failed to sync
        cid: String,
        /// Why the sync failed
        reason: String,
    },

    /// Recovery completed with failures.
    #[error("recovery completed with {failed} failures out of {total} blocks")]
    RecoveryFailed {
        /// Number of blocks successfully recovered
        success: usize,
        /// Number of blocks that failed to recover
        failed: usize,
        /// Total blocks attempted
        total: usize,
    },

    /// Block could not be parsed as IPLD.
    #[error("failed to parse block as IPLD: {reason}")]
    BlockParseError {
        /// Why the block couldn't be parsed
        reason: String,
    },

    /// Invalid configuration value.
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    /// Access denied for P2P operation.
    #[error("access denied: peer {peer_id} not authorized for collection {collection_id}")]
    AccessDenied {
        /// The peer that was denied access
        peer_id: String,
        /// The collection they tried to access
        collection_id: String,
    },

    /// Connection timed out waiting for peer.
    #[error("connection timed out waiting for peer {0}")]
    ConnectionTimeout(String),

    /// Storage error during P2P operation.
    #[error("storage error: {0}")]
    Storage(String),
}

/// Convert a blockstore CID verification error into its P2P counterpart.
///
/// Called at each P2P block ingestion boundary so callers get typed errors
/// instead of the generic `BlockstoreError(String)` variant.
pub fn blockstore_verify_to_p2p(e: blockstore::Error, cid: &cid::Cid) -> Error {
    match e {
        blockstore::Error::CidVerificationFailed { .. } => Error::BlockCidMismatch {
            cid: cid.to_string(),
        },
        blockstore::Error::UnsupportedHashAlgorithm { code, .. } => {
            Error::UnsupportedBlockHashAlgorithm {
                code,
                cid: cid.to_string(),
            }
        }
        other => Error::BlockstoreError(other.to_string()),
    }
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
