//! P2P error types for DefraDB networking.

use std::io;
use thiserror::Error;

/// Result type for P2P operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Error text used when the coordinator rejects a request due to peer backpressure.
pub const RATE_LIMITED_MESSAGE: &str = "rate limited: too many requests, retry later";

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

    /// Blockstore operation failed because of a retriable storage-layer
    /// transaction conflict. Kept distinct from `BlockstoreError` so retry
    /// logic can match on a typed variant instead of a string suffix.
    #[error("blockstore transaction conflict")]
    BlockstoreTxnConflict,

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

    /// Failed to open a P2P stream.
    #[error("failed to open {protocol} stream to {peer_id}: {reason}")]
    StreamOpen {
        peer_id: String,
        protocol: String,
        reason: String,
    },

    /// Failed to write to a P2P stream.
    #[error("failed to write to {protocol} stream: {reason}")]
    StreamWrite { protocol: String, reason: String },

    /// Failed to read from a P2P stream.
    #[error("stream read timed out from peer {peer_id}")]
    StreamReadTimeout { peer_id: String },

    /// Multiaddr does not contain a peer ID component.
    #[error("multiaddr does not contain peer ID: {addr}")]
    MissingPeerIdInMultiaddr { addr: String },

    /// Replicator has no collections specified.
    #[error("replicator collections cannot be empty")]
    EmptyReplicatorCollections,

    /// GossipSub configuration error.
    #[error("gossipsub config error: {0}")]
    GossipSubConfig(String),

    /// Explicit replay capability validation failed.
    #[error("explicit replay capability error: {0}")]
    ExplicitReplayCapability(String),

    /// System clock error (used in explicit replay capability TTL checks).
    #[error("system clock error: {0}")]
    SystemClock(String),

    /// Head provider error (used in DocSync / BranchableSync).
    #[error("head provider error: {0}")]
    HeadProvider(String),
}

impl Error {
    /// Suffix that storage surfaces in stringified transaction-conflict errors.
    ///
    /// Used only as a fallback for legacy call sites that still construct
    /// `BlockstoreError`/`Storage` from an already-stringified error chain.
    /// New call sites should flow typed `blockstore::Error` /
    /// `storage::corekv::Error` values through `Error::from_blockstore`
    /// instead, which preserves `BlockstoreTxnConflict` without relying on
    /// wording stability.
    const TXN_CONFLICT_SUFFIX: &str = "transaction conflict. Please retry";

    /// Convert a typed blockstore error into the nearest P2P error,
    /// preserving the retriable transaction-conflict signal instead of
    /// stringifying it away.
    pub fn from_blockstore(err: blockstore::Error) -> Self {
        if err.is_txn_conflict() {
            Error::BlockstoreTxnConflict
        } else {
            Error::BlockstoreError(err.to_string())
        }
    }

    /// Returns true if this error represents a storage-layer transaction conflict.
    pub fn is_txn_conflict(&self) -> bool {
        match self {
            Error::BlockstoreTxnConflict => true,
            Error::BlockstoreError(msg) | Error::Storage(msg) => {
                msg.ends_with(Self::TXN_CONFLICT_SUFFIX)
            }
            _ => false,
        }
    }

    /// Returns true if retrying the operation may succeed without changing inputs.
    pub fn is_retriable(&self) -> bool {
        self.is_txn_conflict()
    }

    /// Returns true if the error looks like a transport/connection teardown signal.
    ///
    /// This is intentionally broader than a single enum variant because iroh and
    /// libp2p surface many disconnect cases through stringified transport errors.
    pub fn is_connection_like(&self) -> bool {
        match self {
            Error::ConnectionClosed
            | Error::ChannelSend
            | Error::ChannelReceive
            | Error::ResponseTimeout
            | Error::ConnectionTimeout(_) => true,
            Error::Dial(msg)
            | Error::Transport(msg)
            | Error::Codec(msg)
            | Error::ProtocolNegotiation(msg)
            | Error::Noise(msg)
            | Error::Swarm(msg)
            | Error::ResponseSend(msg) => Self::is_connection_loss_reason(msg),
            Error::Io(err) => Self::is_connection_loss_reason(&err.to_string()),
            Error::StreamOpen { reason, .. } | Error::StreamWrite { reason, .. } => {
                Self::is_connection_loss_reason(reason)
            }
            _ => false,
        }
    }

    /// Best-effort classifier for common disconnect / teardown reasons surfaced
    /// as transport strings.
    pub fn is_connection_loss_reason(reason: &str) -> bool {
        let lower = reason.to_ascii_lowercase();
        lower == "closed"
            || lower.ends_with(": closed")
            || lower.contains("connection lost")
            || lower.contains("connection closed")
            || lower.contains("connection reset")
            || lower.contains("stream reset")
            || lower.contains("broken pipe")
            || lower.contains("peer closed")
            || lower.contains("aborted by peer")
            || lower.contains("closed during the handshake")
            || lower.contains("timed out waiting for peer")
            || lower.contains("endpoint closed")
    }

    /// Returns true if this is the coordinator's synthetic rate-limit rejection.
    pub fn is_rate_limited(&self) -> bool {
        matches!(
            self,
            Error::AccessDenied {
                collection_id,
                ..
            } if collection_id == "rate-limited"
        )
    }
}

/// Returns true when a peer reply carries the coordinator's explicit rate-limit signal.
pub fn is_rate_limited_message(message: &str) -> bool {
    message == RATE_LIMITED_MESSAGE
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

#[cfg(feature = "libp2p-transport")]
impl From<libp2p::TransportError<io::Error>> for Error {
    fn from(e: libp2p::TransportError<io::Error>) -> Self {
        Error::Transport(e.to_string())
    }
}

#[cfg(feature = "libp2p-transport")]
impl From<libp2p::multiaddr::Error> for Error {
    fn from(e: libp2p::multiaddr::Error) -> Self {
        Error::InvalidMultiaddr(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::Error;

    #[test]
    fn txn_conflict_detection_matches_wrapped_storage_messages() {
        // Legacy stringified variants: retained so any caller that still
        // builds errors by stringifying stays retriable.
        let blockstore_error =
            Error::BlockstoreError("storage error: transaction conflict. Please retry".into());
        let storage_error = Error::Storage("transaction conflict. Please retry".into());

        assert!(blockstore_error.is_txn_conflict());
        assert!(storage_error.is_txn_conflict());
        assert!(blockstore_error.is_retriable());
        assert!(storage_error.is_retriable());
    }

    #[test]
    fn txn_conflict_detection_matches_typed_variant() {
        let typed = Error::BlockstoreTxnConflict;
        assert!(typed.is_txn_conflict());
        assert!(typed.is_retriable());
    }

    #[test]
    fn from_blockstore_preserves_typed_txn_conflict() {
        let conflict = blockstore::Error::Storage(storage::corekv::Error::TxnConflict);
        assert!(conflict.is_txn_conflict());
        let p2p_err = Error::from_blockstore(conflict);
        assert!(matches!(p2p_err, Error::BlockstoreTxnConflict));
        assert!(p2p_err.is_retriable());
    }

    #[test]
    fn from_blockstore_non_conflict_keeps_stringified_message() {
        let other = blockstore::Error::NotFound("missing".into());
        let p2p_err = Error::from_blockstore(other);
        match p2p_err {
            Error::BlockstoreError(msg) => assert!(msg.contains("missing")),
            other => panic!("expected BlockstoreError, got {other:?}"),
        }
    }

    #[test]
    fn rate_limited_detection_is_typed() {
        let rate_limited = Error::AccessDenied {
            peer_id: "peer-1".into(),
            collection_id: "rate-limited".into(),
        };
        let ordinary_denial = Error::AccessDenied {
            peer_id: "peer-1".into(),
            collection_id: "users".into(),
        };

        assert!(rate_limited.is_rate_limited());
        assert!(!ordinary_denial.is_rate_limited());
    }

    #[test]
    fn connection_like_detection_matches_common_transport_failures() {
        assert!(Error::ChannelSend.is_connection_like());
        assert!(Error::ChannelReceive.is_connection_like());
        assert!(Error::Dial("closed".into()).is_connection_like());
        assert!(Error::Codec("failed to read length: connection lost".into()).is_connection_like());
        assert!(Error::Transport("peer closed stream".into()).is_connection_like());
        assert!(Error::ResponseSend("broken pipe".into()).is_connection_like());
        assert!(!Error::AccessDenied {
            peer_id: "peer-1".into(),
            collection_id: "users".into(),
        }
        .is_connection_like());
    }
}
