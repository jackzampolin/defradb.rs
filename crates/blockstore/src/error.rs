//! Blockstore error types

use thiserror::Error;

/// Result type for blockstore operations
pub type Result<T> = std::result::Result<T, Error>;

/// Blockstore errors
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// Storage layer error
    #[error("storage error: {0}")]
    Storage(#[from] storage::corekv::Error),

    /// CID parsing error
    #[error("invalid CID: {0}")]
    InvalidCid(#[from] cid::Error),

    /// Block not found
    #[error("block not found: {0}")]
    NotFound(String),

    /// Hash mismatch - data doesn't match CID (detected when hash_on_read enabled)
    #[error("hash mismatch for CID {cid}: data hash doesn't match expected hash")]
    HashMismatch { cid: String },

    /// CID verification failed - block data does not hash to the claimed CID
    #[error("CID verification failed for {cid}: block content does not match claimed CID")]
    CidVerificationFailed { cid: String },

    /// Unsupported hash algorithm in CID - only SHA2-256 (0x12) is accepted for P2P blocks
    #[error(
        "unsupported hash algorithm 0x{code:x} in CID {cid}: only SHA2-256 (0x12) is accepted"
    )]
    UnsupportedHashAlgorithm { code: u64, cid: String },

    /// Internal error
    #[error("internal error: {0}")]
    Internal(String),
}

impl Error {
    /// Returns true when this error originates from a storage-layer transaction
    /// conflict. Callers that want to retry safely should prefer this typed
    /// check over matching on the stringified error chain.
    pub fn is_txn_conflict(&self) -> bool {
        matches!(self, Error::Storage(storage_err) if storage_err.is_txn_conflict())
    }
}
