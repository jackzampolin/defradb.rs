//! Blockstore error types

use thiserror::Error;

/// Result type for blockstore operations
pub type Result<T> = std::result::Result<T, Error>;

/// Blockstore errors
#[derive(Debug, Error)]
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

    /// Internal error
    #[error("internal error: {0}")]
    Internal(String),
}
