//! Error types for database merge operations.

use cid::Cid;
use document::NormalValue;

/// Error type for database merge operations.
#[derive(Debug, thiserror::Error)]
pub enum MergeError {
    /// Failed to decode block from DAG-CBOR.
    #[error("block decode failed: {0}")]
    BlockDecode(String),

    /// Unsupported CRDT delta type.
    #[error("unsupported delta type: {0}")]
    UnsupportedDelta(String),

    /// Missing metadata during non-recovery operation.
    #[error("missing metadata: {0}")]
    MissingMetadata(String),

    /// CRDT merge failed.
    #[error("merge failed: {0}")]
    MergeFailed(String),

    /// Database error.
    #[error("database error: {0}")]
    Database(#[from] crate::error::Error),

    /// Storage error.
    #[error("storage error: {0}")]
    Storage(String),

    /// Block signature verification failed — block MUST be rejected.
    #[error("block signature verification failed for cid={cid}: {reason}")]
    SignatureVerificationFailed { cid: Cid, reason: String },

    /// DAG recursion depth limit exceeded.
    ///
    /// A maliciously crafted deeply-nested DAG could otherwise cause a stack overflow.
    #[error("DAG merge depth limit exceeded at cid={cid} depth={depth}")]
    DepthExceeded {
        /// CID that triggered the depth check.
        cid: Cid,
        /// Depth at which the limit was hit.
        depth: usize,
    },
}

impl MergeError {
    /// Construct a `DepthExceeded` error.
    pub(crate) fn depth_exceeded(cid: &Cid, depth: usize) -> Self {
        MergeError::DepthExceeded { cid: *cid, depth }
    }

    /// Check if this error is a transaction conflict that can be retried.
    pub(crate) fn is_txn_conflict(&self) -> bool {
        match self {
            MergeError::Database(db_err) => match db_err {
                crate::error::Error::Datastore(datastore::Error::Storage(storage_err)) => {
                    storage_err.is_txn_conflict()
                }
                crate::error::Error::Storage(storage_err) => storage_err.is_txn_conflict(),
                _ => false,
            },
            _ => false,
        }
    }
}

/// Result of processing an LWW delta, including whether it was applied
/// and the value to use for document reconstruction.
pub(crate) struct LwwMergeResult {
    /// Whether the merge was applied (vs rejected/skipped)
    pub(crate) applied: bool,
    /// The winning value for document reconstruction (if applied, use incoming; else read from store)
    pub(crate) value: Option<NormalValue>,
}

/// Result of processing a Counter delta, including whether it was applied
/// and the accumulated value for document reconstruction.
pub(crate) struct CounterMergeResult {
    /// Whether the merge was applied (vs skipped due to nonce)
    pub(crate) applied: bool,
    /// The accumulated counter value after merge
    pub(crate) value: Option<NormalValue>,
}
