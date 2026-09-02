//! Error types for database merge operations.

use cid::Cid;
use defra_core::merge::MergeErrorDisposition;
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

    /// Explicit replay authorization does not apply to the supplied block.
    #[error("invalid explicit replay authorization: {0}")]
    InvalidReplayAuthorization(String),

    /// CRDT merge failed.
    #[error("merge failed: {0}")]
    MergeFailed(String),

    /// A merge would change an `@immutable` field. This is a deterministic
    /// rejection of the block's content, not a transient failure: the offending
    /// block must be marked terminal (skipped) so it is not retried forever.
    #[error("immutable field rejected: {0}")]
    ImmutableFieldChanged(String),

    /// A merge would violate a unique-index constraint. Like
    /// `ImmutableFieldChanged`, this is a deterministic rejection of the
    /// block's content (the same document content will violate the same
    /// constraint on every replay), not a transient failure.
    #[error("unique constraint violation during merge: {0}")]
    UniqueConstraintViolation(String),

    /// Database error.
    #[error("database error: {0}")]
    Database(#[from] crate::error::Error),

    /// Storage error.
    #[error("storage error: {0}")]
    Storage(String),

    /// KMS lookup or authorization error.
    #[error("KMS error: {0}")]
    Kms(#[from] kms::Error),

    /// Block signature verification failed — block MUST be rejected.
    #[error("block signature verification failed for cid={cid}: {reason}")]
    SignatureVerificationFailed { cid: Cid, reason: String },

    /// The shared per-doc batch gate is currently held (e.g. by a long-lived
    /// local/interactive transaction). Batch merging is an optimization, so the
    /// caller should degrade to the gate-free per-block merge path rather than
    /// block node-wide. Transient signal, not a block-content rejection (#1041).
    #[error("batch gate contended")]
    GateContended,

    /// DAG traversal depth limit exceeded.
    ///
    /// A maliciously crafted deeply-nested DAG could otherwise consume unbounded resources.
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

    /// Classify failures that cannot change while replaying the same block.
    pub(crate) fn disposition(&self) -> MergeErrorDisposition {
        match self {
            Self::BlockDecode(_)
            | Self::UnsupportedDelta(_)
            | Self::InvalidReplayAuthorization(_)
            | Self::ImmutableFieldChanged(_)
            | Self::UniqueConstraintViolation(_)
            | Self::SignatureVerificationFailed { .. }
            | Self::DepthExceeded { .. } => MergeErrorDisposition::Terminal,
            Self::MissingMetadata(_)
            | Self::MergeFailed(_)
            | Self::Database(_)
            | Self::Storage(_)
            | Self::Kms(_)
            | Self::GateContended => MergeErrorDisposition::Retryable,
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
pub struct CounterMergeResult {
    /// Whether the merge was applied (vs skipped due to nonce)
    pub(crate) applied: bool,
    /// The accumulated counter value after merge
    pub(crate) value: Option<NormalValue>,
}

impl CounterMergeResult {
    /// True when the delta was applied rather than skipped on nonce.
    pub fn applied(&self) -> bool {
        self.applied
    }
}
