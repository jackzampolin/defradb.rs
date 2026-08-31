//! What a transaction reads and writes through.

use regolith::{OwnedTransaction, Snapshot};

/// A read-only transaction pins a point in time; a writing one begins a
/// regolith transaction that validates at commit.
///
/// Held behind an `Arc` so an iterator can keep it alive without
/// borrowing the transaction it was opened from.
pub(crate) enum Handle {
    /// Nothing can be written through it, so there is nothing to validate
    /// at commit and no conflict to detect.
    ReadOnly(Snapshot),
    /// Buffers its own writes, tracks its own read set, validates itself.
    ///
    /// Boxed because it is by far the larger of the two and read-only is the
    /// common case: inline it would cost every query's handle the size of a
    /// transaction it does not have.
    Writable(Box<OwnedTransaction>),
}
