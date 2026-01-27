//! Transaction context trait.

use std::sync::Arc;
use storage::corekv::MaybeSendSync;

use crate::mutator::DocMutator;
use crate::runner::DocFetcher;

/// Transaction context that provides storage access within a transaction.
///
/// This is implemented by the database layer to provide transaction-scoped
/// document fetching and mutation.
pub trait TransactionContext: MaybeSendSync {
    /// Get the transaction ID.
    fn id(&self) -> &str;

    /// Check if this is a read-only transaction.
    fn is_readonly(&self) -> bool;

    /// Get a document fetcher scoped to this transaction.
    fn doc_fetcher(&self) -> Arc<dyn DocFetcher>;

    /// Get a document mutator scoped to this transaction.
    ///
    /// Returns `None` if this is a read-only transaction or if mutators
    /// are not supported by this context implementation.
    ///
    /// The mutator shares the same underlying transaction as the fetcher,
    /// so all read and write operations are within the same transaction context.
    fn doc_mutator(&self) -> Option<Arc<dyn DocMutator>> {
        None
    }

    /// Check if the transaction is still active (not yet committed or rolled back).
    ///
    /// Returns `true` if the transaction can still be used for queries.
    /// Returns `false` if the transaction has been consumed via commit/rollback.
    ///
    /// # Implementation Note
    ///
    /// Concrete implementations SHOULD override this method if they track
    /// consumption state. The default returns `true`, which is appropriate
    /// for implementations that don't track state or where checking state
    /// synchronously isn't feasible (e.g., when state is behind an async mutex).
    fn is_active(&self) -> bool {
        true
    }
}
