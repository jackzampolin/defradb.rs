//! Transaction context trait.

use std::sync::Arc;

use crate::runner::DocFetcher;

/// Transaction context that provides storage access within a transaction.
///
/// This is implemented by the database layer to provide transaction-scoped
/// document fetching.
pub trait TransactionContext: Send + Sync {
    /// Get the transaction ID.
    fn id(&self) -> &str;

    /// Check if this is a read-only transaction.
    fn is_readonly(&self) -> bool;

    /// Get a document fetcher scoped to this transaction.
    fn doc_fetcher(&self) -> Arc<dyn DocFetcher>;

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
