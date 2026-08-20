//! Transaction lookup result type.

use std::sync::Arc;

use crate::error::TransactionError;

use super::context::TransactionContext;

/// Result of looking up a transaction by handle.
#[non_exhaustive]
pub enum GetTransactionResult {
    /// Transaction found.
    Found(Arc<dyn TransactionContext>),
    /// Transaction not found (never existed, or already committed/rolled back).
    NotFound,
    /// Lock is poisoned (a panic occurred elsewhere, system may be corrupted).
    LockPoisoned,
}

impl std::fmt::Debug for GetTransactionResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Found(ctx) => f
                .debug_tuple("Found")
                .field(&format!("TransactionContext(id={})", ctx.id()))
                .finish(),
            Self::NotFound => f.write_str("NotFound"),
            Self::LockPoisoned => f.write_str("LockPoisoned"),
        }
    }
}

impl GetTransactionResult {
    /// Convert to a Result, treating NotFound as None and LockPoisoned as an error.
    ///
    /// Use this when you need to distinguish between "not found" and "lock poisoned"
    /// for proper error handling.
    pub fn into_result(
        self,
    ) -> std::result::Result<Option<Arc<dyn TransactionContext>>, TransactionError> {
        match self {
            Self::Found(ctx) => Ok(Some(ctx)),
            Self::NotFound => Ok(None),
            Self::LockPoisoned => Err(TransactionError::lock_poisoned(
                "transaction registry lock poisoned",
            )),
        }
    }

    /// Check if the transaction was found.
    pub fn is_found(&self) -> bool {
        matches!(self, Self::Found(_))
    }

    /// Check if the lock is poisoned.
    pub fn is_lock_poisoned(&self) -> bool {
        matches!(self, Self::LockPoisoned)
    }
}
