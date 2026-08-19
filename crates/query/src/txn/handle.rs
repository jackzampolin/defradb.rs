//! Transaction handle type.

use std::fmt;
use std::ops::Deref;

use crate::error::TransactionError;

/// An opaque handle to an active transaction.
///
/// Handles should be obtained from `TransactionRegistry::begin()`. While the
/// `new()` constructor and `FromStr` implementation are public (for registry
/// implementors and HTTP deserialization), handles not registered with a
/// registry will fail validation when used with `get()`, `commit()`, or
/// `rollback()`.
///
/// The handle is serializable (implements `Display` and `FromStr`) for use in
/// HTTP APIs and other contexts where string serialization is needed.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TransactionHandle(String);

impl TransactionHandle {
    /// Create a new transaction handle.
    ///
    /// # For `TransactionRegistry` Implementors Only
    ///
    /// This constructor is intended for use by `TransactionRegistry::begin()`
    /// implementations. Application code should obtain handles through
    /// `TransactionRegistry::begin()`, not by direct construction.
    ///
    /// Handles created outside of a registry will fail when used with
    /// `get()`, `commit()`, or `rollback()` - the registry won't find them.
    ///
    /// # Panics
    ///
    /// Panics if `id` is empty. Transaction IDs must be non-empty strings.
    #[doc(hidden)]
    pub fn new(id: String) -> Self {
        assert!(!id.is_empty(), "transaction ID cannot be empty");
        Self(id)
    }

    /// Get the underlying transaction ID string.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Convert into the underlying string.
    pub fn into_string(self) -> String {
        self.0
    }
}

impl Deref for TransactionHandle {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl fmt::Display for TransactionHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<TransactionHandle> for String {
    fn from(handle: TransactionHandle) -> Self {
        handle.0
    }
}

/// Parse a transaction handle from a string.
///
/// This allows deserializing transaction IDs from HTTP requests.
/// Note: This does NOT validate that the transaction exists - that's
/// done when you actually use the handle with `get()`, `commit()`, etc.
///
/// Returns an error if the string is empty, since transaction IDs must be non-empty.
impl std::str::FromStr for TransactionHandle {
    type Err = TransactionError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        if s.is_empty() {
            return Err(TransactionError::execution(
                "transaction ID cannot be empty",
            ));
        }
        Ok(Self(s.to_string()))
    }
}
