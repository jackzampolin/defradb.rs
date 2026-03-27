//! Storage traits and interfaces

use crate::Result;
use async_trait::async_trait;

pub(crate) mod private {
    pub trait Sealed {}
}

/// Key-value store trait - basic storage interface
#[async_trait]
pub trait Store: Send + Sync + private::Sealed {
    /// Get a value by key
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Set a key-value pair
    async fn set(&self, key: &[u8], value: &[u8]) -> Result<()>;

    /// Delete a key
    async fn delete(&self, key: &[u8]) -> Result<()>;

    /// Check if a key exists
    async fn has(&self, key: &[u8]) -> Result<bool>;
}

/// Transaction trait for atomic operations
#[async_trait]
pub trait Transaction: Send + Sync + private::Sealed {
    /// Commit the transaction
    async fn commit(self) -> Result<()>;

    /// Rollback the transaction
    async fn rollback(self) -> Result<()>;

    /// Set a key-value pair in the transaction
    async fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()>;

    /// Delete a key in the transaction
    async fn delete(&mut self, key: &[u8]) -> Result<()>;
}
