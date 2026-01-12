//! Storage traits and interfaces

use crate::Result;
use async_trait::async_trait;

/// Key-value store trait - basic storage interface
#[async_trait]
pub trait Store: Send + Sync {
    /// Get a value by key
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Put a key-value pair
    async fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()>;

    /// Delete a key
    async fn delete(&mut self, key: &[u8]) -> Result<()>;

    /// Check if a key exists
    async fn has(&self, key: &[u8]) -> Result<bool>;
}

/// Transaction trait for atomic operations
#[async_trait]
pub trait Transaction: Send + Sync {
    /// Commit the transaction
    async fn commit(self) -> Result<()>;

    /// Rollback the transaction
    async fn rollback(self) -> Result<()>;

    /// Put a key-value pair in the transaction
    async fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()>;

    /// Delete a key in the transaction
    async fn delete(&mut self, key: &[u8]) -> Result<()>;
}
