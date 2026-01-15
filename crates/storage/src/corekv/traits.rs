/// Core KV trait definitions matching the Go corekv package.
///
/// This module defines the complete trait hierarchy for key-value storage:
/// - Reader: Read-only operations
/// - Writer: Write operations
/// - ReaderWriter: Combined read-write
/// - Store: Store that can create transactions
/// - Txn: Transaction interface with ACID guarantees
/// - TxnStore: Store that supports transactions
///
/// All traits use async_trait to support asynchronous operations.
use async_trait::async_trait;
use std::future::Future;
use std::pin::Pin;

use super::errors::Result;
use super::iterator::Iterator;
use super::types::IterOptions;

/// Callback function type for transaction lifecycle events.
///
/// These callbacks are executed synchronously when transaction events occur.
pub type TxnCallback = Box<dyn FnOnce() + Send + 'static>;

/// Asynchronous callback function type for transaction lifecycle events.
///
/// These callbacks are executed asynchronously when transaction events occur.
pub type AsyncTxnCallback =
    Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + 'static>;

/// Reader trait for read-only key-value operations.
///
/// This trait provides the core read operations: get, has, and iterator.
/// All operations are asynchronous to support various backend implementations.
#[async_trait]
pub trait Reader: Send + Sync {
    /// Retrieve the value associated with a key.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to look up
    ///
    /// # Returns
    ///
    /// * `Ok(Some(value))` if the key exists
    /// * `Ok(None)` if the key does not exist
    /// * `Err(Error)` if an error occurred
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Check if a key exists in the store.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to check
    ///
    /// # Returns
    ///
    /// * `Ok(true)` if the key exists
    /// * `Ok(false)` if the key does not exist
    /// * `Err(Error)` if an error occurred
    ///
    /// # Note
    ///
    /// Performance may vary by backend. Some backends may need to fetch the value
    /// to check existence.
    async fn has(&self, key: &[u8]) -> Result<bool>;

    /// Get the size of a value without reading the entire value.
    ///
    /// This is useful for checking the size of large values without incurring
    /// the cost of reading them into memory.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to check
    ///
    /// # Returns
    ///
    /// * `Ok(Some(size))` if the key exists, with size in bytes
    /// * `Ok(None)` if the key does not exist
    /// * `Err(Error)` if an error occurred
    ///
    /// # Note
    ///
    /// Some backends may need to read the entire value to determine its size.
    /// RocksDB can determine size without reading the value in some cases.
    async fn get_size(&self, key: &[u8]) -> Result<Option<usize>>;

    /// Create an iterator over key-value pairs.
    ///
    /// The iterator can be configured with various options for filtering
    /// and ordering using `IterOptions`.
    ///
    /// # Arguments
    ///
    /// * `opts` - Configuration options for the iterator
    ///
    /// # Returns
    ///
    /// * `Ok(Iterator)` - A new iterator instance
    /// * `Err(Error)` if the iterator could not be created
    ///
    /// # Note
    ///
    /// The caller is responsible for closing the iterator when done.
    async fn iterator(&self, opts: IterOptions) -> Result<Box<dyn Iterator>>;
}

/// Writer trait for write operations.
///
/// This trait provides the core write operations: set and delete.
#[async_trait]
pub trait Writer: Send + Sync {
    /// Store a key-value pair.
    ///
    /// If the key already exists, its value is overwritten.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to store (must not be empty)
    /// * `value` - The value to store
    ///
    /// # Returns
    ///
    /// * `Ok(())` on success
    /// * `Err(Error::EmptyKey)` if the key is empty
    /// * `Err(Error::ReadOnlyTxn)` if called on a read-only transaction
    /// * `Err(Error)` for other errors
    async fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()>;

    /// Delete a key from the store.
    ///
    /// If the key doesn't exist, this is a no-op (not an error).
    ///
    /// # Arguments
    ///
    /// * `key` - The key to delete
    ///
    /// # Returns
    ///
    /// * `Ok(())` on success
    /// * `Err(Error::EmptyKey)` if the key is empty
    /// * `Err(Error::ReadOnlyTxn)` if called on a read-only transaction
    /// * `Err(Error)` for other errors
    async fn delete(&mut self, key: &[u8]) -> Result<()>;
}

/// ReaderWriter trait combining read and write operations.
///
/// This is a marker trait that combines Reader and Writer.
/// Most stores and transactions implement this trait.
pub trait ReaderWriter: Reader + Writer {}

/// Blanket implementation: any type that implements both Reader and Writer
/// automatically implements ReaderWriter.
impl<T> ReaderWriter for T where T: Reader + Writer {}

/// Store trait for key-value stores that support transactions.
///
/// This trait defines the basic store interface with transaction support.
#[async_trait]
pub trait Store: Send + Sync {
    /// Create a new transaction.
    ///
    /// # Arguments
    ///
    /// * `readonly` - If true, creates a read-only transaction that cannot
    ///   perform writes. Read-only transactions may have better performance.
    ///
    /// # Returns
    ///
    /// * `Ok(Txn)` - A new transaction instance
    /// * `Err(Error::DBClosed)` if the store is closed
    /// * `Err(Error)` for other errors
    async fn new_txn(&self, readonly: bool) -> Result<Box<dyn Txn>>;

    /// Close the store and release resources.
    ///
    /// After closing, no further operations can be performed on this store.
    /// Attempting to use a closed store will return `Error::DBClosed`.
    ///
    /// # Returns
    ///
    /// * `Ok(())` on success
    /// * `Err(Error)` if an error occurred during close
    async fn close(&self) -> Result<()>;
}

/// Dropable trait for stores that support bulk deletion.
///
/// This is an optional interface implemented by some stores. It provides a
/// convenient and efficient way of deleting all data in a store.
///
/// # Note
///
/// Concurrent writes will be blocked during `drop_all()`. Depending on the
/// underlying store implementation, concurrent reads may or may not be blocked.
#[async_trait]
pub trait Dropable: Store {
    /// Delete all data stored in the store.
    ///
    /// This is a destructive operation that removes all key-value pairs.
    /// Use with caution in production environments.
    ///
    /// # Returns
    ///
    /// * `Ok(())` on success
    /// * `Err(Error::DBClosed)` if the store is closed
    /// * `Err(Error)` for other errors
    async fn drop_all(&self) -> Result<()>;
}

/// Transaction trait with ACID guarantees and callback support.
///
/// Transactions provide atomicity, consistency, isolation, and durability (ACID).
/// All operations within a transaction are isolated from other transactions until
/// commit is called.
///
/// # Lifecycle Callbacks
///
/// Transactions support registering callbacks for lifecycle events:
/// - `on_success`: Called when commit succeeds
/// - `on_error`: Called when commit fails
/// - `on_discard`: Called when transaction is discarded
///
/// Each callback type has both sync and async variants.
///
/// # Example
///
/// ```ignore
/// let mut txn = store.new_txn(false).await?;
///
/// txn.on_success(Box::new(|| println!("Transaction committed!")));
///
/// txn.set(b"key", b"value").await?;
/// txn.commit().await?; // Callbacks execute here
/// ```
#[async_trait]
pub trait Txn: ReaderWriter {
    /// Commit the transaction, making all changes permanent.
    ///
    /// On success, all on_success callbacks are executed. On failure, all
    /// on_error callbacks are executed.
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the transaction committed successfully
    /// * `Err(Error::TxnConflict)` if a conflict occurred (retriable)
    /// * `Err(Error::DiscardedTxn)` if the transaction was already discarded
    /// * `Err(Error)` for other errors
    ///
    /// # Note
    ///
    /// After calling commit (success or failure), the transaction cannot be reused.
    async fn commit(self: Box<Self>) -> Result<()>;

    /// Discard the transaction, rolling back all changes.
    ///
    /// All on_discard callbacks are executed. After calling discard, the
    /// transaction cannot be used for any further operations.
    ///
    /// # Note
    ///
    /// This method consumes self, ensuring the transaction cannot be used after discard.
    fn discard(self: Box<Self>);

    /// Register a synchronous callback to be called on successful commit.
    ///
    /// Multiple callbacks can be registered and will be executed in order.
    fn on_success(&mut self, callback: TxnCallback);

    /// Register an asynchronous callback to be called on successful commit.
    ///
    /// Multiple callbacks can be registered and will be executed sequentially in registration order.
    fn on_success_async(&mut self, callback: AsyncTxnCallback);

    /// Register a synchronous callback to be called on commit error.
    ///
    /// Multiple callbacks can be registered and will be executed in order.
    fn on_error(&mut self, callback: TxnCallback);

    /// Register an asynchronous callback to be called on commit error.
    ///
    /// Multiple callbacks can be registered and will be executed sequentially in registration order.
    fn on_error_async(&mut self, callback: AsyncTxnCallback);

    /// Register a synchronous callback to be called on discard.
    ///
    /// Multiple callbacks can be registered and will be executed in order.
    fn on_discard(&mut self, callback: TxnCallback);

    /// Register an asynchronous callback to be called on discard.
    ///
    /// Multiple callbacks can be registered and will be executed sequentially in registration order.
    fn on_discard_async(&mut self, callback: AsyncTxnCallback);

    /// Downcast to concrete type (for internal use in tests)
    fn as_any(&self) -> &dyn std::any::Any;

    /// Downcast to concrete type (for internal use in tests)
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;

    /// Check if this is a read-only transaction.
    fn is_readonly(&self) -> bool;
}

/// TxnStore trait for stores that support transactions.
///
/// This is a marker trait combining Store with the ability to create transactions.
/// Most production stores implement this trait.
pub trait TxnStore: Store {}

/// Blanket implementation: any Store automatically implements TxnStore.
impl<T> TxnStore for T where T: Store {}

/// Blanket implementation of Reader for Box<dyn Txn>.
///
/// This allows boxed transactions to be used where Reader is required.
#[async_trait]
impl Reader for Box<dyn Txn> {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        (**self).get(key).await
    }

    async fn has(&self, key: &[u8]) -> Result<bool> {
        (**self).has(key).await
    }

    async fn get_size(&self, key: &[u8]) -> Result<Option<usize>> {
        (**self).get_size(key).await
    }

    async fn iterator(&self, opts: IterOptions) -> Result<Box<dyn Iterator>> {
        (**self).iterator(opts).await
    }
}

/// Blanket implementation of Writer for Box<dyn Txn>.
///
/// This allows boxed transactions to be used where Writer is required.
#[async_trait]
impl Writer for Box<dyn Txn> {
    async fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        (**self).set(key, value).await
    }

    async fn delete(&mut self, key: &[u8]) -> Result<()> {
        (**self).delete(key).await
    }
}

/// Helper function to create a simple sync callback from a closure.
///
/// # Example
///
/// ```ignore
/// txn.on_success(make_callback(|| println!("Success!")));
/// ```
pub fn make_callback<F>(f: F) -> TxnCallback
where
    F: FnOnce() + Send + 'static,
{
    Box::new(f)
}

/// Helper function to create an async callback from a closure.
///
/// # Example
///
/// ```ignore
/// txn.on_success_async(make_async_callback(|| async {
///     println!("Async success!");
/// }));
/// ```
pub fn make_async_callback<F, Fut>(f: F) -> AsyncTxnCallback
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    Box::new(move || Box::pin(f()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_make_callback() {
        let _callback = make_callback(|| {
            println!("Test callback");
        });
        // Just ensure it compiles and has the right type
    }

    #[test]
    fn test_make_async_callback() {
        let _callback = make_async_callback(|| async {
            println!("Test async callback");
        });
        // Just ensure it compiles and has the right type
    }
}
