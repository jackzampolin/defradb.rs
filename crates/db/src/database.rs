/// Database struct for DefraDB matching Go's internal/db/db.go.
///
/// The DB struct is the main entry point for DefraDB operations.
/// It manages the root store, creates transactions, and provides
/// access to collections.
use crate::error::{Error, Result};
use crate::txn::DbTxn;
use datastore::BasicTxn;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use storage::corekv::Store;

/// Database options.
#[derive(Debug, Clone, Default)]
pub struct DbOptions {
    /// Maximum number of transaction retries.
    pub max_txn_retries: Option<u32>,
    /// Chunk size for large values in the blockstore.
    pub chunk_size: Option<usize>,
}

/// The main DefraDB database struct.
///
/// This matches Go's DB struct in internal/db/db.go.
pub struct DB<S: Store> {
    /// The underlying store.
    store: Arc<S>,
    /// Options for this database instance.
    options: DbOptions,
    /// Counter for generating unique transaction IDs.
    txn_id_counter: AtomicU64,
}

impl<S: Store> DB<S> {
    /// Create a new database with the given store.
    pub fn new(store: S) -> Self {
        Self::with_options(store, DbOptions::default())
    }

    /// Create a new database with the given store and options.
    pub fn with_options(store: S, options: DbOptions) -> Self {
        Self {
            store: Arc::new(store),
            options,
            txn_id_counter: AtomicU64::new(0),
        }
    }

    /// Get the next transaction ID.
    fn next_txn_id(&self) -> u64 {
        self.txn_id_counter.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Create a new transaction.
    ///
    /// If `readonly` is true, the transaction cannot perform writes.
    pub async fn new_txn(&self, readonly: bool) -> Result<DbTxn<S>> {
        let id = self.next_txn_id();
        let basic_txn = BasicTxn::new(&*self.store, id, readonly)
            .await
            .map_err(Error::Storage)?;
        Ok(DbTxn::new(basic_txn, self.store.clone()))
    }

    /// Execute a function within a transaction.
    ///
    /// If the function returns Ok, the transaction is committed.
    /// If the function returns Err, the transaction is discarded.
    pub async fn with_txn<F, T>(&self, readonly: bool, f: F) -> Result<T>
    where
        F: FnOnce(&DbTxn<S>) -> Result<T>,
    {
        let txn = self.new_txn(readonly).await?;
        let result = f(&txn);
        match result {
            Ok(value) => {
                txn.commit().await?;
                Ok(value)
            }
            Err(e) => {
                txn.discard();
                Err(e)
            }
        }
    }

    /// Execute an async function within a transaction.
    ///
    /// If the function returns Ok, the transaction is committed.
    /// If the function returns Err, the transaction is discarded.
    pub async fn with_txn_async<F, Fut, T>(&self, readonly: bool, f: F) -> Result<T>
    where
        F: FnOnce(DbTxn<S>) -> Fut,
        Fut: std::future::Future<Output = (DbTxn<S>, Result<T>)>,
    {
        let txn = self.new_txn(readonly).await?;
        let (txn, result) = f(txn).await;
        match result {
            Ok(value) => {
                txn.commit().await?;
                Ok(value)
            }
            Err(e) => {
                txn.discard();
                Err(e)
            }
        }
    }

    /// Close the database.
    pub async fn close(&self) -> Result<()> {
        self.store.close().await.map_err(Error::Storage)
    }

    /// Get the database options.
    pub fn options(&self) -> &DbOptions {
        &self.options
    }

    /// Get the current transaction ID counter value.
    pub fn current_txn_id(&self) -> u64 {
        self.txn_id_counter.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use storage::backends::MemoryStore;

    #[tokio::test]
    async fn test_db_new_txn() {
        let store = MemoryStore::new();
        let db = DB::new(store);

        let txn = db.new_txn(false).await.unwrap();
        assert_eq!(txn.id(), 1);

        let txn2 = db.new_txn(false).await.unwrap();
        assert_eq!(txn2.id(), 2);
    }

    #[tokio::test]
    async fn test_db_txn_isolation() {
        let store = MemoryStore::new();
        let db = DB::new(store);

        // Write in first transaction
        let txn1 = db.new_txn(false).await.unwrap();
        txn1.datastore().set(b"key", b"value1").await.unwrap();
        txn1.commit().await.unwrap();

        // Read in second transaction
        let txn2 = db.new_txn(true).await.unwrap();
        let value = txn2.datastore().get(b"key").await.unwrap();
        assert_eq!(value, Some(b"value1".to_vec()));
    }

    #[tokio::test]
    async fn test_db_with_txn() {
        let store = MemoryStore::new();
        let db = DB::new(store);

        // Execute with_txn that commits
        db.with_txn(false, |_txn| {
            // We need async operations inside, but this closure is sync
            // This is a limitation - we'll address in with_txn_async
            Ok(())
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_db_options() {
        let store = MemoryStore::new();
        let options = DbOptions {
            max_txn_retries: Some(5),
            chunk_size: Some(1024 * 1024),
        };
        let db = DB::with_options(store, options.clone());

        assert_eq!(db.options().max_txn_retries, Some(5));
        assert_eq!(db.options().chunk_size, Some(1024 * 1024));
    }
}
