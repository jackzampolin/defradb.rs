/// Datastore - Document and collection data storage
///
/// The Datastore handles storage of document field values, primary keys,
/// secondary indexes, search engine artifacts, and view caching. It includes
/// automatic chunking for values larger than 1MB.

use crate::corekv::{Error, IterOptions, Iterator, Key, Reader, Result, Store, Txn, Writer};
use crate::namespace::{Namespace, NamespacedStore};
use async_trait::async_trait;
use std::sync::Arc;

/// Chunk size for large values (1MB)
pub const CHUNK_SIZE: usize = 1_048_576;

/// Chunk suffix marker - single byte appended to key for each chunk
const CHUNK_SUFFIX_START: u8 = 0x00;

/// Datastore provides storage for documents and collection data
pub struct Datastore<S: Store> {
    store: NamespacedStore<S>,
}

impl<S: Store> Datastore<S> {
    /// Create a new Datastore
    pub fn new(store: Arc<S>) -> Self {
        Self {
            store: NamespacedStore::new(store, Namespace::Datastore),
        }
    }

    /// Check if a value needs chunking
    fn needs_chunking(value: &[u8]) -> bool {
        value.len() > CHUNK_SIZE
    }

    /// Generate chunk key by appending a single-byte suffix
    fn chunk_key(base_key: &[u8], chunk_index: u8) -> Vec<u8> {
        let mut key = Vec::with_capacity(base_key.len() + 1);
        key.extend_from_slice(base_key);
        key.push(chunk_index);
        key
    }
}

#[async_trait]
impl<S: Store> Store for Datastore<S> {
    async fn new_txn(&self, readonly: bool) -> Result<Box<dyn Txn>> {
        let txn = self.store.new_txn(readonly).await?;
        Ok(Box::new(DatastoreTxn { txn }))
    }

    async fn close(&self) -> Result<()> {
        self.store.close().await
    }
}

/// Datastore transaction with chunking support
pub struct DatastoreTxn {
    txn: Box<dyn Txn>,
}

impl DatastoreTxn {
    /// Put a value with automatic chunking if needed
    pub async fn put<K: Key>(&mut self, key: &K, value: &[u8]) -> Result<()> {
        let key_bytes = key.bytes();

        if Datastore::<crate::backends::MemoryStore>::needs_chunking(value) {
            // Split into chunks
            self.put_chunked(&key_bytes, value).await
        } else {
            // Single value
            self.set(&key_bytes, value).await
        }
    }

    /// Get a value, reassembling chunks if needed
    pub async fn get_value<K: Key>(&self, key: &K) -> Result<Option<Vec<u8>>> {
        let key_bytes = key.bytes();

        // Try to get the base value first
        match self.get(&key_bytes).await? {
            Some(value) => {
                // Check if this might be chunked by looking for chunk 1
                let chunk1_key = Datastore::<crate::backends::MemoryStore>::chunk_key(&key_bytes, 1);
                if self.has(&chunk1_key).await? {
                    // This is chunked, reassemble
                    self.get_chunked(&key_bytes).await
                } else {
                    // Single value
                    Ok(Some(value))
                }
            }
            None => Ok(None),
        }
    }

    /// Delete a value, including all chunks if present
    pub async fn delete_value<K: Key>(&mut self, key: &K) -> Result<()> {
        let key_bytes = key.bytes();

        // Delete base key
        self.delete(&key_bytes).await?;

        // Try to delete chunks (if they exist)
        let mut deleted_chunks = 0;
        let mut last_error: Option<Error> = None;

        for i in 0..=255 {
            let chunk_key = Datastore::<crate::backends::MemoryStore>::chunk_key(&key_bytes, i);
            match self.has(&chunk_key).await {
                Ok(true) => {
                    match self.delete(&chunk_key).await {
                        Ok(_) => deleted_chunks += 1,
                        Err(e) => {
                            tracing::error!(
                                chunk_index = i,
                                deleted_so_far = deleted_chunks,
                                error = %e,
                                "Failed to delete chunk during delete_value"
                            );
                            last_error = Some(e);
                            // Continue trying to delete remaining chunks
                        }
                    }
                }
                Ok(false) => break, // No more chunks
                Err(e) => {
                    tracing::error!(
                        chunk_index = i,
                        error = %e,
                        "Failed to check chunk existence during delete_value"
                    );
                    last_error = Some(e);
                    break;
                }
            }
        }

        if deleted_chunks > 0 {
            tracing::debug!(
                deleted_chunks = deleted_chunks,
                "Deleted chunked value"
            );
        }

        // Return error if any chunk deletion failed
        if let Some(err) = last_error {
            return Err(Error::Other(format!(
                "Partial chunk deletion: deleted {} chunks before error: {}",
                deleted_chunks, err
            )));
        }

        Ok(())
    }

    /// Put a chunked value (internal)
    async fn put_chunked(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let num_chunks = (value.len() + CHUNK_SIZE - 1) / CHUNK_SIZE;

        // Validate size before processing
        if num_chunks > 256 {
            return Err(Error::Other(format!(
                "Value too large: {} bytes requires {} chunks (max 256 chunks = 256MB). Size: {:.2} MB",
                value.len(),
                num_chunks,
                value.len() as f64 / 1_048_576.0
            )));
        }

        // Delete any existing chunks first
        let mut deleted_count = 0;
        for i in 0..=255 {
            let chunk_key = Datastore::<crate::backends::MemoryStore>::chunk_key(key, i);
            if self.has(&chunk_key).await? {
                self.delete(&chunk_key).await?;
                deleted_count += 1;
            } else {
                break;
            }
        }

        if deleted_count > 0 {
            tracing::debug!(
                deleted_chunks = deleted_count,
                "Deleted existing chunks before writing new chunked value"
            );
        }

        // Write new chunks
        for (i, chunk) in value.chunks(CHUNK_SIZE).enumerate() {
            let chunk_key = Datastore::<crate::backends::MemoryStore>::chunk_key(key, i as u8);
            self.set(&chunk_key, chunk).await?;
        }

        tracing::debug!(
            chunks_written = num_chunks,
            value_size = value.len(),
            "Wrote chunked value"
        );

        Ok(())
    }

    /// Get a chunked value (internal)
    async fn get_chunked(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut result = Vec::new();
        let mut chunks_found = 0;

        for i in 0..=255 {
            let chunk_key = Datastore::<crate::backends::MemoryStore>::chunk_key(key, i);
            match self.get(&chunk_key).await? {
                Some(chunk) => {
                    result.extend_from_slice(&chunk);
                    chunks_found += 1;
                }
                None => break,
            }
        }

        if chunks_found > 0 {
            tracing::debug!(
                chunks_found = chunks_found,
                total_size = result.len(),
                "Reconstructed chunked value"
            );
            Ok(Some(result))
        } else {
            Ok(None)
        }
    }
}

#[async_trait]
impl Reader for DatastoreTxn {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.txn.get(key).await
    }

    async fn has(&self, key: &[u8]) -> Result<bool> {
        self.txn.has(key).await
    }

    async fn iterator(&self, opts: IterOptions) -> Result<Box<dyn Iterator>> {
        self.txn.iterator(opts).await
    }
}

#[async_trait]
impl Writer for DatastoreTxn {
    async fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.txn.set(key, value).await
    }

    async fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.txn.delete(key).await
    }
}

#[async_trait]
impl Txn for DatastoreTxn {
    async fn commit(self: Box<Self>) -> Result<()> {
        self.txn.commit().await
    }

    fn discard(self: Box<Self>) {
        self.txn.discard()
    }

    fn on_success(&mut self, callback: crate::corekv::TxnCallback) {
        self.txn.on_success(callback)
    }

    fn on_success_async(&mut self, callback: crate::corekv::AsyncTxnCallback) {
        self.txn.on_success_async(callback)
    }

    fn on_error(&mut self, callback: crate::corekv::TxnCallback) {
        self.txn.on_error(callback)
    }

    fn on_error_async(&mut self, callback: crate::corekv::AsyncTxnCallback) {
        self.txn.on_error_async(callback)
    }

    fn on_discard(&mut self, callback: crate::corekv::TxnCallback) {
        self.txn.on_discard(callback)
    }

    fn on_discard_async(&mut self, callback: crate::corekv::AsyncTxnCallback) {
        self.txn.on_discard_async(callback)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn is_readonly(&self) -> bool {
        self.txn.is_readonly()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::MemoryStore;
    use crate::keys::datastore::DataStoreKey;
    use crate::keys::utils::InstanceType;

    #[tokio::test]
    async fn test_datastore_basic() {
        let store = Arc::new(MemoryStore::new());
        let datastore = Datastore::new(store);

        let key = DataStoreKey::new(1, InstanceType::Value, "doc1", "field1");

        // Write
        let mut txn = datastore.new_txn(false).await.unwrap();
        {
            let txn_ds = txn.as_any_mut().downcast_mut::<DatastoreTxn>().unwrap();
            txn_ds.put(&key, b"value1").await.unwrap();
        }
        txn.commit().await.unwrap();

        // Read
        let txn = datastore.new_txn(true).await.unwrap();
        let txn = txn.as_any().downcast_ref::<DatastoreTxn>().unwrap();
        let value = txn.get_value(&key).await.unwrap();
        assert_eq!(value, Some(b"value1".to_vec()));
    }

    #[tokio::test]
    async fn test_datastore_chunking() {
        let store = Arc::new(MemoryStore::new());
        let datastore = Datastore::new(store);

        let key = DataStoreKey::new(1, InstanceType::Value, "doc1", "large_field");

        // Create a 2.5MB value
        let large_value = vec![0xAB; CHUNK_SIZE * 2 + CHUNK_SIZE / 2];

        // Write
        let mut txn = datastore.new_txn(false).await.unwrap();
        {
            let txn_ds = txn.as_any_mut().downcast_mut::<DatastoreTxn>().unwrap();
            txn_ds.put(&key, &large_value).await.unwrap();
        }
        txn.commit().await.unwrap();

        // Read back
        let txn = datastore.new_txn(true).await.unwrap();
        let txn = txn.as_any().downcast_ref::<DatastoreTxn>().unwrap();
        let value = txn.get_value(&key).await.unwrap();
        assert_eq!(value, Some(large_value));
    }

    #[tokio::test]
    async fn test_datastore_delete_chunked() {
        let store = Arc::new(MemoryStore::new());
        let datastore = Datastore::new(store);

        let key = DataStoreKey::new(1, InstanceType::Value, "doc1", "large_field");

        // Create a 2MB value
        let large_value = vec![0xCD; CHUNK_SIZE * 2];

        // Write
        let mut txn = datastore.new_txn(false).await.unwrap();
        {
            let txn_ds = txn.as_any_mut().downcast_mut::<DatastoreTxn>().unwrap();
            txn_ds.put(&key, &large_value).await.unwrap();
        }
        txn.commit().await.unwrap();

        // Verify it exists
        let txn = datastore.new_txn(true).await.unwrap();
        let txn = txn.as_any().downcast_ref::<DatastoreTxn>().unwrap();
        let value = txn.get_value(&key).await.unwrap();
        assert!(value.is_some());
        drop(txn);

        // Delete
        let mut txn = datastore.new_txn(false).await.unwrap();
        {
            let txn_ds = txn.as_any_mut().downcast_mut::<DatastoreTxn>().unwrap();
            txn_ds.delete_value(&key).await.unwrap();
        }
        txn.commit().await.unwrap();

        // Verify it's gone
        let txn = datastore.new_txn(true).await.unwrap();
        let txn = txn.as_any().downcast_ref::<DatastoreTxn>().unwrap();
        let value = txn.get_value(&key).await.unwrap();
        assert_eq!(value, None);
    }

    #[tokio::test]
    async fn test_datastore_isolation() {
        let store = Arc::new(MemoryStore::new());
        let datastore = Datastore::new(store);

        // Write to datastore
        let mut txn = datastore.new_txn(false).await.unwrap();
        txn.set(b"test_key", b"datastore_value").await.unwrap();
        txn.commit().await.unwrap();

        // Read back
        let txn = datastore.new_txn(true).await.unwrap();
        let value = txn.get(b"test_key").await.unwrap();
        assert_eq!(value, Some(b"datastore_value".to_vec()));
    }
}
