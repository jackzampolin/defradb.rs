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
    /// Check if a value needs chunking (> 1MB)
    fn needs_chunking(value: &[u8]) -> bool {
        value.len() > CHUNK_SIZE
    }

    /// Generate chunk key by appending a single-byte suffix
    #[doc(hidden)]
    pub fn chunk_key(base_key: &[u8], chunk_index: u8) -> Vec<u8> {
        let mut key = Vec::with_capacity(base_key.len() + 1);
        key.extend_from_slice(base_key);
        key.push(chunk_index);
        key
    }

    /// Put a value with automatic chunking if needed
    ///
    /// Values larger than CHUNK_SIZE (1MB) are automatically split into chunks.
    /// The maximum supported value size is 256MB (256 chunks).
    pub async fn put<K: Key>(&mut self, key: &K, value: &[u8]) -> Result<()> {
        let key_bytes = key.bytes();

        if Self::needs_chunking(value) {
            // Split into chunks
            self.put_chunked(&key_bytes, value).await
        } else {
            // Single value - store directly at base key
            self.set(&key_bytes, value).await
        }
    }

    /// Get a value, reassembling chunks if needed
    ///
    /// This method automatically handles both chunked and non-chunked values:
    /// - Chunked values are stored at chunk_key(base, 0), chunk_key(base, 1), etc.
    /// - Non-chunked values are stored directly at the base key
    pub async fn get_value<K: Key>(&self, key: &K) -> Result<Option<Vec<u8>>> {
        let key_bytes = key.bytes();

        // Check if this is a chunked value by looking for chunk 0
        let chunk0_key = Self::chunk_key(&key_bytes, 0);
        if self.has(&chunk0_key).await? {
            // This is chunked, reassemble from chunks
            self.get_chunked(&key_bytes).await
        } else {
            // Not chunked, try to get the base key directly
            self.get(&key_bytes).await
        }
    }

    /// Delete a value, including all chunks if present
    ///
    /// This method handles both chunked and non-chunked values:
    /// - Deletes the base key (for non-chunked values)
    /// - Deletes all chunk keys (for chunked values)
    pub async fn delete_value<K: Key>(&mut self, key: &K) -> Result<()> {
        let key_bytes = key.bytes();

        // Delete base key (for non-chunked values)
        // Note: delete() is a no-op for non-existent keys and returns Ok(()),
        // so we propagate any errors immediately as they indicate real problems
        self.delete(&key_bytes).await?;

        // Try to delete chunks (if they exist)
        let mut deleted_chunks = 0;
        let mut failed_chunks: Vec<(u8, Error)> = Vec::new();

        for i in 0..=255u8 {
            let chunk_key = Self::chunk_key(&key_bytes, i);
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
                            failed_chunks.push((i, e));
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
                    failed_chunks.push((i, e));
                    break;
                }
            }
        }

        if deleted_chunks > 0 {
            tracing::debug!(deleted_chunks = deleted_chunks, "Deleted chunked value");
        }

        // Return error if any chunk deletion failed, including all failures
        if !failed_chunks.is_empty() {
            let error_details: Vec<String> = failed_chunks
                .iter()
                .map(|(idx, err)| format!("chunk {}: {}", idx, err))
                .collect();
            return Err(Error::Other(format!(
                "Partial chunk deletion: deleted {} chunks, {} chunks failed: [{}]",
                deleted_chunks,
                failed_chunks.len(),
                error_details.join(", ")
            )));
        }

        Ok(())
    }

    /// Put a chunked value (internal)
    ///
    /// This method writes new chunks first (overwriting existing ones at the same indices),
    /// then deletes any extra old chunks. This ordering ensures that if a write fails,
    /// the old data is still intact (no data loss).
    async fn put_chunked(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let num_chunks = value.len().div_ceil(CHUNK_SIZE);

        // Validate size before processing
        if num_chunks > 256 {
            return Err(Error::Other(format!(
                "Value too large: {} bytes requires {} chunks (max 256 chunks = 256MB). Size: {:.2} MB",
                value.len(),
                num_chunks,
                value.len() as f64 / 1_048_576.0
            )));
        }

        // Write new chunks first (overwrites existing chunks at same indices)
        // This ensures that if write fails, old data is still intact
        for (i, chunk) in value.chunks(CHUNK_SIZE).enumerate() {
            let chunk_key = Self::chunk_key(key, i as u8);
            self.set(&chunk_key, chunk).await?;
        }

        // Delete any extra old chunks (indices beyond what new value needs)
        // Only delete chunks that exist beyond the new chunk count
        let mut deleted_count = 0;
        for i in (num_chunks as u8)..=255u8 {
            let chunk_key = Self::chunk_key(key, i);
            if self.has(&chunk_key).await? {
                self.delete(&chunk_key).await?;
                deleted_count += 1;
            } else {
                // No more old chunks exist
                break;
            }
        }

        if deleted_count > 0 {
            tracing::debug!(
                deleted_extra_chunks = deleted_count,
                "Deleted extra old chunks after writing new chunked value"
            );
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

        for i in 0..=255u8 {
            let chunk_key = Self::chunk_key(key, i);
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

    async fn get_size(&self, key: &[u8]) -> Result<Option<usize>> {
        self.txn.get_size(key).await
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

// Tests extracted to crates/storage/tests/datastore_tests.rs
