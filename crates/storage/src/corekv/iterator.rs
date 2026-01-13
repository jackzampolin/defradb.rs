/// Iterator trait and types for traversing key-value pairs.
///
/// This module provides the iterator abstraction for scanning through
/// key-value pairs in storage, with support for various filtering and
/// ordering options.
use async_trait::async_trait;

use super::errors::Result;

/// A key-value pair returned by an iterator.
///
/// This struct holds both the key and value as byte vectors.
/// For keys-only iteration, the value may be empty.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvPair {
    /// The key as a byte vector.
    pub key: Vec<u8>,

    /// The value as a byte vector.
    ///
    /// This may be empty if the iterator was created with `keys_only` option.
    pub value: Vec<u8>,
}

impl KvPair {
    /// Create a new key-value pair.
    pub fn new(key: Vec<u8>, value: Vec<u8>) -> Self {
        Self { key, value }
    }

    /// Create a key-only pair (empty value).
    ///
    /// This is used for keys-only iteration.
    pub fn key_only(key: Vec<u8>) -> Self {
        Self {
            key,
            value: Vec::new(),
        }
    }

    /// Get the key as a byte slice.
    pub fn key_bytes(&self) -> &[u8] {
        &self.key
    }

    /// Get the value as a byte slice.
    pub fn value_bytes(&self) -> &[u8] {
        &self.value
    }

    /// Convert the key to a string (lossy UTF-8 conversion).
    pub fn key_str(&self) -> String {
        String::from_utf8_lossy(&self.key).to_string()
    }

    /// Convert the value to a string (lossy UTF-8 conversion).
    pub fn value_str(&self) -> String {
        String::from_utf8_lossy(&self.value).to_string()
    }

    /// Check if this is a keys-only pair (empty value).
    pub fn is_key_only(&self) -> bool {
        self.value.is_empty()
    }
}

/// Iterator trait for traversing key-value pairs.
///
/// Iterators are created from stores using the `iterator()` method with
/// `IterOptions` to configure filtering and ordering. Iterators must be
/// explicitly closed to release resources.
///
/// # Example
///
/// ```ignore
/// use storage::corekv::{Iterator, IterOptions};
///
/// let opts = IterOptions::new().with_prefix(b"user_".to_vec());
/// let mut iter = store.iterator(opts).await?;
///
/// while let Some(kv) = iter.next().await? {
///     println!("Key: {}, Value: {}", kv.key_str(), kv.value_str());
/// }
///
/// iter.close().await?;
/// ```
#[async_trait]
pub trait Iterator: Send {
    /// Advance the iterator and return the next key-value pair.
    ///
    /// Returns:
    /// - `Ok(Some(KvPair))` if there is a next item
    /// - `Ok(None)` if the iterator is exhausted
    /// - `Err(Error)` if an error occurred
    ///
    /// The iterator should be consumed until it returns None or an error.
    async fn next(&mut self) -> Result<Option<KvPair>>;

    /// Close the iterator and release resources.
    ///
    /// This should be called when iteration is complete. Failing to close
    /// an iterator may leak resources depending on the backend implementation.
    ///
    /// After calling close(), subsequent calls to next() should return None
    /// or an appropriate error.
    async fn close(&mut self) -> Result<()>;

    /// Seek moves the iterator to the given key.
    ///
    /// If an exact match is not found, the iterator will progress to the next
    /// valid value (depending on the `reverse` option - next greater for forward,
    /// next lesser for reverse).
    ///
    /// Returns `true` if a valid item was found, `false` otherwise.
    ///
    /// Seek will not seek to values outside of the constraints provided in
    /// IterOptions (prefix, start, end bounds).
    ///
    /// # Arguments
    ///
    /// * `key` - The key to seek to
    ///
    /// # Returns
    ///
    /// * `Ok(true)` if a valid item was found at or after the key
    /// * `Ok(false)` if no valid item exists at or after the key
    /// * `Err(Error)` if the iterator is closed or an error occurred
    async fn seek(&mut self, key: &[u8]) -> Result<bool>;

    /// Reset the iterator to the beginning, allowing re-iteration.
    ///
    /// After reset, the iterator behaves as if it was just created.
    /// The next call to `next()` will return the first item.
    ///
    /// # Returns
    ///
    /// * `Ok(())` on success
    /// * `Err(Error)` if the iterator is closed or an error occurred
    async fn reset(&mut self) -> Result<()>;

    /// Check if the iterator is still valid (not closed).
    ///
    /// Returns false if close() has been called.
    fn is_valid(&self) -> bool {
        true // Default implementation, backends should override if needed
    }

    /// Collect all remaining items into a Vec.
    ///
    /// This is a convenience method that consumes the iterator and collects
    /// all remaining key-value pairs. Be careful with large result sets as
    /// this loads everything into memory.
    async fn collect_all(&mut self) -> Result<Vec<KvPair>> {
        let mut results = Vec::new();
        while let Some(kv) = self.next().await? {
            results.push(kv);
        }
        Ok(results)
    }

    /// Count the remaining items without collecting them.
    ///
    /// This consumes the iterator but doesn't allocate memory for values.
    async fn count(&mut self) -> Result<usize> {
        let mut count = 0;
        while (self.next().await?).is_some() {
            count += 1;
        }
        Ok(count)
    }
}

/// Implement Iterator for Box<dyn Iterator> to allow boxing.
#[async_trait]
impl Iterator for Box<dyn Iterator> {
    async fn next(&mut self) -> Result<Option<KvPair>> {
        (**self).next().await
    }

    async fn close(&mut self) -> Result<()> {
        (**self).close().await
    }

    async fn seek(&mut self, key: &[u8]) -> Result<bool> {
        (**self).seek(key).await
    }

    async fn reset(&mut self) -> Result<()> {
        (**self).reset().await
    }

    fn is_valid(&self) -> bool {
        (**self).is_valid()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kv_pair_new() {
        let kv = KvPair::new(b"key".to_vec(), b"value".to_vec());
        assert_eq!(kv.key_bytes(), b"key");
        assert_eq!(kv.value_bytes(), b"value");
        assert!(!kv.is_key_only());
    }

    #[test]
    fn test_kv_pair_key_only() {
        let kv = KvPair::key_only(b"key".to_vec());
        assert_eq!(kv.key_bytes(), b"key");
        assert_eq!(kv.value_bytes(), b"");
        assert!(kv.is_key_only());
    }

    #[test]
    fn test_kv_pair_str() {
        let kv = KvPair::new(b"hello".to_vec(), b"world".to_vec());
        assert_eq!(kv.key_str(), "hello");
        assert_eq!(kv.value_str(), "world");
    }

    #[test]
    fn test_kv_pair_clone() {
        let kv1 = KvPair::new(b"key".to_vec(), b"value".to_vec());
        let kv2 = kv1.clone();
        assert_eq!(kv1, kv2);
    }

    #[test]
    fn test_kv_pair_equality() {
        let kv1 = KvPair::new(b"key".to_vec(), b"value".to_vec());
        let kv2 = KvPair::new(b"key".to_vec(), b"value".to_vec());
        let kv3 = KvPair::new(b"key".to_vec(), b"different".to_vec());

        assert_eq!(kv1, kv2);
        assert_ne!(kv1, kv3);
    }
}
