/// Core KV types and traits for the storage layer.
///
/// This module provides the foundational abstractions for key-value storage,
/// including the Key trait for typed keys and iteration options.
use std::fmt;

/// Trait for keys that can be serialized to bytes.
///
/// All key types in the storage layer must implement this trait to convert
/// their logical representation into a byte sequence for storage.
pub trait Key: Send + Sync {
    /// Convert the key to its byte representation.
    ///
    /// This method should produce a deterministic byte sequence that:
    /// - Uniquely identifies the key
    /// - Maintains lexicographic ordering where appropriate
    /// - Can be used directly as a storage key
    fn bytes(&self) -> Vec<u8>;

    /// Convert the key to a human-readable string representation.
    ///
    /// This is primarily used for debugging and logging purposes.
    fn to_string(&self) -> String {
        format!("{:?}", self.bytes())
    }
}

/// Marker trait for implementing Key on basic types.
impl Key for Vec<u8> {
    fn bytes(&self) -> Vec<u8> {
        self.clone()
    }

    fn to_string(&self) -> String {
        String::from_utf8_lossy(self).to_string()
    }
}

impl Key for &[u8] {
    fn bytes(&self) -> Vec<u8> {
        self.to_vec()
    }

    fn to_string(&self) -> String {
        String::from_utf8_lossy(self).to_string()
    }
}

/// Options for configuring iterators over key-value pairs.
///
/// Iterators can be configured with various filters and behaviors:
/// - Prefix scanning: Only iterate keys with a specific prefix
/// - Range queries: Iterate keys within a lexicographic range
/// - Reverse iteration: Iterate in descending order
/// - Keys-only mode: Skip fetching values for performance
///
/// Use the builder methods to construct IterOptions instead of accessing fields directly.
#[derive(Debug, Clone, Default)]
pub struct IterOptions {
    /// Only iterate keys with this prefix.
    ///
    /// When set, only keys starting with this byte sequence will be returned.
    /// This is more efficient than filtering after iteration.
    prefix: Option<Vec<u8>>,

    /// Start iteration at this key (inclusive).
    ///
    /// Keys lexicographically less than this value will be skipped.
    /// Can be combined with `end` for range queries.
    start: Option<Vec<u8>>,

    /// End iteration before this key (exclusive).
    ///
    /// Keys lexicographically greater than or equal to this value will not be returned.
    /// Can be combined with `start` for range queries.
    end: Option<Vec<u8>>,

    /// Iterate in reverse (descending) order.
    ///
    /// When true, keys are returned in descending lexicographic order.
    /// This affects how `start` and `end` are interpreted.
    reverse: bool,

    /// Only iterate keys without fetching values.
    ///
    /// When true, the iterator will not fetch values from storage,
    /// improving performance when only keys are needed.
    keys_only: bool,
}

impl IterOptions {
    /// Create a new IterOptions with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the prefix filter for this iterator.
    pub fn with_prefix(mut self, prefix: Vec<u8>) -> Self {
        self.prefix = Some(prefix);
        self
    }

    /// Set the start key (inclusive) for this iterator.
    pub fn with_start(mut self, start: Vec<u8>) -> Self {
        self.start = Some(start);
        self
    }

    /// Set the end key (exclusive) for this iterator.
    pub fn with_end(mut self, end: Vec<u8>) -> Self {
        self.end = Some(end);
        self
    }

    /// Enable reverse iteration.
    pub fn with_reverse(mut self, reverse: bool) -> Self {
        self.reverse = reverse;
        self
    }

    /// Enable keys-only mode (don't fetch values).
    pub fn with_keys_only(mut self, keys_only: bool) -> Self {
        self.keys_only = keys_only;
        self
    }

    /// Get the prefix filter, if set.
    pub fn prefix(&self) -> Option<&[u8]> {
        self.prefix.as_deref()
    }

    /// Get the start key, if set.
    pub fn start(&self) -> Option<&[u8]> {
        self.start.as_deref()
    }

    /// Get the end key, if set.
    pub fn end(&self) -> Option<&[u8]> {
        self.end.as_deref()
    }

    /// Check if reverse iteration is enabled.
    pub fn reverse(&self) -> bool {
        self.reverse
    }

    /// Check if keys-only mode is enabled.
    pub fn keys_only(&self) -> bool {
        self.keys_only
    }
}

impl fmt::Display for IterOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IterOptions {{ ")?;
        if let Some(ref prefix) = self.prefix {
            write!(f, "prefix: {:?}, ", String::from_utf8_lossy(prefix))?;
        }
        if let Some(ref start) = self.start {
            write!(f, "start: {:?}, ", String::from_utf8_lossy(start))?;
        }
        if let Some(ref end) = self.end {
            write!(f, "end: {:?}, ", String::from_utf8_lossy(end))?;
        }
        write!(
            f,
            "reverse: {}, keys_only: {} }}",
            self.reverse, self.keys_only
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_iter_options_builder() {
        let opts = IterOptions::new()
            .with_prefix(b"test".to_vec())
            .with_start(b"a".to_vec())
            .with_end(b"z".to_vec())
            .with_reverse(true)
            .with_keys_only(true);

        assert_eq!(opts.prefix(), Some(b"test".as_slice()));
        assert_eq!(opts.start(), Some(b"a".as_slice()));
        assert_eq!(opts.end(), Some(b"z".as_slice()));
        assert!(opts.reverse());
        assert!(opts.keys_only());
    }

    #[test]
    fn test_key_trait_vec() {
        let key = b"test_key".to_vec();
        assert_eq!(key.bytes(), b"test_key".to_vec());
    }

    #[test]
    fn test_key_trait_slice() {
        let key: &[u8] = b"test_key";
        assert_eq!(key.bytes(), b"test_key".to_vec());
    }

    #[test]
    fn test_iter_options_display() {
        let opts = IterOptions::new()
            .with_prefix(b"pre".to_vec())
            .with_reverse(true);

        let display = format!("{}", opts);
        assert!(display.contains("prefix"));
        assert!(display.contains("reverse: true"));
    }
}
