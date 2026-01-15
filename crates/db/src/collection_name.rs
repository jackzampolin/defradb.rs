//! Validated collection name type.

use crate::error::{Error, Result};
use std::fmt;

/// A validated collection name.
///
/// Collection names must be non-empty and cannot contain forward slashes
/// (which are used as key separators in the storage layer).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CollectionName(String);

impl CollectionName {
    /// Create a new validated collection name.
    ///
    /// # Errors
    ///
    /// Returns `Error::InvalidCollectionName` if:
    /// - The name is empty
    /// - The name contains a forward slash (`/`)
    /// - The name contains a null byte
    pub fn new(name: &str) -> Result<Self> {
        if name.is_empty() {
            return Err(Error::InvalidCollectionName(
                "collection name cannot be empty".to_string(),
            ));
        }
        if name.contains('/') {
            return Err(Error::InvalidCollectionName(format!(
                "collection name '{}' cannot contain '/'",
                name
            )));
        }
        if name.contains('\0') {
            return Err(Error::InvalidCollectionName(format!(
                "collection name '{}' cannot contain null bytes",
                name
            )));
        }
        Ok(Self(name.to_string()))
    }

    /// Get the collection name as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consume the CollectionName and return the inner String.
    pub fn into_string(self) -> String {
        self.0
    }
}

impl fmt::Display for CollectionName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for CollectionName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_collection_name() {
        let name = CollectionName::new("Users").unwrap();
        assert_eq!(name.as_str(), "Users");
    }

    #[test]
    fn test_valid_collection_name_with_special_chars() {
        let name = CollectionName::new("User_Posts-2024").unwrap();
        assert_eq!(name.as_str(), "User_Posts-2024");
    }

    #[test]
    fn test_empty_name_fails() {
        let result = CollectionName::new("");
        assert!(matches!(result, Err(Error::InvalidCollectionName(_))));
    }

    #[test]
    fn test_name_with_slash_fails() {
        let result = CollectionName::new("Users/Posts");
        assert!(matches!(result, Err(Error::InvalidCollectionName(_))));
    }

    #[test]
    fn test_name_with_null_byte_fails() {
        let result = CollectionName::new("Users\0");
        assert!(matches!(result, Err(Error::InvalidCollectionName(_))));
    }

    #[test]
    fn test_display() {
        let name = CollectionName::new("Users").unwrap();
        assert_eq!(format!("{}", name), "Users");
    }

    #[test]
    fn test_as_ref() {
        let name = CollectionName::new("Users").unwrap();
        let s: &str = name.as_ref();
        assert_eq!(s, "Users");
    }
}
