
//! Validated key name type for keyring operations

use std::fmt;

use crate::error::{Error, Result};

/// A validated key name that is safe to use with keyring backends.
///
/// `KeyName` ensures that the name:
/// - Is not empty
/// - Does not contain path separators (`/`, `\`, or null bytes)
/// - Is not `.` or `..`
///
/// This provides compile-time safety by ensuring that any `KeyName` instance
/// has already been validated, preventing path traversal attacks.
///
/// # Example
///
/// ```
/// use keyring::KeyName;
///
/// // Valid key names
/// let name = KeyName::new("my-key").unwrap();
/// let name = KeyName::new("peer-key").unwrap();
///
/// // Invalid key names return errors
/// assert!(KeyName::new("../escape").is_err());
/// assert!(KeyName::new("").is_err());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct KeyName(String);

impl KeyName {
    /// Create a new validated key name.
    ///
    /// Returns `Error::InvalidKeyName` if the name is invalid.
    pub fn new(name: impl Into<String>) -> Result<Self> {
        let name = name.into();
        Self::validate(&name)?;
        Ok(Self(name))
    }

    /// Validate a key name without creating a `KeyName` instance.
    ///
    /// This is useful when you need to validate a name but don't need
    /// the type safety of `KeyName`.
    pub fn validate(name: &str) -> Result<()> {
        if name.is_empty() {
            return Err(Error::InvalidKeyName(
                "key name cannot be empty".to_string(),
            ));
        }
        if name.contains('/') || name.contains('\\') || name.contains('\0') {
            return Err(Error::InvalidKeyName(format!(
                "key name cannot contain path separators: {}",
                name
            )));
        }
        if name == "." || name == ".." {
            return Err(Error::InvalidKeyName(format!(
                "key name cannot be '.' or '..': {}",
                name
            )));
        }
        Ok(())
    }

    /// Get the key name as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consume the `KeyName` and return the inner `String`.
    pub fn into_string(self) -> String {
        self.0
    }
}

impl fmt::Display for KeyName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for KeyName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_name_valid() {
        assert!(KeyName::new("simple").is_ok());
        assert!(KeyName::new("with-dashes").is_ok());
        assert!(KeyName::new("with_underscores").is_ok());
        assert!(KeyName::new("with.dots").is_ok());
        assert!(KeyName::new("MixedCase").is_ok());
        assert!(KeyName::new("123numeric").is_ok());
    }

    #[test]
    fn test_key_name_empty() {
        assert!(matches!(KeyName::new(""), Err(Error::InvalidKeyName(_))));
    }

    #[test]
    fn test_key_name_path_separators() {
        assert!(matches!(
            KeyName::new("../escape"),
            Err(Error::InvalidKeyName(_))
        ));
        assert!(matches!(
            KeyName::new("sub/dir"),
            Err(Error::InvalidKeyName(_))
        ));
        assert!(matches!(
            KeyName::new("back\\slash"),
            Err(Error::InvalidKeyName(_))
        ));
        assert!(matches!(
            KeyName::new("null\0byte"),
            Err(Error::InvalidKeyName(_))
        ));
    }

    #[test]
    fn test_key_name_dot_names() {
        assert!(matches!(KeyName::new("."), Err(Error::InvalidKeyName(_))));
        assert!(matches!(KeyName::new(".."), Err(Error::InvalidKeyName(_))));
    }

    #[test]
    fn test_key_name_as_str() {
        let name = KeyName::new("test-key").unwrap();
        assert_eq!(name.as_str(), "test-key");
    }

    #[test]
    fn test_key_name_display() {
        let name = KeyName::new("my-key").unwrap();
        assert_eq!(format!("{}", name), "my-key");
    }
}
