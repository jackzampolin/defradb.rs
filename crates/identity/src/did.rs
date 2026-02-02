//! DID (Decentralized Identifier) newtype.
//!
//! Provides a strongly-typed wrapper around DID strings, ensuring format
//! validity at construction time.

use serde::{Deserialize, Serialize};
use std::fmt;

use crate::Error;

/// The prefix for did:key DIDs.
pub const DID_KEY_PREFIX: &str = "did:key:";

/// A validated DID (Decentralized Identifier).
///
/// This newtype wraps a DID string and guarantees that it is properly formatted
/// with the `did:key:` prefix. Construction validates the format, so any `Did`
/// instance is guaranteed to be valid.
///
/// # Format
///
/// DIDs follow the did:key method format:
/// ```text
/// did:key:z<multibase-encoded-public-key>
/// ```
///
/// The `z` prefix indicates base58btc encoding (multibase).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Did(String);

impl Did {
    /// Creates a new DID from a string, validating the format.
    ///
    /// # Errors
    ///
    /// Returns an error if the string does not start with `did:key:`.
    pub fn new(s: impl Into<String>) -> Result<Self, Error> {
        let s = s.into();
        if !s.starts_with(DID_KEY_PREFIX) {
            return Err(Error::InvalidDid(format!(
                "DID must start with '{}', got: {}",
                DID_KEY_PREFIX, s
            )));
        }
        Ok(Self(s))
    }

    /// Creates a DID without validation.
    ///
    /// # Safety
    ///
    /// The caller must ensure the string is a valid did:key DID.
    /// This is intended for internal use where the DID is known to be valid.
    pub(crate) fn new_unchecked(s: String) -> Self {
        debug_assert!(s.starts_with(DID_KEY_PREFIX));
        Self(s)
    }

    /// Creates a wildcard DID representing "all actors".
    ///
    /// In Go DefraDB, the wildcard `"*"` is used in relationship operations
    /// to mean "all actors." This creates a `Did` wrapping `"*"`.
    pub fn wildcard() -> Self {
        Self("*".to_string())
    }

    /// Returns true if this is the wildcard DID ("*").
    pub fn is_wildcard(&self) -> bool {
        self.0 == "*"
    }

    /// Returns the DID as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns the multibase-encoded key portion of the DID.
    ///
    /// For a DID like `did:key:z6Mk...`, this returns `z6Mk...`.
    pub fn key_portion(&self) -> &str {
        &self.0[DID_KEY_PREFIX.len()..]
    }
}

impl fmt::Display for Did {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for Did {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl TryFrom<String> for Did {
    type Error = Error;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::new(s)
    }
}

impl From<Did> for String {
    fn from(did: Did) -> Self {
        did.0
    }
}

impl AsRef<str> for Did {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_did_new_valid() {
        let did = Did::new("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK").unwrap();
        assert_eq!(
            did.as_str(),
            "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"
        );
    }

    #[test]
    fn test_did_new_invalid_prefix() {
        let result = Did::new("invalid:key:z6Mk...");
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::InvalidDid(_)));
    }

    #[test]
    fn test_did_new_empty() {
        let result = Did::new("");
        assert!(result.is_err());
    }

    #[test]
    fn test_did_key_portion() {
        let did = Did::new("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK").unwrap();
        assert_eq!(
            did.key_portion(),
            "z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"
        );
    }

    #[test]
    fn test_did_display() {
        let did = Did::new("did:key:z6Mk...").unwrap();
        assert_eq!(format!("{}", did), "did:key:z6Mk...");
    }

    #[test]
    fn test_did_from_str() {
        let did: Did = "did:key:z6Mk...".parse().unwrap();
        assert_eq!(did.as_str(), "did:key:z6Mk...");
    }

    #[test]
    fn test_did_serde_roundtrip() {
        let did = Did::new("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK").unwrap();
        let json = serde_json::to_string(&did).unwrap();
        assert_eq!(
            json,
            "\"did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK\""
        );
        let parsed: Did = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, did);
    }

    #[test]
    fn test_did_serde_deserialize_invalid() {
        let result: Result<Did, _> = serde_json::from_str("\"invalid:key:z6Mk...\"");
        assert!(result.is_err());
    }

    #[test]
    fn test_did_into_string() {
        let did = Did::new("did:key:z6Mk...").unwrap();
        let s: String = did.into();
        assert_eq!(s, "did:key:z6Mk...");
    }

    #[test]
    fn test_did_as_ref() {
        let did = Did::new("did:key:z6Mk...").unwrap();
        let s: &str = did.as_ref();
        assert_eq!(s, "did:key:z6Mk...");
    }
}
