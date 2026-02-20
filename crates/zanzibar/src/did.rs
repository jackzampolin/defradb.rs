use serde::{Deserialize, Serialize};
use std::fmt;

use crate::error::Error;

pub const DID_KEY_PREFIX: &str = "did:key:";

/// A validated DID (Decentralized Identifier).
///
/// Wraps a DID string and guarantees `did:key:` prefix format.
///
/// # Format
///
/// ```text
/// did:key:z<multibase-encoded-public-key>
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Did(String);

impl Did {
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

    /// Create a DID without validation.
    ///
    /// Caller must ensure the string is a valid did:key DID.
    pub fn new_unchecked(s: String) -> Self {
        debug_assert!(s.starts_with(DID_KEY_PREFIX) || s == "*");
        Self(s)
    }

    pub fn wildcard() -> Self {
        Self("*".to_string())
    }

    pub fn is_wildcard(&self) -> bool {
        self.0 == "*"
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

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
