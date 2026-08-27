//! JSON path types for indexing
//!
//! Represents paths to leaf values within JSON documents for secondary indexing.

use serde::{Deserialize, Serialize};

/// Part of a JSON path - property name or array index marker.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum JsonPathPart {
    /// Property name in an object
    Property(String),
    /// Array index marker (always encodes as 0 per Go behavior)
    Index,
}

/// Path to a value within JSON, used for indexing.
///
/// Example: For JSON `{"custom": {"height": 168}}`, the path to `168` is
/// `[Property("custom"), Property("height")]`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct JsonPath(pub Vec<JsonPathPart>);

impl JsonPath {
    /// Create an empty path.
    pub fn new() -> Self {
        Self(Vec::new())
    }

    /// Create a new path by appending a property name.
    pub fn append_property(&self, name: &str) -> Self {
        let mut parts = self.0.clone();
        parts.push(JsonPathPart::Property(name.to_string()));
        Self(parts)
    }

    /// Create a new path by appending an array index marker.
    pub fn append_index(&self) -> Self {
        let mut parts = self.0.clone();
        parts.push(JsonPathPart::Index);
        Self(parts)
    }

    /// Return the number of path parts.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Return true if the path is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Get an iterator over the path parts.
    pub fn iter(&self) -> impl Iterator<Item = &JsonPathPart> {
        self.0.iter()
    }
}
