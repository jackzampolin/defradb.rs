//! Collection types and operations

use crate::types::CollectionId;

/// A collection in DefraDB - similar to a table in SQL
#[derive(Debug, Clone)]
pub struct Collection {
    /// Collection identifier
    pub id: CollectionId,

    /// Collection name
    pub name: String,

    /// Schema version
    pub version: u32,
}

impl Collection {
    /// Create a new collection
    pub fn new(id: CollectionId, name: impl Into<String>, version: u32) -> Self {
        Self {
            id,
            name: name.into(),
            version,
        }
    }
}
