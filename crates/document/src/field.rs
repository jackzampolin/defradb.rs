//! Field types for document structure

use schema::CType;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// A field identifier within a document.
///
/// Fields have a name and a CRDT type that determines how conflicts are resolved.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Field {
    /// The field name
    name: String,
    /// The CRDT type for this field
    crdt_type: CType,
}

impl Field {
    /// Create a new field with the given name and CRDT type.
    ///
    /// Returns an error if the name is empty.
    pub fn new(name: impl Into<String>, crdt_type: CType) -> Result<Self> {
        let name = name.into();
        if name.is_empty() {
            return Err(Error::EmptyFieldName);
        }
        Ok(Self { name, crdt_type })
    }

    /// Create a new field with LWW Register CRDT type (default).
    ///
    /// Returns an error if the name is empty.
    pub fn lww(name: impl Into<String>) -> Result<Self> {
        Self::new(name, CType::LwwRegister)
    }

    /// Create a new counter field (PnCounter).
    ///
    /// Returns an error if the name is empty.
    pub fn counter(name: impl Into<String>) -> Result<Self> {
        Self::new(name, CType::PnCounter)
    }

    /// Create a field without validation.
    ///
    /// This is for internal use where the field name is known to be valid.
    pub(crate) fn new_unchecked(name: impl Into<String>, crdt_type: CType) -> Self {
        let name = name.into();
        debug_assert!(!name.is_empty(), "field name cannot be empty");
        Self { name, crdt_type }
    }

    /// Create a LWW field without validation.
    ///
    /// This is for internal use where the field name is known to be valid.
    pub(crate) fn lww_unchecked(name: impl Into<String>) -> Self {
        Self::new_unchecked(name, CType::LwwRegister)
    }

    /// Get the field name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the CRDT type for this field.
    pub fn crdt_type(&self) -> CType {
        self.crdt_type
    }
}

impl std::fmt::Display for Field {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

/// Special field names used by DefraDB
pub mod special {
    /// The document ID field name
    pub const DOC_ID: &str = "_docID";
    /// The deleted marker field name
    pub const DELETED: &str = "_deleted";
    /// The version field name
    pub const VERSION: &str = "_version";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_field() {
        let field = Field::new("name", CType::LwwRegister).unwrap();
        assert_eq!(field.name(), "name");
        assert_eq!(field.crdt_type(), CType::LwwRegister);
    }

    #[test]
    fn test_lww_field() {
        let field = Field::lww("title").unwrap();
        assert_eq!(field.name(), "title");
        assert_eq!(field.crdt_type(), CType::LwwRegister);
    }

    #[test]
    fn test_counter_field() {
        let field = Field::counter("views").unwrap();
        assert_eq!(field.name(), "views");
        assert_eq!(field.crdt_type(), CType::PnCounter);
    }

    #[test]
    fn test_display() {
        let field = Field::lww("email").unwrap();
        assert_eq!(field.to_string(), "email");
    }

    #[test]
    fn test_equality() {
        let f1 = Field::lww("name").unwrap();
        let f2 = Field::lww("name").unwrap();
        let f3 = Field::lww("other").unwrap();
        let f4 = Field::counter("name").unwrap();

        assert_eq!(f1, f2);
        assert_ne!(f1, f3);
        assert_ne!(f1, f4); // Same name, different CRDT type
    }

    #[test]
    fn test_empty_name_rejected() {
        let result = Field::new("", CType::LwwRegister);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::EmptyFieldName));
    }

    #[test]
    fn test_empty_name_rejected_lww() {
        let result = Field::lww("");
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::EmptyFieldName));
    }

    #[test]
    fn test_empty_name_rejected_counter() {
        let result = Field::counter("");
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::EmptyFieldName));
    }
}
