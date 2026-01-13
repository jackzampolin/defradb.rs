// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! Field types for document structure

use schema::CType;
use serde::{Deserialize, Serialize};

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
    pub fn new(name: impl Into<String>, crdt_type: CType) -> Self {
        Self {
            name: name.into(),
            crdt_type,
        }
    }

    /// Create a new field with LWW Register CRDT type (default).
    pub fn lww(name: impl Into<String>) -> Self {
        Self::new(name, CType::LwwRegister)
    }

    /// Create a new counter field (PnCounter).
    pub fn counter(name: impl Into<String>) -> Self {
        Self::new(name, CType::PnCounter)
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
        let field = Field::new("name", CType::LwwRegister);
        assert_eq!(field.name(), "name");
        assert_eq!(field.crdt_type(), CType::LwwRegister);
    }

    #[test]
    fn test_lww_field() {
        let field = Field::lww("title");
        assert_eq!(field.name(), "title");
        assert_eq!(field.crdt_type(), CType::LwwRegister);
    }

    #[test]
    fn test_counter_field() {
        let field = Field::counter("views");
        assert_eq!(field.name(), "views");
        assert_eq!(field.crdt_type(), CType::PnCounter);
    }

    #[test]
    fn test_display() {
        let field = Field::lww("email");
        assert_eq!(field.to_string(), "email");
    }

    #[test]
    fn test_equality() {
        let f1 = Field::lww("name");
        let f2 = Field::lww("name");
        let f3 = Field::lww("other");
        let f4 = Field::counter("name");

        assert_eq!(f1, f2);
        assert_ne!(f1, f3);
        assert_ne!(f1, f4); // Same name, different CRDT type
    }
}
