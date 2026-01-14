// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! Document types for DefraDB
//!
//! This crate provides runtime document types for DefraDB, including:
//! - `Document` - The main document type with fields and values
//! - `DocID` - Content-addressed document identifier
//! - `NormalValue` - Type-safe value enum for all field types
//! - `FieldValue` - Wrapper with CRDT type and dirty tracking
//! - `Field` - Field definition with name and CRDT type
//!
//! ## Example
//!
//! ```
//! use document::{Document, NormalValue};
//!
//! // Create a document from JSON
//! let doc = Document::from_json_str(r#"{"name": "Alice", "age": 30}"#).unwrap();
//!
//! // Access fields
//! assert_eq!(doc.get("name").and_then(|v| v.as_str()), Some("Alice"));
//! assert_eq!(doc.get("age").and_then(|v| v.as_int()), Some(30));
//!
//! // Document has auto-generated ID
//! assert!(doc.id().is_some());
//! ```

mod doc_id;
mod document;
mod encoding;
mod error;
mod field;
mod normal;
mod value;

pub use doc_id::{DocID, DOC_ID_V0, SDN_NAMESPACE_V0};
pub use document::Document;
pub use error::{Error, Result};
pub use field::{special, Field};
pub use normal::NormalValue;
pub use value::FieldValue;

// Re-export schema types commonly used with documents
pub use schema::{CType, CollectionVersion};
