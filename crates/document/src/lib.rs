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
mod encoding_cbor;
mod error;
mod field;
mod json_leaf;
mod json_path;
mod json_traverse;
mod normal;
mod normal_conversions;
mod value;

pub use doc_id::{validate_doc_ids, DocID, DOC_ID_V0, SDN_NAMESPACE_V0};
pub use document::Document;
pub use error::{Error, Result};
pub use field::{special, Field};
pub use json_leaf::{JsonLeafValue, JsonScalarValue};
pub use json_path::{JsonPath, JsonPathPart};
pub use json_traverse::{index_traverse_options, traverse_json, TraverseOptions};
pub use normal::NormalValue;
pub use value::FieldValue;

// Re-export schema types commonly used with documents
pub use schema::{CType, CollectionVersion};
