//! Schema definitions for DefraDB
//!
//! This crate provides types for defining document structure and CRDT types.
//! It is foundational for the query crate and document storage.
//!
//! The numeric values for FieldKind and CType match the Go DefraDB implementation
//! exactly, ensuring Rust and Go can read/write the same datastores.

mod collection;
mod ctype;
mod error;
mod field;
mod field_kind;
mod validation;

pub use collection::{CollectionBuilder, CollectionVersion};
pub use ctype::CType;
pub use error::{Result, SchemaError};
pub use field::FieldDescription;
pub use field_kind::{FieldKind, ScalarArrayKind, ScalarKind};
pub use validation::{validate_schema, SchemaValidator};
