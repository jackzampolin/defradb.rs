//! Schema definitions for DefraDB
//!
//! This crate provides types for defining document structure and CRDT types.
//! It is foundational for the query crate and document storage.
//!
//! The numeric values for FieldKind and CType match the Go DefraDB implementation
//! exactly, ensuring Rust and Go can read/write the same datastores.

mod cid;
mod collection;
mod ctype;
mod embedding;
mod error;
mod field;
mod field_kind;
mod index;
mod policy;
mod relation;
mod source;
mod validation;

pub use cid::{
    generate_collection_block_full, generate_collection_cid, generate_collection_cid_full,
    generate_collection_cid_with_priority, generate_collection_cid_with_priority_and_heads,
    generate_field_block_with_priority_and_heads, generate_field_cid,
    generate_field_cid_with_priority, generate_field_cid_with_priority_and_heads, BlockWithCid,
};
pub use collection::{CollectionBuilder, CollectionVersion};
pub use ctype::CType;
pub use embedding::VectorEmbeddingDescription;
pub use error::{Result, SchemaError};
pub use field::FieldDescription;
pub use field_kind::{FieldKind, ScalarArrayKind, ScalarKind};
pub use index::{
    EncryptedIndexDescription, EncryptedIndexType, IndexDescription, IndexedFieldDescription,
};
pub use policy::PolicyDescription;
pub use source::{CollectionSetDescription, CollectionSource, QuerySource};
pub use validation::{validate_schema, SchemaValidator};
