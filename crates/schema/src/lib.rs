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

pub mod definition_validation;

pub use cid::{
    generate_collection_block_full, generate_collection_block_full_with_query,
    generate_collection_cid, generate_collection_cid_full, generate_collection_cid_full_with_query,
    generate_collection_cid_with_priority, generate_collection_cid_with_priority_and_heads,
    generate_collection_set_cid, generate_field_block_with_priority_and_heads, generate_field_cid,
    generate_field_cid_with_priority, generate_field_cid_with_priority_and_heads, BlockWithCid,
};
pub use collection::{
    legacy_collection_short_id, CollectionBuilder, CollectionVersion, ORPHAN_COLLECTION_ID,
};
pub use ctype::CType;
pub use embedding::VectorEmbeddingDescription;
pub use error::{Result, SchemaError};
pub use field::FieldDescription;
pub use field_kind::{FieldKind, ScalarArrayKind, ScalarKind};
pub use index::{
    DistanceMetric, EncryptedIndexDescription, EncryptedIndexType, FullTextIndexDescription,
    HnswParams, IndexDescription, IndexKind, IndexedFieldDescription, IvfPqParams,
    OrderedIndexDescription, SsgParams, VectorAlgorithm, VectorIndexDescription,
};
pub use policy::PolicyDescription;
pub use source::{
    query_select_json_bytes, CollectionSetDescription, CollectionSource, QuerySource,
};
pub use validation::{validate_schema, SchemaValidator};
