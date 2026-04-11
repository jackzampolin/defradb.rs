//! Shared type definitions for the DefraDB query engine.
//!
//! This crate contains the type vocabulary used across parsing, planning,
//! and execution: Filter, Select, Mutation, DocumentMapping, Doc, QueryError, etc.

pub mod doc;
pub mod document;
pub mod error;
pub mod json_convert;
pub mod mapper;

// Re-export primary types
pub use doc::{Doc, DocFields, DocStatus};
pub use document::{DocumentMapping, RenderKey};
pub use error::{QueryError, Result, TransactionError};
pub use mapper::{Filter, Mutation, MutationType, Select};
