//! REST operations for document CRUD endpoints.
//!
//! This module defines the interface between the HTTP layer and REST-specific operations.
//! It provides collection listing and document CRUD operations separate from GraphQL.

mod error;
mod gql;
mod operations;
mod trait_def;

pub use error::{RestError, RestResult};
pub use operations::RestOperationsImpl;
pub use trait_def::{CollectionDocIdsPage, CollectionDocIdsPagination, RestOperations};

#[cfg(test)]
mod tests;
