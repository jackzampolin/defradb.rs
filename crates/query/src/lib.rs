//! DefraDB Query Module
//!
//! This crate implements the query system for DefraDB, following the Volcano Iterator Model.
//! It provides GraphQL query parsing, planning, and execution.
//!
//! # Architecture
//!
//! ```text
//! GraphQL String → Parser → Mapper → Planner → Plan Nodes → Executor → Results
//!                                        ↓
//!                                Fetcher → Storage
//! ```
//!
//! # Main Components
//!
//! - `document`: Document mapping and field positioning
//! - `mapper`: Query types (Select, Filter, Order, Aggregate)
//! - `planner`: Plan node trait and execution info
//! - `plan`: Concrete plan node implementations

pub mod document;
pub mod error;
pub mod executor;
pub mod fetcher;
mod json_convert;
pub mod mapper;
pub mod plan;
pub mod planner;
pub mod query_parse;
pub mod runner;
pub mod schema_gen;
pub mod sdl_parse;
pub mod test_utils;
pub mod txn;

// Re-exports for convenience
pub use document::{DocumentMapping, RenderKey};
pub use error::{QueryError, Result, TransactionError};
pub use executor::{QueryExecutor, QueryRequest, QueryResponse, QueryResponseError};
pub use mapper::{Filter, Select};
pub use plan::{CreateInput, CreateNode, LimitNode, ScanNode, SelectNode};
pub use planner::{Doc, DocStatus, ExecInfo, PlanNode, Planner};
pub use query_parse::parse_query;
pub use runner::{DocFetcher, FetchByIdsResult, QueryRunner};
pub use sdl_parse::parse_sdl;
pub use txn::{
    GetTransactionResult, NoOpTransactionRegistry, TransactionContext, TransactionGuard,
    TransactionHandle, TransactionRegistry,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crate_compiles() {
        // Basic smoke test that the crate compiles
        let mapping = DocumentMapping::new();
        assert_eq!(mapping.next_index(), 0);
    }
}
