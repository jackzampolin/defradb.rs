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
//! - `document`: Document mapping and field positioning (in query-types crate)
//! - `mapper`: Query types (Select, Filter, Order, Aggregate) (in query-types crate)
//! - `planner`: Plan node trait and execution info
//! - `plan`: Concrete plan node implementations

// Types extracted to query-types crate.
pub use query_types::document;
pub use query_types::error;
pub use query_types::json_convert;
pub use query_types::limits;
pub use query_types::mapper;

// Parsing extracted to query-parse crate.
// Re-export as module aliases so `use crate::query_parse::` etc. still work.
extern crate query_parse as query_parse_ext;
pub use query_parse_ext::query_parse;
pub use query_parse_ext::schema_gen;
pub use query_parse_ext::sdl_parse;
pub use query_parse_ext::select_convert;

pub mod executor;
pub mod rest;
pub mod runner;
pub mod subscription;
#[cfg(test)]
pub mod test_utils;

// Plan layer extracted to query-plan crate (#670). Re-export these four
// modules verbatim so downstream consumers keep using `query::plan::*`,
// `query::planner::*`, `query::fetcher::*`, `query::mutator::*` unchanged.
pub use query_plan::{fetcher, mutator, plan, planner};

// `txn` is split: plan-layer primitives live in `query_plan::txn`, while
// `TransactionGuard` stays in this crate because it is generic over
// `QueryExecutor`. The local `txn` module re-exports both.
pub mod txn;

// Re-exports for convenience
pub use document::{DocumentMapping, RenderKey};
pub use error::{QueryError, Result, TransactionError};
pub use executor::{QueryExecutor, QueryRequest, QueryResponse, QueryResponseError};
pub use fetcher::{CollectionProvider, StaticCollectionProvider};
pub use limits::{
    QueryLimits, DEFAULT_MAX_FILTER_DEPTH, DEFAULT_MAX_QUERY_DEPTH, DEFAULT_MAX_QUERY_WIDTH,
};
pub use mapper::{Filter, Mutation, MutationType, Select};
pub use mutator::{
    BroadcastStatus, CreateResult, DeleteResult, DocMutator, MutationBatch,
    MutationBatchController, UpdateResult,
};
pub use plan::{
    CreateInput, CreateNode, DeleteNode, JoinDirection, JoinSide, LimitNode, ScanNode, SelectNode,
    TypeJoinMany, TypeJoinOne, UpdateInput, UpdateNode, UpsertAction, UpsertInput, UpsertNode,
};
pub use planner::{Doc, DocStatus, ExecInfo, PlanNode, Planner};
pub use query_parse::{
    parse_mutations, parse_mutations_with_limits, parse_query, parse_query_with_limits,
    parse_request, parse_request_with_limits, ExplainType, ParsedOperation,
};
pub use rest::{
    CollectionDocIdsPage, CollectionDocIdsPagination, RestError, RestOperations,
    RestOperationsImpl, RestResult,
};
pub use runner::{DocFetcher, FetchByIdsResult, NacChecker, QueryRunner};
pub use sdl_parse::{parse_sdl, parse_sdl_with_known_types};
pub use select_convert::select_to_go_json;
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
