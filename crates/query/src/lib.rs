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

pub mod query_parse;
pub mod schema_gen;
pub mod sdl_parse;
pub mod select_convert;

pub mod executor;
pub mod rest;
pub mod runner;
pub mod subscription;
#[cfg(test)]
pub mod test_utils;

pub mod doc_stream;
pub mod fetcher;
pub mod mutator;
pub mod plan;
pub mod planner;
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
pub use runner::{DocFetcher, FetchByIdsResult, NacChecker, QueryRunner, SeQueryTransport};
pub use sdl_parse::{parse_sdl, parse_sdl_with_known_types};
pub use select_convert::select_to_go_json;
pub use txn::{
    GetTransactionResult, NoOpTransactionRegistry, TransactionContext, TransactionGuard,
    TransactionHandle, TransactionRegistry,
};
