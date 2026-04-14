//! Query plan tree, planner, and execution-boundary abstractions for DefraDB.
//!
//! This crate was extracted from the main `query` crate as part of #670 to
//! separate the plan/planner layer from the runner/executor layer. The
//! `query` crate depends on this one and re-exports its public API, so
//! downstream consumers keep their existing `query::plan::*`,
//! `query::planner::*`, etc. import paths unchanged.
//!
//! Modules:
//! - [`plan`] — individual plan node implementations (scan, select, limit,
//!   permission filter, joins, aggregates, mutation nodes, ...)
//! - [`planner`] — plan-building logic (builder, index selection, joins,
//!   mapping)
//! - [`fetcher`] — `DocFetcher` trait + result types, the read boundary
//!   between plan nodes and storage
//! - [`mutator`] — `DocMutator` trait + result types, the write boundary
//!   for mutation plan nodes
//! - [`txn`] — transaction-context trait, ACP overlay helpers, transaction
//!   registry. Contains `check_doc_access_with_overlay`, used by
//!   `plan::PermissionFilterNode` and the runner's mutation path.
//!
//! Shared identifier/mapper/error/document types come from the
//! [`query_types`] crate, not from here.

pub mod fetcher;
pub mod mutator;
pub mod plan;
pub mod planner;
pub mod txn;

// Re-export the most commonly used items at crate root for ergonomic access.
pub use fetcher::{DocFetcher, FetchByIdsResult, IndexScanResult};
pub use mutator::{BroadcastStatus, CreateResult, DeleteResult, DocMutator, UpdateResult};
pub use plan::{LensNode, PermissionFilterNode, SEFilterNode, ScanNode, SelectNode};
pub use planner::{Doc, DocStatus, ExecInfo, PlanNode, Planner};
