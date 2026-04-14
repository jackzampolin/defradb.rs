//! Query planner for converting operations to execution plans
//!
//! This module is organized into submodules:
//! - `builder`: Main Planner struct and plan building logic
//! - `index_selection`: Index selection algorithms
//! - `traits`: PlanNode trait and related types
//! - `mapping`: Document mapping utilities (placeholder for extraction)
//! - `aggregates`: Aggregate node building (placeholder for extraction)
//! - `joins`: Join planning utilities (placeholder for extraction)
//! - `views`: View planning utilities (placeholder for extraction)

mod aggregates;
mod builder;
mod cached_view_builder;
pub mod index_selection;
mod joins;
mod mapping;
mod traits;
mod view_builder;

pub use builder::{PlanResult, Planner};
pub use index_selection::{
    can_use_index, extract_field_conditions, filter_to_index_scan, select_best_index,
    ConditionValue, FieldCondition, IndexScanParams, IndexScanType, ScanValueFilter,
};
pub use query_types::doc::{Doc, DocFields, DocStatus};
pub use traits::{ExecInfo, PlanNode};
