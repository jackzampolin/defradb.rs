//! Index selection and filter-to-index translation
//!
//! Provides utilities for determining when filters can use indexes
//! and translating filter conditions to index scan parameters.

mod conditions;
mod filter_to_scan;
mod types;
mod values;

#[cfg(test)]
mod tests;

pub use conditions::{
    can_be_ordered_by_index, can_use_index, extract_field_conditions, select_best_index,
};
pub use filter_to_scan::{can_or_filter_use_index, filter_to_index_scan, or_filter_to_index_scan};
pub use types::{ConditionValue, FieldCondition, IndexScanParams, IndexScanType, ScanValueFilter};
