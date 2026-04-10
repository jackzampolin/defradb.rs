//! Compatibility facade for query planning APIs.

pub mod index_selection {
    pub use query_plan::planner::index_selection::*;
}

pub use query_plan::planner::{
    can_use_index, extract_field_conditions, filter_to_index_scan, select_best_index,
    ConditionValue, Doc, DocFields, DocStatus, ExecInfo, FieldCondition, IndexScanParams,
    IndexScanType, PlanNode, PlanResult, Planner, ScanValueFilter,
};
