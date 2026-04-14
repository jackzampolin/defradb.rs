//! Query plan nodes implementing the Volcano Iterator Model

pub mod aggregate;
mod alldocs;
mod bm25;
mod cached_view_fetcher;
pub mod groupby;
mod index_scan;
pub mod lens_node;
mod limit;
pub mod mutation;
mod orderby;
mod orphan;
mod permission_filter;
mod scan;
mod se_filter;
mod select;
mod sequence;
mod similarity;
mod type_join;
pub mod view;
pub mod view_cache;

pub use aggregate::{
    AverageNode, AvgSourceMeta, CountNode, CountSourceMeta, MaxNode, MaxSourceMeta, MinNode,
    MinSourceMeta, SumNode, SumSourceMeta,
};
pub use alldocs::AllDocsNode;
pub use bm25::BM25Node;
pub use cached_view_fetcher::CachedViewFetcher;
pub use groupby::{ChildSelectMeta, DocumentGroup, GroupAlias, GroupByNode, InnerAggregateDef};
pub use index_scan::IndexScanNode;
pub use lens_node::LensNode;
pub use limit::LimitNode;
pub use mutation::{
    CreateInput, CreateNode, DeleteNode, UpdateInput, UpdateNode, UpsertAction, UpsertInput,
    UpsertNode,
};
pub use orderby::OrderByNode;
pub use orphan::{OrphanNode, SharedYieldedIds};
pub use permission_filter::PermissionFilterNode;
use query_types::mapper::Filter;
pub use scan::ScanNode;
pub use se_filter::{SEFilterCondition, SEFilterNode};
pub use select::SelectNode;
pub use sequence::SequenceNode;
pub use similarity::SimilarityNode;
pub use type_join::{
    compare_json_values, resolve_nested_field, JoinDirection, JoinSide, RelationFilter,
    TypeJoinMany, TypeJoinOne,
};
pub use view::ViewNode;

/// Strip `_docID` conditions from filter for explain output.
/// Go handles docIDs as prefix scans and strips them from filter display.
/// This handles `_docID` at any nesting level within `_and`/`_or` arrays.
pub(crate) fn strip_docid_from_filter(filter: &Filter) -> serde_json::Value {
    filter.to_explain_json_without_docid()
}
