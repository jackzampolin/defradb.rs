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
pub use orphan::OrphanNode;
pub use permission_filter::PermissionFilterNode;
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
pub(crate) fn strip_docid_from_conditions(
    conditions: &serde_json::Map<String, serde_json::Value>,
) -> serde_json::Value {
    strip_docid_value(&serde_json::json!(conditions))
}

fn strip_docid_value(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let mut result = serde_json::Map::new();
            for (key, val) in map {
                if key == "_docID" {
                    continue;
                }
                if key == "_and" || key == "_or" {
                    if let serde_json::Value::Array(arr) = val {
                        let filtered: Vec<serde_json::Value> = arr
                            .iter()
                            .map(strip_docid_value)
                            .filter(|item| {
                                !item.is_null()
                                    && !item.as_object().map(|o| o.is_empty()).unwrap_or(false)
                            })
                            .collect();
                        match filtered.len() {
                            0 => {} // Drop empty logical operator
                            1 => {
                                // Unwrap single-element _and/_or
                                if let Some(serde_json::Value::Object(inner)) =
                                    filtered.into_iter().next()
                                {
                                    for (k, v) in inner {
                                        result.insert(k, v);
                                    }
                                }
                            }
                            _ => {
                                result.insert(key.clone(), serde_json::Value::Array(filtered));
                            }
                        }
                    } else {
                        result.insert(key.clone(), val.clone());
                    }
                } else {
                    result.insert(key.clone(), val.clone());
                }
            }
            if result.is_empty() {
                serde_json::Value::Null
            } else {
                serde_json::Value::Object(result)
            }
        }
        _ => value.clone(),
    }
}
