use crate::planner::Doc;
use query_types::mapper::{AggregateType, Filter, Limit, OrderBy};

/// Definition of an inner aggregate to compute during nested group rendering.
///
/// When a query has nested _group with aggregates (e.g., `_group(groupBy: [Verified]) { _avg(...) }`),
/// the GroupByNode computes these aggregates inline during _group array rendering,
/// so that outer aggregates (e.g., `MAX(GROUP: {field: AVG})`) can read the values.
#[derive(Debug, Clone)]
pub struct InnerAggregateDef {
    pub aggregate_type: AggregateType,
    /// Render key name for the aggregate result (e.g., "AVG" or alias)
    pub output_key: String,
    /// Index of the target field in the parent mapping (e.g., Age field index)
    pub field_index: usize,
}

/// Definition of a _group alias with its specific rendering arguments.
///
/// Each _group reference in a query (including aliases like `G1: _group(limit: 1)`)
/// gets its own GroupAlias with its specific filter, limit, order, and docID filter.
#[derive(Debug, Clone)]
pub struct GroupAlias {
    /// Index in the document mapping where this alias's array should be stored
    pub index: usize,
    /// Optional filter for this alias
    pub filter: Option<Filter>,
    /// Optional limit for this alias
    pub limit: Option<Limit>,
    /// Optional order for this alias
    pub order: Option<OrderBy>,
    /// Optional docID filter for this alias
    pub doc_ids: Option<Vec<String>>,
}

/// Metadata about a child select for explain output.
///
/// This mirrors Go's groupNode.childSelects structure used in explain.
#[derive(Debug, Clone, Default)]
pub struct ChildSelectMeta {
    /// Collection name for this child select
    pub collection_name: String,
    /// Optional docID filter
    pub doc_ids: Option<Vec<String>>,
    /// Optional filter
    pub filter: Option<Filter>,
    /// Optional limit
    pub limit: Option<Limit>,
    /// Optional order
    pub order: Option<OrderBy>,
    /// Optional inner groupBy fields
    pub group_by: Option<Vec<String>>,
}

/// A group of documents with the same group key
#[derive(Debug)]
pub struct DocumentGroup {
    /// The documents in this group
    pub docs: Vec<Doc>,
    /// The representative document (first doc) for this group
    pub representative: Doc,
}

impl DocumentGroup {
    #[allow(dead_code)]
    fn new(first_doc: Doc) -> Self {
        Self {
            representative: first_doc.deep_clone(),
            docs: vec![first_doc],
        }
    }

    #[allow(dead_code)]
    fn add(&mut self, doc: Doc) {
        self.docs.push(doc);
    }
}
