use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::document::DocumentMapping;
use crate::error::{QueryError, Result};
use crate::fetcher::DocFetcher;
use crate::mapper::{GroupBy, OrderBy};
use crate::planner::{Doc, ExecInfo, PlanNode};

use super::super::{JoinChildMetrics, JoinSide, RelationFilter};

#[derive(Clone)]
pub(super) struct IndexedChildFetch {
    pub fetcher: Arc<dyn DocFetcher>,
    pub collection_name: String,
    pub fk_field_name: String,
    pub index_name: String,
}

/// TypeJoinMany implements one-to-many relation joins.
///
/// The join flow:
/// 1. Parent plan yields a document (e.g., Author)
/// 2. Lookup all child docs where their FK matches parent's _docID
/// 3. Collect all matching child documents into an array
/// 4. Set the array on the parent document under the relation field key
///
/// # Optimization
///
/// Child documents are pre-loaded and indexed during `init()` to avoid
/// O(N * M) nested loop scans. Lookups are O(1) via HashMap.
///
/// # Memory Considerations
///
/// The child cache is unbounded - all child documents matching the query are loaded
/// into memory during `init()`. For collections with very large numbers of documents
/// (e.g., millions of posts for a popular author), this may cause significant memory
/// usage. Consider using pagination or separate queries for large datasets. Future
/// versions may implement LRU caching or streaming lookups to address this limitation.
pub struct TypeJoinMany {
    /// Parent side of the join (the "one" side)
    pub(super) parent_side: JoinSide,
    /// Child side of the join (the "many" side)
    pub(super) child_side: JoinSide,
    /// The parent plan node
    pub(super) parent_plan: Box<dyn PlanNode>,
    /// The child plan node (scanned once during init)
    pub(super) child_plan: Box<dyn PlanNode>,
    /// Document mapping for this join
    pub(super) document_mapping: DocumentMapping,
    /// Current document (merged parent + children array)
    pub(super) current_doc: Doc,
    /// The FK field index on the child side (validated at construction).
    /// Stored directly to avoid runtime option unwrapping.
    pub(super) child_fk_index: usize,
    /// Whether initialized
    pub(super) initialized: bool,
    /// Cached child documents indexed by FK field value.
    /// Key is the child's FK value (points to parent's _docID).
    pub(super) child_cache: HashMap<String, Vec<Doc>>,
    /// Per-parent limit on children (None = no limit)
    pub(super) child_limit: Option<u64>,
    /// Per-parent offset on children
    pub(super) child_offset: u64,
    /// Order by specification for children
    pub(super) child_order_by: Option<OrderBy>,
    /// Optional relation filter to apply during join.
    pub(super) relation_filter: Option<RelationFilter>,
    /// Optional groupBy for nested grouping of children.
    pub(super) child_group_by: Option<GroupBy>,
    /// Mapping for rendering documents inside the _group array.
    pub(super) group_mapping: Option<DocumentMapping>,
    /// Execution statistics for this join node
    pub(super) exec_info: ExecInfo,
    /// Cached child plan execution info (captured before child is closed)
    pub(super) child_exec_info: ExecInfo,
    /// Simulated Go-compatible child metrics.
    /// Go re-initializes the child scan per parent, reading ALL children from
    /// the collection each time. Metrics accumulate across all parent scans.
    pub(super) go_child_metrics: JoinChildMetrics,
    /// Accumulated calls to the per-parent child limit node.
    pub(super) child_limit_iterations: u64,
    /// Total children in the cache (docs per full collection scan)
    pub(super) total_children_in_cache: u64,
    /// Total field fetches per full collection scan
    pub(super) total_fields_per_scan: u64,
    /// When true, re-run the child plan per parent instead of caching all children.
    /// Used when the child plan uses an index for ordering, matching Go's per-parent
    /// scan behavior for correct metrics and limit support.
    pub(super) per_parent_child_scan: bool,
    /// When true, per-parent scans with child ordering must collect all matches before
    /// applying the limit so exhaustive nested relation ordering can merge null/orphan
    /// children ahead of later indexed matches.
    pub(super) preserve_ordered_orphans: bool,
    /// When true, the child plan itself yields documents in the requested child
    /// order, so per-parent scans can stop once the child limit is satisfied.
    pub(super) child_plan_provides_ordering: bool,
    /// Child documents in storage scan order: (fk_value, stored_field_count).
    /// Used to simulate Go's per-parent filteredFetcher behavior for explain metrics
    /// when a child limit is set.
    pub(super) child_scan_order: Vec<(String, u64)>,
    /// Optional separate child plan for indexed relation filter evaluation.
    /// When present, this plan uses an index to find children matching the
    /// relation filter. The main child_plan still provides ALL children for display.
    /// This matches Go's inverted index join behavior.
    pub(super) filter_child_plan: Option<Box<dyn PlanNode>>,
    /// Cache of children from filter_child_plan, indexed by FK value.
    pub(super) filter_child_cache: HashMap<String, Vec<Doc>>,
    /// FK field index in the filter child plan's documents.
    pub(super) filter_child_fk_index: Option<usize>,
    /// When true, pre-scan parent doc IDs and only retain matching children in caches.
    /// This narrows high-cardinality child scopes like `session -> messages` before
    /// downstream nested ordering/BM25 work without changing query results.
    pub(super) parent_scoped_child_cache: bool,
    /// Optional direct indexed child-fetch path used to build child_cache from FK lookups
    /// instead of scanning the entire child plan.
    pub(super) indexed_child_fetch: Option<IndexedChildFetch>,
}

impl std::fmt::Debug for TypeJoinMany {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TypeJoinMany")
            .field("parent_side", &self.parent_side)
            .field("child_side", &self.child_side)
            .field(
                "parent_plan",
                &format_args!("<PlanNode: {}>", self.parent_plan.kind()),
            )
            .field(
                "child_plan",
                &format_args!("<PlanNode: {}>", self.child_plan.kind()),
            )
            .field("child_fk_index", &self.child_fk_index)
            .field("initialized", &self.initialized)
            .finish()
    }
}

impl TypeJoinMany {
    /// Create a new TypeJoinMany node.
    ///
    /// # Errors
    /// Returns an error if `child_side` does not have a `relation_id_field_index` (FK field).
    /// One-to-many joins require the child to have an FK field pointing to the parent.
    pub fn new(
        parent_plan: Box<dyn PlanNode>,
        child_plan: Box<dyn PlanNode>,
        parent_side: JoinSide,
        child_side: JoinSide,
        document_mapping: DocumentMapping,
    ) -> Result<Self> {
        // Validate and extract child FK field index - required for one-to-many joins
        let expected_fk_name = schema::CollectionVersion::relation_id_field_name(
            child_side.relation_field().name.as_str(),
        );
        let child_fk_index = child_side.relation_id_field_index().ok_or_else(|| {
            QueryError::internal(format!(
                "TypeJoinMany requires child side to have FK field. \
                 Child collection '{}' relation field '{}' is missing expected FK field '{}'. \
                 Ensure the schema includes a '{}: DocID' field on the 'many' side of the relation.",
                child_side.collection().name,
                child_side.relation_field().name,
                expected_fk_name,
                expected_fk_name
            ))
        })?;

        Ok(Self {
            parent_side,
            child_side,
            parent_plan,
            child_plan,
            document_mapping,
            current_doc: Doc::default(),
            child_fk_index,
            initialized: false,
            child_cache: HashMap::new(),
            child_limit: None,
            child_offset: 0,
            child_order_by: None,
            relation_filter: None,
            child_group_by: None,
            group_mapping: None,
            exec_info: ExecInfo::default(),
            child_exec_info: ExecInfo::default(),
            go_child_metrics: JoinChildMetrics::new(),
            child_limit_iterations: 0,
            total_children_in_cache: 0,
            total_fields_per_scan: 0,
            per_parent_child_scan: false,
            preserve_ordered_orphans: false,
            child_plan_provides_ordering: false,
            child_scan_order: Vec::new(),
            filter_child_plan: None,
            filter_child_cache: HashMap::new(),
            filter_child_fk_index: None,
            parent_scoped_child_cache: false,
            indexed_child_fetch: None,
        })
    }

    /// Set the per-parent limit on children.
    pub fn with_limit(mut self, limit: u64) -> Self {
        self.child_limit = Some(limit);
        self
    }

    /// Set the per-parent offset on children.
    pub fn with_offset(mut self, offset: u64) -> Self {
        self.child_offset = offset;
        self
    }

    pub(super) fn record_child_limit_iterations(&mut self, child_count: usize) {
        if self.child_limit.is_none() && self.child_offset == 0 {
            return;
        }

        let available = (child_count as u64).saturating_sub(self.child_offset);
        let returned = self
            .child_limit
            .map(|limit| available.min(limit))
            .unwrap_or(available);

        // Go reinitializes the limit node per parent without resetting its
        // metrics, and collectDocs makes one final false call.
        self.child_limit_iterations += returned + 1;
    }

    /// Set the order by specification for children.
    pub fn with_order_by(mut self, order_by: OrderBy) -> Self {
        self.child_order_by = Some(order_by);
        self
    }

    /// Set a relation filter to apply during the join.
    ///
    /// When set, parent documents will only be included if they have at least one
    /// child document that passes this filter. This is used for queries like
    /// `Author(filter: {published: {rating: {_gt: 4}}})` - only include authors
    /// who have published at least one book with rating > 4.
    pub fn with_relation_filter(mut self, filter: RelationFilter) -> Self {
        self.relation_filter = Some(filter);
        self
    }

    /// Set a groupBy specification for grouping children.
    ///
    /// When set, children will be grouped by the specified fields. The output
    /// will be an array of objects, each containing the groupBy field values
    /// and a `_group` array of documents in that group.
    ///
    /// Example: `published(groupBy: [rating]) { rating, _group { name } }`
    /// Groups books by rating, outputting: `[{rating: 4.9, _group: [{name: "..."}]}, ...]`
    pub fn with_group_by(mut self, group_by: GroupBy) -> Self {
        self.child_group_by = Some(group_by);
        self
    }

    /// Set the mapping for rendering documents inside the _group array.
    ///
    /// This mapping determines which fields are rendered for documents inside _group.
    /// Only used when child_group_by is set.
    pub fn with_group_mapping(mut self, mapping: DocumentMapping) -> Self {
        self.group_mapping = Some(mapping);
        self
    }

    /// Enable per-parent child scanning.
    ///
    /// When enabled, the child plan is re-run for each parent instead of caching
    /// all children up front. This matches Go's behavior when using an index for
    /// ordering: each parent triggers a fresh index scan, with FK filtering and
    /// per-parent limit applied during the scan.
    pub fn with_per_parent_child_scan(mut self) -> Self {
        self.per_parent_child_scan = true;
        self
    }

    /// Preserve null/orphan children when ordering exhaustive nested relation scopes.
    pub fn with_preserve_ordered_orphans(mut self) -> Self {
        self.preserve_ordered_orphans = true;
        self
    }

    /// Mark that the child plan is already streaming in child order.
    pub fn with_child_plan_provides_ordering(mut self) -> Self {
        self.child_plan_provides_ordering = true;
        self
    }

    /// Set a separate filter child plan for indexed relation filter evaluation.
    ///
    /// When set, this plan uses an index to find children matching the relation
    /// filter. The main child_plan still provides ALL children for display.
    /// This avoids narrowing the display scan while still getting indexed filter evaluation.
    pub fn with_filter_child_plan(mut self, plan: Box<dyn PlanNode>) -> Self {
        // Determine the FK field index in the filter plan's mapping
        let fk_name = schema::CollectionVersion::relation_id_field_name(
            self.child_side.relation_field().name.as_str(),
        );
        let fk_idx = plan.document_map().first_index_of_name(&fk_name);
        self.filter_child_fk_index = fk_idx;
        self.filter_child_plan = Some(plan);
        self
    }

    /// Restrict child cache construction to the parent scope collected from parent_plan.
    ///
    /// This is a targeted optimization for nested one-to-many queries where the parent
    /// side is already narrow but the child collection is large.
    pub fn with_parent_scoped_child_cache(mut self) -> Self {
        self.parent_scoped_child_cache = true;
        self
    }

    /// Build child_cache from direct FK index lookups instead of scanning child_plan.
    ///
    /// This is intentionally narrow: the planner only enables it for simple child shapes
    /// where raw child docs can be converted with the current child scan mapping.
    pub fn with_indexed_child_fetch(
        mut self,
        fetcher: Arc<dyn DocFetcher>,
        collection_name: impl Into<String>,
        fk_field_name: impl Into<String>,
        index_name: impl Into<String>,
    ) -> Self {
        self.indexed_child_fetch = Some(IndexedChildFetch {
            fetcher,
            collection_name: collection_name.into(),
            fk_field_name: fk_field_name.into(),
            index_name: index_name.into(),
        });
        self
    }

    pub(super) async fn collect_parent_doc_ids(&mut self) -> Result<HashSet<String>> {
        let mut parent_doc_ids = HashSet::new();

        self.parent_plan.init().await?;
        self.parent_plan.start().await?;

        while self.parent_plan.next().await? {
            if let Some(doc_id) = self.parent_plan.value().doc_id() {
                parent_doc_ids.insert(doc_id.to_string());
            }
        }

        self.parent_plan.close().await?;

        Ok(parent_doc_ids)
    }
}
