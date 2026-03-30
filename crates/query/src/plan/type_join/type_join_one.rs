//! TypeJoinOne - one-to-one relation joins

use async_trait::async_trait;
use document::NormalValue;
use serde_json::Value as JsonValue;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::warn;

use crate::document::{documents_to_plan_docs, DocumentMapping};
use crate::error::{QueryError, Result};
use crate::fetcher::DocFetcher;
use crate::mapper::Filter;
use crate::plan::IndexScanNode;
use crate::planner::index_selection::{IndexScanParams, IndexScanType};
use crate::planner::{Doc, ExecInfo, PlanNode};

use super::{JoinChildMetrics, JoinDirection, JoinSide};

/// TypeJoinOne implements one-to-one relation joins.
///
/// **Primary side join flow** (when parent has the FK, e.g., `Book.author`):
/// 1. Parent plan yields a document (e.g., Book with `author_id: "bae-123"`)
/// 2. Extract the FK value from the relation's ID field (e.g., `author_id`)
/// 3. Lookup child document where `_docID` matches the FK value
/// 4. Merge the child document into the parent under the relation field key
///
/// **Secondary/inverted side join flow** (when parent lacks FK, e.g., `Author.book`):
/// 1. Parent plan yields a document (e.g., Author with `_docID: "bae-123"`)
/// 2. Lookup child document where their FK matches parent's `_docID`
/// 3. Merge the first matching child document
///
/// # Optimization
///
/// Child documents are pre-loaded and indexed during `init()` to avoid
/// O(N * M) nested loop scans. Lookups are O(1) via HashMap.
///
/// # Memory Considerations
///
/// The child cache is unbounded - all child documents matching the query are loaded
/// into memory during `init()`. For collections with very large numbers of documents,
/// this may cause significant memory usage. Consider using pagination or separate
/// queries for large datasets. Future versions may implement LRU caching or streaming
/// lookups to address this limitation.
pub struct TypeJoinOne {
    /// Parent side of the join (outer loop)
    parent_side: JoinSide,
    /// Child side of the join (lookup)
    child_side: JoinSide,
    /// The parent plan node
    parent_plan: Box<dyn PlanNode>,
    /// The child plan node (scanned once during init)
    child_plan: Box<dyn PlanNode>,
    /// Document mapping for this join
    document_mapping: DocumentMapping,
    /// Current document (merged parent + child)
    current_doc: Doc,
    /// The direction of this join, determined by which side holds the FK.
    pub(crate) direction: JoinDirection,
    /// Whether initialized
    initialized: bool,
    /// Cached child documents indexed by lookup key.
    /// For Primary joins: key is child's _docID
    /// For Inverted joins: key is child's FK field value
    child_cache: HashMap<String, Doc>,
    /// Optional relation filter to apply during join.
    /// This filter is evaluated against the child document and determines
    /// whether the parent document should be included in results.
    /// Example: `{author: {verified: {_eq: true}}}` means only include parents
    /// where their related child (author) has verified=true.
    relation_filter: Option<RelationFilter>,
    /// Execution statistics for this join node
    exec_info: ExecInfo,
    /// Cached child plan execution info (captured before child is closed)
    child_exec_info: ExecInfo,
    /// Simulated Go-compatible child metrics.
    /// Go re-initializes the child scan per parent with a specific docID prefix,
    /// calling Next() exactly once per parent. Metrics accumulate across parents.
    go_child_metrics: JoinChildMetrics,
    /// For InvertedIndex mode: fetcher for creating per-child parent lookups
    fetcher: Option<Arc<dyn DocFetcher>>,
    /// For InvertedIndex mode: parent collection schema
    parent_collection: Option<schema::CollectionVersion>,
    /// For InvertedIndex mode: parent document mapping for per-child lookups
    parent_scan_mapping: Option<DocumentMapping>,
    /// For InvertedIndex mode: queue of parent docs to yield
    docs_to_yield: Vec<Doc>,
    /// Whether to include orphan parents (for @exhaustive directive)
    include_orphans: bool,
    /// Parent docIDs already yielded during child-driven scan
    yielded_parent_ids: HashSet<String>,
    /// Whether the child-driven scan is exhausted and we're now yielding orphans
    orphan_phase: bool,
}

/// A filter condition on a relation field.
/// Contains the relation field name and the nested filter conditions.
#[derive(Debug, Clone)]
pub struct RelationFilter {
    /// The name of the relation field (e.g., "author")
    pub relation_field: String,
    /// The nested filter conditions to apply to the child document
    pub conditions: Filter,
}

impl std::fmt::Debug for TypeJoinOne {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TypeJoinOne")
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
            .field("direction", &self.direction)
            .field("initialized", &self.initialized)
            .finish()
    }
}

impl TypeJoinOne {
    /// Create a new TypeJoinOne node.
    pub fn new(
        parent_plan: Box<dyn PlanNode>,
        child_plan: Box<dyn PlanNode>,
        parent_side: JoinSide,
        child_side: JoinSide,
        document_mapping: DocumentMapping,
    ) -> Self {
        // Determine join direction based on which side holds the FK
        let direction = match parent_side.relation_id_field_index() {
            Some(idx) => JoinDirection::Primary {
                parent_fk_index: idx,
            },
            None => JoinDirection::Inverted,
        };

        Self {
            parent_side,
            child_side,
            parent_plan,
            child_plan,
            document_mapping,
            current_doc: Doc::default(),
            direction,
            initialized: false,
            child_cache: HashMap::new(),
            relation_filter: None,
            exec_info: ExecInfo::default(),
            child_exec_info: ExecInfo::default(),
            go_child_metrics: JoinChildMetrics::new(),
            fetcher: None,
            parent_collection: None,
            parent_scan_mapping: None,
            docs_to_yield: Vec::new(),
            include_orphans: false,
            yielded_parent_ids: HashSet::new(),
            orphan_phase: false,
        }
    }

    /// Set a relation filter to apply during the join.
    ///
    /// When set, parent documents will only be included if their child
    /// document passes this filter. This is used for queries like
    /// `Book(filter: {author: {verified: {_eq: true}}})`.
    pub fn with_relation_filter(mut self, filter: RelationFilter) -> Self {
        self.relation_filter = Some(filter);
        self
    }

    /// Configure for inverted index join mode.
    ///
    /// In this mode, the child is scanned first using its index, then for each
    /// child doc we look up the parent by its FK index (FK field == child's _docID).
    pub fn with_inverted_index(
        mut self,
        fk_index_name: String,
        fk_field_index: usize,
        parent_collection: schema::CollectionVersion,
        parent_scan_mapping: DocumentMapping,
        fetcher: Arc<dyn DocFetcher>,
    ) -> Self {
        self.direction = JoinDirection::InvertedIndex {
            parent_fk_index_name: fk_index_name,
            parent_fk_field_index: fk_field_index,
        };
        self.parent_collection = Some(parent_collection);
        self.parent_scan_mapping = Some(parent_scan_mapping);
        self.fetcher = Some(fetcher);
        self
    }

    /// Configure for ordered inverted join (primary-first) mode.
    ///
    /// In this mode, the child drives iteration in sorted order (via index scan).
    /// For each child, the parent is found by reading the child's FK field value
    /// (which is the parent's _docID) and doing a direct prefix-based lookup.
    pub fn with_ordered_inverted_primary(
        mut self,
        child_fk_index: usize,
        parent_collection: schema::CollectionVersion,
        parent_scan_mapping: DocumentMapping,
        fetcher: Arc<dyn DocFetcher>,
    ) -> Self {
        self.direction = JoinDirection::OrderedInvertedPrimary { child_fk_index };
        self.parent_collection = Some(parent_collection);
        self.parent_scan_mapping = Some(parent_scan_mapping);
        self.fetcher = Some(fetcher);
        self
    }

    /// Enable orphan inclusion (for @exhaustive directive).
    pub fn with_include_orphans(mut self) -> Self {
        self.include_orphans = true;
        self
    }

    /// Returns the direction of this join.
    pub fn direction(&self) -> &JoinDirection {
        &self.direction
    }

    /// Extract the foreign key value from the parent document.
    ///
    /// For primary joins, extracts the FK field value.
    /// For inverted joins, extracts the parent's `_docID`.
    ///
    /// Logs a warning if the FK field has an unexpected type or is missing.
    fn extract_fk(&self, parent_doc: &Doc) -> Option<String> {
        match &self.direction {
            JoinDirection::InvertedIndex { .. } | JoinDirection::OrderedInvertedPrimary { .. } => {
                // Not used in inverted index / ordered inverted modes (child drives the loop)
                None
            }
            JoinDirection::Inverted => {
                // Secondary side: use parent's _docID as the lookup key
                let doc_id = parent_doc.doc_id();
                if doc_id.is_none() {
                    warn!(
                        parent_collection = %self.parent_side.collection().name,
                        "Parent document missing _docID for inverted join lookup. \
                         This may indicate data corruption or a schema mismatch."
                    );
                }
                doc_id.map(String::from)
            }
            JoinDirection::Primary { parent_fk_index } => {
                // Primary side: extract from the FK field (e.g., author_id)
                let value = match parent_doc.get(*parent_fk_index) {
                    Some(v) => v,
                    None => {
                        warn!(
                            parent_collection = %self.parent_side.collection().name,
                            relation_field = %self.parent_side.relation_field().name,
                            fk_index = parent_fk_index,
                            doc_id = ?parent_doc.doc_id(),
                            "FK field not found at expected index in document. \
                             This may indicate a schema migration issue or document corruption."
                        );
                        return None;
                    }
                };

                // Check for type mismatch (FK should be string or null)
                if !value.is_null() && !value.is_string() {
                    warn!(
                        parent_collection = %self.parent_side.collection().name,
                        relation_field = %self.parent_side.relation_field().name,
                        fk_index = parent_fk_index,
                        actual_type = ?value,
                        "FK field has unexpected type, expected string or null"
                    );
                }

                value.as_str().map(String::from)
            }
        }
    }

    /// Find child document by FK lookup using the pre-built cache.
    fn find_child_doc(&self, fk: &str) -> Option<Doc> {
        self.child_cache.get(fk).map(|doc| doc.deep_clone())
    }

    /// Build the child cache by scanning child_plan once.
    /// For Primary joins: index by child's _docID
    /// For Inverted joins: index by child's FK field value
    async fn build_child_cache(&mut self) -> Result<()> {
        self.child_plan.init().await?;
        self.child_plan.start().await?;

        let child_fk_idx = self.child_side.relation_id_field_index();

        // Debug: Log join configuration
        tracing::debug!(
            direction = ?self.direction,
            child_collection = %self.child_side.collection().name,
            child_fk_idx = ?child_fk_idx,
            child_relation_field = %self.child_side.relation_field().name,
            child_relation_is_primary = self.child_side.relation_field().is_primary,
            "TypeJoinOne: Building child cache"
        );

        while self.child_plan.next().await? {
            let child_doc = self.child_plan.value().deep_clone();

            let key = match &self.direction {
                JoinDirection::Primary { .. } => {
                    // Index by child's _docID for FK → doc lookup
                    child_doc.doc_id().map(String::from)
                }
                JoinDirection::Inverted => {
                    // Index by child's FK field value for reverse lookup
                    let fk_value = child_fk_idx.and_then(|idx| child_doc.get(idx).cloned());
                    fk_value.and_then(|v| v.as_str().map(String::from))
                }
                JoinDirection::InvertedIndex { .. }
                | JoinDirection::OrderedInvertedPrimary { .. } => {
                    // build_child_cache is not called in inverted/ordered modes
                    unreachable!()
                }
            };

            if let Some(k) = key {
                // For one-to-one, we only keep the first match
                self.child_cache.entry(k).or_insert(child_doc);
            } else {
                warn!(
                    child_collection = %self.child_side.collection().name,
                    doc_id = ?child_doc.doc_id(),
                    direction = ?self.direction,
                    "Child document skipped during cache building - no valid lookup key"
                );
            }
        }

        // Capture child plan's execution info before closing
        self.child_exec_info = self.child_plan.exec_info();
        // For JoinDirection::Primary, set initial index_fetches from child scan.
        // For Inverted, the child scan's index fetches go in the child plan's own metrics,
        // NOT in go_child_metrics (which tracks per-parent lookups only).
        if matches!(self.direction, JoinDirection::Primary { .. }) {
            self.go_child_metrics.index_fetches = self.child_exec_info.indexes_fetched;
        }

        self.child_plan.close().await?;
        Ok(())
    }

    /// Execute one step of the inverted index join.
    ///
    /// In this mode, the child plan (index scan on child's filtered field) drives
    /// the outer loop. For each child, we look up the parent by creating an
    /// IndexScanNode on the parent's FK field (FK == child._docID).
    async fn next_inverted_index(&mut self) -> Result<bool> {
        // If we have queued parent docs from a previous child, yield one
        if let Some(doc) = self.docs_to_yield.pop() {
            self.current_doc = doc;
            return Ok(true);
        }

        let fk_index_name = match &self.direction {
            JoinDirection::InvertedIndex {
                parent_fk_index_name,
                ..
            } => parent_fk_index_name.clone(),
            _ => unreachable!(),
        };

        while self.child_plan.next().await? {
            let child_doc = self.child_plan.value().deep_clone();

            // Extract child's _docID to look up parent
            let child_doc_id = match child_doc.doc_id() {
                Some(id) => id.to_string(),
                None => continue,
            };

            // Build index scan params: ExactMatch on FK field = child_docID
            let params = IndexScanParams {
                index_name: fk_index_name.clone(),
                scan_type: IndexScanType::ExactMatch {
                    values: vec![NormalValue::String(child_doc_id)],
                },
                limit: None,
                offset: 0,
                value_filter: None,
            };

            // Create and run an IndexScanNode for the parent lookup
            let parent_collection = self
                .parent_collection
                .as_ref()
                .ok_or_else(|| {
                    QueryError::internal("inverted index join: parent_collection not initialized")
                })?
                .clone();
            let parent_mapping = self
                .parent_scan_mapping
                .as_ref()
                .ok_or_else(|| {
                    QueryError::internal("inverted index join: parent_scan_mapping not initialized")
                })?
                .clone();
            let fetcher = self
                .fetcher
                .as_ref()
                .ok_or_else(|| {
                    QueryError::internal("inverted index join: fetcher not initialized")
                })?
                .clone();

            let mut index_scan =
                IndexScanNode::new(parent_collection, parent_mapping, params).with_fetcher(fetcher);

            index_scan.init().await?;
            index_scan.start().await?;

            let mut parent_docs = Vec::new();
            while index_scan.next().await? {
                parent_docs.push(index_scan.value().deep_clone());
            }

            let scan_info = index_scan.exec_info();
            index_scan.close().await?;

            // Track parent FK index fetches
            self.go_child_metrics.index_fetches += scan_info.indexes_fetched;
            self.go_child_metrics.iterations += scan_info.iterations;
            self.go_child_metrics.doc_fetches += scan_info.docs_fetched;
            self.go_child_metrics.field_fetches += scan_info.fields_fetched;

            if parent_docs.is_empty() {
                continue;
            }

            // Merge child into each parent and queue for yielding
            for mut parent_doc in parent_docs {
                // Apply relation filter if present
                if let Some(ref rel_filter) = self.relation_filter {
                    if !self.check_relation_filter(&Some(child_doc.deep_clone()), rel_filter)? {
                        continue;
                    }
                }

                if let Some(pid) = parent_doc.doc_id() {
                    self.yielded_parent_ids.insert(pid.to_string());
                }
                self.merge_child(&mut parent_doc, Some(child_doc.deep_clone()));
                self.docs_to_yield.push(parent_doc);
            }

            if let Some(doc) = self.docs_to_yield.pop() {
                self.current_doc = doc;
                return Ok(true);
            }
        }

        if self.include_orphans {
            return self.next_orphan().await;
        }
        Ok(false)
    }

    /// Execute one step of the ordered inverted primary join.
    ///
    /// In this mode, the child plan (sorted index scan) drives iteration.
    /// For each child doc, we read its FK field (which contains the parent's _docID),
    /// then fetch the parent directly by docID using the fetcher.
    async fn next_ordered_primary(&mut self) -> Result<bool> {
        let child_fk_index = match &self.direction {
            JoinDirection::OrderedInvertedPrimary { child_fk_index } => *child_fk_index,
            _ => unreachable!(),
        };

        while self.child_plan.next().await? {
            let child_doc = self.child_plan.value().deep_clone();

            // Read the FK value from the child doc (e.g., _ownerID = parent's docID)
            let parent_doc_id = match child_doc.get(child_fk_index) {
                Some(val) => match val.as_str() {
                    Some(s) => s.to_string(),
                    None => continue,
                },
                None => continue,
            };

            // Fetch the parent doc by docID
            let parent_collection = self.parent_collection.as_ref().ok_or_else(|| {
                QueryError::internal("ordered primary join: parent_collection not initialized")
            })?;
            let parent_mapping = self.parent_scan_mapping.as_ref().ok_or_else(|| {
                QueryError::internal("ordered primary join: parent_scan_mapping not initialized")
            })?;
            let fetcher = self.fetcher.as_ref().ok_or_else(|| {
                QueryError::internal("ordered primary join: fetcher not initialized")
            })?;

            let result = fetcher
                .get_by_ids(&parent_collection.name, &[parent_doc_id])
                .await?;

            let parent_docs = documents_to_plan_docs(result.docs(), parent_mapping)?;

            // Track parent lookup metrics (Go counts this as child metrics)
            self.go_child_metrics.iterations += 1;
            if let Some(parent) = parent_docs.first() {
                self.go_child_metrics.doc_fetches += 1;
                self.go_child_metrics.field_fetches += parent.stored_field_count as u64;
            }

            let mut parent_doc = match parent_docs.into_iter().next() {
                Some(doc) => doc,
                None => continue,
            };

            // Apply relation filter if present
            if let Some(ref rel_filter) = self.relation_filter {
                if !self.check_relation_filter(&Some(child_doc.deep_clone()), rel_filter)? {
                    continue;
                }
            }

            if let Some(pid) = parent_doc.doc_id() {
                self.yielded_parent_ids.insert(pid.to_string());
            }
            self.merge_child(&mut parent_doc, Some(child_doc));
            self.current_doc = parent_doc;
            return Ok(true);
        }

        if self.include_orphans {
            return self.next_orphan().await;
        }
        Ok(false)
    }

    /// Yield orphan parents after child-driven scan exhausts.
    async fn next_orphan(&mut self) -> Result<bool> {
        if !self.orphan_phase {
            self.orphan_phase = true;
            self.parent_plan.init().await?;
            self.parent_plan.start().await?;
        }

        while self.parent_plan.next().await? {
            let mut parent_doc = self.parent_plan.value().deep_clone();
            let parent_id = match parent_doc.doc_id() {
                Some(id) => id.to_string(),
                None => continue,
            };

            if self.yielded_parent_ids.contains(&parent_id) {
                continue;
            }

            self.merge_child(&mut parent_doc, None);
            self.current_doc = parent_doc;
            return Ok(true);
        }

        Ok(false)
    }

    /// Merge child document into parent at the relation field index.
    ///
    /// For inverted joins (secondary side), this also populates the parent's
    /// `_relID` field with the child's `_docID`, matching Go DefraDB's behavior.
    fn merge_child(&self, parent_doc: &mut Doc, child_doc: Option<Doc>) {
        // Get child mapping. Falls back to child plan's mapping if not explicitly
        // set in parent mapping - this happens for simple queries where child
        // mapping was not pre-configured during planning.
        let (child_value, child_doc_id) = match &child_doc {
            Some(doc) => {
                let child_mapping = self
                    .document_mapping
                    .child_at(self.parent_side.relation_field_index())
                    .unwrap_or(self.child_plan.document_map());
                let rendered = child_mapping.render_doc_to_json(doc);
                let doc_id = doc.doc_id().map(String::from);
                (rendered, doc_id)
            }
            None => (JsonValue::Null, None),
        };

        // Set the relation object field (e.g., `author: {...}`)
        parent_doc.set(self.parent_side.relation_field_index(), child_value);

        // For inverted joins, also set the parent's _relID field (e.g., `_authorID`)
        // with the child's _docID. This matches Go DefraDB's behavior where the
        // secondary side's relation ID is dynamically populated at query time.
        if matches!(
            self.direction,
            JoinDirection::Inverted
                | JoinDirection::InvertedIndex { .. }
                | JoinDirection::OrderedInvertedPrimary { .. }
        ) {
            let rel_id_field_name = schema::CollectionVersion::relation_id_field_name(
                &self.parent_side.relation_field().name,
            );
            tracing::debug!(
                direction = ?self.direction,
                relation_field_name = %self.parent_side.relation_field().name,
                rel_id_field_name = %rel_id_field_name,
                child_doc_id = ?child_doc_id,
                parent_doc_id = ?parent_doc.doc_id(),
                "TypeJoinOne: merge_child for inverted join"
            );
            if let Some(doc_id) = child_doc_id {
                // Find the _relID field index in the parent's scan mapping
                if let Some(idx) = self
                    .parent_plan
                    .document_map()
                    .first_index_of_name(&rel_id_field_name)
                {
                    tracing::debug!(
                        rel_id_field_name = %rel_id_field_name,
                        idx = idx,
                        doc_id = %doc_id,
                        "TypeJoinOne: Setting _relID field"
                    );
                    parent_doc.set(idx, JsonValue::String(doc_id));
                } else {
                    tracing::warn!(
                        rel_id_field_name = %rel_id_field_name,
                        parent_doc_map_fields = ?self.parent_plan.document_map(),
                        "TypeJoinOne: _relID field index not found in parent's document_map"
                    );
                }
            }
        }
    }

    /// Check if the child document passes the relation filter.
    ///
    /// Returns true if:
    /// - There's no filter (always pass)
    /// - There's a child document and it passes the filter conditions
    ///
    /// Returns false if:
    /// - There's no child document (null relation can't pass any filter)
    /// - The child document doesn't pass the filter conditions
    fn check_relation_filter(
        &self,
        child_doc: &Option<Doc>,
        rel_filter: &RelationFilter,
    ) -> Result<bool> {
        match child_doc {
            None => {
                // No child document - relation filter cannot pass
                // This handles queries like `filter: {author: {verified: true}}`
                // where if there's no author, the filter fails
                Ok(false)
            }
            Some(doc) => {
                // Evaluate the filter conditions against the child's scan mapping
                // The child_plan's document_map is the scan mapping with all fields
                let child_mapping = self.child_plan.document_map();
                rel_filter.conditions.matches(doc.fields(), child_mapping)
            }
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl PlanNode for TypeJoinOne {
    async fn init(&mut self) -> Result<()> {
        // Reset execution stats
        self.exec_info = ExecInfo::default();
        self.child_exec_info = ExecInfo::default();
        self.go_child_metrics.reset();
        self.docs_to_yield.clear();
        self.yielded_parent_ids.clear();
        self.orphan_phase = false;

        if matches!(
            self.direction,
            JoinDirection::InvertedIndex { .. } | JoinDirection::OrderedInvertedPrimary { .. }
        ) {
            // Inverted/ordered mode: only init the child plan (index scan on child).
            // Parent lookups happen per-child in next_inverted_index() / next_ordered_primary().
            self.child_plan.init().await?;
            self.child_plan.start().await?;
            // Capture child's index fetches from init (the initial index scan)
            self.child_exec_info = self.child_plan.exec_info();
        } else {
            // Normal mode: build child cache, then init parent
            self.build_child_cache().await?;
            self.parent_plan.init().await?;
        }
        self.initialized = true;
        Ok(())
    }

    async fn start(&mut self) -> Result<()> {
        if matches!(
            self.direction,
            JoinDirection::InvertedIndex { .. } | JoinDirection::OrderedInvertedPrimary { .. }
        ) {
            // Child plan already started in init()
            Ok(())
        } else {
            self.parent_plan.start().await
        }
    }

    async fn next(&mut self) -> Result<bool> {
        if !self.initialized {
            return Err(QueryError::execution(
                "TypeJoinOne.next() called before init()",
            ));
        }

        // Track iterations (Go counts each call to next, including final false)
        self.exec_info.iterations += 1;

        if matches!(self.direction, JoinDirection::InvertedIndex { .. }) {
            return self.next_inverted_index().await;
        }
        if matches!(self.direction, JoinDirection::OrderedInvertedPrimary { .. }) {
            return self.next_ordered_primary().await;
        }

        loop {
            if !self.parent_plan.next().await? {
                return Ok(false);
            }

            let mut parent_doc = self.parent_plan.value().deep_clone();

            // Extract FK and lookup child in cache (O(1) lookup)
            let fk = self.extract_fk(&parent_doc);
            let child_doc = fk.and_then(|fk| self.find_child_doc(&fk));

            // Simulate Go's per-parent child scan metrics.
            // The behavior differs by join direction:
            //
            // Primary: fetchDocWithIDAndItsSubDocs calls Init() + Next() once per parent
            // with a docID-specific prefix. Next() is called exactly once (no false call).
            //
            // Inverted: fetchPrimaryDocsReferencingSecondaryDoc sets a filter + unique index
            // on the child scanNode, then calls collectDocs(0). This calls Next() twice
            // per parent (true + false), and uses the index (1 indexFetch per match).
            if child_doc.is_some() {
                match &self.direction {
                    JoinDirection::Primary { .. } => {
                        // 1 iteration (Next()=true only), no index
                        self.go_child_metrics.iterations += 1;
                    }
                    JoinDirection::Inverted => {
                        // 2 iterations (Next()=true, Next()=false), 1 indexFetch
                        self.go_child_metrics.iterations += 2;
                        self.go_child_metrics.index_fetches += 1;
                    }
                    // InvertedIndex and OrderedInvertedPrimary early-return
                    // via next_inverted_index() / next_ordered_primary() above
                    JoinDirection::InvertedIndex { .. }
                    | JoinDirection::OrderedInvertedPrimary { .. } => unreachable!(),
                }
                self.go_child_metrics.doc_fetches += 1;
                if let Some(ref doc) = child_doc {
                    self.go_child_metrics.field_fetches += doc.stored_field_count as u64;
                }
            }
            // When FK is null/empty, Go doesn't call fetchDocWithIDAndItsSubDocs at all,
            // so no child metrics are accumulated for that parent.

            // Apply relation filter if present
            if let Some(ref rel_filter) = self.relation_filter {
                if !self.check_relation_filter(&child_doc, rel_filter)? {
                    // Filter didn't pass - skip this parent
                    continue;
                }
            }

            // Merge child into parent
            self.merge_child(&mut parent_doc, child_doc);
            self.current_doc = parent_doc;

            return Ok(true);
        }
    }

    fn value(&self) -> &Doc {
        &self.current_doc
    }

    async fn close(&mut self) -> Result<()> {
        if matches!(
            self.direction,
            JoinDirection::InvertedIndex { .. } | JoinDirection::OrderedInvertedPrimary { .. }
        ) {
            self.child_plan.close().await?;
            if self.orphan_phase {
                self.parent_plan.close().await?;
            }
        } else {
            self.parent_plan.close().await?;
        }
        self.child_cache.clear();
        self.docs_to_yield.clear();
        self.yielded_parent_ids.clear();
        self.orphan_phase = false;
        self.initialized = false;
        Ok(())
    }

    fn source(&self) -> Option<&dyn PlanNode> {
        Some(self.parent_plan.as_ref())
    }

    fn document_map(&self) -> &DocumentMapping {
        &self.document_mapping
    }

    fn kind(&self) -> &'static str {
        // Go's explain uses "typeIndexJoin" as the wrapper node
        "typeIndexJoin"
    }

    fn explain_inner(&self) -> JsonValue {
        // Simple/Default mode: typeIndexJoin contains both attributes and tree structure
        let mut obj = serde_json::Map::new();

        // direction: primary or secondary (Go uses "secondary" not "inverted")
        let direction = match self.direction {
            JoinDirection::Primary { .. } => "primary",
            JoinDirection::Inverted
            | JoinDirection::InvertedIndex { .. }
            | JoinDirection::OrderedInvertedPrimary { .. } => "secondary",
        };
        obj.insert("direction".to_string(), serde_json::json!(direction));

        // joinType: "typeJoinOne" for one-to-one joins
        obj.insert("joinType".to_string(), serde_json::json!("typeJoinOne"));

        // rootName: the child side's relation field name (points back to parent)
        // Go uses immutable.Option[string], but areResultOptionsEqual compares the inner value
        let root_name = self.child_side.relation_field().name.clone();
        obj.insert("rootName".to_string(), serde_json::json!(root_name));

        // subTypeName: the child side's relation field name (from parent perspective)
        obj.insert(
            "subTypeName".to_string(),
            serde_json::json!(self.parent_side.relation_field().name),
        );

        // root: the parent plan's explain (contains scanNode)
        let root_explain = self.parent_plan.explain();
        obj.insert("root".to_string(), root_explain);

        // subType: the child plan's explain wrapped in selectTopNode > selectNode
        // selectNode must include docID and filter attributes (Go always includes these)
        let child_explain = self.child_plan.explain();
        let child_is_select = self.child_plan.kind() == "selectNode";

        let select_node_content = if child_is_select {
            // Child is SelectNode - extract inner content to avoid double wrapping
            child_explain
                .as_object()
                .and_then(|o| o.get("selectNode"))
                .cloned()
                .unwrap_or(child_explain.clone())
        } else {
            let mut select_node_inner = serde_json::Map::new();
            select_node_inner.insert("docID".to_string(), serde_json::Value::Null);
            select_node_inner.insert("filter".to_string(), serde_json::Value::Null);
            // Merge child explain (e.g., scanNode) into selectNode
            if let Some(child_obj) = child_explain.as_object() {
                for (key, value) in child_obj {
                    select_node_inner.insert(key.clone(), value.clone());
                }
            }
            serde_json::Value::Object(select_node_inner)
        };
        let sub_type = serde_json::json!({
            "selectTopNode": {
                "selectNode": select_node_content
            }
        });
        obj.insert("subType".to_string(), sub_type);

        serde_json::Value::Object(obj)
    }

    fn explain_debug_inner(&self) -> JsonValue {
        // Debug mode: typeIndexJoin contains typeJoinOne wrapper with full tree structure
        let mut inner_obj = serde_json::Map::new();

        // root: the parent plan's explain_debug (contains scanNode)
        let root_explain = self.parent_plan.explain_debug();
        inner_obj.insert("root".to_string(), root_explain);

        // subType: the child plan's explain_debug wrapped in selectTopNode > selectNode
        let child_explain = self.child_plan.explain_debug();
        let child_is_select = self.child_plan.kind() == "selectNode";

        let select_node_content = if child_is_select {
            child_explain
                .as_object()
                .and_then(|o| o.get("selectNode"))
                .cloned()
                .unwrap_or(child_explain.clone())
        } else {
            let mut select_node_inner = serde_json::Map::new();
            // Merge child explain into selectNode
            if let Some(child_obj) = child_explain.as_object() {
                for (key, value) in child_obj {
                    select_node_inner.insert(key.clone(), value.clone());
                }
            }
            serde_json::Value::Object(select_node_inner)
        };
        let sub_type = serde_json::json!({
            "selectTopNode": {
                "selectNode": select_node_content
            }
        });
        inner_obj.insert("subType".to_string(), sub_type);

        // Wrap in typeJoinOne
        let mut obj = serde_json::Map::new();
        obj.insert(
            "typeJoinOne".to_string(),
            serde_json::Value::Object(inner_obj),
        );

        serde_json::Value::Object(obj)
    }

    fn exec_info(&self) -> ExecInfo {
        self.exec_info.clone()
    }

    fn explain_execute_inner(&self) -> JsonValue {
        let mut obj = serde_json::Map::new();

        obj.insert(
            "iterations".to_string(),
            serde_json::json!(self.exec_info.iterations),
        );

        let mut inner_obj = serde_json::Map::new();

        if matches!(
            self.direction,
            JoinDirection::InvertedIndex { .. } | JoinDirection::OrderedInvertedPrimary { .. }
        ) {
            // Inverted/ordered modes: child drives the loop, parent is looked up per-child.
            // root = child plan's execute explain (the driving scan)
            inner_obj.insert("root".to_string(), self.child_plan.explain_execute());

            // subType = parent lookup metrics as a synthetic scanNode
            let sub_type = serde_json::json!({
                "selectTopNode": {
                    "selectNode": {
                        "scanNode": self.go_child_metrics.to_json()
                    }
                }
            });
            inner_obj.insert("subType".to_string(), sub_type);
        } else {
            // Normal (Primary/Inverted) mode:
            // root = parent plan's execute explain
            inner_obj.insert("root".to_string(), self.parent_plan.explain_execute());

            // subType = child plan's execute explain wrapped in selectTopNode > selectNode
            let child_execute = self.child_plan.explain_execute();
            let child_is_select = self.child_plan.kind() == "selectNode";

            let select_node_content = if child_is_select {
                // Extract inner content to avoid double wrapping (selectNode > selectNode)
                child_execute
                    .as_object()
                    .and_then(|o| o.get("selectNode"))
                    .cloned()
                    .unwrap_or(child_execute)
            } else {
                // Child is not a SelectNode (e.g., ScanNode or nested TypeJoinOne).
                // Synthesize selectNode metrics from captured child exec info,
                // matching Go's selectNode wrapper which tracks its own iterations.
                let mut select_inner = serde_json::Map::new();
                select_inner.insert(
                    "iterations".to_string(),
                    serde_json::json!(self.child_exec_info.iterations),
                );
                select_inner.insert(
                    "filterMatches".to_string(),
                    serde_json::json!(self.child_exec_info.docs_fetched),
                );
                if let Some(child_obj) = child_execute.as_object() {
                    for (key, value) in child_obj {
                        select_inner.insert(key.clone(), value.clone());
                    }
                }
                serde_json::Value::Object(select_inner)
            };

            let sub_type = serde_json::json!({
                "selectTopNode": {
                    "selectNode": select_node_content
                }
            });
            inner_obj.insert("subType".to_string(), sub_type);
        }

        obj.insert(
            "typeJoinOne".to_string(),
            serde_json::Value::Object(inner_obj),
        );

        serde_json::Value::Object(obj)
    }
}
