//! Query planner implementation
//!
//! Converts Select operations into executable plan trees.

use std::collections::HashMap;
use std::sync::Arc;

use schema::CollectionVersion;
use tracing::{debug, warn};

use crate::document::DocumentMapping;
use crate::error::{QueryError, Result};
use crate::fetcher::DocFetcher;
use crate::mapper::{Filter, Requestable, Select};
use serde_json::Value as JsonValue;
use crate::plan::{
    IndexScanNode, JoinSide, LimitNode, OrderByNode, RelationFilter, ScanNode, SelectNode,
    TypeJoinMany, TypeJoinOne,
};
use crate::planner::index_selection::{filter_to_index_scan, select_best_index, IndexScanParams};
use crate::planner::PlanNode;

/// Maximum allowed nesting depth for nested queries (0-indexed).
/// A depth of 0 is the root query, depth 1 is the first nested level, etc.
/// With MAX_NESTING_DEPTH = 10, queries can nest up to 11 levels deep (depths 0-10).
/// Prevents stack overflow from deeply nested or circular query structures.
const MAX_NESTING_DEPTH: usize = 10;

/// Result of planning a query, containing both the plan and optional index scan info.
pub struct PlanResult {
    /// The execution plan
    pub plan: Box<dyn PlanNode>,
    /// Index scan parameters if an index will be used
    pub index_scan: Option<IndexScanParams>,
}

impl PlanResult {
    /// Check if this plan uses an index scan
    pub fn uses_index(&self) -> bool {
        self.index_scan.is_some()
    }
}

/// Query planner that builds execution plans from Select operations.
///
/// The planner can optionally be configured with a `DocFetcher` to enable
/// ScanNodes to load their own data during execution. Without a fetcher,
/// ScanNodes must have their data pre-loaded via `with_docs()`.
pub struct Planner {
    /// Available collection schemas by name
    collections: HashMap<String, Arc<CollectionVersion>>,
    /// Available collection schemas by CollectionID (CID)
    /// This is needed because FieldKind::Relation stores the CollectionID, not the name
    collections_by_id: HashMap<String, Arc<CollectionVersion>>,
    /// Optional fetcher for ScanNodes to load data on-demand
    fetcher: Option<Arc<dyn DocFetcher>>,
}

impl Planner {
    /// Create a new planner with the given collection schemas.
    pub fn new(collections: Vec<CollectionVersion>) -> Self {
        let collections: HashMap<String, Arc<CollectionVersion>> = collections
            .into_iter()
            .map(|c| (c.name.clone(), Arc::new(c)))
            .collect();
        // Build a second map by CollectionID for relation field resolution
        let collections_by_id: HashMap<String, Arc<CollectionVersion>> = collections
            .values()
            .map(|c| (c.collection_id.clone(), c.clone()))
            .collect();
        Self {
            collections,
            collections_by_id,
            fetcher: None,
        }
    }

    /// Set a document fetcher for on-demand data loading.
    ///
    /// When set, ScanNodes created by this planner will use the fetcher
    /// to load documents during initialization if no docs are pre-loaded.
    pub fn with_fetcher(mut self, fetcher: Arc<dyn DocFetcher>) -> Self {
        self.fetcher = Some(fetcher);
        self
    }

    /// Get a collection by name or CollectionID.
    ///
    /// Relation fields store the CollectionID (CID) in their Kind, but we need
    /// the collection to resolve the relation. This method tries both lookups:
    /// 1. First by name (for Named kind fields and root queries)
    /// 2. Then by CollectionID (for Relation kind fields)
    fn get_collection(&self, name_or_id: &str) -> Option<Arc<CollectionVersion>> {
        self.collections
            .get(name_or_id)
            .or_else(|| self.collections_by_id.get(name_or_id))
            .cloned()
    }

    /// Build an execution plan from a Select operation.
    ///
    /// This method returns only the plan for backwards compatibility.
    /// Use `plan_with_index_info` to also get index scan information.
    pub fn plan(&self, select: &Select) -> Result<Box<dyn PlanNode>> {
        Ok(self.plan_with_index_info(select)?.plan)
    }

    /// Build an execution plan with index scan information.
    ///
    /// Returns a `PlanResult` containing both the plan and optional `IndexScanParams`
    /// when an index can be used to optimize the query.
    pub fn plan_with_index_info(&self, select: &Select) -> Result<PlanResult> {
        let collection = self
            .collections
            .get(&select.collection_name)
            .ok_or_else(|| QueryError::collection_not_found(&select.collection_name))?
            .clone();

        // Build the document mapping for this query (controls which fields appear in output)
        let render_mapping = self.build_mapping(select, &collection)?;

        // Check if this query has nested selections that require joins
        let has_nested = select
            .fields
            .iter()
            .any(|f| matches!(f, Requestable::Select(_)));

        // Check if filter references relation fields (needs joins even if not selected)
        let filter_relation_fields: Vec<String> = select
            .filter
            .as_ref()
            .map(|f| f.relation_field_names())
            .unwrap_or_default();
        let filter_has_relations = !filter_relation_fields.is_empty();

        // Check if order references relation fields (needs joins even if not selected)
        let order_relation_fields: Vec<String> = select
            .order_by
            .as_ref()
            .map(|o| o.relation_field_names())
            .unwrap_or_default();
        let order_has_relations = !order_relation_fields.is_empty();

        // Build scan mapping: for queries with nested selections, relation filters, or relation ordering,
        // use full schema mapping so that FK fields are available for TypeJoin lookups.
        // For simple queries, use the render_mapping directly.
        let needs_joins = has_nested || filter_has_relations || order_has_relations;
        let mut scan_mapping = if needs_joins {
            self.build_scan_mapping_for_join(&collection, &render_mapping)
        } else {
            render_mapping.clone()
        };

        // Add ORDER BY fields to scan mapping if not already present (Go compatibility).
        // Go DefraDB allows ordering by fields not in the SELECT clause.
        if let Some(ref order_by) = select.order_by {
            for condition in &order_by.conditions {
                if let Some(field_name) = condition.fields.first() {
                    // Skip if already in mapping
                    if scan_mapping.first_index_of_name(field_name).is_some() {
                        continue;
                    }
                    // Find field in collection schema and add to mapping
                    if let Some((schema_idx, _)) = collection
                        .fields
                        .iter()
                        .enumerate()
                        .find(|(_, f)| &f.name == field_name)
                    {
                        scan_mapping.add(schema_idx, field_name);
                    }
                }
            }
        }

        // Check if an index can be used for the filter.
        // Note: Index selection is disabled when a fetcher is attached because:
        // 1. IndexScanNode expects pre-loaded documents from index lookups
        // 2. The DocFetcher trait doesn't support index-aware fetching
        // 3. The Runner handles index lookups for simple queries; the Planner path
        //    (with fetcher) is used for nested selections where ScanNode suffices
        let index_scan = if self.fetcher.is_some() {
            if select.filter.is_some() && !collection.indexes.is_empty() {
                debug!(
                    collection = %select.collection_name,
                    available_indexes = collection.indexes.len(),
                    "Index selection disabled for nested query path - using full scan"
                );
            }
            None // Skip index selection when using fetcher-based data loading
        } else {
            self.try_select_index(select, &collection)
        };

        // Build the plan tree bottom-up:
        // ScanNode/IndexScanNode -> SelectNode (scalar filter) -> JoinNodes -> SelectNode (complex filter) -> LimitNode
        //
        // Filter handling depends on complexity:
        // - Simple filters: Split into scalar (before join) and relation (inside TypeJoin)
        // - Complex filters (_and/_or with mixed scalar+relation): Apply whole filter after join
        // - Multi-level relation filters: Apply after all joins (like complex filters)

        // Check if filter is complex (has relation conditions inside logical operators)
        // or has multi-level relation paths (e.g., {author: {published: {rating: ...}}})
        let is_complex_filter = select
            .filter
            .as_ref()
            .map(|f| f.is_complex() || !f.get_multi_level_relation_paths().is_empty())
            .unwrap_or(false);

        // Split filter into scalar and relation parts (only useful for non-complex filters)
        // Note: JSON field nested access looks like relation filters structurally, but should
        // be treated as scalar filters. We recombine them below based on schema info.
        let (scalar_filter_raw, relation_filter) = select
            .filter
            .as_ref()
            .map(|f| f.split_by_relation())
            .unwrap_or((None, None));

        // Move JSON field conditions from relation_filter back to scalar_filter.
        // The split_by_relation function can't distinguish JSON nested access from relation
        // traversal without schema info. Here we have the collection, so we can fix it.
        let scalar_filter = {
            let mut combined_conditions: HashMap<String, JsonValue> = scalar_filter_raw
                .as_ref()
                .map(|f| f.conditions().clone())
                .unwrap_or_default();

            if let Some(ref rel_filter) = relation_filter {
                for (field_name, condition) in rel_filter.conditions() {
                    // Check if this field is NOT a relation (could be JSON, etc.)
                    if !collection
                        .field_by_name(field_name)
                        .is_some_and(|f| f.kind.is_relation())
                    {
                        combined_conditions.insert(field_name.clone(), condition.clone());
                    }
                }
            }

            if combined_conditions.is_empty() {
                None
            } else {
                Some(Filter::from_conditions(combined_conditions))
            }
        };

        // 1. Choose between IndexScanNode and ScanNode based on index availability
        let mut plan: Box<dyn PlanNode> = if let Some(ref params) = index_scan {
            Box::new(
                IndexScanNode::new((*collection).clone(), scan_mapping.clone(), params.clone())
                    .with_show_deleted(select.show_deleted),
            )
        } else {
            let mut scan = ScanNode::new((*collection).clone(), scan_mapping.clone())
                .with_show_deleted(select.show_deleted);
            // Attach fetcher if available for on-demand data loading
            if let Some(ref fetcher) = self.fetcher {
                scan = scan.with_fetcher(fetcher.clone());
            }
            Box::new(scan)
        };

        // 2. Apply scalar filter before join (if present and not complex)
        // For complex filters, we apply the whole filter after join instead.
        // Note: Even with IndexScanNode, we may need a SelectNode for:
        //   - Field projection
        //   - Conditions not covered by the index
        if !is_complex_filter && (scalar_filter.is_some() || !select.fields.is_empty()) {
            let mut select_node = SelectNode::new(plan, scan_mapping.clone());
            if let Some(filter) = scalar_filter {
                select_node = select_node.with_filter(filter);
            }
            plan = Box::new(select_node);
        } else if is_complex_filter && !select.fields.is_empty() {
            // For complex filters, still need SelectNode for field projection (no filter yet)
            plan = Box::new(SelectNode::new(plan, scan_mapping.clone()));
        }

        // 3. Apply join nodes for relation fields in the selection set
        // For simple filters: relation filters are extracted and applied inside TypeJoin nodes
        // For complex filters: pass None, the full filter is applied after join
        let filter_for_joins = if is_complex_filter {
            None // Don't pass filter to TypeJoin for complex filters
        } else {
            select.filter.as_ref()
        };
        plan = self.apply_joins(
            plan,
            select,
            &collection,
            scan_mapping.clone(),
            0,
            filter_for_joins,
        )?;

        // 3b. Apply joins for multi-level relation filter paths where the first relation
        // is NOT in the selection set. If the first relation IS selected, then apply_joins
        // already handles it via apply_multi_level_sub_joins.
        // Example: Book(filter: {author: {published: {rating: {_eq: 4.9}}}}) { name }
        // (no author selection, so we need to add the full join chain here)
        if let Some(ref filter) = select.filter {
            // Get relation names from selection
            let selected_relation_names: Vec<&str> = select
                .fields
                .iter()
                .filter_map(|f| {
                    if let Requestable::Select(s) = f {
                        Some(s.field.name.as_str())
                    } else {
                        None
                    }
                })
                .collect();

            let multi_level_paths = filter.get_multi_level_relation_paths();
            for path in multi_level_paths {
                // Only handle paths where the first relation is NOT in the selection set
                if let Some(first_relation) = path.first() {
                    if selected_relation_names.contains(&first_relation.as_str()) {
                        // This path is handled by apply_joins via apply_multi_level_sub_joins
                        continue;
                    }
                    plan = self.apply_multi_level_filter_joins(
                        plan,
                        &path,
                        &collection,
                        filter,
                        scan_mapping.clone(),
                    )?;
                }
            }
        }

        // 3c. Apply joins for single-level relation fields referenced in filter but NOT in selection set.
        // This allows complex filters to evaluate conditions on relations even when those
        // relations aren't being returned in the output.
        // Example: Book(filter: {author: {verified: true}}) { name rating }
        // The `author` relation must be joined for the filter even though it's not selected.
        //
        // Skip relations that are part of multi-level paths (already handled above).
        if filter_has_relations {
            // Get the names of relation fields already joined from selection
            let selected_relation_names: Vec<&str> = select
                .fields
                .iter()
                .filter_map(|f| {
                    if let Requestable::Select(s) = f {
                        Some(s.field.name.as_str())
                    } else {
                        None
                    }
                })
                .collect();

            // Get relation names that are already handled as part of multi-level paths
            let multi_level_first_relations: Vec<String> = select
                .filter
                .as_ref()
                .map(|f| {
                    f.get_multi_level_relation_paths()
                        .into_iter()
                        .filter_map(|path| path.into_iter().next())
                        .collect()
                })
                .unwrap_or_default();

            // Create joins for filter relations not in selection and not in multi-level paths
            for relation_field_name in &filter_relation_fields {
                if selected_relation_names.contains(&relation_field_name.as_str()) {
                    continue; // Already joined from selection
                }
                if multi_level_first_relations.contains(relation_field_name) {
                    continue; // Already joined as part of multi-level path
                }

                // Find the relation field in the parent collection
                let relation_field = match collection.field_by_name(relation_field_name) {
                    Some(f) if f.kind.is_relation() => f,
                    _ => continue, // Not a valid relation field
                };

                plan = self.apply_filter_relation_join(
                    plan,
                    &collection,
                    relation_field,
                    relation_field_name,
                    scan_mapping.clone(),
                )?;
            }
        }

        // 3c. Apply joins for relation fields referenced in order but NOT in selection set or filter.
        // This allows ordering through relations even when those relations aren't being returned.
        // Example: Book(order: {author: {age: DESC}}) { name rating }
        // The `author` relation must be joined for ordering even though it's not selected.
        if order_has_relations {
            // Get the names of relation fields already joined from selection or filter
            let mut already_joined: Vec<&str> = select
                .fields
                .iter()
                .filter_map(|f| {
                    if let Requestable::Select(s) = f {
                        Some(s.field.name.as_str())
                    } else {
                        None
                    }
                })
                .collect();
            // Add filter relations that were joined
            for f in &filter_relation_fields {
                if !already_joined.contains(&f.as_str()) {
                    already_joined.push(f.as_str());
                }
            }

            // Create joins for order relations not already joined
            for relation_field_name in &order_relation_fields {
                if already_joined.contains(&relation_field_name.as_str()) {
                    continue; // Already joined
                }

                // Find the relation field in the parent collection
                let relation_field = match collection.field_by_name(relation_field_name) {
                    Some(f) if f.kind.is_relation() => f,
                    _ => continue, // Not a valid relation field
                };

                plan = self.apply_filter_relation_join(
                    plan,
                    &collection,
                    relation_field,
                    relation_field_name,
                    scan_mapping.clone(),
                )?;
            }
        }

        // 4. Apply complex filter after join (when merged document is available)
        // Complex filters contain _and/_or with mixed scalar and relation conditions
        // that must be evaluated together on the merged document.
        if is_complex_filter {
            if let Some(ref filter) = select.filter {
                plan = Box::new(
                    SelectNode::new(plan, scan_mapping.clone()).with_filter(filter.clone()),
                );
            }
        }

        // 5. Apply order by after joins (so nested fields are available)
        if let Some(ref order_by) = select.order_by {
            plan = Box::new(OrderByNode::new(
                plan,
                order_by.clone(),
                scan_mapping.clone(),
            ));
        }

        // 6. Apply limit/offset if present
        // Note: limit=0 means "no limit" in Go DefraDB, so we convert Some(0) to None
        if let Some(ref limit) = select.limit {
            let effective_limit = match limit.limit {
                Some(0) => None, // limit: 0 means no limit (Go compatibility)
                other => other,
            };
            // Only create LimitNode if there's actually a limit or offset to apply
            if effective_limit.is_some() || limit.offset > 0 {
                plan = Box::new(LimitNode::new(plan, effective_limit, limit.offset));
            }
        }

        Ok(PlanResult { plan, index_scan })
    }

    /// Try to select an index for the given query.
    ///
    /// Returns `Some(IndexScanParams)` if an index can be used, `None` otherwise.
    fn try_select_index(
        &self,
        select: &Select,
        collection: &CollectionVersion,
    ) -> Option<IndexScanParams> {
        // Only try index selection if there's a filter
        let filter = select.filter.as_ref()?;

        // Get available indexes for this collection
        if collection.indexes.is_empty() {
            return None;
        }

        // Select the best index for this filter
        let best_index = select_best_index(filter, &collection.indexes)?;

        // Convert filter to index scan parameters
        filter_to_index_scan(filter, best_index)
    }

    /// Apply join nodes for nested selects (relation fields)
    ///
    /// The `depth` parameter tracks recursion depth to prevent stack overflow
    /// from deeply nested or circular query structures.
    ///
    /// If `parent_filter` is provided, relation filters are extracted and passed
    /// to the TypeJoin nodes to filter parents based on their children.
    fn apply_joins(
        &self,
        mut plan: Box<dyn PlanNode>,
        select: &Select,
        parent_collection: &CollectionVersion,
        mut mapping: DocumentMapping,
        depth: usize,
        parent_filter: Option<&crate::mapper::Filter>,
    ) -> Result<Box<dyn PlanNode>> {
        // Check recursion depth to prevent stack overflow
        if depth > MAX_NESTING_DEPTH {
            return Err(QueryError::execution(format!(
                "Query nesting depth {} exceeds maximum allowed depth of {}. \
                 Consider simplifying the query or using separate queries for deeply nested data.",
                depth, MAX_NESTING_DEPTH
            )));
        }

        for requestable in &select.fields {
            if let Requestable::Select(nested_select) = requestable {
                let relation_field_name = &nested_select.field.name;

                // Find the relation field in the parent collection
                let relation_field = parent_collection
                    .field_by_name(relation_field_name)
                    .ok_or_else(|| QueryError::unknown_field(relation_field_name))?;

                // Verify it's a relation field
                if !relation_field.kind.is_relation() {
                    return Err(QueryError::execution(format!(
                        "field '{}' on collection '{}' is not a relation (type: {}). \
                         Only relation fields can have nested selections.",
                        relation_field_name,
                        parent_collection.name,
                        relation_field.kind.graphql_type_name()
                    )));
                }

                // Get the target collection
                // The relation_collection_id() returns either:
                // - CollectionID (CID) for FieldKind::Relation
                // - RelativeID for FieldKind::SelfRef (empty string for same-type self-refs)
                // - Collection name for FieldKind::Named
                let target_collection_id = relation_field
                    .kind
                    .relation_collection_id()
                    .ok_or_else(|| {
                        QueryError::internal(format!(
                            "relation field '{}' has no target collection",
                            relation_field_name
                        ))
                    })?;

                // For self-referential relations (empty relative_id), use the parent collection
                let target_collection = if target_collection_id.is_empty() {
                    // Self-reference: target is the same collection as parent
                    Arc::new(parent_collection.clone())
                } else {
                    self.get_collection(target_collection_id)
                        .ok_or_else(|| QueryError::collection_not_found(target_collection_id))?
                };

                // Build child mapping for rendering (only selected fields)
                let child_render_mapping = self.build_mapping(nested_select, &target_collection)?;

                // Build scan mapping that includes ALL fields at schema indices.
                // This is required because JoinSide derives FK field indices from the schema,
                // so the doc fields must be at their schema positions for FK lookups to work.
                let mut child_scan_mapping =
                    self.build_scan_mapping_for_join(&target_collection, &child_render_mapping);

                // Check if parent's order_by references fields in this relation.
                // If so, add those fields to child_scan_mapping.render_keys so they're
                // available in the merged JSON for ordering. The fields won't appear in
                // the final output unless they're also in the selection set.
                if let Some(ref order_by) = select.order_by {
                    for condition in &order_by.conditions {
                        // Check if this order condition starts with this relation field
                        if condition.fields.len() > 1 && condition.fields[0] == *relation_field_name
                        {
                            // Get the nested field name (e.g., "verified" from ["author", "verified"])
                            let nested_field = &condition.fields[1];
                            // Find the schema index for this field
                            if let Some(idx) = child_scan_mapping.first_index_of_name(nested_field)
                            {
                                // Add render_key if not already present
                                if !child_scan_mapping
                                    .render_keys
                                    .iter()
                                    .any(|rk| rk.key == *nested_field)
                                {
                                    child_scan_mapping.add_render_key(idx, nested_field);
                                }
                            }
                        }
                    }
                }

                // Check if parent's filter has multi-level paths starting with this relation.
                // If so, we need to add nested relation fields to the child mapping and build
                // sub-joins for them so the filter can be evaluated on the merged document.
                let multi_level_paths_for_relation: Vec<Vec<String>> = select
                    .filter
                    .as_ref()
                    .map(|f| {
                        f.get_multi_level_relation_paths()
                            .into_iter()
                            .filter(|path| {
                                path.first()
                                    .map_or(false, |first| first == relation_field_name)
                            })
                            .map(|path| path[1..].to_vec()) // Get remaining path after this relation
                            .filter(|remaining| !remaining.is_empty())
                            .collect()
                    })
                    .unwrap_or_default();

                // Add render_keys for nested relation fields needed by multi-level filters
                for remaining_path in &multi_level_paths_for_relation {
                    if let Some(first_nested) = remaining_path.first() {
                        if let Some(idx) = child_scan_mapping.first_index_of_name(first_nested) {
                            if !child_scan_mapping
                                .render_keys
                                .iter()
                                .any(|rk| rk.key == *first_nested)
                            {
                                child_scan_mapping.add_render_key(idx, first_nested);
                            }
                        }
                    }
                }

                // Get the relation field index in the parent mapping.
                // Use output_name (alias if set) to find the correct index.
                // This ensures aliased fields like "p1: published" and "p2: published"
                // get distinct indices and separate child mappings/filters.
                let output_name = nested_select.field.output_name();
                let relation_field_index = mapping
                    .try_find_index_from_render_key(output_name)
                    .ok_or_else(|| {
                        QueryError::internal(format!(
                            "relation field '{}' (output name '{}') not in mapping",
                            relation_field_name, output_name
                        ))
                    })?;

                // Set up child scan mapping in parent (for TypeJoin to render children).
                // We use child_scan_mapping (not child_render_mapping) because child docs
                // have fields at schema indices, and render_keys need to match those indices.
                mapping.set_child_at(relation_field_index, child_scan_mapping.clone());

                // Create the child scan plan with scan_mapping (includes FK fields for joins)
                let mut child_scan =
                    ScanNode::new((*target_collection).clone(), child_scan_mapping.clone());
                if let Some(ref fetcher) = self.fetcher {
                    child_scan = child_scan.with_fetcher(fetcher.clone());
                }

                // Reject unsupported arguments on nested selections.
                // Order and limit on nested selections require per-parent ordering/limiting
                // which is not yet implemented. Return a clear error rather than silently ignoring.
                if nested_select.order_by.is_some() {
                    return Err(QueryError::execution(format!(
                        "order on nested selection '{}' is not yet supported. \
                         Use separate queries to order nested results.",
                        relation_field_name
                    )));
                }
                if nested_select.limit.is_some() {
                    return Err(QueryError::execution(format!(
                        "limit/offset on nested selection '{}' is not yet supported. \
                         Use separate queries to limit nested results.",
                        relation_field_name
                    )));
                }

                // Wrap in SelectNode if there's a filter on the nested select
                let mut child_plan: Box<dyn PlanNode> =
                    if let Some(ref filter) = nested_select.filter {
                        // Validate that all filter-referenced fields exist in the render mapping.
                        // We validate against render_mapping (user-selected fields), not scan_mapping,
                        // because filtering on unselected fields is likely a user error.
                        for field in filter.referenced_fields() {
                            if !child_render_mapping.has_field(&field) {
                                return Err(QueryError::filter_field_not_selected(
                                    &field,
                                    &target_collection.name,
                                ));
                            }
                        }

                        Box::new(
                            SelectNode::new(Box::new(child_scan), child_scan_mapping.clone())
                                .with_filter(filter.clone()),
                        )
                    } else {
                        Box::new(child_scan)
                    };

                // Recursively apply joins for any nested selections within this nested select.
                // This handles multi-level nesting like Users -> Posts -> Comments.
                // Note: We pass None for parent_filter since relation filters only apply at the top level.
                child_plan = self.apply_joins(
                    child_plan,
                    nested_select,
                    &target_collection,
                    child_scan_mapping.clone(),
                    depth + 1,
                    None, // Nested relation filters handled differently
                )?;

                // Apply sub-joins for multi-level filter paths within this relation.
                // For example, if we're joining Book → Author and the filter has path
                // ["author", "published"], we need to add a sub-join for "published" here.
                for remaining_path in &multi_level_paths_for_relation {
                    let (new_child_plan, new_child_mapping) = self.apply_multi_level_sub_joins(
                        child_plan,
                        remaining_path,
                        &target_collection,
                        child_scan_mapping.clone(),
                    )?;
                    child_plan = new_child_plan;
                    child_scan_mapping = new_child_mapping;
                }

                // Update parent mapping with the final child mapping (after sub-joins)
                // This ensures the nested relation mappings are included
                mapping.set_child_at(relation_field_index, child_scan_mapping.clone());

                // Find the other side of the relation
                let target_relation_field = if let Some(rel_name) = &relation_field.relation_name {
                    target_collection.field_by_relation(
                        rel_name,
                        &parent_collection.name,
                        relation_field_name,
                    )
                } else {
                    None
                };

                // Debug: Log relation field resolution
                debug!(
                    parent_collection = %parent_collection.name,
                    target_collection = %target_collection.name,
                    relation_field_name = %relation_field_name,
                    relation_name = ?relation_field.relation_name,
                    parent_is_primary = relation_field.is_primary,
                    target_relation_field_found = target_relation_field.is_some(),
                    target_field_name = ?target_relation_field.map(|f| &f.name),
                    target_is_primary = ?target_relation_field.map(|f| f.is_primary),
                    "Resolving relation for join"
                );

                // Get child relation field index (if it exists).
                // For bidirectional relations, this is the index of the back-reference field
                // (e.g., `author` field on posts when joining from users.posts).
                // For unidirectional relations (no back-reference), we default to index 0.
                // This is safe because TypeJoin nodes use the relation_id_field_index()
                // (derived from the FK field) for actual join matching, not this index.
                let child_relation_index = target_relation_field
                    .and_then(|f| {
                        target_collection
                            .fields
                            .iter()
                            .position(|tf| tf.name == f.name)
                    })
                    .unwrap_or_else(|| {
                        warn!(
                            parent_collection = %parent_collection.name,
                            target_collection = %target_collection.name,
                            relation_field = %relation_field_name,
                            "No back-reference field found for relation, using default index 0. \
                             This may indicate a unidirectional relation or schema misconfiguration."
                        );
                        0
                    });

                // Create join sides
                let parent_side = JoinSide::new(
                    parent_collection.clone(),
                    relation_field.clone(),
                    relation_field_index,
                )?;

                let child_side = JoinSide::new(
                    (*target_collection).clone(),
                    target_relation_field
                        .cloned()
                        .unwrap_or_else(|| relation_field.clone()),
                    child_relation_index,
                )?;

                // Extract relation filter for this join if parent has a filter
                let relation_filter = parent_filter.and_then(|f| {
                    f.extract_relation_filter(relation_field_name)
                        .map(|nested_filter| RelationFilter {
                            relation_field: relation_field_name.clone(),
                            conditions: nested_filter,
                        })
                });

                // Create the appropriate join node
                // Note: We pass child_render_mapping as the output mapping (for TypeJoin to render children)
                // but the child_plan uses child_scan_mapping internally (for FK lookups)
                if relation_field.kind.is_array() {
                    // One-to-many: TypeJoinMany (relation filter support tracked in #187)
                    plan = Box::new(TypeJoinMany::new(
                        plan,
                        child_plan,
                        parent_side,
                        child_side,
                        mapping.clone(),
                    )?);
                } else {
                    // One-to-one: TypeJoinOne
                    let mut join = TypeJoinOne::new(
                        plan,
                        child_plan,
                        parent_side,
                        child_side,
                        mapping.clone(),
                    );
                    if let Some(rel_filter) = relation_filter {
                        join = join.with_relation_filter(rel_filter);
                    }
                    plan = Box::new(join);
                }
            }
        }

        Ok(plan)
    }

    /// Build a scan mapping for join child plans that includes ALL fields at schema indices.
    ///
    /// TypeJoin nodes use `JoinSide::relation_id_field_index()` which returns the FK field's
    /// position in the collection schema. For FK lookups to work correctly, documents must
    /// have fields at their schema positions. This method ensures the mapping includes all
    /// schema fields, while render_keys only include the user-selected fields.
    ///
    /// # Aliased Relation Fields
    ///
    /// When multiple aliases reference the same relation field (e.g., `p1: published` and
    /// `p2: published`), each alias MUST get a unique index. This is critical because
    /// TypeJoinMany nodes use their relation_field_index to set children on the parent
    /// document. If aliases share the same index, later joins overwrite earlier ones.
    ///
    /// The solution: track which indices already have render_keys. If a schema_index
    /// already has a render_key, allocate a new index for subsequent aliases.
    fn build_scan_mapping_for_join(
        &self,
        collection: &CollectionVersion,
        render_mapping: &DocumentMapping,
    ) -> DocumentMapping {
        let mut mapping = DocumentMapping::new();

        // Add ALL fields from the schema at their schema indices
        for (i, field) in collection.fields.iter().enumerate() {
            mapping.add(i, &field.name);
        }

        // Track which schema indices already have render_keys assigned
        let mut indices_with_render_keys = std::collections::HashSet::new();

        // Map render_keys from render_mapping to schema indices.
        // render_mapping uses sparse indices (0, 1, 2, ...) for only selected fields,
        // but scan_mapping uses schema indices which may differ.
        //
        // IMPORTANT: render_key.key may be an alias (e.g., "headline" for field "title").
        // We must look up the *field name* from render_mapping to find the schema index,
        // then use render_key.key (the alias) as the output key.
        //
        // For aliased fields referencing the same underlying field, each alias gets
        // its own unique index to prevent TypeJoinMany nodes from overwriting each other.
        for render_key in &render_mapping.render_keys {
            // Find the field name that corresponds to this render_key's index in render_mapping
            if let Some(field_name) = render_mapping.try_find_name_from_index(render_key.index) {
                // Look up the schema index for this field name in the new mapping
                if let Some(schema_index) = mapping.first_index_of_name(field_name) {
                    if indices_with_render_keys.contains(&schema_index) {
                        // This schema_index already has a render_key (aliased field).
                        // Allocate a new index to avoid TypeJoinMany nodes overwriting each other.
                        let new_index = mapping.next_index();
                        mapping.add(new_index, field_name);
                        mapping.add_render_key(new_index, &render_key.key);
                        indices_with_render_keys.insert(new_index);
                    } else {
                        // First render_key for this schema_index
                        mapping.add_render_key(schema_index, &render_key.key);
                        indices_with_render_keys.insert(schema_index);
                    }
                }
            }
        }

        mapping
    }

    /// Apply a join for a relation field that's in the filter but not in the selection.
    ///
    /// This creates a TypeJoinOne that brings in the child documents so that complex
    /// filters can evaluate conditions on the relation. The child documents are merged
    /// into the parent but won't appear in the final output (no render_key).
    fn apply_filter_relation_join(
        &self,
        plan: Box<dyn PlanNode>,
        parent_collection: &CollectionVersion,
        relation_field: &schema::FieldDescription,
        relation_field_name: &str,
        mut mapping: DocumentMapping,
    ) -> Result<Box<dyn PlanNode>> {
        // Get the target collection for this relation
        let target_collection_id =
            relation_field
                .kind
                .relation_collection_id()
                .ok_or_else(|| {
                    QueryError::internal(format!(
                        "relation field '{}' has no target collection",
                        relation_field_name
                    ))
                })?;

        let target_collection = if target_collection_id.is_empty() {
            Arc::new(parent_collection.clone())
        } else {
            self.get_collection(target_collection_id)
                .ok_or_else(|| QueryError::collection_not_found(target_collection_id))?
        };

        // Build a child scan mapping with all fields for filter evaluation.
        // We MUST include render_keys for all fields so that when the child doc
        // is merged via render_doc_to_json(), the fields are present in the JSON.
        // The filter will then be able to evaluate conditions on those fields.
        // The relation field won't appear in the final output because the parent
        // mapping doesn't have a render_key for it.
        let child_scan_mapping = {
            let mut m = DocumentMapping::new();
            for (i, field) in target_collection.fields.iter().enumerate() {
                m.add(i, &field.name);
                // Add render_key so field appears in merged JSON for filter evaluation
                m.add_render_key(i, &field.name);
            }
            m
        };

        // Get the relation field index in the parent mapping
        let relation_field_index = mapping
            .first_index_of_name(relation_field_name)
            .ok_or_else(|| QueryError::internal("relation field not in parent mapping"))?;

        // Set up child mapping in parent for TypeJoin
        mapping.set_child_at(relation_field_index, child_scan_mapping.clone());

        // Create the child scan plan
        let mut child_scan =
            ScanNode::new((*target_collection).clone(), child_scan_mapping.clone());
        if let Some(ref fetcher) = self.fetcher {
            child_scan = child_scan.with_fetcher(fetcher.clone());
        }
        let child_plan: Box<dyn PlanNode> = Box::new(child_scan);

        // Find the other side of the relation
        let target_relation_field = if let Some(rel_name) = &relation_field.relation_name {
            target_collection.field_by_relation(
                rel_name,
                &parent_collection.name,
                relation_field_name,
            )
        } else {
            None
        };

        let child_relation_index = target_relation_field
            .and_then(|f| {
                target_collection
                    .fields
                    .iter()
                    .position(|tf| tf.name == f.name)
            })
            .unwrap_or(0);

        // Create join sides
        let parent_side = JoinSide::new(
            parent_collection.clone(),
            relation_field.clone(),
            relation_field_index,
        )?;

        let child_side = JoinSide::new(
            (*target_collection).clone(),
            target_relation_field
                .cloned()
                .unwrap_or_else(|| relation_field.clone()),
            child_relation_index,
        )?;

        // Create TypeJoinOne (filter relations are typically one-to-one)
        // No relation filter here - the complex filter is applied after all joins
        let join = TypeJoinOne::new(plan, child_plan, parent_side, child_side, mapping);

        Ok(Box::new(join))
    }

    /// Apply sub-joins for remaining elements of a multi-level filter path.
    ///
    /// This is called when processing a relation that's the start of a multi-level filter path.
    /// For example, when processing the "author" relation and the filter has path ["author", "published"],
    /// this method adds a sub-join for "published" within the author's child plan.
    ///
    /// Returns both the updated plan and the updated mapping, since the mapping must be modified
    /// to include the child mappings for the nested relations.
    fn apply_multi_level_sub_joins(
        &self,
        mut plan: Box<dyn PlanNode>,
        remaining_path: &[String],
        parent_collection: &CollectionVersion,
        mut mapping: DocumentMapping,
    ) -> Result<(Box<dyn PlanNode>, DocumentMapping)> {
        if remaining_path.is_empty() {
            return Ok((plan, mapping));
        }

        let mut current_collection = parent_collection.clone();

        // Build sub-joins for each remaining element in the path
        for relation_field_name in remaining_path {
            // Find the relation field in the current collection
            let relation_field = current_collection
                .field_by_name(relation_field_name)
                .ok_or_else(|| QueryError::unknown_field(relation_field_name))?;

            if !relation_field.kind.is_relation() {
                return Err(QueryError::execution(format!(
                    "field '{}' on collection '{}' is not a relation",
                    relation_field_name, current_collection.name
                )));
            }

            // Get the target collection for this relation
            let target_collection_id =
                relation_field
                    .kind
                    .relation_collection_id()
                    .ok_or_else(|| {
                        QueryError::internal(format!(
                            "relation field '{}' has no target collection",
                            relation_field_name
                        ))
                    })?;

            let target_collection = if target_collection_id.is_empty() {
                Arc::new(current_collection.clone())
            } else {
                self.get_collection(target_collection_id)
                    .ok_or_else(|| QueryError::collection_not_found(target_collection_id))?
            };

            // Build a child scan mapping with all fields for filter evaluation
            let child_scan_mapping = {
                let mut m = DocumentMapping::new();
                for (i, field) in target_collection.fields.iter().enumerate() {
                    m.add(i, &field.name);
                    // Add render_key so field appears in merged JSON for filter evaluation
                    m.add_render_key(i, &field.name);
                }
                m
            };

            // Get the relation field index in the parent mapping
            let relation_field_index = mapping
                .first_index_of_name(relation_field_name)
                .ok_or_else(|| {
                    QueryError::internal(format!(
                        "relation field '{}' not in mapping",
                        relation_field_name
                    ))
                })?;

            // Set up child mapping in parent for TypeJoin
            mapping.set_child_at(relation_field_index, child_scan_mapping.clone());

            // Create the child scan plan
            let mut child_scan =
                ScanNode::new((*target_collection).clone(), child_scan_mapping.clone());
            if let Some(ref fetcher) = self.fetcher {
                child_scan = child_scan.with_fetcher(fetcher.clone());
            }
            let child_plan: Box<dyn PlanNode> = Box::new(child_scan);

            // Find the other side of the relation
            let target_relation_field = if let Some(rel_name) = &relation_field.relation_name {
                target_collection.field_by_relation(
                    rel_name,
                    &current_collection.name,
                    relation_field_name,
                )
            } else {
                None
            };

            let child_relation_index = target_relation_field
                .and_then(|f| {
                    target_collection
                        .fields
                        .iter()
                        .position(|tf| tf.name == f.name)
                })
                .unwrap_or(0);

            // Create join sides
            let parent_side = JoinSide::new(
                current_collection.clone(),
                relation_field.clone(),
                relation_field_index,
            )?;

            let child_side = JoinSide::new(
                (*target_collection).clone(),
                target_relation_field
                    .cloned()
                    .unwrap_or_else(|| relation_field.clone()),
                child_relation_index,
            )?;

            // Create TypeJoinOne for the sub-join
            let join = TypeJoinOne::new(plan, child_plan, parent_side, child_side, mapping.clone());

            plan = Box::new(join);

            // Update current collection for next iteration
            current_collection = (*target_collection).clone();
        }

        Ok((plan, mapping))
    }

    /// Apply joins for a multi-level relation filter path.
    ///
    /// For a path like ["author", "published"] with filter {author: {published: {rating: {_eq: 4.9}}}},
    /// this builds a chain of TypeJoin nodes:
    /// 1. Join parent (Book) → first relation (Author) via "author"
    /// 2. Join first relation (Author) → second relation (Book) via "published"
    /// 3. Apply the scalar filter (rating == 4.9) at the innermost level
    ///
    /// The filter is extracted at each level of the path and applied to the appropriate join.
    fn apply_multi_level_filter_joins(
        &self,
        mut plan: Box<dyn PlanNode>,
        path: &[String],
        start_collection: &CollectionVersion,
        filter: &crate::mapper::Filter,
        mut mapping: DocumentMapping,
    ) -> Result<Box<dyn PlanNode>> {
        if path.is_empty() {
            return Ok(plan);
        }

        let mut current_collection = start_collection.clone();

        // Build nested joins for each level of the path
        for (level, relation_field_name) in path.iter().enumerate() {
            // Find the relation field in the current collection
            let relation_field = current_collection
                .field_by_name(relation_field_name)
                .ok_or_else(|| QueryError::unknown_field(relation_field_name))?;

            if !relation_field.kind.is_relation() {
                return Err(QueryError::execution(format!(
                    "field '{}' on collection '{}' is not a relation",
                    relation_field_name, current_collection.name
                )));
            }

            // Get the target collection for this relation
            let target_collection_id =
                relation_field
                    .kind
                    .relation_collection_id()
                    .ok_or_else(|| {
                        QueryError::internal(format!(
                            "relation field '{}' has no target collection",
                            relation_field_name
                        ))
                    })?;

            let target_collection = if target_collection_id.is_empty() {
                Arc::new(current_collection.clone())
            } else {
                self.get_collection(target_collection_id)
                    .ok_or_else(|| QueryError::collection_not_found(target_collection_id))?
            };

            // Build a child scan mapping with all fields for filter evaluation
            let child_scan_mapping = {
                let mut m = DocumentMapping::new();
                for (i, field) in target_collection.fields.iter().enumerate() {
                    m.add(i, &field.name);
                    // Add render_key so field appears in merged JSON for filter evaluation
                    m.add_render_key(i, &field.name);
                }
                m
            };

            // Get the relation field index in the parent mapping
            let relation_field_index = mapping
                .first_index_of_name(relation_field_name)
                .ok_or_else(|| QueryError::internal("relation field not in parent mapping"))?;

            // Set up child mapping in parent for TypeJoin
            mapping.set_child_at(relation_field_index, child_scan_mapping.clone());

            // Create the child scan plan
            let mut child_scan =
                ScanNode::new((*target_collection).clone(), child_scan_mapping.clone());
            if let Some(ref fetcher) = self.fetcher {
                child_scan = child_scan.with_fetcher(fetcher.clone());
            }

            // Check if this is the last level in the path (where scalar filter applies)
            let is_last_level = level == path.len() - 1;

            let child_plan: Box<dyn PlanNode> = if is_last_level {
                // Extract and apply the scalar filter at the deepest level
                // The filter at this path level should be the scalar conditions
                if let Some(leaf_filter) = filter.extract_filter_at_path(path) {
                    Box::new(
                        SelectNode::new(Box::new(child_scan), child_scan_mapping.clone())
                            .with_filter(leaf_filter),
                    )
                } else {
                    Box::new(child_scan)
                }
            } else {
                Box::new(child_scan)
            };

            // Find the other side of the relation
            let target_relation_field = if let Some(rel_name) = &relation_field.relation_name {
                target_collection.field_by_relation(
                    rel_name,
                    &current_collection.name,
                    relation_field_name,
                )
            } else {
                None
            };

            let child_relation_index = target_relation_field
                .and_then(|f| {
                    target_collection
                        .fields
                        .iter()
                        .position(|tf| tf.name == f.name)
                })
                .unwrap_or(0);

            // Create join sides
            let parent_side = JoinSide::new(
                current_collection.clone(),
                relation_field.clone(),
                relation_field_index,
            )?;

            let child_side = JoinSide::new(
                (*target_collection).clone(),
                target_relation_field
                    .cloned()
                    .unwrap_or_else(|| relation_field.clone()),
                child_relation_index,
            )?;

            // For multi-level filters, create TypeJoinOne with relation filter
            // that filters parents based on whether their children exist after the nested filter
            let join = if is_last_level {
                // Last level: create a relation filter that uses the scalar filter from the leaf
                // This ensures parents are only included if their matched child exists
                TypeJoinOne::new(plan, child_plan, parent_side, child_side, mapping.clone())
            } else {
                TypeJoinOne::new(plan, child_plan, parent_side, child_side, mapping.clone())
            };

            plan = Box::new(join);

            // Update current collection for next iteration
            current_collection = (*target_collection).clone();
        }

        Ok(plan)
    }

    /// Build the document mapping for a Select operation.
    ///
    /// IMPORTANT: _docID is ALWAYS placed at index 0 because Doc::doc_id() expects it there.
    /// TypeJoinOne/TypeJoinMany use doc_id() to match related documents.
    fn build_mapping(
        &self,
        select: &Select,
        collection: &CollectionVersion,
    ) -> Result<DocumentMapping> {
        let mut mapping = DocumentMapping::new();

        // Track whether _docID was explicitly requested (for render_keys)
        let mut doc_id_requested = false;
        let mut doc_id_alias: Option<String> = None;

        // Check if _docID is explicitly selected
        for requestable in &select.fields {
            if let Requestable::Field(field) = requestable {
                if field.name == "_docID" {
                    doc_id_requested = true;
                    doc_id_alias = Some(field.output_name().to_string());
                    break;
                }
            }
        }

        // ALWAYS add _docID at index 0 (required for Doc::doc_id() to work)
        mapping.add(0, "_docID");
        // Only add to render_keys if explicitly selected
        if doc_id_requested {
            mapping.add_render_key(0, doc_id_alias.as_deref().unwrap_or("_docID"));
        }

        // Add remaining requested fields (starting from index 1)
        for requestable in &select.fields {
            match requestable {
                Requestable::Field(field) => {
                    // Skip _docID (already handled at index 0)
                    if field.name == "_docID" {
                        continue;
                    }
                    // Validate field exists in schema
                    if collection.field_by_name(&field.name).is_none() {
                        return Err(QueryError::unknown_field(&field.name));
                    }
                    let index = mapping.next_index();

                    mapping.add(index, &field.name);
                    mapping.add_render_key(index, field.output_name());
                }
                Requestable::Select(nested_select) => {
                    // Nested select (relation) - add the field but don't recurse here
                    // Child mapping will be built when applying joins
                    let index = mapping.next_index();
                    mapping.add(index, &nested_select.field.name);
                    mapping.add_render_key(index, nested_select.field.output_name());
                }
                Requestable::Aggregate(agg) => {
                    return Err(QueryError::execution(format!(
                        "aggregate '{:?}' not yet implemented",
                        agg.aggregate_type
                    )));
                }
            }
        }

        // If no fields specified (besides _docID), add all collection fields
        if mapping.next_index() == 1 {
            // Only _docID was added; add all collection fields at schema indices
            for (i, field) in collection.fields.iter().enumerate() {
                if field.name != "_docID" {
                    mapping.add(i, &field.name);
                    mapping.add_render_key(i, &field.name);
                } else if !doc_id_requested {
                    // Add _docID to render_keys since we're returning all fields
                    mapping.add_render_key(0, "_docID");
                }
            }
        }

        Ok(mapping)
    }

    /// Get a collection schema by name.
    pub fn collection(&self, name: &str) -> Option<&Arc<CollectionVersion>> {
        self.collections.get(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mapper::{Field, Filter};
    use crate::planner::index_selection::IndexScanType;
    use schema::{FieldDescription, FieldKind, IndexDescription, IndexedFieldDescription};

    fn make_test_collection() -> CollectionVersion {
        CollectionVersion::new(
            "Users",
            "v1",
            "coll-1",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "name", FieldKind::string()),
                FieldDescription::new("3", "age", FieldKind::int()),
            ],
        )
    }

    fn make_test_collection_with_index() -> CollectionVersion {
        CollectionVersion::new(
            "Users",
            "v1",
            "coll-1",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "name", FieldKind::string()),
                FieldDescription::new("3", "age", FieldKind::int()),
            ],
        )
        .with_index(IndexDescription {
            id: 1,
            name: "name_idx".to_string(),
            unique: false,
            fields: vec![IndexedFieldDescription {
                name: "name".to_string(),
                descending: false,
            }],
        })
        .with_index(IndexDescription {
            id: 2,
            name: "age_idx".to_string(),
            unique: false,
            fields: vec![IndexedFieldDescription {
                name: "age".to_string(),
                descending: false,
            }],
        })
    }

    fn make_users_collection() -> CollectionVersion {
        CollectionVersion::new(
            "users",
            "v1",
            "coll-users",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "name", FieldKind::string()),
                // One-to-many relation to posts (array)
                FieldDescription::new("3", "posts", FieldKind::relation("posts", true))
                    .with_relation_name("author_posts"),
            ],
        )
    }

    fn make_posts_collection() -> CollectionVersion {
        CollectionVersion::new(
            "posts",
            "v1",
            "coll-posts",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "title", FieldKind::string()),
                // Many-to-one relation to users (singular)
                FieldDescription::new("3", "author", FieldKind::relation("users", false))
                    .with_relation_name("author_posts")
                    .as_primary(),
                // Auto-generated FK field
                FieldDescription::new("4", "author_id", FieldKind::doc_id())
                    .with_relation_name("author_posts")
                    .as_primary(),
            ],
        )
    }

    #[test]
    fn test_planner_new() {
        let planner = Planner::new(vec![make_test_collection()]);
        assert!(planner.collection("Users").is_some());
        assert!(planner.collection("Posts").is_none());
    }

    #[tokio::test]
    async fn test_plan_simple_select() {
        let planner = Planner::new(vec![make_test_collection()]);

        let select = Select::new("Users")
            .with_field(Field::new("_docID"))
            .with_field(Field::new("name"));

        let plan = planner.plan(&select).unwrap();
        assert_eq!(plan.kind(), "selectNode");
    }

    #[tokio::test]
    async fn test_plan_with_limit() {
        let planner = Planner::new(vec![make_test_collection()]);

        let select = Select::new("Users")
            .with_field(Field::new("name"))
            .with_limit(10);

        let plan = planner.plan(&select).unwrap();
        assert_eq!(plan.kind(), "limitNode");
    }

    #[tokio::test]
    async fn test_plan_unknown_collection() {
        let planner = Planner::new(vec![make_test_collection()]);

        let select = Select::new("Posts").with_field(Field::new("title"));

        let result = planner.plan(&select);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_plan_with_filter() {
        use std::collections::HashMap;

        let planner = Planner::new(vec![make_test_collection()]);

        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            serde_json::json!({"_eq": "Alice"}),
        )]));

        let select = Select::new("Users")
            .with_field(Field::new("name"))
            .with_filter(filter);

        let plan = planner.plan(&select).unwrap();
        assert_eq!(plan.kind(), "selectNode");
    }

    #[test]
    fn test_build_mapping() {
        let planner = Planner::new(vec![make_test_collection()]);
        let collection = planner.collection("Users").unwrap();

        let select = Select::new("Users")
            .with_field(Field::new("_docID"))
            .with_field(Field::new("name"));

        let mapping = planner.build_mapping(&select, collection).unwrap();

        assert!(mapping.has_field("_docID"));
        assert!(mapping.has_field("name"));
        assert!(!mapping.has_field("age"));
    }

    #[test]
    fn test_build_mapping_with_alias() {
        let planner = Planner::new(vec![make_test_collection()]);
        let collection = planner.collection("Users").unwrap();

        let select = Select::new("Users").with_field(Field::with_alias("name", "userName"));

        let mapping = planner.build_mapping(&select, collection).unwrap();

        assert!(mapping.has_field("name"));
        // Should have render key "userName"
        assert_eq!(mapping.render_keys.len(), 1);
        assert_eq!(mapping.render_keys[0].key, "userName");
    }

    // === Index-Aware Planning Tests ===

    #[tokio::test]
    async fn test_plan_uses_index_for_eq_filter() {
        use std::collections::HashMap;

        let planner = Planner::new(vec![make_test_collection_with_index()]);

        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            serde_json::json!({"_eq": "Alice"}),
        )]));

        let select = Select::new("Users")
            .with_field(Field::new("name"))
            .with_filter(filter);

        let result = planner.plan_with_index_info(&select).unwrap();

        // Should use index
        assert!(result.uses_index());
        assert_eq!(result.index_scan.as_ref().unwrap().index_name, "name_idx");

        // Plan should have indexScanNode at the leaf
        // (wrapped by selectNode for field projection)
        assert_eq!(result.plan.kind(), "selectNode");
    }

    #[tokio::test]
    async fn test_plan_uses_index_for_range_filter() {
        use std::collections::HashMap;

        let planner = Planner::new(vec![make_test_collection_with_index()]);

        let filter = Filter::from_conditions(HashMap::from([(
            "age".to_string(),
            serde_json::json!({"_gte": 18, "_lt": 65}),
        )]));

        let select = Select::new("Users")
            .with_field(Field::new("age"))
            .with_filter(filter);

        let result = planner.plan_with_index_info(&select).unwrap();

        // Should use age index
        assert!(result.uses_index());
        assert_eq!(result.index_scan.as_ref().unwrap().index_name, "age_idx");

        // Verify it's a range scan
        match &result.index_scan.as_ref().unwrap().scan_type {
            IndexScanType::RangeScan { .. } => {}
            _ => panic!("expected RangeScan"),
        }
    }

    #[tokio::test]
    async fn test_plan_no_index_without_filter() {
        let planner = Planner::new(vec![make_test_collection_with_index()]);

        let select = Select::new("Users").with_field(Field::new("name"));

        let result = planner.plan_with_index_info(&select).unwrap();

        // No filter, so no index should be used
        assert!(!result.uses_index());
    }

    #[tokio::test]
    async fn test_plan_no_index_for_non_indexed_field() {
        use std::collections::HashMap;

        // Collection without indexes
        let planner = Planner::new(vec![make_test_collection()]);

        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            serde_json::json!({"_eq": "Alice"}),
        )]));

        let select = Select::new("Users")
            .with_field(Field::new("name"))
            .with_filter(filter);

        let result = planner.plan_with_index_info(&select).unwrap();

        // No indexes available, so shouldn't use index
        assert!(!result.uses_index());
    }

    #[tokio::test]
    async fn test_plan_no_index_for_ne_filter() {
        use std::collections::HashMap;

        let planner = Planner::new(vec![make_test_collection_with_index()]);

        // _ne is not index-friendly
        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            serde_json::json!({"_ne": "Alice"}),
        )]));

        let select = Select::new("Users")
            .with_field(Field::new("name"))
            .with_filter(filter);

        let result = planner.plan_with_index_info(&select).unwrap();

        // _ne cannot use index efficiently
        assert!(!result.uses_index());
    }

    #[tokio::test]
    async fn test_plan_uses_index_for_in_filter() {
        use std::collections::HashMap;

        let planner = Planner::new(vec![make_test_collection_with_index()]);

        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            serde_json::json!({"_in": ["Alice", "Bob"]}),
        )]));

        let select = Select::new("Users")
            .with_field(Field::new("name"))
            .with_filter(filter);

        let result = planner.plan_with_index_info(&select).unwrap();

        // _in can use index
        assert!(result.uses_index());
        assert_eq!(result.index_scan.as_ref().unwrap().index_name, "name_idx");

        match &result.index_scan.as_ref().unwrap().scan_type {
            IndexScanType::InScan { values } => {
                assert_eq!(values.len(), 2);
            }
            _ => panic!("expected InScan"),
        }
    }

    #[tokio::test]
    async fn test_plan_result_uses_index_method() {
        use std::collections::HashMap;

        let planner = Planner::new(vec![make_test_collection_with_index()]);

        // With index
        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            serde_json::json!({"_eq": "Alice"}),
        )]));
        let select = Select::new("Users")
            .with_field(Field::new("name"))
            .with_filter(filter);
        let result = planner.plan_with_index_info(&select).unwrap();
        assert!(result.uses_index());

        // Without index (no filter)
        let select_no_filter = Select::new("Users").with_field(Field::new("name"));
        let result_no_filter = planner.plan_with_index_info(&select_no_filter).unwrap();
        assert!(!result_no_filter.uses_index());
    }

    // ========================================================================
    // Join Planning Tests
    // ========================================================================

    #[tokio::test]
    async fn test_plan_with_one_to_one_relation() {
        // Query: posts { title, author { name } }
        let planner = Planner::new(vec![make_users_collection(), make_posts_collection()]);

        // Build nested select for author - field name is "author" (relation field), collection is "users"
        let author_select = Select::new("users")
            .with_field_name("author")
            .with_field(Field::new("name"));

        let select = Select::new("posts")
            .with_field(Field::new("title"))
            .with_select(author_select);

        let plan = planner.plan(&select).unwrap();

        // The plan should be a TypeJoinOne (for one-to-one)
        assert_eq!(plan.kind(), "typeJoinOne");
    }

    #[tokio::test]
    async fn test_plan_with_one_to_many_relation() {
        // Query: users { name, posts { title } }
        let planner = Planner::new(vec![make_users_collection(), make_posts_collection()]);

        // Build nested select for posts - field name is "posts" (relation field), collection is "posts"
        let posts_select = Select::new("posts")
            .with_field_name("posts")
            .with_field(Field::new("title"));

        let select = Select::new("users")
            .with_field(Field::new("name"))
            .with_select(posts_select);

        let plan = planner.plan(&select).unwrap();

        // The plan should be a TypeJoinMany (for one-to-many)
        assert_eq!(plan.kind(), "typeJoinMany");
    }

    #[tokio::test]
    async fn test_plan_relation_unknown_field() {
        let planner = Planner::new(vec![make_users_collection(), make_posts_collection()]);

        // Try to select a non-existent relation field
        let nested = Select::new("users")
            .with_field_name("nonexistent")
            .with_field(Field::new("name"));

        let select = Select::new("posts")
            .with_field(Field::new("title"))
            .with_select(nested);

        let result = planner.plan(&select);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_plan_relation_with_limit() {
        // Query: users { name, posts { title } } limit 5
        let planner = Planner::new(vec![make_users_collection(), make_posts_collection()]);

        let posts_select = Select::new("posts")
            .with_field_name("posts")
            .with_field(Field::new("title"));

        let select = Select::new("users")
            .with_field(Field::new("name"))
            .with_select(posts_select)
            .with_limit(5);

        let plan = planner.plan(&select).unwrap();

        // The outermost node should be a LimitNode wrapping the join
        assert_eq!(plan.kind(), "limitNode");

        // The source should be the join
        let source = plan.source().unwrap();
        assert_eq!(source.kind(), "typeJoinMany");
    }

    // ========================================================================
    // Nested Relation Filter Tests
    // ========================================================================

    #[tokio::test]
    async fn test_plan_with_nested_filter_on_type_join_many() {
        // Query: users { name, posts(filter: { title: { _eq: "Hello" } }) { title } }
        let planner = Planner::new(vec![make_users_collection(), make_posts_collection()]);

        // Build nested select with filter
        let filter = Filter::from_conditions(HashMap::from([(
            "title".to_string(),
            serde_json::json!({"_eq": "Hello"}),
        )]));

        let posts_select = Select::new("posts")
            .with_field_name("posts")
            .with_field(Field::new("title"))
            .with_filter(filter);

        let select = Select::new("users")
            .with_field(Field::new("name"))
            .with_select(posts_select);

        let plan = planner.plan(&select).unwrap();

        // The outermost node should be TypeJoinMany
        assert_eq!(plan.kind(), "typeJoinMany");

        // The child source of the join should be a selectNode (with the filter)
        // not a raw scanNode
        let source = plan.source().unwrap();
        // Parent source is selectNode
        assert_eq!(source.kind(), "selectNode");
    }

    #[tokio::test]
    async fn test_plan_with_nested_filter_on_type_join_one() {
        // Query: posts { title, author(filter: { name: { _eq: "Alice" } }) { name } }
        let planner = Planner::new(vec![make_users_collection(), make_posts_collection()]);

        // Build nested select with filter
        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            serde_json::json!({"_eq": "Alice"}),
        )]));

        let author_select = Select::new("users")
            .with_field_name("author")
            .with_field(Field::new("name"))
            .with_filter(filter);

        let select = Select::new("posts")
            .with_field(Field::new("title"))
            .with_select(author_select);

        let plan = planner.plan(&select).unwrap();

        // The outermost node should be TypeJoinOne
        assert_eq!(plan.kind(), "typeJoinOne");

        // The parent source of the join should be a selectNode
        let source = plan.source().unwrap();
        assert_eq!(source.kind(), "selectNode");
    }

    #[tokio::test]
    async fn test_plan_nested_filter_with_parent_filter() {
        // Query: users(filter: { name: { _eq: "Bob" } }) {
        //   name,
        //   posts(filter: { title: { _like: "Hello%" } }) { title }
        // }
        let planner = Planner::new(vec![make_users_collection(), make_posts_collection()]);

        // Parent filter
        let parent_filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            serde_json::json!({"_eq": "Bob"}),
        )]));

        // Child filter
        let child_filter = Filter::from_conditions(HashMap::from([(
            "title".to_string(),
            serde_json::json!({"_like": "Hello%"}),
        )]));

        let posts_select = Select::new("posts")
            .with_field_name("posts")
            .with_field(Field::new("title"))
            .with_filter(child_filter);

        let select = Select::new("users")
            .with_field(Field::new("name"))
            .with_select(posts_select)
            .with_filter(parent_filter);

        let plan = planner.plan(&select).unwrap();

        // The outermost node should be TypeJoinMany
        assert_eq!(plan.kind(), "typeJoinMany");

        // The parent source should be selectNode (with parent filter)
        let source = plan.source().unwrap();
        assert_eq!(source.kind(), "selectNode");
    }

    #[tokio::test]
    async fn test_plan_nested_filter_references_unselected_field_fails_at_planning() {
        // Query: users { posts(filter: { author_id: { _eq: "user-1" } }) { title } }
        // The filter references "author_id" but the select only includes "title"
        // This should fail at planning time with a clear error message
        let planner = Planner::new(vec![make_users_collection(), make_posts_collection()]);

        // Filter references "author_id" which is NOT in the select list
        let filter = Filter::from_conditions(HashMap::from([(
            "author_id".to_string(),
            serde_json::json!({"_eq": "user-1"}),
        )]));

        let posts_select = Select::new("posts")
            .with_field_name("posts")
            .with_field(Field::new("title")) // Only selecting "title", not "author_id"
            .with_filter(filter);

        let select = Select::new("users")
            .with_field(Field::new("name"))
            .with_select(posts_select);

        let result = planner.plan(&select);

        // Should fail at planning time
        let err = match result {
            Ok(_) => panic!("Expected error but got Ok"),
            Err(e) => e,
        };

        // Error message should indicate the filter field is not in the select list
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("author_id"),
            "Error should mention the field name: {}",
            err_msg
        );
        assert!(
            err_msg.contains("select list") || err_msg.contains("posts"),
            "Error should mention select list or collection: {}",
            err_msg
        );
    }
}
