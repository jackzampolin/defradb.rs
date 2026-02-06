//! Join planning utilities
//!
//! Contains methods for applying relation joins to query plans:
//! - `apply_joins()` - Main join application for nested selects
//! - `build_scan_mapping_for_join()` - Scan mapping with schema indices
//! - `apply_filter_relation_join()` - Join for filter-only relations
//! - `apply_multi_level_sub_joins()` - Sub-joins for multi-level filter paths
//! - `apply_multi_level_filter_joins()` - Join chains for deep filter paths

use std::collections::HashMap;
use std::sync::Arc;

use schema::CollectionVersion;
use tracing::{debug, warn};

use crate::document::DocumentMapping;
use crate::error::{QueryError, Result};
use crate::mapper::{AggregateType, Field, Filter, OrderBy, OrderCondition, Requestable, Select};
use crate::plan::{
    IndexScanNode, JoinSide, RelationFilter, ScanNode, SelectNode, TypeJoinMany, TypeJoinOne,
};
use crate::planner::PlanNode;

use super::builder::{Planner, MAX_NESTING_DEPTH};

/// Result of applying joins: the updated plan node, document mapping, aggregate internal keys,
/// and whether a child index scan provides the parent's ORDER BY.
type JoinResult = Result<(
    Box<dyn PlanNode>,
    DocumentMapping,
    HashMap<String, (String, String)>,
    bool, // join_provides_ordering
)>;

impl Planner {
    /// Apply join nodes for nested selects (relation fields)
    ///
    /// The `depth` parameter tracks recursion depth to prevent stack overflow
    /// from deeply nested or circular query structures.
    ///
    /// If `parent_filter` is provided, relation filters are extracted and passed
    /// to the TypeJoin nodes to filter parents based on their children.
    pub(super) fn apply_joins(
        &self,
        mut plan: Box<dyn PlanNode>,
        select: &Select,
        parent_collection: &CollectionVersion,
        mut mapping: DocumentMapping,
        depth: usize,
        parent_filter: Option<&crate::mapper::Filter>,
    ) -> JoinResult {
        // Internal keys for aggregate relation data when there's a collision with a relation selection.
        let mut aggregate_internal_keys: HashMap<String, (String, String)> = HashMap::new();
        let mut join_provides_ordering = false;

        // Check recursion depth to prevent stack overflow
        if depth > MAX_NESTING_DEPTH {
            return Err(QueryError::execution(format!(
                "Query nesting depth {} exceeds maximum allowed depth of {}. \
                 Consider simplifying the query or using separate queries for deeply nested data.",
                depth, MAX_NESTING_DEPTH
            )));
        }

        // Collect all Select items to process, including those inside _group.
        // Track the _group index for relation fields inside _group so we can update
        // the _group child mapping with the correct relation field index later.
        // Tuple: (select, _group_index if from inside _group)
        let mut selects_to_process: Vec<(&Select, Option<usize>)> = Vec::new();
        for requestable in &select.fields {
            if let Requestable::Select(nested_select) = requestable {
                if nested_select.field.name == "_group" {
                    // Get the _group index in the parent mapping
                    let group_index = mapping.first_index_of_name("_group");
                    // _group is a virtual field - process its inner relation fields
                    for inner_requestable in &nested_select.fields {
                        if let Requestable::Select(inner_select) = inner_requestable {
                            // Skip special fields
                            if !inner_select.field.name.starts_with('_') {
                                selects_to_process.push((inner_select, group_index));
                            }
                        }
                    }
                } else {
                    selects_to_process.push((nested_select, None));
                }
            }
        }

        // Add synthetic selects for ORDER BY relation fields not already in the selection.
        // Go's resolveOrderDependencies creates joins for relations referenced in ORDER BY
        // even when they're not explicitly selected in the query.
        let mut synthetic_order_selects: Vec<Select> = Vec::new();
        if let Some(ref order_by) = select.order_by {
            let already_selected: std::collections::HashSet<&str> = selects_to_process
                .iter()
                .map(|(s, _)| s.field.name.as_str())
                .collect();

            for condition in &order_by.conditions {
                // Path like ["device", "model"] — first element is the relation field
                if condition.fields.len() >= 2 {
                    let relation_name = &condition.fields[0];
                    if !already_selected.contains(relation_name.as_str()) {
                        // Check this is actually a relation field
                        if let Some(field) = parent_collection.field_by_name(relation_name) {
                            if field.kind.is_relation() {
                                let mut syn_select = Select::new("");
                                syn_select.field = Field::new(relation_name);
                                synthetic_order_selects.push(syn_select);
                            }
                        }
                    }
                }
            }
        }
        for syn_select in &synthetic_order_selects {
            selects_to_process.push((syn_select, None));
        }

        // Collect info about selection joins so aggregates can share when compatible.
        // Go shares joins when: same relation, same filter, no limit on selection.
        struct SelectionJoinInfo {
            filter_json: Option<String>,
            has_limit: bool,
        }
        let selection_join_info: std::collections::HashMap<String, SelectionJoinInfo> =
            selects_to_process
                .iter()
                .map(|(s, _)| {
                    let filter_json = s
                        .filter
                        .as_ref()
                        .map(|f| serde_json::to_string(f.conditions()).unwrap_or_default());
                    let has_limit = s.limit.as_ref().is_some_and(|l| l.limit.is_some());
                    (
                        s.field.name.clone(),
                        SelectionJoinInfo {
                            filter_json,
                            has_limit,
                        },
                    )
                })
                .collect();

        for (nested_select, group_index) in selects_to_process {
            let relation_field_name = &nested_select.field.name;
            let output_name = nested_select.field.output_name();

            // Ensure the relation field is in the parent mapping.
            // This is especially important for relation fields inside _group
            // which aren't direct children of the select.
            if mapping.first_index_of_name(relation_field_name).is_none() {
                let index = mapping.next_index();
                mapping.add(index, relation_field_name);
                // Don't add render_key - for _group fields, rendering is handled by GroupByNode
            }

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

            // For self-referential relations (empty relative_id), use the parent collection
            let target_collection = if target_collection_id.is_empty() {
                // Self-reference: target is the same collection as parent
                Arc::new(parent_collection.clone())
            } else {
                self.get_collection(target_collection_id)
                    .or_else(|| {
                        // CID lookup failed - try to find target by matching relation_name.
                        // Handles CID mismatch from circular schema set-based versioning.
                        let rel_name = relation_field.relation_name.as_deref().unwrap_or("");
                        if rel_name.is_empty() {
                            return None;
                        }
                        for coll in self.collections.values() {
                            if coll.name == parent_collection.name {
                                continue;
                            }
                            for f in &coll.fields {
                                if f.relation_name.as_deref() == Some(rel_name) {
                                    return Some(coll.clone());
                                }
                            }
                        }
                        None
                    })
                    .ok_or_else(|| QueryError::collection_not_found(target_collection_id))?
            };

            // Build child mapping for rendering (only selected fields)
            let child_render_mapping = self.build_mapping(nested_select, &target_collection)?;

            // Build scan mapping that includes ALL fields at schema indices.
            // This is required because JoinSide derives FK field indices from the schema,
            // so the doc fields must be at their schema positions for FK lookups to work.
            let mut child_scan_mapping =
                self.build_scan_mapping_for_join(&target_collection, &child_render_mapping);

            // Check if aggregates reference this relation and need additional fields for filters.
            // For example, _count(published: {filter: {rating: {_gt: 4.8}}}) needs the 'rating'
            // field to be included even if it's not in the selection set.
            for requestable in &select.fields {
                if let Requestable::Aggregate(agg) = requestable {
                    for target in &agg.targets {
                        if target.host_name == *relation_field_name {
                            // This aggregate references this relation - add filter fields
                            if let Some(ref filter) = target.filter {
                                for filter_field in filter.referenced_fields() {
                                    // Skip special fields
                                    if filter_field.starts_with('_') {
                                        continue;
                                    }
                                    // Find the field index in the target collection
                                    if let Some(idx) = target_collection
                                        .fields
                                        .iter()
                                        .position(|f| f.name == filter_field)
                                    {
                                        // Add the field to child_scan_mapping if not present
                                        if child_scan_mapping
                                            .first_index_of_name(&filter_field)
                                            .is_none()
                                        {
                                            child_scan_mapping.add(idx, &filter_field);
                                        }
                                        // Add render_key so the field appears in the output
                                        if !child_scan_mapping
                                            .render_keys
                                            .iter()
                                            .any(|rk| rk.key == filter_field)
                                        {
                                            child_scan_mapping.add_render_key(idx, &filter_field);
                                        }
                                    }
                                }
                            }
                            // Also add the aggregate target field if specified
                            if let Some(ref field_name) = target.field_name {
                                if let Some(idx) = target_collection
                                    .fields
                                    .iter()
                                    .position(|f| f.name == *field_name)
                                {
                                    if child_scan_mapping.first_index_of_name(field_name).is_none()
                                    {
                                        child_scan_mapping.add(idx, field_name);
                                    }
                                    if !child_scan_mapping
                                        .render_keys
                                        .iter()
                                        .any(|rk| rk.key == *field_name)
                                    {
                                        child_scan_mapping.add_render_key(idx, field_name);
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Check if parent's filter has a relation filter for this relation.
            // If so, add those fields to child_scan_mapping so the filter can be evaluated.
            // For example, Author(filter: {published: {rating: {_gt: 3}}}) needs the 'rating'
            // field to be included even if it's not in the selection set.
            if let Some(pf) = parent_filter {
                if let Some(nested_filter) = pf.extract_relation_filter(relation_field_name) {
                    for filter_field in nested_filter.referenced_fields() {
                        // Skip special fields
                        if filter_field.starts_with('_') {
                            continue;
                        }
                        // Find the field index in the target collection
                        if let Some(idx) = target_collection
                            .fields
                            .iter()
                            .position(|f| f.name == filter_field)
                        {
                            // Add the field to child_scan_mapping if not present
                            if child_scan_mapping
                                .first_index_of_name(&filter_field)
                                .is_none()
                            {
                                child_scan_mapping.add(idx, &filter_field);
                            }
                            // Add render_key so the field appears in the output
                            if !child_scan_mapping
                                .render_keys
                                .iter()
                                .any(|rk| rk.key == filter_field)
                            {
                                child_scan_mapping.add_render_key(idx, &filter_field);
                            }
                        }
                    }
                }
            }

            // Check if parent's order_by references fields in this relation.
            // If so, add those fields to child_scan_mapping.render_keys so they're
            // available in the merged JSON for ordering. The fields won't appear in
            // the final output unless they're also in the selection set.
            if let Some(ref order_by) = select.order_by {
                for condition in &order_by.conditions {
                    // Check if this order condition starts with this relation field
                    if condition.fields.len() > 1 && condition.fields[0] == *relation_field_name {
                        // Get the nested field name (e.g., "verified" from ["author", "verified"])
                        let nested_field = &condition.fields[1];
                        // Find the schema index for this field
                        if let Some(idx) = child_scan_mapping.first_index_of_name(nested_field) {
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
                                .is_some_and(|first| first == relation_field_name)
                        })
                        .map(|path| path[1..].to_vec()) // Get remaining path after this relation
                        .filter(|remaining| !remaining.is_empty())
                        .collect()
                })
                .unwrap_or_default();

            // Add render_keys for nested relation fields needed by multi-level filters.
            // The relation filter in check_relation_filter evaluates via
            // filter.matches(child.fields(), child_mapping), and the field value
            // at publisher_index is the rendered JSON from TypeJoinOne. The filter
            // needs the mapping to have the field indexed but the render_key is
            // needed for the relation filter to find the publisher JSON object.
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
            // First try by render_key (for aliased fields), then fall back to name lookup.
            // The fallback handles relation fields inside _group which are added without render_keys.
            let relation_field_index = mapping
                .try_find_index_from_render_key(output_name)
                .or_else(|| mapping.first_index_of_name(relation_field_name))
                .ok_or_else(|| {
                    QueryError::internal(format!(
                        "relation field '{}' (output name '{}') not in mapping",
                        relation_field_name, output_name
                    ))
                })?;

            // If this relation field is inside _group, update the _group child mapping
            // to use the correct index for rendering. TypeJoinMany stores the relation
            // data at relation_field_index, so the child mapping must use the same index.
            if let Some(grp_idx) = group_index {
                if let Some(group_child_mapping) = mapping.child_at_mut(grp_idx) {
                    // Update the child mapping: replace the dynamic index with relation_field_index
                    // First, find and remove any existing entry for this field
                    let old_index = group_child_mapping.first_index_of_name(relation_field_name);
                    if let Some(old_idx) = old_index {
                        // Remove old render_key with the wrong index
                        group_child_mapping
                            .render_keys
                            .retain(|rk| rk.index != old_idx);
                    }
                    // Add with the correct index
                    group_child_mapping.add(relation_field_index, relation_field_name);
                    group_child_mapping.add_render_key(relation_field_index, output_name);
                }
            }

            // Set up child scan mapping in parent (for TypeJoin to render children).
            // We use child_scan_mapping (not child_render_mapping) because child docs
            // have fields at schema indices, and render_keys need to match those indices.
            mapping.set_child_at(relation_field_index, child_scan_mapping.clone());

            // Collect aggregate target filters for the child scan.
            // For example, _avg(books: {field: pages, filter: {pages: {_neq: null}}})
            // should apply the filter {pages: {_neq: null}} to the books scan node.
            // Go places these filters on the scanNode (not a wrapping SelectNode).
            let mut agg_scan_filter: Option<Filter> = None;
            for requestable in &select.fields {
                if let Requestable::Aggregate(agg) = requestable {
                    for target in &agg.targets {
                        if target.host_name == *relation_field_name {
                            if let Some(ref filter) = target.filter {
                                // Only apply non-relation filters to scan node.
                                // Relation filters need sub-joins (handled separately).
                                if !filter.has_relation_filters() {
                                    agg_scan_filter = Some(match agg_scan_filter {
                                        Some(existing) => existing.and(filter.clone()),
                                        None => filter.clone(),
                                    });
                                }
                            }
                        }
                    }
                }
            }

            // Determine if the child scan can use an index.
            // Only nested_select.filter is eligible here. Parent-level relation
            // filters (e.g., User(filter: {devices: {model: ...}})) need the
            // full child set because TypeJoin uses check_relation_filter to gate
            // the *parent*, but all children of matching parents must appear.
            //
            // For ordering, extract parent's order_by conditions that reference this relation
            // and convert them to child-level order_by. e.g., parent's `{device: {model: ASC}}`
            // becomes child's `{model: ASC}` for index selection on the Device collection.
            let parent_order_for_child: Option<OrderBy> = select.order_by.as_ref().and_then(|o| {
                let child_conditions: Vec<OrderCondition> = o
                    .conditions
                    .iter()
                    .filter_map(|c| {
                        if c.fields.len() >= 2 && c.fields[0] == *relation_field_name {
                            // Convert nested path to child-relative path
                            Some(OrderCondition {
                                fields: c.fields[1..].to_vec(),
                                direction: c.direction,
                            })
                        } else {
                            None
                        }
                    })
                    .collect();
                if child_conditions.is_empty() {
                    None
                } else {
                    Some(OrderBy {
                        conditions: child_conditions,
                    })
                }
            });

            // Combine child's own order_by with parent's order for this relation
            let combined_child_order = match (&nested_select.order_by, &parent_order_for_child) {
                (Some(child), Some(parent)) => {
                    // Child's explicit order takes precedence, but append parent's for index selection
                    let mut combined = child.conditions.clone();
                    for pc in &parent.conditions {
                        if !combined.iter().any(|c| c.fields == pc.fields) {
                            combined.push(pc.clone());
                        }
                    }
                    Some(OrderBy {
                        conditions: combined,
                    })
                }
                (Some(child), None) => Some(child.clone()),
                (None, Some(parent)) => Some(parent.clone()),
                (None, None) => None,
            };

            // Extract relation filter from parent for child index selection and join filter.
            // e.g., User(filter: {address: {city: {_eq: "Munich"}}}) → child filter: {city: {_eq: "Munich"}}
            let parent_relation_filter_for_child =
                parent_filter.and_then(|f| f.extract_relation_filter(relation_field_name));

            // Use parent relation filter for child index selection only for one-to-one relations.
            // For one-to-many, the child scan must return ALL children because TypeJoinMany
            // renders all children of matching parents, not just those matching the filter.
            let parent_rel_for_index = if !relation_field.kind.is_array() {
                parent_relation_filter_for_child.as_ref()
            } else {
                None
            };

            let child_filter_for_index = match (&nested_select.filter, parent_rel_for_index) {
                (Some(child_f), Some(parent_rf)) => Some(child_f.and(parent_rf.clone())),
                (Some(child_f), None) => Some(child_f.clone()),
                (None, Some(parent_rf)) => Some(parent_rf.clone()),
                (None, None) => None,
            };

            let child_index_result = self.try_select_child_index(
                child_filter_for_index.as_ref(),
                combined_child_order.as_ref(),
                &target_collection,
            );
            let child_uses_index = child_index_result.is_some();

            // Create the child scan plan with scan_mapping (includes FK fields for joins)
            let mut child_plan: Box<dyn PlanNode> = if let Some((params, _)) = child_index_result {
                let mut index_scan = IndexScanNode::new(
                    (*target_collection).clone(),
                    child_scan_mapping.clone(),
                    params,
                )
                .with_show_deleted(select.show_deleted);
                if let Some(ref fetcher) = self.fetcher {
                    index_scan = index_scan.with_fetcher(fetcher.clone());
                }
                if let Some(filter) = agg_scan_filter {
                    index_scan = index_scan.with_residual_filter(filter);
                }
                Box::new(index_scan)
            } else {
                let mut child_scan =
                    ScanNode::new((*target_collection).clone(), child_scan_mapping.clone())
                        .with_show_deleted(select.show_deleted);
                if let Some(ref fetcher) = self.fetcher {
                    child_scan = child_scan.with_fetcher(fetcher.clone());
                }
                if let Some(filter) = agg_scan_filter {
                    child_scan = child_scan.with_filter(filter);
                }
                Box::new(child_scan)
            };

            // For OneToMany with parent relation filter: create a separate filter child plan
            // that uses an index for efficient filter evaluation. The main child_plan handles
            // display (ALL children), while filter_child_plan handles filter evaluation only.
            // This matches Go's inverted index join behavior.
            let filter_child_plan: Option<Box<dyn PlanNode>> = if relation_field.kind.is_array() {
                parent_relation_filter_for_child
                    .as_ref()
                    .and_then(|rel_filter| {
                        let filter_index =
                            self.try_select_child_index(Some(rel_filter), None, &target_collection);
                        if let Some((params, _)) = filter_index {
                            // Build mapping with FK field and filter fields at schema positions
                            let mut filter_mapping = DocumentMapping::new();
                            filter_mapping.add(0, "_docID");
                            // Find the FK field on the child (target) side.
                            // For OneToMany, the child has the FK (e.g., Device has _ownerID).
                            // Find the back-reference field on the target collection.
                            let fk_field_name = relation_field
                                .relation_name
                                .as_ref()
                                .and_then(|rel_name| {
                                    target_collection.field_by_relation(
                                        rel_name,
                                        &parent_collection.name,
                                        relation_field_name,
                                    )
                                })
                                .map(|f| schema::CollectionVersion::relation_id_field_name(&f.name))
                                .unwrap_or_else(|| {
                                    schema::CollectionVersion::relation_id_field_name(
                                        relation_field_name,
                                    )
                                });
                            if let Some(fk_idx) = target_collection
                                .fields
                                .iter()
                                .position(|f| f.name == fk_field_name)
                            {
                                filter_mapping.add(fk_idx, &fk_field_name);
                            }
                            // Add filter referenced fields at schema positions
                            for field_name in rel_filter.referenced_fields() {
                                if filter_mapping.first_index_of_name(&field_name).is_none() {
                                    if let Some(idx) = target_collection
                                        .fields
                                        .iter()
                                        .position(|f| f.name == field_name)
                                    {
                                        filter_mapping.add(idx, &field_name);
                                    }
                                }
                            }
                            let mut index_scan = IndexScanNode::new(
                                (*target_collection).clone(),
                                filter_mapping,
                                params,
                            );
                            if let Some(ref fetcher) = self.fetcher {
                                index_scan = index_scan.with_fetcher(fetcher.clone());
                            }
                            Some(Box::new(index_scan) as Box<dyn PlanNode>)
                        } else {
                            None // No index for filter, use in-memory evaluation
                        }
                    })
            } else {
                None
            };

            // Extract nested limit/offset and order_by for per-parent application in TypeJoin.
            let nested_limit = nested_select.limit.as_ref().and_then(|l| l.limit);
            let nested_offset = nested_select.limit.as_ref().map(|l| l.offset).unwrap_or(0);
            let nested_order_by = nested_select.order_by.clone();

            // Build a combined filter from doc_ids and explicit filter
            let doc_ids_filter = if let Some(ref doc_ids) = nested_select.doc_ids {
                // Create a filter: _docID IN [...]
                if doc_ids.len() == 1 {
                    // Single ID: _docID == "..."
                    let mut conditions = HashMap::new();
                    conditions.insert("_docID".to_string(), serde_json::json!({"_eq": doc_ids[0]}));
                    Some(Filter::from_conditions(conditions))
                } else {
                    // Multiple IDs: _docID IN [...]
                    let mut conditions = HashMap::new();
                    conditions.insert("_docID".to_string(), serde_json::json!({"_in": doc_ids}));
                    Some(Filter::from_conditions(conditions))
                }
            } else {
                None
            };

            // Combine doc_ids filter with explicit filter
            let combined_filter = match (&doc_ids_filter, &nested_select.filter) {
                (Some(doc_filter), Some(explicit_filter)) => {
                    // Both: AND them together
                    Some(doc_filter.and(explicit_filter.clone()))
                }
                (Some(doc_filter), None) => Some(doc_filter.clone()),
                (None, Some(explicit_filter)) => Some(explicit_filter.clone()),
                (None, None) => None,
            };

            // Validate that all explicitly-filtered fields exist in the render mapping.
            if let Some(ref explicit_filter) = nested_select.filter {
                for field in explicit_filter.referenced_fields() {
                    if !child_render_mapping.has_field(&field) {
                        return Err(QueryError::filter_field_not_selected(
                            &field,
                            &target_collection.name,
                        ));
                    }
                }
            }

            // Recursively apply joins for any nested selections within this nested select.
            // This handles multi-level nesting like Users -> Posts -> Comments.
            // Note: We pass None for parent_filter since relation filters only apply at the top level.
            //
            // IMPORTANT: We do NOT reassign child_scan_mapping from the recursive result.
            // The recursive call may modify the mapping's nested child mappings (for deeper relations),
            // but the render_keys at THIS level were already correctly set when child_scan_mapping
            // was built. Reassigning would lose those render_keys, causing empty selection items
            // when both an aggregate and selection target the same relation.
            let nested_joins_result = self.apply_joins(
                child_plan,
                nested_select,
                &target_collection,
                child_scan_mapping.clone(),
                depth + 1,
                None, // Nested relation filters handled differently
            )?;
            child_plan = nested_joins_result.0;
            // Merge nested aggregate internal keys into our collection
            aggregate_internal_keys.extend(nested_joins_result.2);

            // Apply sub-joins for order_by references to relation fields within this nested select.
            // For example, if the nested select is `book(order: {publisher: {yearOpened: ASC}})`,
            // the child plan for Book needs a TypeJoinOne for Book→Publisher so the publisher
            // data is available for sorting.
            if let Some(ref order_by) = nested_select.order_by {
                // Collect relation fields already joined from the nested selection
                let already_joined: Vec<&str> = nested_select
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

                for condition in &order_by.conditions {
                    if condition.fields.len() >= 2 {
                        let order_relation_name = &condition.fields[0];
                        if already_joined.contains(&order_relation_name.as_str()) {
                            continue; // Already joined from selection
                        }
                        // Check if this is a relation field on the target collection
                        if let Some(order_rel_field) =
                            target_collection.field_by_name(order_relation_name)
                        {
                            if order_rel_field.kind.is_relation() {
                                let (new_child_plan, new_child_mapping) = self
                                    .apply_filter_relation_join(
                                        child_plan,
                                        &target_collection,
                                        order_rel_field,
                                        order_relation_name,
                                        child_scan_mapping.clone(),
                                    )?;
                                child_plan = new_child_plan;
                                child_scan_mapping = new_child_mapping;
                            }
                        }
                    }
                }
            }

            // Also apply sub-joins for order_by in aggregate targets.
            // For example, `_sum(book: {field: rating, order: {publisher: {yearOpened: ASC}}})`.
            for requestable in &select.fields {
                if let Requestable::Aggregate(agg) = requestable {
                    for target in &agg.targets {
                        if target.host_name != *relation_field_name {
                            continue;
                        }
                        if let Some(ref order) = target.order {
                            let already_joined: Vec<&str> = nested_select
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

                            for condition in &order.conditions {
                                if condition.fields.len() >= 2 {
                                    let order_relation_name = &condition.fields[0];
                                    if already_joined.contains(&order_relation_name.as_str()) {
                                        continue;
                                    }
                                    // Check if already joined from nested_select.order_by above
                                    if child_scan_mapping
                                        .first_index_of_name(order_relation_name)
                                        .is_some()
                                    {
                                        continue;
                                    }
                                    if let Some(order_rel_field) =
                                        target_collection.field_by_name(order_relation_name)
                                    {
                                        if order_rel_field.kind.is_relation() {
                                            let (new_child_plan, new_child_mapping) = self
                                                .apply_filter_relation_join(
                                                    child_plan,
                                                    &target_collection,
                                                    order_rel_field,
                                                    order_relation_name,
                                                    child_scan_mapping.clone(),
                                                )?;
                                            child_plan = new_child_plan;
                                            child_scan_mapping = new_child_mapping;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Apply sub-joins for filter relation fields in aggregate targets.
            // For example, _sum(book: {field: rating, filter: {publisher: {yearOpened: {_eq: 2013}}}})
            // needs a TypeJoinOne for publisher inside the book plan so publisher data appears
            // in rendered JSON for post-processing filter evaluation in compute_relation_aggregates.
            for requestable in &select.fields {
                if let Requestable::Aggregate(agg) = requestable {
                    for target in &agg.targets {
                        if target.host_name != *relation_field_name {
                            continue;
                        }
                        if let Some(ref filter) = target.filter {
                            for filter_field in filter.referenced_fields() {
                                if filter_field.starts_with('_') {
                                    continue;
                                }
                                if let Some(filter_rel_field) =
                                    target_collection.field_by_name(&filter_field)
                                {
                                    if filter_rel_field.kind.is_relation() {
                                        // Skip if already joined
                                        if let Some(rel_idx) =
                                            child_scan_mapping.first_index_of_name(&filter_field)
                                        {
                                            if child_scan_mapping.child_at(rel_idx).is_some() {
                                                continue;
                                            }
                                        }
                                        let (new_child_plan, new_child_mapping) = self
                                            .apply_filter_relation_join(
                                                child_plan,
                                                &target_collection,
                                                filter_rel_field,
                                                &filter_field,
                                                child_scan_mapping.clone(),
                                            )?;
                                        child_plan = new_child_plan;
                                        child_scan_mapping = new_child_mapping;
                                        // Aggregate filters evaluate on rendered JSON via
                                        // compute_relation_aggregates → matches_json_object,
                                        // so publisher must appear in the rendered output.
                                        if let Some(rel_idx) =
                                            child_scan_mapping.first_index_of_name(&filter_field)
                                        {
                                            if !child_scan_mapping
                                                .render_keys
                                                .iter()
                                                .any(|rk| rk.key == filter_field)
                                            {
                                                child_scan_mapping
                                                    .add_render_key(rel_idx, &filter_field);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Apply sub-joins for multi-level filter paths within this relation.
            // For example, if we're joining Book → Author and the filter has path
            // ["author", "published"], we need to add a sub-join for "published" here.
            // Skip relations already joined from the nested select's fields to avoid
            // duplicate sub-joins that would overwrite the selection's mapping.
            let nested_joined_relations: Vec<&str> = nested_select
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
            for remaining_path in &multi_level_paths_for_relation {
                if let Some(first_nested) = remaining_path.first() {
                    if nested_joined_relations.contains(&first_nested.as_str()) {
                        // Already joined from the nested selection (e.g., publisher in
                        // book { publisher { yearOpened } }). Skip to avoid overwriting
                        // the selection's child mapping with a full-field mapping.
                        continue;
                    }
                }
                let (new_child_plan, new_child_mapping) = self.apply_multi_level_sub_joins(
                    child_plan,
                    remaining_path,
                    &target_collection,
                    child_scan_mapping.clone(),
                )?;
                child_plan = new_child_plan;
                child_scan_mapping = new_child_mapping;
            }

            // Add TypeJoinOne sub-joins for relation fields in the nested select's own filter.
            // For example, book(filter: {publisher: {yearOpened: {_geq: 2020}}}) needs a
            // TypeJoinOne for publisher so the filter can evaluate on joined data.
            if let Some(ref explicit_filter) = nested_select.filter {
                for filter_field in explicit_filter.referenced_fields() {
                    if filter_field.starts_with('_') {
                        continue;
                    }
                    if let Some(filter_rel_field) = target_collection.field_by_name(&filter_field) {
                        if filter_rel_field.kind.is_relation() {
                            // Skip if already joined from selection or other sub-joins
                            if let Some(rel_idx) =
                                child_scan_mapping.first_index_of_name(&filter_field)
                            {
                                if child_scan_mapping.child_at(rel_idx).is_some() {
                                    continue;
                                }
                            }
                            let (new_plan, new_mapping) = self.apply_filter_relation_join(
                                child_plan,
                                &target_collection,
                                filter_rel_field,
                                &filter_field,
                                child_scan_mapping.clone(),
                            )?;
                            child_plan = new_plan;
                            child_scan_mapping = new_mapping;
                            // No render_key needed: SelectNode evaluates filters on raw
                            // Doc fields via DocumentMapping, not rendered JSON. Adding
                            // a render_key would leak the relation into the output.
                        }
                    }
                }
            }

            // Now wrap with SelectNode if there's a filter (deferred from earlier).
            // At this point, relation sub-joins are in place so the filter can evaluate
            // conditions on joined relation data (e.g., publisher.yearOpened).
            if let Some(ref filter) = combined_filter {
                child_plan = Box::new(
                    SelectNode::new(child_plan, child_scan_mapping.clone())
                        .with_filter(filter.clone()),
                );
            }

            // Insert ACP permission filter for the child collection (if ACP-protected).
            child_plan = self.maybe_wrap_with_acp_filter(child_plan, &target_collection);

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

            // Build RelationFilter from the already-extracted parent relation filter
            let relation_filter = parent_relation_filter_for_child
                .as_ref()
                .map(|nested_filter| RelationFilter {
                    relation_field: relation_field_name.clone(),
                    conditions: nested_filter.clone(),
                });

            // Create the appropriate join node
            // Note: We pass child_render_mapping as the output mapping (for TypeJoin to render children)
            // but the child_plan uses child_scan_mapping internally (for FK lookups)
            if relation_field.kind.is_array() {
                // One-to-many: TypeJoinMany
                let mut join_many =
                    TypeJoinMany::new(plan, child_plan, parent_side, child_side, mapping.clone())?;

                // Apply relation filter (filters parents by children)
                if let Some(rel_filter) = relation_filter {
                    join_many = join_many.with_relation_filter(rel_filter);
                }

                // Check if child has an index on its FK field for this relation.
                // When FK is indexed, a global child scan can efficiently map children
                // to parents, so per-parent scanning is not needed for ordering.
                let has_child_fk_index = target_relation_field
                    .and_then(|trf| {
                        let rel_name = trf.relation_name.as_deref()?;
                        // Find the FK ID field (same relation, kind Scalar(DocID))
                        let fk_field = target_collection.fields.iter().find(|f| {
                            f.relation_name.as_deref() == Some(rel_name)
                                && matches!(
                                    f.kind,
                                    schema::FieldKind::Scalar(schema::ScalarKind::DocID)
                                )
                        })?;
                        // Check if any index covers this FK field
                        target_collection
                            .indexes
                            .iter()
                            .find(|idx| idx.fields.first().is_some_and(|f| f.name == fk_field.name))
                    })
                    .is_some();

                // Determine per-parent mode before moving filter_child_plan.
                // Per-parent scanning re-inits the child plan for each parent:
                // - With limit: needed for early termination per parent
                // - With child sub-filter but no parent filter_child_plan: child filter
                //   runs per parent (when filter_child_plan exists, it handles globally)
                // - With ordering + no FK index + no parent filter: Go scans the
                //   ordering index per parent without FK index for efficient matching
                let use_per_parent = child_uses_index
                    && (nested_limit.is_some()
                        || (nested_select.filter.is_some() && filter_child_plan.is_none())
                        || (filter_child_plan.is_none()
                            && nested_order_by.is_some()
                            && !has_child_fk_index));

                // Apply filter child plan for indexed relation filter evaluation
                if let Some(fcp) = filter_child_plan {
                    join_many = join_many.with_filter_child_plan(fcp);
                }

                // Apply per-parent limit/offset/ordering
                if let Some(limit) = nested_limit {
                    join_many = join_many.with_limit(limit);
                }
                if nested_offset > 0 {
                    join_many = join_many.with_offset(nested_offset);
                }
                if let Some(order_by) = nested_order_by.clone() {
                    join_many = join_many.with_order_by(order_by);
                }
                if use_per_parent {
                    join_many = join_many.with_per_parent_child_scan();
                }

                // Apply nested groupBy if present
                if let Some(ref group_by) = nested_select.group_by {
                    join_many = join_many.with_group_by(group_by.clone());

                    // Find the _group nested select and build its mapping
                    // Use indices from child_scan_mapping so the mapping matches
                    // the child document's field array indices.
                    for field in &nested_select.fields {
                        if let Requestable::Select(group_select) = field {
                            if group_select.field.name == "_group" {
                                // Build mapping for _group contents using child_scan_mapping indices
                                let mut group_mapping = DocumentMapping::new();
                                for group_field in &group_select.fields {
                                    if let Requestable::Field(f) = group_field {
                                        // Use the index from child_scan_mapping
                                        if let Some(idx) =
                                            child_scan_mapping.first_index_of_name(&f.name)
                                        {
                                            let output_name = f.output_name().to_string();
                                            group_mapping.add(idx, &output_name);
                                            group_mapping.add_render_key(idx, output_name);
                                        }
                                    }
                                }
                                join_many = join_many.with_group_mapping(group_mapping);
                                break;
                            }
                        }
                    }
                }

                plan = Box::new(join_many);
            } else {
                // One-to-one: TypeJoinOne
                //
                // Check if we should invert the join for ordering:
                // When the parent's ORDER BY references a child field with an index,
                // invert the join so the child's sorted index scan drives iteration.
                let should_invert_for_ordering = parent_order_for_child.is_some()
                    && child_uses_index
                    && relation_filter.is_none(); // Don't invert if there's also a relation filter

                tracing::debug!(
                    parent_order_for_child_is_some = parent_order_for_child.is_some(),
                    child_uses_index = child_uses_index,
                    relation_filter_is_none = relation_filter.is_none(),
                    should_invert_for_ordering = should_invert_for_ordering,
                    relation_field_name = %relation_field.name,
                    "TypeJoinOne: checking ordering inversion"
                );

                if should_invert_for_ordering {
                    // Inverted join for ordering: child index scan drives iteration.
                    // Determine how to look up the parent for each child:
                    //
                    // Case 1 (primary-first): Child has FK (e.g., Device._ownerID → User)
                    //   - Read FK from child doc → direct docID lookup on parent
                    //
                    // Case 2 (secondary-first): Parent has FK (e.g., User._deviceID → Device)
                    //   - Scan parent's FK index for child._docID
                    let child_has_fk = target_relation_field.map(|f| f.is_primary).unwrap_or(false);

                    if child_has_fk {
                        // Case 1: Child has FK → use InvertedIndex with docID-based parent lookup.
                        // The child's FK field (e.g., _ownerID) contains the parent's _docID.
                        let child_fk_field_name = target_relation_field
                            .map(|f| schema::CollectionVersion::relation_id_field_name(&f.name))
                            .unwrap_or_default();
                        let child_fk_idx = child_scan_mapping
                            .first_index_of_name(&child_fk_field_name)
                            .unwrap_or(0);

                        let parent_scan_mapping = plan.document_map().clone();
                        let parent_col = parent_collection.clone();
                        let fetcher = self.fetcher.clone().unwrap();

                        let join = TypeJoinOne::new(
                            plan,
                            child_plan,
                            parent_side,
                            child_side,
                            mapping.clone(),
                        )
                        .with_ordered_inverted_primary(
                            child_fk_idx,
                            parent_col,
                            parent_scan_mapping,
                            fetcher,
                        );
                        plan = Box::new(join);
                        join_provides_ordering = true;
                    } else {
                        // Case 2: Parent has FK → use InvertedIndex with FK index scan on parent.
                        // Same mechanism as filter-based InvertedIndex.
                        let parent_fk_field_name =
                            schema::CollectionVersion::relation_id_field_name(&relation_field.name);
                        let parent_fk_index = parent_collection.indexes.iter().find(|idx| {
                            idx.fields
                                .first()
                                .is_some_and(|f| f.name == parent_fk_field_name)
                        });

                        if let Some(fk_index) = parent_fk_index {
                            let fk_index_name = fk_index.name.clone();
                            let parent_scan_mapping = plan.document_map().clone();
                            let parent_col = parent_collection.clone();
                            let fk_field_index = parent_scan_mapping
                                .first_index_of_name(&parent_fk_field_name)
                                .unwrap_or(0);
                            let fetcher = self.fetcher.clone().unwrap();

                            let join = TypeJoinOne::new(
                                plan,
                                child_plan,
                                parent_side,
                                child_side,
                                mapping.clone(),
                            )
                            .with_inverted_index(
                                fk_index_name,
                                fk_field_index,
                                parent_col,
                                parent_scan_mapping,
                                fetcher,
                            );
                            plan = Box::new(join);
                            join_provides_ordering = true;
                        } else {
                            // No FK index on parent → fall back to normal join + OrderByNode
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
                } else {
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

        // Handle relation filters without corresponding selections.
        // When a filter references a relation (e.g., `Author(filter: {published: {_docID: ...}})`),
        // but the relation is not selected, we still need to create a join to filter the parent.
        if let Some(filter) = parent_filter {
            // Get relations referenced by the filter
            for (relation_name, nested_conditions) in filter.relation_conditions() {
                // Skip if already joined via selection
                let already_joined = select
                    .fields
                    .iter()
                    .any(|f| matches!(f, Requestable::Select(s) if s.field.name == relation_name));
                if already_joined {
                    continue;
                }

                // Find the relation field in the parent collection
                let relation_field = match parent_collection.field_by_name(&relation_name) {
                    Some(f) if f.kind.is_relation() => f,
                    _ => continue, // Not a relation field
                };

                // Get the target collection
                let target_collection_id = match relation_field.kind.relation_collection_id() {
                    Some(id) => id,
                    None => continue,
                };

                let target_collection = if target_collection_id.is_empty() {
                    Arc::new(parent_collection.clone())
                } else {
                    match self.get_collection(target_collection_id) {
                        Some(c) => c,
                        None => continue,
                    }
                };

                // Find the target relation field (the other side of the relation)
                let target_relation_field = if let Some(rel_name) = &relation_field.relation_name {
                    target_collection.field_by_relation(
                        rel_name,
                        &parent_collection.name,
                        &relation_name,
                    )
                } else {
                    None
                };

                // Build child mapping for filter-only join
                // Include _docID and the FK field for the join to work correctly
                let mut child_mapping = DocumentMapping::new();
                child_mapping.add(0, "_docID");
                child_mapping.add_render_key(0, "_docID");

                // Add the FK field (e.g., _authorID) - needed for TypeJoinMany cache indexing
                let fk_field_name = if let Some(target_rel) = target_relation_field {
                    schema::CollectionVersion::relation_id_field_name(&target_rel.name)
                } else {
                    schema::CollectionVersion::relation_id_field_name(&relation_name)
                };
                if let Some(fk_idx) = target_collection
                    .fields
                    .iter()
                    .position(|f| f.name == fk_field_name)
                {
                    child_mapping.add(fk_idx, &fk_field_name);
                    child_mapping.add_render_key(fk_idx, &fk_field_name);
                }

                // Add any fields referenced by the filter
                for field_name in nested_conditions.referenced_fields() {
                    if field_name != "_docID" && field_name != fk_field_name {
                        if let Some(idx) = target_collection
                            .fields
                            .iter()
                            .position(|f| f.name == field_name)
                        {
                            child_mapping.add(idx, &field_name);
                            child_mapping.add_render_key(idx, &field_name);
                        }
                    }
                }

                // Build child scan, using an index if the filter is index-eligible.
                let child_index_result =
                    self.try_select_child_index(Some(&nested_conditions), None, &target_collection);
                let child_has_index = child_index_result.is_some();
                let child_plan: Box<dyn PlanNode> = if let Some((params, _)) = child_index_result {
                    let mut index_scan = IndexScanNode::new(
                        (*target_collection).clone(),
                        child_mapping.clone(),
                        params,
                    );
                    if let Some(ref fetcher) = self.fetcher {
                        index_scan = index_scan.with_fetcher(fetcher.clone());
                    }
                    Box::new(index_scan)
                } else {
                    let mut child_scan =
                        ScanNode::new((*target_collection).clone(), child_mapping.clone());
                    if let Some(ref fetcher) = self.fetcher {
                        child_scan = child_scan.with_fetcher(fetcher.clone());
                    }
                    Box::new(child_scan)
                };

                // Get field indices
                let relation_field_index = mapping
                    .first_index_of_name(&relation_name)
                    .unwrap_or_else(|| {
                        // Add to mapping if not present
                        let idx = mapping.next_index();
                        mapping.add(idx, &relation_name);
                        idx
                    });

                mapping.set_child_at(relation_field_index, child_mapping.clone());

                // Find child relation index
                let child_relation_index = target_relation_field
                    .as_ref()
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

                // Create the relation filter
                let rel_filter = RelationFilter {
                    relation_field: relation_name.clone(),
                    conditions: nested_conditions.clone(),
                };

                // Create the appropriate join node based on cardinality
                if relation_field.kind.is_array() {
                    // One-to-many: TypeJoinMany with filter
                    let join_many = TypeJoinMany::new(
                        plan,
                        child_plan,
                        parent_side,
                        child_side,
                        mapping.clone(),
                    )?
                    .with_relation_filter(rel_filter);
                    plan = Box::new(join_many);
                } else {
                    // One-to-one (or many-to-one): TypeJoinOne with filter
                    // Check if we should use inverted index join:
                    // child has index on filtered field AND parent has index on FK field.
                    let parent_fk_field_name =
                        schema::CollectionVersion::relation_id_field_name(&relation_field.name);
                    let parent_fk_index = if child_has_index {
                        parent_collection.indexes.iter().find(|idx| {
                            idx.fields
                                .first()
                                .is_some_and(|f| f.name == parent_fk_field_name)
                        })
                    } else {
                        None
                    };

                    if let Some(fk_index) = parent_fk_index {
                        // Inverted index join: child scanned with index, parent looked up per-child
                        let fk_index_name = fk_index.name.clone();
                        let parent_scan_mapping = plan.document_map().clone();
                        let parent_col = parent_collection.clone();
                        let fk_field_index = parent_scan_mapping
                            .first_index_of_name(&parent_fk_field_name)
                            .unwrap_or(0);
                        let fetcher = self.fetcher.clone().unwrap();

                        let join = TypeJoinOne::new(
                            plan,
                            child_plan,
                            parent_side,
                            child_side,
                            mapping.clone(),
                        )
                        .with_relation_filter(rel_filter)
                        .with_inverted_index(
                            fk_index_name,
                            fk_field_index,
                            parent_col,
                            parent_scan_mapping,
                            fetcher,
                        );
                        plan = Box::new(join);
                    } else {
                        let join = TypeJoinOne::new(
                            plan,
                            child_plan,
                            parent_side,
                            child_side,
                            mapping.clone(),
                        )
                        .with_relation_filter(rel_filter);
                        plan = Box::new(join);
                    }
                }
            }
        }

        // Handle secondary relation ID fields (e.g., `_authorID` for a secondary `author` relation).
        // When a secondary relation ID field is selected but the relation object is not,
        // we need to add a TypeJoin to compute the ID by doing a reverse lookup.
        for requestable in &select.fields {
            if let Requestable::Field(field) = requestable {
                // Check if this is a relation ID field (pattern: `_{relationName}ID`)
                let field_name = &field.name;
                if !field_name.starts_with('_') || !field_name.ends_with("ID") {
                    continue;
                }

                // Extract the relation name from the field name (e.g., "author" from "_authorID")
                let relation_name = &field_name[1..field_name.len() - 2];
                if relation_name.is_empty() {
                    continue;
                }

                // Find the relation field in the parent collection
                let relation_field = match parent_collection.field_by_name(relation_name) {
                    Some(f) => f,
                    None => continue, // Not a valid relation name
                };

                // Check if it's a relation field and NOT primary (secondary relation)
                if !relation_field.kind.is_relation() || relation_field.is_primary {
                    continue; // Only handle secondary relations
                }

                // Check if this relation is already being joined (via a Select)
                let already_joined = select.fields.iter().any(|f| {
                    if let Requestable::Select(s) = f {
                        s.field.name == relation_name
                    } else {
                        false
                    }
                });

                if already_joined {
                    continue; // Join already exists, _relID will be populated by merge_child
                }

                // Get the target collection
                let target_collection_id = match relation_field.kind.relation_collection_id() {
                    Some(id) => id,
                    None => continue,
                };

                let target_collection = if target_collection_id.is_empty() {
                    Arc::new(parent_collection.clone())
                } else {
                    match self.get_collection(target_collection_id) {
                        Some(c) => c,
                        None => continue,
                    }
                };

                // Find the target relation field (the other side of the relation)
                let target_relation_field = if let Some(rel_name) = &relation_field.relation_name {
                    target_collection.field_by_relation(
                        rel_name,
                        &parent_collection.name,
                        relation_name,
                    )
                } else {
                    None
                };

                // Build a minimal child mapping (just _docID for the reverse lookup).
                // Include render_key so the merged child renders with _docID for groupBy.
                let mut child_mapping = DocumentMapping::new();
                child_mapping.add(0, "_docID");
                child_mapping.add_render_key(0, "_docID");

                // Build scan mapping for the child
                let child_scan_mapping =
                    self.build_scan_mapping_for_join(&target_collection, &child_mapping);

                // Get relation field index in parent mapping
                let relation_field_index = parent_collection
                    .fields
                    .iter()
                    .position(|f| f.name == relation_name)
                    .unwrap_or(0);

                // Set up child mapping in parent for TypeJoin
                mapping.set_child_at(relation_field_index, child_scan_mapping.clone());

                // Build child plan (simple scan with fetcher)
                let mut child_scan =
                    ScanNode::new((*target_collection).clone(), child_scan_mapping.clone());
                if let Some(ref fetcher) = self.fetcher {
                    child_scan = child_scan.with_fetcher(fetcher.clone());
                }
                let child_plan: Box<dyn PlanNode> = Box::new(child_scan);

                // Get child relation field index
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

                // Create TypeJoinOne for the secondary relation ID lookup
                // This join will populate the _relID field via merge_child
                plan = Box::new(TypeJoinOne::new(
                    plan,
                    child_plan,
                    parent_side,
                    child_side,
                    mapping.clone(),
                ));

                debug!(
                    parent_collection = %parent_collection.name,
                    target_collection = %target_collection.name,
                    relation_id_field = %field_name,
                    relation_field = %relation_name,
                    relation_field_is_primary = %relation_field.is_primary,
                    target_relation_field_name = ?target_relation_field.as_ref().map(|f| &f.name),
                    target_relation_field_is_primary = ?target_relation_field.as_ref().map(|f| f.is_primary),
                    "Added TypeJoinOne for secondary relation ID field"
                );
            }
        }

        // Also handle relation-based and inline-array aggregates.
        // Relation aggregates (e.g., _count(books: {})) need joins to fetch data.
        // Inline array aggregates (e.g., _count(favouriteIntegers: {})) need the
        // array field added to the render mapping so the data appears in output.
        let mut aggregate_joined_relations: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        for requestable in &select.fields {
            if let Requestable::Aggregate(agg) = requestable {
                for target in &agg.targets {
                    // Only handle aggregates with a named target (non-empty host_name)
                    if target.host_name.is_empty() {
                        continue;
                    }

                    let relation_field_name = &target.host_name;

                    // Check if this relation is already joined by a prior aggregate
                    // or by a selection. Multiple aggregates on the same relation share
                    // one TypeJoinMany; compute_relation_aggregates() handles per-aggregate
                    // limit/offset/order in post-processing.
                    // Go also shares joins between selections and aggregates targeting
                    // the same relation (e.g., books(filter: X) + _count(books: {filter: X})).
                    let already_joined = aggregate_joined_relations
                        .contains(relation_field_name.as_str())
                        || selection_join_info
                            .get(relation_field_name.as_str())
                            .is_some_and(|info| {
                                if info.has_limit {
                                    return false;
                                }
                                // If aggregate has no filter but specifies a field_name, it's a
                                // field-level operation (e.g. _avg(books: {field: rating})) that
                                // piggybacks on the selection's join. Share unconditionally.
                                if target.filter.is_none() {
                                    return target.field_name.is_some();
                                }
                                // If aggregate has a filter, share only if it matches the
                                // selection's filter exactly.
                                let agg_filter_json = target.filter.as_ref().map(|f| {
                                    serde_json::to_string(f.conditions()).unwrap_or_default()
                                });
                                info.filter_json == agg_filter_json
                            });

                    // Find the field in the parent collection
                    let relation_field = match parent_collection.field_by_name(relation_field_name)
                    {
                        Some(f) => f,
                        None => continue,
                    };

                    // Inline array fields are handled by scan_mapping setup
                    // in build_plan() — no join needed.
                    if !relation_field.kind.is_relation() {
                        continue;
                    }

                    // Get the target collection
                    let target_collection_id = match relation_field.kind.relation_collection_id() {
                        Some(id) => id,
                        None => continue,
                    };

                    let target_collection = if target_collection_id.is_empty() {
                        Arc::new(parent_collection.clone())
                    } else {
                        match self.get_collection(target_collection_id) {
                            Some(c) => c,
                            None => {
                                // CID lookup failed - try to find target by matching relation_name.
                                // This handles cases where the relation's collection_id CID differs
                                // from the target collection's current collection_id/version_id
                                // (e.g., circular schema definitions with set-based versioning).
                                let parent_rel_name =
                                    relation_field.relation_name.as_deref().unwrap_or("");
                                let mut found_by_relation = None;
                                if !parent_rel_name.is_empty() {
                                    for coll in self.collections.values() {
                                        if coll.name == parent_collection.name {
                                            continue;
                                        }
                                        for f in &coll.fields {
                                            if f.relation_name.as_deref() == Some(parent_rel_name) {
                                                found_by_relation = Some(coll.clone());
                                                break;
                                            }
                                        }
                                        if found_by_relation.is_some() {
                                            break;
                                        }
                                    }
                                }
                                match found_by_relation {
                                    Some(c) => c,
                                    None => continue,
                                }
                            }
                        }
                    };

                    // Get relation field index in parent collection (needed in both paths)
                    let relation_field_index = parent_collection
                        .fields
                        .iter()
                        .position(|f| f.name == *relation_field_name)
                        .unwrap_or(0);

                    // If the relation is already joined via a selection, we still need to
                    // ensure the filter fields are available in the output for post-processing.
                    // Add filter fields to the existing child mapping.
                    if already_joined {
                        // Add filter fields from aggregate target to existing child mapping.
                        // Always ensure render_key exists even when the field is already
                        // in the mapping (build_scan_mapping_for_join adds ALL fields but
                        // only adds render_keys for explicitly selected ones).
                        if let Some(ref filter) = target.filter {
                            for filter_field in filter.referenced_fields() {
                                if filter_field.starts_with('_') {
                                    continue;
                                }
                                if let Some(idx) = target_collection
                                    .fields
                                    .iter()
                                    .position(|f| f.name == filter_field)
                                {
                                    if let Some(child_mapping) =
                                        mapping.child_at_mut(relation_field_index)
                                    {
                                        if child_mapping
                                            .first_index_of_name(&filter_field)
                                            .is_none()
                                        {
                                            child_mapping.add(idx, &filter_field);
                                        }
                                        if !child_mapping
                                            .render_keys
                                            .iter()
                                            .any(|rk| rk.key == filter_field)
                                        {
                                            child_mapping.add_render_key(idx, &filter_field);
                                        }
                                    }
                                }
                            }
                        }
                        // Add order fields from aggregate target to existing child mapping.
                        // Always ensure render_key exists even when the field is already
                        // in the mapping (build_scan_mapping_for_join adds ALL fields but
                        // only adds render_keys for explicitly selected ones).
                        if let Some(ref order) = target.order {
                            for condition in &order.conditions {
                                if condition.fields.is_empty()
                                    || condition.fields[0].starts_with('_')
                                {
                                    continue;
                                }
                                let order_field = &condition.fields[0];
                                if let Some(idx) = target_collection
                                    .fields
                                    .iter()
                                    .position(|f| f.name == *order_field)
                                {
                                    if let Some(child_mapping) =
                                        mapping.child_at_mut(relation_field_index)
                                    {
                                        if child_mapping.first_index_of_name(order_field).is_none()
                                        {
                                            child_mapping.add(idx, order_field);
                                        }
                                        if !child_mapping
                                            .render_keys
                                            .iter()
                                            .any(|rk| rk.key == *order_field)
                                        {
                                            child_mapping.add_render_key(idx, order_field);
                                        }
                                    }
                                }
                            }
                        }
                        continue;
                    }

                    // Build a minimal child mapping for the aggregate
                    // For count, we just need to fetch the documents
                    // For sum/avg, we need the specific field
                    // For any aggregate with filter, we need the filter fields
                    let mut child_mapping = DocumentMapping::new();
                    child_mapping.add(0, "_docID");

                    // If there's a field to aggregate, add it with render_key
                    if let Some(ref field_name) = target.field_name {
                        if let Some(idx) = target_collection
                            .fields
                            .iter()
                            .position(|f| f.name == *field_name)
                        {
                            child_mapping.add(idx, field_name);
                            child_mapping.add_render_key(idx, field_name);
                        }
                    }

                    // Add fields referenced by the filter so they appear in the output
                    // for post-processing filter evaluation
                    if let Some(ref filter) = target.filter {
                        for filter_field in filter.referenced_fields() {
                            // Skip special fields
                            if filter_field.starts_with('_') {
                                continue;
                            }
                            // Find the field in the target collection
                            if let Some(idx) = target_collection
                                .fields
                                .iter()
                                .position(|f| f.name == filter_field)
                            {
                                // Add to child mapping if not already present
                                if child_mapping.first_index_of_name(&filter_field).is_none() {
                                    child_mapping.add(idx, &filter_field);
                                    child_mapping.add_render_key(idx, &filter_field);
                                }
                            }
                        }
                    }

                    // Add fields referenced by the order so they appear in the output
                    // for post-processing sort before limit/offset
                    if let Some(ref order) = target.order {
                        for condition in &order.conditions {
                            if let Some(order_field) = condition.fields.first() {
                                if order_field.starts_with('_') {
                                    continue;
                                }
                                if let Some(idx) = target_collection
                                    .fields
                                    .iter()
                                    .position(|f| f.name == *order_field)
                                {
                                    if child_mapping.first_index_of_name(order_field).is_none() {
                                        child_mapping.add(idx, order_field);
                                        child_mapping.add_render_key(idx, order_field);
                                    }
                                }
                            }
                        }
                    }

                    // Build scan mapping for the child
                    let mut child_scan_mapping =
                        self.build_scan_mapping_for_join(&target_collection, &child_mapping);

                    // Determine the mapping index for this aggregate's relation data.
                    // When a selection already uses this relation (e.g., `books2020: book(...)`),
                    // we need a separate index so the aggregate gets independent, unfiltered data.
                    // We also need a unique internal key to avoid collision with the selection's
                    // limited/filtered data.
                    let selection_has_relation = select.fields.iter().any(|f| {
                        if let Requestable::Select(s) = f {
                            s.field.name == *relation_field_name
                        } else {
                            false
                        }
                    });
                    let effective_relation_index = if selection_has_relation {
                        // Selection already uses relation_field_index with its own filter/limit.
                        // Use a new index and a unique internal key for the aggregate's data
                        // to avoid collision with the selection's data in rendered JSON.
                        let idx = mapping.next_index();
                        let internal_key =
                            format!("__agg_{}_{}", relation_field_name, agg.output_name());
                        mapping.add(idx, relation_field_name);
                        mapping.add_render_key(idx, &internal_key);
                        // Store the mapping so the runner can look up data using the internal key
                        aggregate_internal_keys.insert(
                            agg.output_name().to_string(),
                            (relation_field_name.clone(), internal_key),
                        );
                        idx
                    } else {
                        if mapping.first_index_of_name(relation_field_name).is_none() {
                            mapping.add(relation_field_index, relation_field_name);
                        }
                        mapping.add_render_key(relation_field_index, relation_field_name);
                        relation_field_index
                    };

                    // Build child plan (simple scan with fetcher)
                    let mut child_scan =
                        ScanNode::new((*target_collection).clone(), child_scan_mapping.clone());
                    if let Some(ref fetcher) = self.fetcher {
                        child_scan = child_scan.with_fetcher(fetcher.clone());
                    }
                    // Apply aggregate target filter to the scan node.
                    // Go places these filters on the scanNode in explain output.
                    // Also synthesize {field: {_neq: null}} for Average aggregates
                    // to exclude null values (matching Go behavior).
                    let mut scan_filter = target.filter.clone();
                    if agg.aggregate_type == AggregateType::Average {
                        if let Some(ref field_name) = target.field_name {
                            let neq_null_filter = Filter::from_conditions({
                                let mut c = HashMap::new();
                                c.insert(
                                    field_name.clone(),
                                    serde_json::json!({"_neq": serde_json::Value::Null}),
                                );
                                c
                            });
                            scan_filter = Some(match scan_filter {
                                Some(existing) => {
                                    // Merge {field: {_neq: null}} into existing conditions
                                    let mut merged = existing.conditions().clone();
                                    merged
                                        .entry(field_name.clone())
                                        .and_modify(|v| {
                                            if let serde_json::Value::Object(ref mut ops) = v {
                                                ops.insert(
                                                    "_neq".to_string(),
                                                    serde_json::Value::Null,
                                                );
                                            }
                                        })
                                        .or_insert(
                                            serde_json::json!({"_neq": serde_json::Value::Null}),
                                        );
                                    Filter::from_conditions(merged)
                                }
                                None => neq_null_filter,
                            });
                        }
                    }
                    if let Some(ref filter) = scan_filter {
                        if !filter.has_relation_filters() {
                            child_scan = child_scan.with_filter(filter.clone());
                        }
                    }
                    let mut child_plan: Box<dyn PlanNode> = Box::new(child_scan);

                    // Add sub-joins for deep order relation fields (e.g., publisher.yearOpened).
                    // When the aggregate order references a nested relation, we need a TypeJoinOne
                    // inside the child plan so the relation data is available for sorting.
                    if let Some(ref order) = target.order {
                        for condition in &order.conditions {
                            if condition.fields.len() >= 2 {
                                let order_relation_name = &condition.fields[0];
                                if let Some(order_rel_field) =
                                    target_collection.field_by_name(order_relation_name)
                                {
                                    if order_rel_field.kind.is_relation() {
                                        let (new_plan, new_mapping) = self
                                            .apply_filter_relation_join(
                                                child_plan,
                                                &target_collection,
                                                order_rel_field,
                                                order_relation_name,
                                                child_scan_mapping.clone(),
                                            )?;
                                        child_plan = new_plan;
                                        child_scan_mapping = new_mapping;
                                        // Ensure render_key for the relation so it appears
                                        // in rendered JSON for post-processing sort
                                        if let Some(rel_idx) = child_scan_mapping
                                            .first_index_of_name(order_relation_name)
                                        {
                                            if !child_scan_mapping
                                                .render_keys
                                                .iter()
                                                .any(|rk| rk.key == *order_relation_name)
                                            {
                                                child_scan_mapping
                                                    .add_render_key(rel_idx, order_relation_name);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // Add sub-joins for deep filter relation fields (e.g., publisher.yearOpened).
                    // When the aggregate filter references a relation, we need a TypeJoinOne
                    // inside the child plan so the relation data is available for filter evaluation.
                    if let Some(ref filter) = target.filter {
                        for filter_field in filter.referenced_fields() {
                            if filter_field.starts_with('_') {
                                continue;
                            }
                            if let Some(filter_rel_field) =
                                target_collection.field_by_name(&filter_field)
                            {
                                if filter_rel_field.kind.is_relation() {
                                    // Skip if a sub-join already exists (from order handling)
                                    if let Some(rel_idx) =
                                        child_scan_mapping.first_index_of_name(&filter_field)
                                    {
                                        if child_scan_mapping.child_at(rel_idx).is_some() {
                                            continue;
                                        }
                                    }
                                    let (new_plan, new_mapping) = self.apply_filter_relation_join(
                                        child_plan,
                                        &target_collection,
                                        filter_rel_field,
                                        &filter_field,
                                        child_scan_mapping.clone(),
                                    )?;
                                    child_plan = new_plan;
                                    child_scan_mapping = new_mapping;
                                    // Ensure render_key for the relation so it appears
                                    // in rendered JSON for post-processing filter
                                    if let Some(rel_idx) =
                                        child_scan_mapping.first_index_of_name(&filter_field)
                                    {
                                        if !child_scan_mapping
                                            .render_keys
                                            .iter()
                                            .any(|rk| rk.key == filter_field)
                                        {
                                            child_scan_mapping
                                                .add_render_key(rel_idx, &filter_field);
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // Insert ACP permission filter for the aggregate child collection (if ACP-protected).
                    child_plan = self.maybe_wrap_with_acp_filter(child_plan, &target_collection);

                    // Set up child mapping in parent for TypeJoin (after sub-join modifications)
                    mapping.set_child_at(effective_relation_index, child_scan_mapping.clone());

                    // Find the back-reference field
                    let target_relation_field = target_collection.fields.iter().find(|f| {
                        if !f.kind.is_relation() {
                            return false;
                        }
                        if let Some(rel_id) = f.kind.relation_collection_id() {
                            rel_id == parent_collection.version_id
                                || rel_id == parent_collection.name
                        } else {
                            false
                        }
                    });

                    let child_relation_index = target_relation_field
                        .and_then(|f| {
                            target_collection
                                .fields
                                .iter()
                                .position(|tf| tf.name == f.name)
                        })
                        .unwrap_or(0);

                    // Create join sides - use effective_relation_index so children
                    // are stored at the correct (possibly new) index
                    let parent_side = JoinSide::new(
                        parent_collection.clone(),
                        relation_field.clone(),
                        effective_relation_index,
                    )?;

                    let child_side = JoinSide::new(
                        (*target_collection).clone(),
                        target_relation_field
                            .cloned()
                            .unwrap_or_else(|| relation_field.clone()),
                        child_relation_index,
                    )?;

                    // For aggregates, always use TypeJoinMany since we're aggregating an array
                    // The aggregate field name becomes the key in the mapping
                    let aggregate_key = agg.output_name();

                    // Add the aggregate's output name to the mapping for later processing
                    if mapping.first_index_of_name(aggregate_key).is_none() {
                        let idx = mapping.next_index();
                        mapping.add(idx, aggregate_key);
                        // Note: render_key is already added in build_mapping_for_select
                    }

                    plan = Box::new(TypeJoinMany::new(
                        plan,
                        child_plan,
                        parent_side,
                        child_side,
                        mapping.clone(),
                    )?);

                    aggregate_joined_relations.insert(relation_field_name.to_string());
                }
            }
        }

        Ok((
            plan,
            mapping,
            aggregate_internal_keys,
            join_provides_ordering,
        ))
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
    pub(super) fn build_scan_mapping_for_join(
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

        // Copy _deleted virtual field from render_mapping if present.
        // _deleted is not in the schema, so it must be explicitly added to the scan mapping.
        if let Some(deleted_render_idx) = render_mapping.first_index_of_name("_deleted") {
            let new_index = mapping.next_index();
            mapping.add(new_index, "_deleted");
            for rk in &render_mapping.render_keys {
                if rk.index == deleted_render_idx && rk.key == "_deleted" {
                    mapping.add_render_key(new_index, &rk.key);
                    break;
                }
            }
        }

        // Copy type_info from render_mapping if set (for __typename support)
        // Also need to copy the __typename render_key since it's a virtual field not in schema
        // IMPORTANT: Use collection.name as the type name, not the one from render_mapping,
        // because nested selects have collection_name=field_name (e.g., "author") not the
        // actual collection name (e.g., "Author")
        if render_mapping.type_name().is_some() {
            mapping.set_type_name(&collection.name);
            // Find the __typename render_key in render_mapping and copy it
            if let Some(typename_index) = mapping.first_index_of_name("__typename") {
                for rk in &render_mapping.render_keys {
                    // Find the render_key for __typename (key might be __typename or an alias)
                    if let Some(field_name) = render_mapping.try_find_name_from_index(rk.index) {
                        if field_name == "__typename" {
                            mapping.add_render_key(typename_index, &rk.key);
                            break;
                        }
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
    ///
    /// Returns both the updated plan and mapping. The mapping is updated with child mappings
    /// for the relation field so that post-join filter evaluation can traverse into the
    /// merged child document.
    pub(super) fn apply_filter_relation_join(
        &self,
        plan: Box<dyn PlanNode>,
        parent_collection: &CollectionVersion,
        relation_field: &schema::FieldDescription,
        relation_field_name: &str,
        mut mapping: DocumentMapping,
    ) -> Result<(Box<dyn PlanNode>, DocumentMapping)> {
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
        let mut child_plan: Box<dyn PlanNode> = Box::new(child_scan);

        // Insert ACP permission filter for the child collection (if ACP-protected).
        child_plan = self.maybe_wrap_with_acp_filter(child_plan, &target_collection);

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

        // Create the appropriate join type based on the relation cardinality.
        // One-to-many relations need TypeJoinMany to collect all children into an array,
        // one-to-one relations use TypeJoinOne.
        if relation_field.kind.is_array() {
            let join_many =
                TypeJoinMany::new(plan, child_plan, parent_side, child_side, mapping.clone())?;
            Ok((Box::new(join_many), mapping))
        } else {
            let join = TypeJoinOne::new(plan, child_plan, parent_side, child_side, mapping.clone());
            Ok((Box::new(join), mapping))
        }
    }

    /// Apply sub-joins for remaining elements of a multi-level filter path.
    ///
    /// This is called when processing a relation that's the start of a multi-level filter path.
    /// For example, when processing the "author" relation and the filter has path ["author", "published"],
    /// this method adds a sub-join for "published" within the author's child plan.
    ///
    /// Returns both the updated plan and the updated mapping, since the mapping must be modified
    /// to include the child mappings for the nested relations.
    pub(super) fn apply_multi_level_sub_joins(
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
            let mut child_plan: Box<dyn PlanNode> = Box::new(child_scan);

            // Insert ACP permission filter for the child collection (if ACP-protected).
            child_plan = self.maybe_wrap_with_acp_filter(child_plan, &target_collection);

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
    pub(super) fn apply_multi_level_filter_joins(
        &self,
        plan: Box<dyn PlanNode>,
        path: &[String],
        start_collection: &CollectionVersion,
        _filter: &crate::mapper::Filter,
        mut mapping: DocumentMapping,
    ) -> Result<(Box<dyn PlanNode>, DocumentMapping)> {
        if path.is_empty() {
            return Ok((plan, mapping));
        }

        // Handle only the first level here; remaining levels are nested sub-joins
        // within the first level's child plan.
        let first_name = &path[0];
        let first_field = start_collection
            .field_by_name(first_name)
            .ok_or_else(|| QueryError::unknown_field(first_name))?;

        if !first_field.kind.is_relation() {
            return Err(QueryError::execution(format!(
                "field '{}' on collection '{}' is not a relation",
                first_name, start_collection.name
            )));
        }

        let target_collection_id = first_field.kind.relation_collection_id().ok_or_else(|| {
            QueryError::internal(format!(
                "relation field '{}' has no target collection",
                first_name
            ))
        })?;

        let target_collection = if target_collection_id.is_empty() {
            Arc::new(start_collection.clone())
        } else {
            self.get_collection(target_collection_id)
                .ok_or_else(|| QueryError::collection_not_found(target_collection_id))?
        };

        // Build child scan mapping with all fields for filter evaluation
        let mut child_scan_mapping = {
            let mut m = DocumentMapping::new();
            for (i, field) in target_collection.fields.iter().enumerate() {
                m.add(i, &field.name);
                m.add_render_key(i, &field.name);
            }
            m
        };

        let relation_field_index = mapping.first_index_of_name(first_name).ok_or_else(|| {
            QueryError::internal(format!(
                "relation field '{}' not in parent mapping",
                first_name
            ))
        })?;

        // Build child scan
        let mut child_scan =
            ScanNode::new((*target_collection).clone(), child_scan_mapping.clone());
        if let Some(ref fetcher) = self.fetcher {
            child_scan = child_scan.with_fetcher(fetcher.clone());
        }
        let mut child_plan: Box<dyn PlanNode> = Box::new(child_scan);

        // Insert ACP permission filter for the child collection (if ACP-protected).
        child_plan = self.maybe_wrap_with_acp_filter(child_plan, &target_collection);

        // Add sub-joins for remaining path levels within the child plan
        if path.len() > 1 {
            let remaining = &path[1..];
            let (new_plan, new_mapping) = self.apply_multi_level_sub_joins(
                child_plan,
                remaining,
                &target_collection,
                child_scan_mapping,
            )?;
            child_plan = new_plan;
            child_scan_mapping = new_mapping;
        }

        // Set child mapping in parent
        mapping.set_child_at(relation_field_index, child_scan_mapping.clone());

        // Find the other side of the relation
        let target_relation_field = if let Some(rel_name) = &first_field.relation_name {
            target_collection.field_by_relation(rel_name, &start_collection.name, first_name)
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

        let parent_side = JoinSide::new(
            start_collection.clone(),
            first_field.clone(),
            relation_field_index,
        )?;

        let child_side = JoinSide::new(
            (*target_collection).clone(),
            target_relation_field
                .cloned()
                .unwrap_or_else(|| first_field.clone()),
            child_relation_index,
        )?;

        // Create appropriate join based on relation cardinality
        let new_plan = if first_field.kind.is_array() {
            Box::new(TypeJoinMany::new(
                plan,
                child_plan,
                parent_side,
                child_side,
                mapping.clone(),
            )?) as Box<dyn PlanNode>
        } else {
            Box::new(TypeJoinOne::new(
                plan,
                child_plan,
                parent_side,
                child_side,
                mapping.clone(),
            )) as Box<dyn PlanNode>
        };

        Ok((new_plan, mapping))
    }
}
