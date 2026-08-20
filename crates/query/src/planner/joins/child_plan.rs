//! Child plan construction for selection relation joins.

use std::sync::Arc;

use schema::{CollectionVersion, FieldDescription};

use crate::document::DocumentMapping;
use crate::error::{QueryError, Result};
use crate::mapper::{Filter, OrderBy, OrderCondition, Requestable, Select};
use crate::plan::{IndexScanNode, ScanNode};
use crate::planner::PlanNode;

use super::super::builder::Planner;

/// Intermediate result of building the child scan plan for a selection join.
///
/// Plan nodes are stored as `Option` so later stages can `take()` ownership
/// without inventing placeholder trait objects.
pub(super) struct RelationChildPlan {
    pub child_plan: Option<Box<dyn PlanNode>>,
    pub filter_child_plan: Option<Box<dyn PlanNode>>,
    pub child_scan_mapping: DocumentMapping,
    pub combined_filter: Option<Filter>,
    pub nested_limit: Option<u64>,
    pub nested_offset: u64,
    pub nested_order_by: Option<OrderBy>,
    pub parent_order_for_child: Option<OrderBy>,
    pub parent_relation_filter_for_child: Option<Filter>,
    pub child_uses_index: bool,
    pub has_deferred_scoped_fulltext: bool,
    pub multi_level_paths_for_relation: Vec<Vec<String>>,
    pub relation_field_index: usize,
    pub target_collection: Arc<CollectionVersion>,
    pub relation_field: FieldDescription,
    pub relation_field_name: String,
    pub output_name: String,
}

impl Planner {
    /// Resolve the target collection for a relation field.
    ///
    /// Handles self-referential relations and CID mismatch from circular schema
    /// set-based versioning via relation_name fallback.
    pub(super) fn resolve_relation_target_collection(
        &self,
        parent_collection: &CollectionVersion,
        relation_field: &FieldDescription,
        relation_field_name: &str,
    ) -> Result<Arc<CollectionVersion>> {
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

        Ok(target_collection)
    }

    /// Build the child scan plan and related filter/index state for a selection join.
    ///
    /// Assumes `mapping` already has the relation field index and child mapping set.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn build_relation_child_plan(
        &self,
        nested_select: &Select,
        select: &Select,
        parent_collection: &CollectionVersion,
        relation_field: &FieldDescription,
        relation_field_name: &str,
        output_name: &str,
        relation_field_index: usize,
        mut child_scan_mapping: DocumentMapping,
        child_render_mapping: DocumentMapping,
        target_collection: Arc<CollectionVersion>,
        multi_level_paths_for_relation: Vec<Vec<String>>,
        parent_filter: Option<&Filter>,
    ) -> Result<RelationChildPlan> {
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

        // Extract nested limit/offset and order_by for child index selection and
        // per-parent application in TypeJoin.
        let nested_limit = nested_select.limit.as_ref().and_then(|l| l.limit);
        let nested_offset = nested_select.limit.as_ref().map(|l| l.offset).unwrap_or(0);
        let nested_order_by = nested_select.order_by.clone();

        let force_child_full_scan_for_relation_order = relation_field.kind.is_array()
            && combined_child_order
                .as_ref()
                .is_some_and(|order| order.has_relation_order());

        let child_index_result = if force_child_full_scan_for_relation_order {
            None
        } else {
            self.try_select_child_index(
                child_filter_for_index.as_ref(),
                combined_child_order.as_ref(),
                &target_collection,
                nested_limit,
                nested_offset,
            )
        };
        let child_uses_index = child_index_result.is_some();

        // Create the child scan plan with scan_mapping (includes FK fields for joins)
        let child_plan: Box<dyn PlanNode> = if let Some((params, _)) = child_index_result {
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
                    let filter_index = self.try_select_child_index(
                        Some(rel_filter),
                        None,
                        &target_collection,
                        None,
                        0,
                    );
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

        // Build a combined filter from doc_ids and explicit filter
        let doc_ids_filter = if let Some(ref doc_ids) = nested_select.doc_ids {
            let max_depth = nested_select
                .filter
                .as_ref()
                .map(Filter::max_depth)
                .unwrap_or(self.query_limits.max_filter_depth);
            // Create a filter: _docID IN [...]
            if doc_ids.len() == 1 {
                // Single ID: _docID == "..."
                let mut conditions = serde_json::Map::new();
                conditions.insert("_docID".to_string(), serde_json::json!({"_eq": doc_ids[0]}));
                Some(Filter::from_conditions_with_max_depth(
                    conditions, max_depth,
                ))
            } else {
                // Multiple IDs: _docID IN [...]
                let mut conditions = serde_json::Map::new();
                conditions.insert("_docID".to_string(), serde_json::json!({"_in": doc_ids}));
                Some(Filter::from_conditions_with_max_depth(
                    conditions, max_depth,
                ))
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
                if field.starts_with('_') {
                    continue;
                }
                if !child_render_mapping.has_field(&field) {
                    return Err(QueryError::filter_field_not_selected(
                        &field,
                        &target_collection.name,
                    ));
                }
            }
        }

        let has_deferred_scoped_fulltext =
            super::child_mapping::has_deferred_scoped_fulltext(nested_select);

        super::child_mapping::enrich_child_scan_mapping_for_deferred_scoped_fulltext(
            &mut child_scan_mapping,
            nested_select,
            has_deferred_scoped_fulltext,
        );

        Ok(RelationChildPlan {
            child_plan: Some(child_plan),
            filter_child_plan,
            child_scan_mapping,
            combined_filter,
            nested_limit,
            nested_offset,
            nested_order_by,
            parent_order_for_child,
            parent_relation_filter_for_child,
            child_uses_index,
            has_deferred_scoped_fulltext,
            multi_level_paths_for_relation,
            relation_field_index,
            target_collection,
            relation_field: relation_field.clone(),
            relation_field_name: relation_field_name.to_string(),
            output_name: output_name.to_string(),
        })
    }
}
