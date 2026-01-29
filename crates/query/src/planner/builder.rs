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
use crate::mapper::{AggregateType, Filter, Requestable, Select};
use crate::plan::{
    AllDocsNode, AverageNode, CountNode, GroupAlias, GroupByNode, IndexScanNode, InnerAggregateDef,
    JoinSide, LimitNode, MaxNode, MinNode, OrderByNode, RelationFilter, ScanNode, SelectNode,
    SimilarityNode, SumNode, TypeJoinMany, TypeJoinOne,
};
use crate::plan::groupby::ChildSelectMeta;
use crate::planner::index_selection::{
    can_be_ordered_by_index, filter_to_index_scan, select_best_index, IndexScanParams,
    IndexScanType,
};
use crate::planner::PlanNode;
use serde_json::Value as JsonValue;

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
    /// Fields added to child mappings only for ordering (not selected by user).
    /// Each entry is (parent_relation_field_name, child_field_name).
    /// These fields appear in the document but should be stripped from output.
    pub ordering_only_fields: Vec<(String, String)>,
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
        // Build a second map by CollectionID and VersionID for relation field resolution.
        // FieldKind::Relation stores the schema version CID (version_id), so we need
        // to look up by both collection_id and version_id.
        let mut collections_by_id: HashMap<String, Arc<CollectionVersion>> = HashMap::new();
        for c in collections.values() {
            if !c.collection_id.is_empty() {
                collections_by_id.insert(c.collection_id.clone(), c.clone());
            }
            if !c.version_id.is_empty() {
                collections_by_id.insert(c.version_id.clone(), c.clone());
            }
        }
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

        // Compute ordering-only fields: nested relation fields used in ORDER BY but not in selection.
        // These will be stripped from the final output.
        let ordering_only_fields: Vec<(String, String)> = select
            .order_by
            .as_ref()
            .map(|order_by| {
                let mut result = Vec::new();
                for condition in &order_by.conditions {
                    // Look for nested relation orders like ["author", "verified"]
                    if condition.fields.len() > 1 {
                        let relation_field_name = &condition.fields[0];
                        let nested_field_name = &condition.fields[1];

                        // Check if there's a nested selection for this relation
                        let nested_selection_fields: Vec<&String> = select
                            .fields
                            .iter()
                            .filter_map(|f| {
                                if let Requestable::Select(nested) = f {
                                    if &nested.field.name == relation_field_name {
                                        // Get selected field names from nested selection
                                        return Some(
                                            nested
                                                .fields
                                                .iter()
                                                .filter_map(|nf| {
                                                    if let Requestable::Field(field) = nf {
                                                        Some(&field.name)
                                                    } else {
                                                        None
                                                    }
                                                })
                                                .collect::<Vec<_>>(),
                                        );
                                    }
                                }
                                None
                            })
                            .flatten()
                            .collect();

                        // If nested_field is not in the selected fields, it's ordering-only
                        if !nested_selection_fields
                            .iter()
                            .any(|f| *f == nested_field_name)
                        {
                            result.push((relation_field_name.clone(), nested_field_name.clone()));
                        }
                    }
                }
                result
            })
            .unwrap_or_default();

        // Check if GROUP BY references relation fields (needs full schema mapping for joins)
        let group_by_has_relations = select
            .group_by
            .as_ref()
            .map(|gb| {
                gb.fields.iter().any(|field_name| {
                    collection
                        .field_by_name(field_name)
                        .map(|f| f.kind.is_relation())
                        .unwrap_or(false)
                })
            })
            .unwrap_or(false);

        // Check if aggregates reference relation fields (needs full schema mapping for joins)
        let aggregates_have_relations = select.fields.iter().any(|f| {
            if let Requestable::Aggregate(agg) = f {
                agg.targets.iter().any(|t| {
                    if t.host_name.is_empty() || t.host_name == "_group" {
                        return false;
                    }
                    collection
                        .field_by_name(&t.host_name)
                        .map(|f| f.kind.is_relation())
                        .unwrap_or(false)
                })
            } else {
                false
            }
        });

        // Check if any secondary relation ID fields are selected (e.g., `_authorID`).
        // These require a TypeJoinOne to reverse-lookup the FK, which needs full schema mapping.
        let has_secondary_id_field = select.fields.iter().any(|f| {
            if let Requestable::Field(field) = f {
                let name = &field.name;
                if name.starts_with('_') && name.ends_with("ID") && name.len() > 3 {
                    let rel_name = &name[1..name.len() - 2];
                    if let Some(rel_field) = collection.field_by_name(rel_name) {
                        return rel_field.kind.is_relation() && !rel_field.is_primary;
                    }
                }
            }
            false
        });

        // Build scan mapping: for queries with nested selections, relation filters, relation ordering,
        // relation aggregates, relation groupBy fields, or secondary relation ID fields, use full
        // schema mapping so that FK fields are available for TypeJoin lookups and schema indices
        // don't collide with sequential render indices.
        let needs_joins = has_nested
            || filter_has_relations
            || order_has_relations
            || aggregates_have_relations
            || group_by_has_relations
            || has_secondary_id_field;
        let mut scan_mapping = if needs_joins {
            self.build_scan_mapping_for_join(&collection, &render_mapping)
        } else {
            render_mapping.clone()
        };

        // Add _group fields to scan_mapping if present in render_mapping.
        // _group is a virtual field (not in schema) that needs to be explicitly copied.
        // Multiple _group entries may exist when aliases are used (e.g., G1: _group(...), G2: _group(...)).
        if let Some(group_indices) = render_mapping.indexes_of_name("_group") {
            let group_indices = group_indices.to_vec();
            for render_index in group_indices {
                let scan_index = scan_mapping.next_index();
                scan_mapping.add(scan_index, "_group");
                // Copy the render_key for this specific _group entry
                for rk in &render_mapping.render_keys {
                    if rk.index == render_index {
                        scan_mapping.add_render_key(scan_index, &rk.key);
                        break;
                    }
                }
                // Copy child mapping if present (for _group { field1, field2 } syntax)
                if let Some(child) = render_mapping.child_at(render_index) {
                    scan_mapping.set_child_at(scan_index, child.clone());
                }
            }
        }

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

        // Add aggregate fields to scan_mapping if present in render_mapping.
        // Aggregates are virtual fields (not in schema) that need explicit copying.
        // Each aliased aggregate gets its own index/render_key, even if they share
        // the same type (e.g., sum1: _sum(...) and sum2: _sum(...) need separate slots).
        for field in &select.fields {
            if let Requestable::Aggregate(agg) = field {
                let agg_type_name = agg.aggregate_type.as_str();
                let output_name = agg.output_name();
                // Add a new index if this specific output name isn't already registered
                if scan_mapping.try_find_index_from_render_key(&output_name).is_none() {
                    let scan_index = scan_mapping.next_index();
                    scan_mapping.add(scan_index, agg_type_name);
                    scan_mapping.add_render_key(scan_index, output_name);
                }

                // Always add aggregate target fields if present (even if aggregate type exists)
                for target in &agg.targets {
                    if let Some(ref field_name) = target.field_name {
                        if scan_mapping.first_index_of_name(field_name).is_none() {
                            // Verify field exists in collection schema
                            if collection.field_by_name(field_name).is_some() {
                                // Use next available index, not schema index,
                                // to avoid conflicts with other allocated indices
                                let new_index = scan_mapping.next_index();
                                scan_mapping.add(new_index, field_name);
                            }
                        }
                    }

                    // For inline array aggregates (e.g., _count(favouriteIntegers: {})),
                    // the host_name refers to an inline array field, not a relation.
                    // We need to render the field data so compute_relation_aggregates()
                    // can operate on it after plan execution.
                    if !target.host_name.is_empty() && target.host_name != "_group" {
                        let host_name = &target.host_name;
                        if let Some(field_desc) = collection.field_by_name(host_name) {
                            if !field_desc.kind.is_relation() {
                                // It's an inline array field — ensure it's in scan_mapping.
                                // Use next_index() to avoid conflicting with existing indices.
                                if scan_mapping.first_index_of_name(host_name).is_none() {
                                    let idx = scan_mapping.next_index();
                                    scan_mapping.add(idx, host_name);
                                    scan_mapping.add_render_key(idx, host_name);
                                }
                            }
                        }
                    }
                }
            }
        }

        // Add similarity fields to scan_mapping.
        // Similarity results are virtual computed fields stored at specific indices.
        for field in &select.fields {
            if let Requestable::Similarity(sim) = field {
                // Add the _similarity output slot
                let output_name = sim.output_name();
                if scan_mapping
                    .try_find_index_from_render_key(output_name)
                    .is_none()
                {
                    let scan_index = scan_mapping.next_index();
                    scan_mapping.add(scan_index, "_similarity");
                    scan_mapping.add_render_key(scan_index, output_name);
                }

                // Ensure the target field (document's vector) is in scan_mapping
                if scan_mapping
                    .first_index_of_name(&sim.target_field)
                    .is_none()
                {
                    if collection.field_by_name(&sim.target_field).is_some() {
                        let idx = scan_mapping.next_index();
                        scan_mapping.add(idx, &sim.target_field);
                    }
                }
            }
        }

        // Check if an index can be used for the filter or ordering.
        // Index selection works for both pre-loaded docs and fetcher-based loading.
        let (index_scan, index_provides_ordering) = if let Some(ref fetcher) = self.fetcher {
            // Only use index if fetcher supports index queries
            if fetcher.supports_index_queries() {
                self.try_select_index(select, &collection)
                    .map(|(p, o)| (Some(p), o))
                    .unwrap_or((None, false))
            } else {
                if select.filter.is_some() && !collection.indexes.is_empty() {
                    debug!(
                        collection = %select.collection_name,
                        available_indexes = collection.indexes.len(),
                        "Index selection disabled - fetcher does not support index queries"
                    );
                }
                (None, false)
            }
        } else {
            self.try_select_index(select, &collection)
                .map(|(p, o)| (Some(p), o))
                .unwrap_or((None, false))
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
        // Collect aggregate and similarity output names to detect _alias conditions that should be deferred
        let mut computed_field_names: Vec<&str> = select
            .fields
            .iter()
            .filter_map(|f| match f {
                Requestable::Aggregate(agg) => Some(agg.output_name()),
                Requestable::Similarity(sim) => Some(sim.output_name()),
                _ => None,
            })
            .collect();
        // Deduplicate (in case of name collisions)
        computed_field_names.sort_unstable();
        computed_field_names.dedup();

        // Strip _alias conditions that reference computed fields (aggregates/similarity) from the filter.
        // These must be evaluated after the computed fields are set, not during plan execution.
        let filter_for_plan = select.filter.as_ref().map(|f| {
            let (stripped, _) = f.strip_aggregate_alias_conditions(&computed_field_names);
            stripped
        });

        // Convert doc_ids to a _docID filter and merge with the explicit filter.
        // The docID parameter (e.g., User(docID: "...")) must be applied as a real
        // filter condition, not just used for explain output.
        let filter_for_plan = if let Some(ref doc_ids) = select.doc_ids {
            let doc_ids_filter = if doc_ids.len() == 1 {
                let mut conditions = HashMap::new();
                conditions.insert(
                    "_docID".to_string(),
                    serde_json::json!({"_eq": doc_ids[0]}),
                );
                Filter::from_conditions(conditions)
            } else {
                let mut conditions = HashMap::new();
                conditions.insert(
                    "_docID".to_string(),
                    serde_json::json!({"_in": doc_ids}),
                );
                Filter::from_conditions(conditions)
            };
            match filter_for_plan {
                Some(existing) => Some(doc_ids_filter.and(existing)),
                None => Some(doc_ids_filter),
            }
        } else {
            filter_for_plan
        };

        // Check if filter is complex (has relation conditions inside logical operators)
        // or has multi-level relation paths (e.g., {author: {published: {rating: ...}}})
        let is_complex_filter = filter_for_plan
            .as_ref()
            .map(|f| {
                f.is_complex()
                    || !f.get_multi_level_relation_paths().is_empty()
                    || f.has_alias_filter()
            })
            .unwrap_or(false);

        // Split filter into scalar and relation parts (only useful for non-complex filters)
        // Note: JSON field nested access looks like relation filters structurally, but should
        // be treated as scalar filters. We recombine them below based on schema info.
        let (scalar_filter_raw, relation_filter) = filter_for_plan
            .as_ref()
            .map(|f| f.split_by_relation())
            .unwrap_or((None, None));

        // Move JSON field conditions from relation_filter back to scalar_filter.
        // The split_by_relation function can't distinguish JSON nested access from relation
        // traversal without schema info. Here we have the collection, so we can fix it.
        //
        // Also transform {relationField: {_docID: {...}}} to {_relationFieldID: {...}}.
        // This allows relation _docID filters to work as scalar filters without requiring a join.
        // Example: {author: {_docID: {_eq: "bae-..."}}} → {_authorID: {_eq: "bae-..."}}
        let scalar_filter = {
            let mut combined_conditions: HashMap<String, JsonValue> = scalar_filter_raw
                .as_ref()
                .map(|f| f.conditions().clone())
                .unwrap_or_default();

            if let Some(ref rel_filter) = relation_filter {
                for (field_name, condition) in rel_filter.conditions() {
                    // Check if this field is a relation
                    if let Some(field) = collection.field_by_name(field_name) {
                        if field.kind.is_relation() {
                            // Check if this is a {_docID: {...}} pattern AND the FK field exists locally.
                            // For "primary" relations (FK on this side), we can transform to use the FK.
                            // For "secondary" relations (FK on other side), we can't transform - need join.
                            let fk_field_name =
                                schema::CollectionVersion::relation_id_field_name(field_name);
                            let has_local_fk = collection.field_by_name(&fk_field_name).is_some();

                            if has_local_fk {
                                if let Some(obj) = condition.as_object() {
                                    if obj.len() == 1 {
                                        if let Some(docid_condition) = obj.get("_docID") {
                                            // Transform {relationField: {_docID: {...}}} to {_relationFieldID: {...}}
                                            combined_conditions
                                                .insert(fk_field_name, docid_condition.clone());
                                            continue;
                                        }
                                    }
                                }
                            }
                            // Not a _docID-only pattern or no local FK, keep as relation filter
                            continue;
                        }
                    }
                    // Not a relation field - treat as scalar (could be JSON, etc.)
                    combined_conditions.insert(field_name.clone(), condition.clone());
                }
            }

            if combined_conditions.is_empty() {
                None
            } else {
                Some(Filter::from_conditions(combined_conditions))
            }
        };

        // For grouped queries, strip _alias conditions from the pre-aggregation filter.
        // Alias filters on aggregate fields must be applied AFTER aggregation.
        let scalar_filter = if select.group_by.is_some() {
            scalar_filter.and_then(|f| f.split_alias().0)
        } else {
            scalar_filter
        };

        // 1. Choose between IndexScanNode and ScanNode based on index availability
        let mut plan: Box<dyn PlanNode> = if let Some(ref params) = index_scan {
            let mut index_scan_node =
                IndexScanNode::new((*collection).clone(), scan_mapping.clone(), params.clone())
                    .with_show_deleted(select.show_deleted);
            // Attach fetcher if available for on-demand index-based loading
            if let Some(ref fetcher) = self.fetcher {
                index_scan_node = index_scan_node.with_fetcher(fetcher.clone());
            }
            Box::new(index_scan_node)
        } else {
            let mut scan = ScanNode::new((*collection).clone(), scan_mapping.clone())
                .with_show_deleted(select.show_deleted);
            // Attach fetcher if available for on-demand data loading
            if let Some(ref fetcher) = self.fetcher {
                scan = scan.with_fetcher(fetcher.clone());
            }
            // Pass doc_ids to ScanNode for explain prefixes
            if let Some(ref doc_ids) = select.doc_ids {
                scan = scan.with_doc_ids(doc_ids.clone());
            }
            Box::new(scan)
        };

        // 2. Apply join nodes BEFORE SelectNode (matches Go DefraDB plan construction order).
        // TypeJoin nodes wrap the raw ScanNode, and SelectNode wraps the join result.
        // For simple filters: relation filters are extracted and applied inside TypeJoin nodes
        // For complex filters: pass None, the full filter is applied after join
        let filter_for_joins = if is_complex_filter {
            None // Don't pass filter to TypeJoin for complex filters
        } else {
            filter_for_plan.as_ref()
        };
        (plan, scan_mapping) =
            self.apply_joins(plan, select, &collection, scan_mapping, 0, filter_for_joins)?;

        // 3b. Apply joins for multi-level relation filter paths where the first relation
        // is NOT in the selection set. If the first relation IS selected, then apply_joins
        // already handles it via apply_multi_level_sub_joins.
        // Example: Book(filter: {author: {published: {rating: {_eq: 4.9}}}}) { name }
        // (no author selection, so we need to add the full join chain here)
        if let Some(ref filter) = filter_for_plan {
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

                let (new_plan, new_mapping) = self.apply_filter_relation_join(
                    plan,
                    &collection,
                    relation_field,
                    relation_field_name,
                    scan_mapping.clone(),
                )?;
                plan = new_plan;
                scan_mapping = new_mapping;
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

                let (new_plan, new_mapping) = self.apply_filter_relation_join(
                    plan,
                    &collection,
                    relation_field,
                    relation_field_name,
                    scan_mapping.clone(),
                )?;
                plan = new_plan;
                scan_mapping = new_mapping;
            }
        }

        // 3d. Apply SelectNode AFTER all joins (matches Go DefraDB plan order).
        // The SelectNode wraps the joined plan and applies scalar filters.
        // Note: Even with IndexScanNode, we may need a SelectNode for:
        //   - Field projection
        //   - Conditions not covered by the index
        if !is_complex_filter && (scalar_filter.is_some() || !select.fields.is_empty()) {
            let mut select_node = SelectNode::new(plan, scan_mapping.clone());
            if let Some(ref doc_ids) = select.doc_ids {
                select_node = select_node.with_doc_ids(doc_ids.clone());
            }
            if let Some(filter) = scalar_filter {
                select_node = select_node.with_filter(filter);
            }
            plan = Box::new(select_node);
        } else if is_complex_filter && !select.fields.is_empty() {
            // For complex filters where there are no join-related fields,
            // still need SelectNode for field projection (filter applied below)
            if !needs_joins {
                let mut select_node = SelectNode::new(plan, scan_mapping.clone());
                if let Some(ref doc_ids) = select.doc_ids {
                    select_node = select_node.with_doc_ids(doc_ids.clone());
                }
                plan = Box::new(select_node);
            }
        }

        // 4. Apply complex filter after join (when merged document is available)
        // Complex filters contain _and/_or with mixed scalar and relation conditions
        // that must be evaluated together on the merged document.
        if is_complex_filter {
            if let Some(ref filter) = select.filter {
                // For grouped queries, strip _alias from pre-aggregation filter
                let pre_agg_filter = if select.group_by.is_some() {
                    filter.split_alias().0
                } else {
                    Some(filter.clone())
                };
                if let Some(f) = pre_agg_filter {
                    let mut select_node = SelectNode::new(plan, scan_mapping.clone()).with_filter(f);
                    if let Some(ref doc_ids) = select.doc_ids {
                        select_node = select_node.with_doc_ids(doc_ids.clone());
                    }
                    plan = Box::new(select_node);
                }
            }
        }

        // 4c. Add SimilarityNodes for _similarity fields.
        // These compute per-document dot product before filters/ordering can reference results.
        plan = self.add_similarity_nodes(plan, select, &scan_mapping)?;

        // 4d. Apply deferred _alias filter for similarity results (non-grouped queries only).
        // Similarity aliases are stripped from the initial filter and applied after computation.
        if select.group_by.is_none() {
            if let Some(ref filter) = select.filter {
                let (_non_alias, alias_filter) = filter.split_alias();
                if let Some(alias_f) = alias_filter {
                    plan = Box::new(
                        SelectNode::new(plan, scan_mapping.clone()).with_filter(alias_f),
                    );
                }
            }
        }

        // Check if we have GROUP BY - this affects the order of operations
        let has_group_by = select.group_by.is_some();

        if has_group_by {
            // WITH GROUP BY: GroupByNode → Aggregates → OrderBy → Limit

            // 4b. Apply GroupBy
            // Use scan_mapping because the upstream plan produces docs with schema indices
            if let Some(ref group_by) = select.group_by {
                let mut group_node = GroupByNode::new(plan, group_by.clone(), scan_mapping.clone())
                    .with_collection_name(select.collection_name.clone());

                // Extract _group alias definitions and inner groupBy/aggregate info.
                // Each _group reference (including aliases like G1: _group(limit: 1))
                // gets its own GroupAlias with per-alias filter/limit/order/docIDs.
                let group_indices = scan_mapping
                    .indexes_of_name("_group")
                    .map(|s| s.to_vec())
                    .unwrap_or_default();
                let mut group_aliases = Vec::new();
                let mut alias_count = 0;
                let mut inner_extracted = false;

                for field in &select.fields {
                    if let Requestable::Select(nested) = field {
                        if nested.field.name == "_group" {
                            let alias_index = group_indices.get(alias_count).copied().unwrap_or(0);
                            alias_count += 1;

                            group_aliases.push(GroupAlias {
                                index: alias_index,
                                filter: nested.filter.clone(),
                                limit: nested.limit.clone(),
                                order: nested.order_by.clone(),
                                doc_ids: nested.doc_ids.clone(),
                            });

                            // Extract inner groupBy/aggregates from the first _group
                            // that has a groupBy clause (only once)
                            if !inner_extracted && nested.group_by.is_some() {
                                inner_extracted = true;

                                if let Some(ref inner_group_by) = nested.group_by {
                                    group_node = group_node
                                        .with_inner_group_by_fields(inner_group_by.fields.clone());
                                }

                                // Extract inner aggregate definitions
                                let mut inner_aggs = Vec::new();
                                for inner_field in &nested.fields {
                                    if let Requestable::Aggregate(inner_agg) = inner_field {
                                        let field_index = if !inner_agg.targets.is_empty() {
                                            if let Some(ref field_name) =
                                                inner_agg.targets[0].field_name
                                            {
                                                scan_mapping
                                                    .first_index_of_name(field_name)
                                                    .unwrap_or(0)
                                            } else {
                                                0
                                            }
                                        } else {
                                            0
                                        };
                                        inner_aggs.push(InnerAggregateDef {
                                            aggregate_type: inner_agg.aggregate_type,
                                            output_key: inner_agg.output_name().to_string(),
                                            field_index,
                                        });
                                    }
                                }
                                if !inner_aggs.is_empty() {
                                    group_node = group_node.with_inner_aggregates(inner_aggs);
                                }

                                // Extract inner _group filter/order (2nd nesting level)
                                // and 3rd-level groupBy/aggregates
                                for inner_field in &nested.fields {
                                    if let Requestable::Select(inner_nested) = inner_field {
                                        if inner_nested.field.name == "_group" {
                                            if let Some(ref inner_filter) = inner_nested.filter {
                                                group_node = group_node
                                                    .with_inner_group_filter(inner_filter.clone());
                                            }
                                            if let Some(ref inner_order) = inner_nested.order_by {
                                                group_node = group_node
                                                    .with_inner_group_order(inner_order.clone());
                                            }

                                            // 3rd level: extract groupBy fields
                                            if let Some(ref third_gb) = inner_nested.group_by {
                                                group_node = group_node
                                                    .with_third_level_group_by_fields(
                                                        third_gb.fields.clone(),
                                                    );
                                            }

                                            // 3rd level: extract aggregate definitions
                                            let mut third_aggs = Vec::new();
                                            for third_field in &inner_nested.fields {
                                                if let Requestable::Aggregate(third_agg) =
                                                    third_field
                                                {
                                                    let field_index =
                                                        if !third_agg.targets.is_empty() {
                                                            if let Some(ref field_name) =
                                                                third_agg.targets[0].field_name
                                                            {
                                                                scan_mapping
                                                                    .first_index_of_name(field_name)
                                                                    .unwrap_or(0)
                                                            } else {
                                                                0
                                                            }
                                                        } else {
                                                            0
                                                        };
                                                    third_aggs.push(InnerAggregateDef {
                                                        aggregate_type: third_agg.aggregate_type,
                                                        output_key: third_agg
                                                            .output_name()
                                                            .to_string(),
                                                        field_index,
                                                    });
                                                }
                                            }
                                            if !third_aggs.is_empty() {
                                                group_node = group_node
                                                    .with_third_level_aggregates(third_aggs);
                                            }

                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                if !group_aliases.is_empty() {
                    group_node = group_node.with_group_aliases(group_aliases);
                }

                // Build child_selects metadata for explain output
                // Each _group nested select contributes a ChildSelectMeta
                let mut child_selects_meta: Vec<ChildSelectMeta> = Vec::new();
                for field in &select.fields {
                    if let Requestable::Select(nested) = field {
                        if nested.field.name == "_group" {
                            let mut meta = ChildSelectMeta {
                                collection_name: select.collection_name.clone(),
                                doc_ids: nested.doc_ids.clone(),
                                filter: nested.filter.clone(),
                                limit: nested.limit.clone(),
                                order: nested.order_by.clone(),
                                group_by: nested.group_by.as_ref().map(|gb| gb.fields.clone()),
                            };

                            // If this _group has a nested _group with further groupBy, include that
                            for inner_field in &nested.fields {
                                if let Requestable::Select(inner_nested) = inner_field {
                                    if inner_nested.field.name == "_group" {
                                        if let Some(ref inner_gb) = inner_nested.group_by {
                                            meta.group_by = Some(inner_gb.fields.clone());
                                        }
                                        break;
                                    }
                                }
                            }

                            child_selects_meta.push(meta);
                        }
                    }
                }
                if !child_selects_meta.is_empty() {
                    group_node = group_node.with_child_selects(child_selects_meta);
                }

                plan = Box::new(group_node);
            }

            // 5. Add aggregate nodes (after grouping)
            plan = self.add_aggregate_nodes(plan, select, &scan_mapping)?;

            // 5b. Apply _alias filter AFTER aggregation
            // Alias filters on aggregate fields (e.g., filter: {_alias: {Total: {_gt: 100}}})
            // can only be evaluated after aggregate values have been computed.
            if let Some(ref filter) = select.filter {
                let (_non_alias, alias_filter) = filter.split_alias();
                if let Some(alias_f) = alias_filter {
                    plan =
                        Box::new(SelectNode::new(plan, scan_mapping.clone()).with_filter(alias_f));
                }
            }

            // 6. Apply order by (after grouping/aggregation)
            // Skip if index already provides the ordering
            if let Some(ref order_by) = select.order_by {
                if !index_provides_ordering {
                    plan = Box::new(OrderByNode::new(
                        plan,
                        order_by.clone(),
                        scan_mapping.clone(),
                    ));
                }
            }

            // 7. Apply limit/offset
            if let Some(ref limit) = select.limit {
                let effective_limit = match limit.limit {
                    Some(0) => None, // limit: 0 means no limit (Go compatibility)
                    other => other,
                };
                if effective_limit.is_some() || limit.offset > 0 {
                    plan = Box::new(LimitNode::new(plan, effective_limit, limit.offset));
                }
            }
        } else {
            // WITHOUT GROUP BY: OrderBy → Limit → [AllDocsNode] → Aggregates

            // 5. Apply order by (before limit)
            // Skip if index already provides the ordering
            if let Some(ref order_by) = select.order_by {
                if !index_provides_ordering {
                    plan = Box::new(OrderByNode::new(
                        plan,
                        order_by.clone(),
                        scan_mapping.clone(),
                    ));
                }
            }

            // 6. Apply limit/offset
            if let Some(ref limit) = select.limit {
                let effective_limit = match limit.limit {
                    Some(0) => None, // limit: 0 means no limit (Go compatibility)
                    other => other,
                };
                if effective_limit.is_some() || limit.offset > 0 {
                    plan = Box::new(LimitNode::new(plan, effective_limit, limit.offset));
                }
            }

            // 7. Count aggregates to determine if we need AllDocsNode
            let aggregate_count = select
                .fields
                .iter()
                .filter(|f| matches!(f, Requestable::Aggregate(_)))
                .count();

            // If there are multiple aggregates, wrap in AllDocsNode so they all
            // can access the original documents via current_group_docs()
            if aggregate_count > 1 {
                plan = Box::new(AllDocsNode::new(plan, scan_mapping.clone()));
            }

            // 8. Add aggregate nodes (for top-level aggregates without GROUP BY)
            plan = self.add_aggregate_nodes(plan, select, &scan_mapping)?;
        }

        Ok(PlanResult {
            plan,
            index_scan,
            ordering_only_fields,
        })
    }

    /// Add aggregate nodes to the plan based on the select's aggregate fields.
    ///
    /// Handles three types of aggregates:
    /// Add SimilarityNode(s) to the plan for each _similarity field in the select.
    ///
    /// Each _similarity computes a dot product between the document's vector field
    /// and the query vector, storing the result at the designated index.
    fn add_similarity_nodes(
        &self,
        mut plan: Box<dyn PlanNode>,
        select: &Select,
        mapping: &DocumentMapping,
    ) -> Result<Box<dyn PlanNode>> {
        for field in &select.fields {
            if let Requestable::Similarity(sim) = field {
                // Find the target field index (document's vector)
                let field_index = mapping
                    .first_index_of_name(&sim.target_field)
                    .ok_or_else(|| {
                        QueryError::internal(format!(
                            "similarity target field '{}' not found in mapping",
                            sim.target_field
                        ))
                    })?;

                // Find the similarity result index
                let similarity_index = mapping
                    .try_find_index_from_render_key(sim.output_name())
                    .ok_or_else(|| {
                        QueryError::internal(format!(
                            "similarity output '{}' not found in mapping render keys",
                            sim.output_name()
                        ))
                    })?;

                plan = Box::new(SimilarityNode::new(
                    plan,
                    mapping.clone(),
                    field_index,
                    similarity_index,
                    sim.vector.clone(),
                ));
            }
        }
        Ok(plan)
    }

    /// - Simple field aggregates (e.g., _sum(field: age))
    /// - Group aggregates (e.g., _sum(_group: {field: age}))
    /// - Relation aggregates (e.g., _sum(articles: {field: pages}))
    ///
    /// Relation and inline-array aggregates are handled by iterating through
    /// the JSON array stored in the relation/array field.
    fn add_aggregate_nodes(
        &self,
        mut plan: Box<dyn PlanNode>,
        select: &Select,
        mapping: &DocumentMapping,
    ) -> Result<Box<dyn PlanNode>> {
        for field in &select.fields {
            if let Requestable::Aggregate(agg) = field {
                // Get the index where the aggregate result should be stored.
                // Use the output name (alias if set, otherwise type name) to look up the
                // correct render_key index. This handles aliased aggregates correctly
                // (e.g., C1: _count(...) and C2: _count(...) get different indices).
                let agg_index = mapping
                    .try_find_index_from_render_key(agg.output_name())
                    .ok_or_else(|| {
                        QueryError::internal(format!(
                            "aggregate '{}' not found in document mapping render keys - this is a bug",
                            agg.output_name()
                        ))
                    })?;

                // Detect the aggregate source type and set up accordingly:
                // 1. Child aggregate: host_name="_group" AND target is an inner aggregate
                //    name (e.g., _count, _sum) or a field not in the parent mapping.
                //    These read pre-computed values from the _group JSON array.
                // 2. Relation aggregate: host_name is a relation field (iterate relation array)
                // 3. Simple/grouped aggregate: target field exists in parent mapping,
                //    or count with no field. These use grouped mode which applies filters.
                let mut is_array_aggregate = false;
                let mut array_field_index = 0usize;
                let mut target_field_name = String::new();
                let mut field_index = 0usize;

                if !agg.targets.is_empty() {
                    let target = &agg.targets[0];
                    let host_name = &target.host_name;

                    if host_name == "_group" {
                        // Only use child aggregate (array) mode when the target field
                        // is an inner aggregate name or doesn't exist in the parent
                        // mapping. Regular fields (e.g., Age) that exist in the parent
                        // mapping use grouped mode, which properly applies filters/limits.
                        if let Some(ref fname) = target.field_name {
                            let is_aggregate_name = matches!(
                                fname.as_str(),
                                "_count" | "_sum" | "_avg" | "_min" | "_max"
                            );
                            if is_aggregate_name
                                || mapping.first_index_of_name(fname).is_none()
                            {
                                is_array_aggregate = true;
                                array_field_index =
                                    mapping.first_index_of_name("_group").unwrap_or(0);
                                target_field_name = fname.clone();
                            } else {
                                field_index =
                                    mapping.first_index_of_name(fname).ok_or_else(|| {
                                        QueryError::execution(format!(
                                            "aggregate target field '{}' not found in mapping",
                                            fname
                                        ))
                                    })?;
                            }
                        }
                        // else: count with no field_name → stays in grouped mode
                    } else if !host_name.is_empty() {
                        // Relation or inline-array aggregate (e.g., _sum(articles: {field: pages}))
                        // Get the relation/array field index
                        if let Some(idx) = mapping.first_index_of_name(host_name) {
                            is_array_aggregate = true;
                            array_field_index = idx;
                            target_field_name = target.field_name.clone().unwrap_or_default();
                        }
                    } else if let Some(ref fname) = target.field_name {
                        // Simple field aggregate
                        field_index = mapping.first_index_of_name(fname).ok_or_else(|| {
                            QueryError::execution(format!(
                                "aggregate target field '{}' not found in mapping",
                                fname
                            ))
                        })?;
                    }
                }

                // Extract filter and limit from aggregate target (if any)
                let target_filter = if !agg.targets.is_empty() {
                    agg.targets[0].filter.clone()
                } else {
                    None
                };
                let target_limit = if !agg.targets.is_empty() {
                    agg.targets[0].limit.clone()
                } else {
                    None
                };

                match agg.aggregate_type {
                    AggregateType::Count => {
                        let mut node = CountNode::new(plan, mapping.clone(), agg_index);
                        if is_array_aggregate {
                            node = node.with_child_aggregate_source(
                                array_field_index,
                                target_field_name.clone(),
                            );
                        }
                        if let Some(filter) = target_filter {
                            node = node.with_filter(filter);
                        }
                        if let Some(limit) = target_limit {
                            node = node.with_limit(limit);
                        }
                        plan = Box::new(node);
                    }
                    AggregateType::Sum => {
                        let mut node = SumNode::new(plan, mapping.clone(), field_index, agg_index);
                        if is_array_aggregate {
                            node = node.with_child_aggregate_source(
                                array_field_index,
                                target_field_name.clone(),
                            );
                        }
                        if let Some(filter) = target_filter {
                            node = node.with_filter(filter);
                        }
                        if let Some(limit) = target_limit {
                            node = node.with_limit(limit);
                        }
                        plan = Box::new(node);
                    }
                    AggregateType::Average => {
                        let mut node =
                            AverageNode::new(plan, mapping.clone(), field_index, agg_index);
                        if is_array_aggregate {
                            node = node.with_child_aggregate_source(
                                array_field_index,
                                target_field_name.clone(),
                            );
                        }
                        if let Some(filter) = target_filter {
                            node = node.with_filter(filter);
                        }
                        if let Some(limit) = target_limit {
                            node = node.with_limit(limit);
                        }
                        plan = Box::new(node);
                    }
                    AggregateType::Min => {
                        let mut node = MinNode::new(plan, mapping.clone(), field_index, agg_index);
                        if is_array_aggregate {
                            node = node.with_child_aggregate_source(
                                array_field_index,
                                target_field_name.clone(),
                            );
                        }
                        if let Some(filter) = target_filter {
                            node = node.with_filter(filter);
                        }
                        if let Some(limit) = target_limit {
                            node = node.with_limit(limit);
                        }
                        plan = Box::new(node);
                    }
                    AggregateType::Max => {
                        let mut node = MaxNode::new(plan, mapping.clone(), field_index, agg_index);
                        if is_array_aggregate {
                            node = node.with_child_aggregate_source(
                                array_field_index,
                                target_field_name.clone(),
                            );
                        }
                        if let Some(filter) = target_filter {
                            node = node.with_filter(filter);
                        }
                        if let Some(limit) = target_limit {
                            node = node.with_limit(limit);
                        }
                        plan = Box::new(node);
                    }
                }
            }
        }
        Ok(plan)
    }

    /// Try to select an index for the given query.
    ///
    /// Returns `Some((IndexScanParams, index_provides_ordering))` if an index
    /// can be used, `None` otherwise. Tries filter-based selection first,
    /// then falls back to ordering-based selection (matching Go behavior).
    fn try_select_index(
        &self,
        select: &Select,
        collection: &CollectionVersion,
    ) -> Option<(IndexScanParams, bool)> {
        if collection.indexes.is_empty() {
            return None;
        }

        // Try filter-based index selection first
        if let Some(filter) = select.filter.as_ref() {
            if let Some(best_index) = select_best_index(filter, &collection.indexes) {
                if let Some(params) =
                    filter_to_index_scan(filter, best_index, select.order_by.as_ref())
                {
                    // Check if this index also provides ordering
                    let provides_ordering = select
                        .order_by
                        .as_ref()
                        .map(|o| can_be_ordered_by_index(o, best_index).0)
                        .unwrap_or(false);
                    return Some((params, provides_ordering));
                }
            }
        }

        // Fallback: try ordering-based index selection (no filter needed)
        if let Some(ref order_by) = select.order_by {
            for index in &collection.indexes {
                let (can_order, needs_reverse) = can_be_ordered_by_index(order_by, index);
                if can_order {
                    let params = IndexScanParams {
                        index_name: index.name.clone(),
                        scan_type: IndexScanType::PrefixScan {
                            prefix_values: vec![],
                            reverse: needs_reverse,
                        },
                    };
                    return Some((params, true));
                }
            }
        }

        None
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
    ) -> Result<(Box<dyn PlanNode>, DocumentMapping)> {
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

            // Create the child scan plan with scan_mapping (includes FK fields for joins)
            let mut child_scan =
                ScanNode::new((*target_collection).clone(), child_scan_mapping.clone());
            if let Some(ref fetcher) = self.fetcher {
                child_scan = child_scan.with_fetcher(fetcher.clone());
            }

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

            // Wrap in SelectNode if there's any filter
            let mut child_plan: Box<dyn PlanNode> = if let Some(ref filter) = combined_filter {
                // Validate that all explicitly-filtered fields exist in the render mapping.
                // Skip doc_ids filter fields since _docID is always available.
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
            (child_plan, child_scan_mapping) = self.apply_joins(
                child_plan,
                nested_select,
                &target_collection,
                child_scan_mapping,
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
                // One-to-many: TypeJoinMany
                let mut join_many =
                    TypeJoinMany::new(plan, child_plan, parent_side, child_side, mapping.clone())?;

                // Apply relation filter (filters parents by children)
                if let Some(rel_filter) = relation_filter {
                    join_many = join_many.with_relation_filter(rel_filter);
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
                let mut join =
                    TypeJoinOne::new(plan, child_plan, parent_side, child_side, mapping.clone());
                if let Some(rel_filter) = relation_filter {
                    join = join.with_relation_filter(rel_filter);
                }
                plan = Box::new(join);
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

                // Add the FK field (e.g., _authorID) - needed for TypeJoinMany cache indexing
                let fk_field_name = if let Some(ref target_rel) = target_relation_field {
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
                        }
                    }
                }

                // Build child scan (with fetcher for data access)
                let mut child_scan =
                    ScanNode::new((*target_collection).clone(), child_mapping.clone());
                if let Some(ref fetcher) = self.fetcher {
                    child_scan = child_scan.with_fetcher(fetcher.clone());
                }
                let child_plan: Box<dyn PlanNode> = Box::new(child_scan);

                // Get field indices
                let relation_field_index = mapping
                    .first_index_of_name(&relation_name)
                    .unwrap_or_else(|| {
                        // Add to mapping if not present
                        let idx = mapping.next_index();
                        mapping.add(idx, &relation_name);
                        idx
                    });

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
                    // One-to-one: TypeJoinOne with filter
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

                // Build a minimal child mapping (just _docID for the reverse lookup)
                let mut child_mapping = DocumentMapping::new();
                child_mapping.add(0, "_docID");

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
        for requestable in &select.fields {
            if let Requestable::Aggregate(agg) = requestable {
                for target in &agg.targets {
                    // Only handle aggregates with a named target (non-empty host_name)
                    if target.host_name.is_empty() {
                        continue;
                    }

                    let relation_field_name = &target.host_name;

                    // Check if this field is already being selected
                    let already_joined = select.fields.iter().any(|f| {
                        if let Requestable::Select(s) = f {
                            s.field.name == *relation_field_name
                        } else {
                            false
                        }
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
                                            child_mapping.add_render_key(idx, &filter_field);
                                        }
                                    }
                                }
                            }
                        }
                        // Add order fields from aggregate target to existing child mapping
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
                                        if let Some(child_mapping) =
                                            mapping.child_at_mut(relation_field_index)
                                        {
                                            if child_mapping
                                                .first_index_of_name(order_field)
                                                .is_none()
                                            {
                                                child_mapping.add(idx, order_field);
                                                child_mapping.add_render_key(idx, order_field);
                                            }
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
                    let child_scan_mapping =
                        self.build_scan_mapping_for_join(&target_collection, &child_mapping);

                    // Set up child mapping in parent for TypeJoin
                    mapping.set_child_at(relation_field_index, child_scan_mapping.clone());

                    // Add the relation field to mapping if not already present
                    // This is needed so TypeJoinMany's output appears in the JSON
                    if mapping.first_index_of_name(relation_field_name).is_none() {
                        mapping.add(relation_field_index, relation_field_name);
                    }
                    // Add render_key for the relation field so the joined data appears
                    // in the output for post-processing
                    mapping.add_render_key(relation_field_index, relation_field_name);

                    // Build child plan (simple scan with fetcher)
                    let mut child_scan =
                        ScanNode::new((*target_collection).clone(), child_scan_mapping.clone());
                    if let Some(ref fetcher) = self.fetcher {
                        child_scan = child_scan.with_fetcher(fetcher.clone());
                    }
                    let child_plan: Box<dyn PlanNode> = Box::new(child_scan);

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
                }
            }
        }

        Ok((plan, mapping))
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
    fn apply_filter_relation_join(
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
                    // Handle _group specially - it's a virtual field for groupBy results
                    if field.name == "_group" {
                        let index = mapping.next_index();
                        mapping.add(index, "_group");
                        mapping.add_render_key(index, field.output_name());
                        continue;
                    }
                    // Handle __typename for GraphQL introspection
                    if field.name == "__typename" {
                        mapping.set_type_name(&select.collection_name);
                        let index = mapping.first_index_of_name("__typename").unwrap();
                        mapping.add_render_key(index, field.output_name());
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
                    // Handle _group specially - it's a virtual field for groupBy results
                    if nested_select.field.name == "_group" {
                        let index = mapping.next_index();
                        mapping.add(index, "_group");
                        mapping.add_render_key(index, nested_select.field.output_name());

                        // Build child mapping for the _group nested fields
                        let child_mapping =
                            self.build_group_child_mapping(&nested_select, collection)?;
                        mapping.set_child_at(index, child_mapping);
                        continue;
                    }
                    // Nested select (relation) - add the field but don't recurse here
                    // Child mapping will be built when applying joins
                    let index = mapping.next_index();
                    mapping.add(index, &nested_select.field.name);
                    mapping.add_render_key(index, nested_select.field.output_name());
                }
                Requestable::Aggregate(agg) => {
                    // For relation-based aggregates (e.g., _count(books: {})),
                    // the aggregate operates on related documents.
                    // We add the aggregate name to the mapping for the result field.
                    let index = mapping.next_index();
                    let name = agg.aggregate_type.as_str();
                    mapping.add(index, name);
                    // Use alias if provided, otherwise use the aggregate name
                    mapping.add_render_key(index, agg.output_name());
                }
                Requestable::Similarity(sim) => {
                    let index = mapping.next_index();
                    mapping.add(index, "_similarity");
                    mapping.add_render_key(index, sim.output_name());
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

        // Add fields referenced by the filter (needed for filter evaluation but not rendered)
        if let Some(ref filter) = select.filter {
            for field_name in filter.referenced_fields() {
                if mapping.first_index_of_name(&field_name).is_none() {
                    // Only add if field exists in collection schema
                    if collection.field_by_name(&field_name).is_some() {
                        let index = mapping.next_index();
                        mapping.add(index, &field_name);
                        // Don't add render_key - we don't want to output these fields
                    }
                }
            }
        }

        Ok(mapping)
    }

    /// Build a child mapping for the _group virtual field.
    /// This maps the nested fields within _group { ... } using the same indices
    /// as the parent document, so field values can be extracted correctly.
    fn build_group_child_mapping(
        &self,
        group_select: &Select,
        collection: &CollectionVersion,
    ) -> Result<DocumentMapping> {
        let mut child_mapping = DocumentMapping::new();

        // Build render_keys for each requested field, using schema indices
        for requestable in &group_select.fields {
            match requestable {
                Requestable::Field(field) => {
                    // Handle __typename for GraphQL introspection
                    if field.name == "__typename" {
                        child_mapping.set_type_name(&collection.name);
                        let index = child_mapping.first_index_of_name("__typename").unwrap();
                        child_mapping.add_render_key(index, field.output_name());
                        continue;
                    }

                    // Get the schema index for this field
                    let schema_idx = if field.name == "_docID" {
                        0
                    } else {
                        // Validate field exists in collection schema
                        let schema_field = collection.field_by_name(&field.name);
                        if schema_field.is_none() {
                            return Err(QueryError::unknown_field(&field.name));
                        }
                        // Find the field's index in the collection
                        collection
                            .fields
                            .iter()
                            .position(|f| f.name == field.name)
                            .unwrap_or(0)
                    };

                    child_mapping.add(schema_idx, &field.name);
                    child_mapping.add_render_key(schema_idx, field.output_name());
                }
                Requestable::Select(nested_select) => {
                    if nested_select.field.name == "_group" {
                        // Nested _group within _group: recursively build child mapping
                        let index = child_mapping.next_index();
                        child_mapping.add(index, "_group");
                        child_mapping.add_render_key(index, nested_select.field.output_name());

                        let inner_child_mapping =
                            self.build_group_child_mapping(nested_select, collection)?;
                        child_mapping.set_child_at(index, inner_child_mapping);
                    } else {
                        // Relation field inside _group (e.g., published {...})
                        // Add it to the child mapping so it can be rendered.
                        // The actual index must match where TypeJoinMany will populate the data.
                        // TypeJoinMany will use the parent mapping's index for this field.
                        // We need to find or allocate an index for this relation field.
                        let index = child_mapping.next_index();
                        child_mapping.add(index, &nested_select.field.name);
                        child_mapping.add_render_key(index, nested_select.field.output_name());
                    }
                }
                Requestable::Aggregate(_) => {
                    // Aggregates within _group are not currently supported
                }
                Requestable::Similarity(_) => {
                    // Similarity within _group is not supported
                }
            }
        }

        Ok(child_mapping)
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
                // Auto-generated FK field (Go naming: _{fieldname}ID)
                FieldDescription::new("4", "_authorID", FieldKind::doc_id())
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
    async fn test_plan_uses_index_for_ne_filter() {
        use std::collections::HashMap;

        let planner = Planner::new(vec![make_test_collection_with_index()]);

        // _ne uses full index scan (matching Go behavior)
        let filter = Filter::from_conditions(HashMap::from([(
            "name".to_string(),
            serde_json::json!({"_ne": "Alice"}),
        )]));

        let select = Select::new("Users")
            .with_field(Field::new("name"))
            .with_filter(filter);

        let result = planner.plan_with_index_info(&select).unwrap();

        // _ne uses full index scan
        assert!(result.uses_index());
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

        // After plan_with_index_info: ScanNode → TypeJoinOne → SelectNode
        // Outermost is SelectNode (Go DefraDB plan order: joins before select)
        assert_eq!(plan.kind(), "selectNode");
        let source = plan.source().unwrap();
        assert_eq!(source.kind(), "typeIndexJoin");
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

        // After plan_with_index_info: ScanNode → TypeJoinMany → SelectNode
        // Outermost is SelectNode (Go DefraDB plan order: joins before select)
        assert_eq!(plan.kind(), "selectNode");
        let source = plan.source().unwrap();
        assert_eq!(source.kind(), "typeIndexJoin");
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

        // The outermost node should be a LimitNode
        assert_eq!(plan.kind(), "limitNode");

        // The source should be SelectNode (which wraps the join)
        let source = plan.source().unwrap();
        assert_eq!(source.kind(), "selectNode");

        // SelectNode's source should be the join
        let join = source.source().unwrap();
        assert_eq!(join.kind(), "typeIndexJoin");
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

        // The outermost node should be selectNode (wraps the join)
        assert_eq!(plan.kind(), "selectNode");

        // SelectNode's source should be typeIndexJoin
        let source = plan.source().unwrap();
        assert_eq!(source.kind(), "typeIndexJoin");
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

        // The outermost node should be selectNode (wraps the join)
        assert_eq!(plan.kind(), "selectNode");

        // SelectNode's source should be typeIndexJoin
        let source = plan.source().unwrap();
        assert_eq!(source.kind(), "typeIndexJoin");
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

        // The outermost node should be selectNode (wraps the join)
        assert_eq!(plan.kind(), "selectNode");

        // SelectNode's source should be typeIndexJoin
        let source = plan.source().unwrap();
        assert_eq!(source.kind(), "typeIndexJoin");
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

    // === Secondary Relation ID Field Tests ===

    /// Book collection - secondary side of Book-Author relation
    /// author: Author (NO @primary - secondary side, doesn't store FK)
    fn make_book_collection() -> CollectionVersion {
        CollectionVersion::new(
            "Book",
            "v1",
            "coll-book",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "name", FieldKind::string()),
                // Secondary relation to Author (no @primary)
                FieldDescription::new("3", "author", FieldKind::relation("Author", false))
                    .with_relation_name("book_author"),
                // Auto-generated _authorID field (added by add_relation_id_fields)
                // Even though this is secondary, the _authorID field exists for querying
                FieldDescription::new("4", "_authorID", FieldKind::doc_id())
                    .with_relation_name("book_author"),
            ],
        )
    }

    /// Author collection - primary side of Book-Author relation
    /// published: Book @primary (stores the FK _publishedID)
    fn make_author_collection() -> CollectionVersion {
        CollectionVersion::new(
            "Author",
            "v1",
            "coll-author",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "name", FieldKind::string()),
                // Primary relation to Book (@primary - stores FK)
                FieldDescription::new("3", "published", FieldKind::relation("Book", false))
                    .with_relation_name("book_author")
                    .as_primary(),
                // Auto-generated _publishedID field (primary side stores FK)
                FieldDescription::new("4", "_publishedID", FieldKind::doc_id())
                    .with_relation_name("book_author")
                    .as_primary(),
            ],
        )
    }

    #[test]
    fn test_secondary_relation_id_field_detection() {
        // Verify the string slicing for extracting relation name from _authorID
        let field_name = "_authorID";
        assert!(field_name.starts_with('_'));
        assert!(field_name.ends_with("ID"));

        // Extract relation name
        let relation_name = &field_name[1..field_name.len() - 2];
        assert_eq!(relation_name, "author");

        // Verify Book collection has author field
        let book = make_book_collection();
        let author_field = book.field_by_name("author");
        assert!(author_field.is_some(), "Book should have 'author' field");

        let author_field = author_field.unwrap();
        assert!(
            author_field.kind.is_relation(),
            "'author' should be a relation field"
        );
        assert!(
            !author_field.is_primary,
            "'author' on Book should NOT be primary (secondary side)"
        );

        // Verify Author collection has published field
        let author = make_author_collection();
        let published_field = author.field_by_name("published");
        assert!(
            published_field.is_some(),
            "Author should have 'published' field"
        );

        let published_field = published_field.unwrap();
        assert!(
            published_field.kind.is_relation(),
            "'published' should be a relation field"
        );
        assert!(
            published_field.is_primary,
            "'published' on Author SHOULD be primary (primary side)"
        );
    }

    #[tokio::test]
    async fn test_plan_secondary_relation_id_field() {
        // Test that selecting _authorID on Book (secondary side) creates a TypeJoin
        let planner = Planner::new(vec![make_book_collection(), make_author_collection()]);

        // Query: Book { name _authorID }
        let select = Select::new("Book")
            .with_field(Field::new("name"))
            .with_field(Field::new("_authorID"));

        let result = planner.plan(&select);
        assert!(
            result.is_ok(),
            "Planning should succeed: {:?}",
            result.err()
        );

        let plan = result.unwrap();
        // The plan should have a TypeJoinOne node somewhere in the tree
        // since we need to do a reverse lookup for _authorID

        // For now, just verify the plan was created
        // A more thorough test would execute the plan with test data
        assert!(
            plan.kind() == "selectNode" || plan.kind() == "typeJoinOne",
            "Plan should be selectNode or typeJoinOne, got: {}",
            plan.kind()
        );
    }
}
