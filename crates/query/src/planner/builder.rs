//! Query planner implementation
//!
//! Converts Select operations into executable plan trees.

use std::collections::HashMap;
use std::sync::Arc;

use acp::DocumentACP;
use identity::Did;
use schema::CollectionVersion;
use tracing::{debug, instrument, warn};

use crate::error::{QueryError, Result};
use crate::fetcher::DocFetcher;
use crate::mapper::{AggregateType, Filter, OrderBy, Requestable, Select};
use crate::plan::groupby::ChildSelectMeta;
use crate::plan::{
    AllDocsNode, GroupAlias, GroupByNode, IndexScanNode, InnerAggregateDef, LimitNode, OrderByNode,
    PermissionFilterNode, ScanNode, SelectNode,
};
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
pub(super) const MAX_NESTING_DEPTH: usize = 10;

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
    /// Internal render keys for aggregate relation data when there's a collision
    /// with a relation selection (e.g., both `_count(published: {})` and `published(limit: 2)`).
    /// Maps: aggregate_output_name -> (relation_field_name, internal_key)
    pub aggregate_internal_keys: HashMap<String, (String, String)>,
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
    pub(super) collections: HashMap<String, Arc<CollectionVersion>>,
    /// Available collection schemas by CollectionID (CID)
    /// This is needed because FieldKind::Relation stores the CollectionID, not the name
    pub(super) collections_by_id: HashMap<String, Arc<CollectionVersion>>,
    /// Optional fetcher for ScanNodes to load data on-demand
    pub(super) fetcher: Option<Arc<dyn DocFetcher>>,
    /// Optional lens transform store for view queries with transforms
    pub(crate) lens_store: Option<Arc<dyn lens::TransformStore>>,
    /// Optional ACP for permission filtering in plans
    acp: Option<Arc<dyn DocumentACP>>,
    /// Identity for ACP permission checks
    identity_did: Option<Did>,
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
            lens_store: None,
            acp: None,
            identity_did: None,
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

    /// Set a lens transform store for view queries with transforms.
    pub fn with_lens_store(mut self, store: Arc<dyn lens::TransformStore>) -> Self {
        self.lens_store = Some(store);
        self
    }

    /// Set ACP and identity for permission filtering in plans.
    pub fn with_acp(mut self, acp: Arc<dyn DocumentACP>, identity_did: Option<Did>) -> Self {
        self.acp = Some(acp);
        self.identity_did = identity_did;
        self
    }

    /// Conditionally wrap a plan with a PermissionFilterNode if the collection has an ACP policy.
    pub(super) fn maybe_wrap_with_acp_filter(
        &self,
        plan: Box<dyn PlanNode>,
        collection: &CollectionVersion,
    ) -> Box<dyn PlanNode> {
        if let (Some(ref acp), Some(ref policy)) = (&self.acp, &collection.policy) {
            Box::new(PermissionFilterNode::from_optional_did(
                plan,
                acp.clone(),
                self.identity_did.clone(),
                &policy.id,
                &policy.resource_name,
            ))
        } else {
            plan
        }
    }

    /// Get a collection by name or CollectionID.
    ///
    /// Relation fields store the CollectionID (CID) in their Kind, but we need
    /// the collection to resolve the relation. This method tries both lookups:
    /// 1. First by name (for Named kind fields and root queries)
    /// 2. Then by CollectionID (for Relation kind fields)
    pub(crate) fn get_collection(&self, name_or_id: &str) -> Option<Arc<CollectionVersion>> {
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
    #[instrument(
        name = "query.plan",
        skip(self, select),
        fields(collection = %select.collection_name)
    )]
    pub fn plan_with_index_info(&self, select: &Select) -> Result<PlanResult> {
        let collection = self
            .collections
            .get(&select.collection_name)
            .ok_or_else(|| QueryError::collection_not_found(&select.collection_name))?
            .clone();

        // If this collection is a non-materialized view, build a view plan instead
        if let Some(ref query_source) = collection.query {
            return self.build_view_plan(select, &collection, query_source);
        }

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
                            .contains(&nested_field_name)
                        {
                            result.push((relation_field_name.clone(), nested_field_name.clone()));
                        }
                    }
                }
                result
            })
            .unwrap_or_default();

        // Internal keys for aggregate relation data when there's a collision with a relation selection.
        // Maps: aggregate_output_name -> (relation_field_name, internal_key)
        #[allow(unused_assignments)]
        let mut aggregate_internal_keys: HashMap<String, (String, String)> = HashMap::new();

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
                if scan_mapping
                    .try_find_index_from_render_key(output_name)
                    .is_none()
                {
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
                                // It's an inline array field — ensure it's in scan_mapping
                                // with a render_key so data appears in output for
                                // compute_relation_aggregates().
                                let idx = if let Some(existing) =
                                    scan_mapping.first_index_of_name(host_name)
                                {
                                    existing
                                } else {
                                    let new_idx = scan_mapping.next_index();
                                    scan_mapping.add(new_idx, host_name);
                                    new_idx
                                };
                                if !scan_mapping
                                    .render_keys
                                    .iter()
                                    .any(|rk| rk.key == *host_name)
                                {
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
                    && collection.field_by_name(&sim.target_field).is_some()
                {
                    let idx = scan_mapping.next_index();
                    scan_mapping.add(idx, &sim.target_field);
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
                conditions.insert("_docID".to_string(), serde_json::json!({"_eq": doc_ids[0]}));
                Filter::from_conditions(conditions)
            } else {
                let mut conditions = HashMap::new();
                conditions.insert("_docID".to_string(), serde_json::json!({"_in": doc_ids}));
                Filter::from_conditions(conditions)
            };
            match filter_for_plan {
                Some(existing) => {
                    // Flat-merge _docID into existing conditions instead of wrapping in _and.
                    // Using .and() would create {_and: [conditions, {_docID: ...}]} which defeats
                    // split_by_relation() — the entire _and block gets classified as "relation"
                    // if any inner condition references a relation field.
                    let mut merged = existing.conditions().clone();
                    for (k, v) in doc_ids_filter.conditions() {
                        merged.insert(k.clone(), v.clone());
                    }
                    Some(Filter::from_conditions(merged))
                }
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
                    // Skip logical operators (_and, _or, _not) that contain relation filters.
                    // These were put in relation_filter by split_by_relation() because they
                    // contain relation conditions. They must be evaluated AFTER joins, not here.
                    if field_name == "_and" || field_name == "_or" || field_name == "_not" {
                        continue;
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
            // Apply scalar filter as residual filter on IndexScanNode.
            // The index may only cover part of the filter (e.g., first field of composite index),
            // so remaining conditions are applied as post-filtering on the fetched documents.
            // Strip _alias for complex filters (same reason as ScanNode below).
            if let Some(ref filter) = scalar_filter {
                if is_complex_filter && filter.has_alias_filter() {
                    let (non_alias, _alias) = filter.split_alias();
                    if let Some(f) = non_alias {
                        index_scan_node = index_scan_node.with_residual_filter(f);
                    }
                } else {
                    index_scan_node = index_scan_node.with_residual_filter(filter.clone());
                }
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
            // Push scalar filter to ScanNode (matches Go DefraDB plan construction).
            // For complex filters, strip _alias conditions: they reference aliased
            // relation fields whose data is only available after TypeJoin populates
            // the relation index. The full filter (including _alias) is applied after
            // joins at the complex filter SelectNode.
            if let Some(ref filter) = scalar_filter {
                if is_complex_filter && filter.has_alias_filter() {
                    let (non_alias, _alias) = filter.split_alias();
                    if let Some(f) = non_alias {
                        scan = scan.with_filter(f);
                    }
                } else {
                    scan = scan.with_filter(filter.clone());
                }
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
        let joins_result =
            self.apply_joins(plan, select, &collection, scan_mapping, 0, filter_for_joins)?;
        plan = joins_result.0;
        scan_mapping = joins_result.1;
        aggregate_internal_keys = joins_result.2;

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
                    let (new_plan, new_mapping) = self.apply_multi_level_filter_joins(
                        plan,
                        &path,
                        &collection,
                        filter,
                        scan_mapping.clone(),
                    )?;
                    plan = new_plan;
                    scan_mapping = new_mapping;
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
        // Also skip when filter_for_joins was provided (non-complex filter) because
        // apply_joins already handles relation filter joins via parent_filter.
        if filter_has_relations && filter_for_joins.is_none() {
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
            // Set relation filter on SelectNode for explain display (matches Go DefraDB).
            // The actual relation filtering is handled by TypeJoin's RelationFilter,
            // but Go's selectNode stores the relation filter and shows it in explain output.
            if let Some(ref rel_filter) = relation_filter {
                select_node = select_node.with_filter(rel_filter.clone());
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
                    let mut select_node =
                        SelectNode::new(plan, scan_mapping.clone()).with_filter(f);
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
        // Only apply alias conditions that do NOT reference aggregate fields.
        // Aggregate alias conditions (e.g., _alias: {publishedCount: {_gt: 0}}) must wait
        // until after aggregate nodes compute their values (handled by compute_relation_aggregates).
        if select.group_by.is_none() {
            if let Some(ref filter) = select.filter {
                let (_non_alias, alias_filter) = filter.split_alias();
                if let Some(alias_f) = alias_filter {
                    // Build a list of aggregate-only output names (not similarity).
                    // Similarity alias conditions CAN be applied here (after SimilarityNode),
                    // but aggregate alias conditions must be deferred.
                    let aggregate_only_names: Vec<&str> = select
                        .fields
                        .iter()
                        .filter_map(|f| {
                            if let Requestable::Aggregate(agg) = f {
                                Some(agg.output_name())
                            } else {
                                None
                            }
                        })
                        .collect();
                    let (stripped_alias, _) =
                        alias_f.strip_aggregate_alias_conditions(&aggregate_only_names);
                    // Only apply alias filter if there are non-aggregate alias conditions remaining
                    if !stripped_alias.conditions().is_empty() {
                        plan = Box::new(
                            SelectNode::new(plan, scan_mapping.clone()).with_filter(stripped_alias),
                        );
                    }
                }
            }
        }

        // Insert ACP permission filter for the root collection (if ACP-protected).
        // Position: after Select/joins/similarity but before GroupBy/Aggregates/OrderBy/Limit.
        plan = self.maybe_wrap_with_acp_filter(plan, &collection);

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

                            // Merge outer groupBy fields into the _group's groupBy.
                            // Go convention: childSelects.groupBy = inner fields ++ outer fields.
                            if let Some(ref outer_gb) = select.group_by {
                                if let Some(ref mut inner_fields) = meta.group_by {
                                    for field in &outer_gb.fields {
                                        if !inner_fields.contains(field) {
                                            inner_fields.push(field.clone());
                                        }
                                    }
                                }
                            }

                            child_selects_meta.push(meta);
                        }
                    }
                }
                // Go adds {field: {_neq: null}} to childSelects filter for average aggregates.
                // Average excludes null values, so the filter is needed on the group's child select.
                // Collect field names from average aggregates targeting _group.
                // Only regular fields (not aggregate refs like _avg) get the neq filter.
                let mut avg_group_fields: Vec<String> = Vec::new();
                for field in &select.fields {
                    if let Requestable::Aggregate(agg) = field {
                        if agg.aggregate_type == AggregateType::Average {
                            for target in &agg.targets {
                                if target.host_name == "_group" {
                                    if let Some(ref field_name) = target.field_name {
                                        if !field_name.starts_with('_') {
                                            avg_group_fields.push(field_name.clone());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                if !avg_group_fields.is_empty() {
                    // Ensure at least one default child select exists
                    // (query may have _avg(_group: ...) without explicit _group { ... } select)
                    if child_selects_meta.is_empty() {
                        child_selects_meta.push(ChildSelectMeta {
                            collection_name: select.collection_name.clone(),
                            ..Default::default()
                        });
                    }
                    // Inject {field_name: {_neq: null}} for each avg target field
                    for field_name in &avg_group_fields {
                        for cs in &mut child_selects_meta {
                            let mut conditions = cs
                                .filter
                                .as_ref()
                                .map(|f| f.conditions().clone())
                                .unwrap_or_default();
                            conditions
                                .entry(field_name.clone())
                                .and_modify(|v| {
                                    if let serde_json::Value::Object(ref mut ops) = v {
                                        ops.insert("_neq".to_string(), serde_json::Value::Null);
                                    }
                                })
                                .or_insert(serde_json::json!({
                                    "_neq": serde_json::Value::Null
                                }));
                            cs.filter = Some(Filter::from_conditions(conditions));
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
            // WITHOUT GROUP BY: OrderBy → [AllDocsNode] → Aggregates → Limit
            // Go applies limit AFTER aggregates so limit restricts the final output,
            // not the documents fed to aggregation.

            // 5. Apply order by (before aggregates)
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

            // 6. Count simple (non-per-row) aggregates to determine if we need AllDocsNode.
            // Relation-based and inline-array aggregates use child_aggregate_source and
            // operate per-row (each parent gets its own aggregate). They do NOT need
            // AllDocsNode. Only simple field aggregates (e.g., _sum(Age: {})) need it
            // because they accumulate across all documents.
            let simple_aggregate_count = select
                .fields
                .iter()
                .filter(|f| {
                    if let Requestable::Aggregate(agg) = f {
                        // Simple aggregate: all targets have empty host_name
                        agg.targets.iter().all(|t| t.host_name.is_empty())
                    } else {
                        false
                    }
                })
                .count();

            // If there are multiple simple aggregates, wrap in AllDocsNode so they all
            // can access the original documents via current_group_docs()
            if simple_aggregate_count > 1 {
                plan = Box::new(AllDocsNode::new(plan, scan_mapping.clone()));
            }

            // 7. Add aggregate nodes (for top-level aggregates without GROUP BY)
            plan = self.add_aggregate_nodes(plan, select, &scan_mapping)?;

            // 8. Apply limit/offset (AFTER aggregates, matching Go behavior)
            if let Some(ref limit) = select.limit {
                let effective_limit = match limit.limit {
                    Some(0) => None, // limit: 0 means no limit (Go compatibility)
                    other => other,
                };
                if effective_limit.is_some() || limit.offset > 0 {
                    plan = Box::new(LimitNode::new(plan, effective_limit, limit.offset));
                }
            }
        }

        Ok(PlanResult {
            plan,
            index_scan,
            ordering_only_fields,
            aggregate_internal_keys,
        })
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

        // Extract limit/offset from select for passing to index scan
        let limit = select.limit.as_ref().and_then(|l| l.limit);
        let offset = select.limit.as_ref().map(|l| l.offset).unwrap_or(0);

        // Try filter-based index selection first
        if let Some(filter) = select.filter.as_ref() {
            if let Some(best_index) = select_best_index(filter, &collection.indexes) {
                if let Some(params) = filter_to_index_scan(
                    filter,
                    best_index,
                    select.order_by.as_ref(),
                    &collection.fields,
                    limit,
                    offset,
                ) {
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
                        // Pass limit/offset for early termination (index provides ordering)
                        limit,
                        offset,
                    };
                    return Some((params, true));
                }
            }
        }

        None
    }

    /// Try to select an index for a child collection scan.
    ///
    /// Returns `Some((IndexScanParams, per_parent_scan))` if an index can service
    /// the filter or ordering, `None` otherwise. `per_parent_scan` is true when
    /// the index is used (enabling per-parent re-scanning for correct Go metrics).
    pub(super) fn try_select_child_index(
        &self,
        filter: Option<&Filter>,
        order_by: Option<&OrderBy>,
        collection: &CollectionVersion,
    ) -> Option<(IndexScanParams, bool)> {
        if collection.indexes.is_empty() {
            return None;
        }
        // Require a fetcher that supports index queries (matches top-level logic)
        match self.fetcher {
            Some(ref fetcher) if fetcher.supports_index_queries() => {}
            _ => return None,
        }
        // Try filter-based index first
        if let Some(filter) = filter {
            if let Some(best_index) = select_best_index(filter, &collection.indexes) {
                if let Some(params) =
                    filter_to_index_scan(filter, best_index, None, &collection.fields, None, 0)
                {
                    return Some((params, true));
                }
            }
        }
        // Fallback: try ordering-based index selection (scan all in index order)
        if let Some(order_by) = order_by {
            for index in &collection.indexes {
                let (can_order, needs_reverse) = can_be_ordered_by_index(order_by, index);
                if can_order {
                    return Some((
                        IndexScanParams {
                            index_name: index.name.clone(),
                            scan_type: IndexScanType::PrefixScan {
                                prefix_values: vec![],
                                reverse: needs_reverse,
                            },
                            limit: None,
                            offset: 0,
                        },
                        true,
                    ));
                }
            }
        }
        None
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
            IndexScanType::InScan { values, .. } => {
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
