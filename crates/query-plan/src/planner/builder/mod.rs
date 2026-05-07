//! Query planner implementation
//!
//! Converts Select operations into executable plan trees.

mod filter_prep;
mod groupby;
mod index_methods;
mod scan_setup;
pub(crate) mod se_detection;

#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::sync::Arc;

use acp::DocumentACP;
use identity::Did;
use schema::CollectionVersion;
use tracing::{debug, instrument};

use crate::fetcher::DocFetcher;
use crate::plan::{IndexScanNode, PermissionFilterNode, SEFilterNode, ScanNode, SelectNode};
use crate::planner::index_selection::IndexScanParams;
use crate::planner::PlanNode;
use query_types::error::{QueryError, Result};
use query_types::limits::QueryLimits;
use query_types::mapper::{Requestable, Select};

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
    /// Pre-computed FTS scores: output_name → (doc_id → score)
    pub(crate) fts_scores: HashMap<String, HashMap<String, f64>>,
    /// Query parsing and filter evaluation guardrails.
    pub(crate) query_limits: QueryLimits,
}

impl Planner {
    pub fn fts_score_key(scope_path: &[String], output_name: &str) -> String {
        if scope_path.is_empty() {
            output_name.to_string()
        } else {
            format!("{}::{}", scope_path.join("."), output_name)
        }
    }

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
            fts_scores: HashMap::new(),
            query_limits: QueryLimits::default(),
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

    /// Set pre-computed FTS scores for BM25 nodes.
    pub fn with_fts_scores(mut self, scores: HashMap<String, HashMap<String, f64>>) -> Self {
        self.fts_scores = scores;
        self
    }

    /// Set query parsing and filter evaluation limits.
    pub fn with_query_limits(mut self, limits: QueryLimits) -> Self {
        self.query_limits = limits;
        self
    }

    /// Set ACP and identity for permission filtering in plans.
    pub fn with_acp(mut self, acp: Arc<dyn DocumentACP>, identity_did: Option<Did>) -> Self {
        self.acp = Some(acp);
        self.identity_did = identity_did;
        self
    }

    /// Whether this planner should insert ACP permission filters into plans.
    pub(super) fn has_acp(&self) -> bool {
        self.acp.is_some()
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

        // Route views through either the cached or live view planner based on materialization.
        if let Some(ref query_source) = collection.query {
            if collection.is_materialized {
                return self.build_cached_view_plan(select, &collection);
            }
            return self.build_view_plan(select, &collection, query_source);
        }

        // Build scan setup: mappings, join flags, ordering-only fields
        let scan_setup = self.build_scan_setup(select, &collection)?;
        let mut scan_mapping = scan_setup.scan_mapping;
        let needs_joins = scan_setup.needs_joins;
        let filter_relation_fields = scan_setup.filter_relation_fields;
        let filter_has_relations = scan_setup.filter_has_relations;
        let ordering_only_fields = scan_setup.ordering_only_fields;

        // Collect aggregate and similarity output names to detect _alias conditions
        // that should be deferred
        let mut computed_field_names: Vec<&str> = select
            .fields
            .iter()
            .filter_map(|f| match f {
                Requestable::Aggregate(agg) => Some(agg.output_name()),
                Requestable::Similarity(sim) => Some(sim.output_name()),
                Requestable::FullTextSearch(fts) => Some(fts.output_name()),
                _ => None,
            })
            .collect();
        // Deduplicate (in case of name collisions)
        computed_field_names.sort_unstable();
        computed_field_names.dedup();

        // Prepare filter components: scalar, relation, plan-level, complexity flag
        let filter_parts = self.prepare_filter(select, &collection, &computed_field_names);
        let scalar_filter = filter_parts.scalar_filter;
        let relation_filter = filter_parts.relation_filter;
        let filter_for_plan = filter_parts.filter_for_plan;
        let is_complex_filter = filter_parts.is_complex_filter;

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

        // 1b. Detect encrypted-indexed fields in filter and wrap with SEFilterNode.
        // This enables the planner to recognize SE indexes and route equality filters
        // through tag-based matching instead of plaintext comparison on remote nodes.
        if let Some(ref filter) = select.filter {
            let se_conditions =
                se_detection::detect_se_filter_conditions(filter, &collection, &scan_mapping);
            if !se_conditions.is_empty() {
                debug!(
                    collection = %select.collection_name,
                    encrypted_fields = se_conditions.len(),
                    "Detected encrypted-indexed fields in filter, adding SEFilterNode"
                );
                plan = Box::new(SEFilterNode::new(plan, se_conditions));
            }
        }

        // 2. Apply join nodes BEFORE SelectNode (matches Go DefraDB plan construction order).
        // TypeJoin nodes wrap the raw ScanNode, and SelectNode wraps the join result.
        // For simple filters: relation filters are extracted and applied inside TypeJoin nodes
        // For complex filters: pass None, the full filter is applied after join
        let filter_for_joins = if is_complex_filter {
            None // Don't pass filter to TypeJoin for complex filters
        } else {
            filter_for_plan.as_ref()
        };
        let joins_result = self.apply_joins(
            plan,
            select,
            &collection,
            scan_mapping,
            0,
            select.exhaustive,
            filter_for_joins,
            &[select.field.output_name().to_string()],
        )?;
        plan = joins_result.0;
        scan_mapping = joins_result.1;
        let aggregate_internal_keys = joins_result.2;
        let join_provides_ordering = joins_result.3;

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

        // 3c. ORDER BY relation joins are now handled by apply_joins via synthetic selects,
        // which also enables join direction inversion for index-based ordering.

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
            // Store relation filter on SelectNode for explain display only.
            // The actual relation filtering is handled by TypeJoin's RelationFilter.
            // We must NOT apply it as a real filter because after TypeJoinMany merges
            // sub-filtered children, re-evaluating the parent's relation filter would
            // fail when sub-filter and parent filter target different values.
            if let Some(ref rel_filter) = relation_filter {
                select_node = select_node.with_explain_filter(rel_filter.clone());
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

        // 4c2. Add BM25Nodes for BM25 full-text search fields.
        plan = self.add_bm25_nodes(
            plan,
            select,
            &scan_mapping,
            &[select.field.output_name().to_string()],
        )?;

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

        // Apply GroupBy/OrderBy/Limit (or just OrderBy/Aggregates/Limit without GROUP BY)
        plan = self.apply_groupby_ordering_limit(
            plan,
            select,
            &scan_mapping,
            index_provides_ordering,
            join_provides_ordering,
        )?;

        Ok(PlanResult {
            plan,
            index_scan,
            ordering_only_fields,
            aggregate_internal_keys,
        })
    }

    /// Get a collection schema by name.
    pub fn collection(&self, name: &str) -> Option<&Arc<CollectionVersion>> {
        self.collections.get(name)
    }
}
