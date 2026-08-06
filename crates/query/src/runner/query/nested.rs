//! Planner orchestration and post-processing for nested queries.

use identity::Did;
use schema::CollectionVersion;
use serde_json::Value as JsonValue;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, instrument};

use crate::error::Result;
use crate::mapper::{Requestable, Select};
use crate::planner::Planner;
use crate::txn::TransactionRegistry;

use super::super::fetcher::FetcherWrapper;
use super::super::plan_drive;
use super::super::{DocFetcher, QueryRunner};
use super::nested_profile::{NestedQueryProfile, ScopedFulltextProfile};

impl<F: DocFetcher + 'static, R: TransactionRegistry> QueryRunner<F, R> {
    /// Execute a query with nested selections using the Planner.
    ///
    /// The Planner builds a proper join plan with TypeJoinOne/TypeJoinMany nodes.
    /// ScanNodes fetch their own data via the attached fetcher.
    /// ACP permission filtering is applied per-collection via PermissionFilterNode in the plan.
    #[instrument(
        name = "query.execute_nested_select",
        level = "debug",
        skip(self, select, fetcher, identity),
        fields(collection = %select.collection_name, field = %select.field.output_name())
    )]
    pub(crate) async fn execute_nested_select_with_planner(
        &self,
        select: &Select,
        fetcher: &dyn DocFetcher,
        identity: Option<Did>,
    ) -> Result<JsonValue> {
        let mut profile = NestedQueryProfile::default();

        // Build the plan using the Planner with fetcher support
        // Get all collections from provider for join planning
        let collections_map = self.collections_map().await?;

        // Validate groupBy and field references before planning
        let collection = self.get_collection(&select.collection_name).await?;
        super::super::plan::validate_select(select, &collection)?;

        // Create a fetcher wrapper that can be shared across plan nodes
        // We need to wrap the reference in an Arc-compatible struct
        let fetcher_arc = FetcherWrapper::new(fetcher);
        let collections: Vec<CollectionVersion> =
            collections_map.values().map(|c| (**c).clone()).collect();

        // Pre-compute FTS scores from the inverted index before planning.
        // Supports dotted relation paths like `file.name` and `functions.content`
        // by querying the leaf collection's BM25 index and lifting scores back
        // onto the root collection through relation foreign keys.
        let precompute_fulltext_start = Instant::now();
        let fts_scores = self
            .precompute_fulltext_scores(select, fetcher, &collections_map)
            .await?;
        profile.precompute_fulltext_elapsed = precompute_fulltext_start.elapsed();

        let plan_build_start = Instant::now();
        let mut planner = Planner::new(collections)
            .with_query_limits(self.query_limits)
            .with_fetcher(Arc::new(fetcher_arc))
            .with_acp(self.acp.clone(), identity);
        if !fts_scores.is_empty() {
            planner = planner.with_fts_scores(fts_scores);
        }
        if let Some(ref lens_store) = self.lens_store {
            planner = planner.with_lens_store(lens_store.clone());
        }
        let plan_result = planner.plan_with_index_info(select)?;
        profile.plan_build_elapsed = plan_build_start.elapsed();
        let mut plan = plan_result.plan;
        let ordering_only_fields = plan_result.ordering_only_fields;
        let aggregate_internal_keys = plan_result.aggregate_internal_keys;

        // Get the mapping from the plan
        let mapping = plan.document_map().clone();

        // Execute the plan and collect results
        let plan_init_start = Instant::now();
        plan.init().await?;
        profile.plan_init_elapsed = plan_init_start.elapsed();
        let plan_start_start = Instant::now();
        plan.start().await?;
        profile.plan_start_elapsed = plan_start_start.elapsed();

        let mut results = Vec::new();

        loop {
            let plan_iteration_start = Instant::now();
            let has_next = plan.next().await?;
            profile.plan_iteration_elapsed += plan_iteration_start.elapsed();
            if !has_next {
                break;
            }

            let doc = plan.value();

            let doc_render_start = Instant::now();
            let mut json = self.doc_to_json(doc, &mapping)?;
            profile.doc_render_elapsed += doc_render_start.elapsed();

            // Strip ordering-only fields from nested objects.
            // These fields were added for ORDER BY but shouldn't appear in output.
            let ordering_only_strip_start = Instant::now();
            for (relation_field, nested_field) in &ordering_only_fields {
                if let Some(obj) = json.as_object_mut() {
                    if let Some(relation_value) = obj.get_mut(relation_field) {
                        if let Some(nested_obj) = relation_value.as_object_mut() {
                            nested_obj.remove(nested_field);
                        }
                    }
                }
            }
            profile.ordering_only_strip_elapsed += ordering_only_strip_start.elapsed();

            results.push(json);
        }

        let outcome = Ok::<_, crate::error::QueryError>((
            results,
            plan.exec_info(),
            // Capture cursor page-info BEFORE close() releases plan resources.
            // Non-cursor selects don't need it, so skip the wrapper-node
            // traversal entirely — it would just return `None` after walking
            // the plan tree.
            if select.is_cursor {
                plan.page_info()
            } else {
                None
            },
        ));

        let plan_close_start = Instant::now();
        let (results, plan_exec_info, cursor_page_info) =
            plan_drive::close_after(plan.as_mut(), outcome).await?;
        profile.plan_close_elapsed = plan_close_start.elapsed();

        // Post-process relation-based aggregates
        // For aggregates like _count(books: {}), compute the value from joined data
        let relation_aggregate_start = Instant::now();
        let results =
            self.compute_relation_aggregates(results, select, &aggregate_internal_keys)?;
        profile.relation_aggregate_elapsed = relation_aggregate_start.elapsed();

        // For nested relation-local BM25, score the already-joined relation scope instead of
        // precomputing against the full leaf collection. This preserves correct nested ordering
        // while avoiding a full-corpus BM25 pass for session-scoped child queries.
        let mut scoped_fulltext = ScopedFulltextProfile::default();
        let scoped_fulltext_start = Instant::now();
        let results = Self::apply_scoped_relation_fulltext_with_profile(
            results,
            select,
            &mut scoped_fulltext,
        );
        profile.scoped_fulltext_elapsed = scoped_fulltext_start.elapsed();
        profile.scoped_fulltext = scoped_fulltext;

        // Strip fields from relation data that were added for filter evaluation
        // but not explicitly requested in the selection set.
        let clean_filter_only_fields_start = Instant::now();
        let results = Self::clean_filter_only_relation_fields(results, select);
        profile.clean_filter_only_fields_elapsed = clean_filter_only_fields_start.elapsed();

        // Apply deferred limit/offset to relation fields.
        // TypeJoinMany stores ALL children (for aggregates to count), so we apply
        // the select's limit/offset here after aggregates have been computed.
        let relation_limits_start = Instant::now();
        let results = Self::apply_relation_limits(results, select);
        profile.relation_limits_elapsed = relation_limits_start.elapsed();
        profile.result_count = results.len();

        debug!(
            plan_docs_fetched = plan_exec_info.docs_fetched,
            plan_fields_fetched = plan_exec_info.fields_fetched,
            plan_indexes_fetched = plan_exec_info.indexes_fetched,
            plan_iterations = plan_exec_info.iterations,
            precompute_fulltext = ?profile.precompute_fulltext_elapsed,
            plan_build = ?profile.plan_build_elapsed,
            plan_init = ?profile.plan_init_elapsed,
            plan_start = ?profile.plan_start_elapsed,
            plan_iteration = ?profile.plan_iteration_elapsed,
            doc_render = ?profile.doc_render_elapsed,
            ordering_only_strip = ?profile.ordering_only_strip_elapsed,
            plan_close = ?profile.plan_close_elapsed,
            relation_aggregates = ?profile.relation_aggregate_elapsed,
            scoped_fulltext = ?profile.scoped_fulltext_elapsed,
            clean_filter_only_fields = ?profile.clean_filter_only_fields_elapsed,
            relation_limits = ?profile.relation_limits_elapsed,
            scoped_fulltext_calls = profile.scoped_fulltext.scoring_calls,
            scoped_fulltext_sort_calls = profile.scoped_fulltext.sort_calls,
            scoped_fulltext_top_k_calls = profile.scoped_fulltext.top_k_calls,
            scoped_fulltext_items_seen = profile.scoped_fulltext.items_seen,
            scoped_fulltext_target_fields_seen = profile.scoped_fulltext.target_fields_seen,
            scoped_fulltext_docs_indexed = profile.scoped_fulltext.docs_indexed,
            scoped_fulltext_scoring = ?profile.scoped_fulltext.scoring_elapsed,
            scoped_fulltext_sort = ?profile.scoped_fulltext.sort_elapsed,
            scoped_fulltext_top_k = ?profile.scoped_fulltext.top_k_elapsed,
            result_count = profile.result_count,
            "nested query profile"
        );

        // For cursor queries, wrap results in the cursor response envelope.
        if let Some(pi) = cursor_page_info {
            let inner_key = select.field.output_name().to_string();
            let mut cursor_obj = serde_json::Map::new();
            cursor_obj.insert(inner_key, JsonValue::Array(results));
            if pi.fields.any_selected() {
                let mut pageinfo = serde_json::Map::new();
                if let Some(key) = pi.fields.has_next.as_ref() {
                    pageinfo.insert(key.clone(), JsonValue::Bool(pi.has_next));
                }
                if let Some(key) = pi.fields.has_prev.as_ref() {
                    pageinfo.insert(key.clone(), JsonValue::Bool(pi.has_prev));
                }
                if let Some(key) = pi.fields.start_cursor.as_ref() {
                    pageinfo.insert(
                        key.clone(),
                        pi.start_cursor
                            .map(JsonValue::String)
                            .unwrap_or(JsonValue::Null),
                    );
                }
                if let Some(key) = pi.fields.end_cursor.as_ref() {
                    pageinfo.insert(
                        key.clone(),
                        pi.end_cursor
                            .map(JsonValue::String)
                            .unwrap_or(JsonValue::Null),
                    );
                }
                // Go always renders the literal `_pageInfo` key regardless of any alias.
                cursor_obj.insert("_pageInfo".to_string(), JsonValue::Object(pageinfo));
            }
            return Ok(JsonValue::Object(cursor_obj));
        }

        Ok(JsonValue::Array(results))
    }

    /// Strip filter-only fields from relation data in query results.
    ///
    /// When the planner adds relation joins for filter evaluation (e.g., filtering
    /// Author by book.publisher.yearOpened), those relations get render_keys so
    /// the filter can evaluate on rendered JSON. This causes the relation field to
    /// appear in output even though the user didn't request it. This function
    /// retains only the fields explicitly listed in each nested Select.
    fn clean_filter_only_relation_fields(
        mut results: Vec<JsonValue>,
        select: &Select,
    ) -> Vec<JsonValue> {
        // Build map of relation output_name -> allowed sub-field names
        let mut relation_allowed_fields: Vec<(String, HashSet<String>)> = Vec::new();

        for requestable in &select.fields {
            if let Requestable::Select(nested_select) = requestable {
                if nested_select.field.name == "GROUP" {
                    continue;
                }
                let mut allowed = HashSet::new();
                // _docID is always implicit
                allowed.insert("_docID".to_string());
                for sub_field in &nested_select.fields {
                    match sub_field {
                        Requestable::Field(f) => {
                            allowed.insert(f.output_name().to_string());
                        }
                        Requestable::Select(s) => {
                            allowed.insert(s.field.output_name().to_string());
                        }
                        Requestable::Aggregate(a) => {
                            allowed.insert(a.output_name().to_string());
                        }
                        Requestable::Similarity(s) => {
                            allowed.insert(s.output_name().to_string());
                        }
                        Requestable::FullTextSearch(fts) => {
                            allowed.insert(fts.output_name().to_string());
                        }
                    }
                }
                relation_allowed_fields
                    .push((nested_select.field.output_name().to_string(), allowed));
            }
        }

        if relation_allowed_fields.is_empty() {
            return results;
        }

        for result in &mut results {
            if let JsonValue::Object(ref mut obj) = result {
                for (relation_name, allowed_fields) in &relation_allowed_fields {
                    if let Some(relation_data) = obj.get_mut(relation_name.as_str()) {
                        match relation_data {
                            JsonValue::Array(items) => {
                                for item in items.iter_mut() {
                                    if let JsonValue::Object(item_obj) = item {
                                        item_obj.retain(|k, _| allowed_fields.contains(k));
                                    }
                                }
                            }
                            JsonValue::Object(item_obj) => {
                                item_obj.retain(|k, _| allowed_fields.contains(k));
                            }
                            _ => {}
                        }
                    }
                }
            }
        }

        results
    }

    /// Apply deferred limit/offset to relation fields in query results.
    ///
    /// TypeJoinMany stores ALL children so that relation aggregates (e.g., _count)
    /// can see the full set. This function applies the limit/offset from the select's
    /// nested relation fields after aggregates have been computed.
    pub(crate) fn apply_relation_limits(
        mut results: Vec<JsonValue>,
        select: &Select,
    ) -> Vec<JsonValue> {
        // Collect relation fields with limits
        let mut relation_limits: Vec<(String, u64, u64)> = Vec::new(); // (field_name, limit, offset)
        for requestable in &select.fields {
            if let Requestable::Select(nested_select) = requestable {
                if nested_select.field.name == "GROUP" {
                    continue; // _group is handled by GroupByNode
                }
                if let Some(ref limit) = nested_select.limit {
                    let limit_val = limit.limit.unwrap_or(0); // 0 means no limit
                    let offset_val = limit.offset;
                    if limit_val > 0 || offset_val > 0 {
                        relation_limits.push((
                            nested_select.field.output_name().to_string(),
                            limit_val,
                            offset_val,
                        ));
                    }
                }
            }
        }

        if relation_limits.is_empty() {
            return results;
        }

        for result in &mut results {
            if let JsonValue::Object(ref mut obj) = result {
                for (field_name, limit, offset) in &relation_limits {
                    if let Some(JsonValue::Array(items)) = obj.get_mut(field_name) {
                        let offset = *offset as usize;
                        let total = items.len();
                        if offset >= total {
                            *items = Vec::new();
                        } else {
                            let remaining: Vec<JsonValue> = items.drain(offset..).collect();
                            *items = if *limit > 0 {
                                remaining.into_iter().take(*limit as usize).collect()
                            } else {
                                remaining
                            };
                        }
                    }
                }
            }
        }

        results
    }
}

#[cfg(test)]
#[path = "nested_tests.rs"]
mod tests;
