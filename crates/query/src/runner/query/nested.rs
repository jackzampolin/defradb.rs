//! Planner orchestration and post-processing for nested queries.

use identity::Did;
use schema::CollectionVersion;
use serde_json::Value as JsonValue;
use std::collections::HashSet;
use std::sync::Arc;

use crate::error::Result;
use crate::mapper::{Requestable, Select};
use crate::planner::Planner;
use crate::txn::TransactionRegistry;
use std::collections::HashMap;

use super::super::fetcher::FetcherWrapper;
use super::super::{DocFetcher, QueryRunner};

impl<F: DocFetcher + 'static, R: TransactionRegistry> QueryRunner<F, R> {
    /// Execute a query with nested selections using the Planner.
    ///
    /// The Planner builds a proper join plan with TypeJoinOne/TypeJoinMany nodes.
    /// ScanNodes fetch their own data via the attached fetcher.
    /// ACP permission filtering is applied per-collection via PermissionFilterNode in the plan.
    pub(crate) async fn execute_nested_select_with_planner(
        &self,
        select: &Select,
        fetcher: &dyn DocFetcher,
        identity: Option<Did>,
    ) -> Result<JsonValue> {
        // Create a fetcher wrapper that can be shared across plan nodes
        // We need to wrap the reference in an Arc-compatible struct
        let fetcher_arc = FetcherWrapper::new(fetcher);

        // Build the plan using the Planner with fetcher support
        // Get all collections from provider for join planning
        let collections_map = self.collections_map().await?;
        let collections: Vec<CollectionVersion> =
            collections_map.values().map(|c| (**c).clone()).collect();

        // Pre-compute FTS scores from the inverted index before planning
        let mut fts_scores: HashMap<String, HashMap<String, f64>> = HashMap::new();
        for field in &select.fields {
            if let Requestable::FullTextSearch(fts) = field {
                let mut combined_scores: HashMap<String, f64> = HashMap::new();
                for target_field in &fts.target_fields {
                    if let Ok(scores) = fetcher
                        .search_fulltext_scored(&select.collection_name, target_field, &fts.query)
                        .await
                    {
                        for (doc_id, score) in scores {
                            *combined_scores.entry(doc_id).or_insert(0.0) += score;
                        }
                    }
                }
                fts_scores.insert(fts.output_name().to_string(), combined_scores);
            }
        }

        let mut planner = Planner::new(collections).with_fetcher(Arc::new(fetcher_arc));
        if !fts_scores.is_empty() {
            planner = planner.with_fts_scores(fts_scores);
        }
        if let Some(ref acp) = self.acp {
            planner = planner.with_acp(acp.clone(), identity);
        }
        if let Some(ref lens_store) = self.lens_store {
            planner = planner.with_lens_store(lens_store.clone());
        }
        let plan_result = planner.plan_with_index_info(select)?;
        let mut plan = plan_result.plan;
        let ordering_only_fields = plan_result.ordering_only_fields;
        let aggregate_internal_keys = plan_result.aggregate_internal_keys;

        // Get the mapping from the plan
        let mapping = plan.document_map().clone();

        // Execute the plan and collect results
        plan.init().await?;
        plan.start().await?;

        let mut results = Vec::new();

        while plan.next().await? {
            let doc = plan.value();
            let mut json = self.doc_to_json(doc, &mapping)?;

            // Strip ordering-only fields from nested objects.
            // These fields were added for ORDER BY but shouldn't appear in output.
            for (relation_field, nested_field) in &ordering_only_fields {
                if let Some(obj) = json.as_object_mut() {
                    if let Some(relation_value) = obj.get_mut(relation_field) {
                        if let Some(nested_obj) = relation_value.as_object_mut() {
                            nested_obj.remove(nested_field);
                        }
                    }
                }
            }

            results.push(json);
        }

        plan.close().await?;

        // Post-process relation-based aggregates
        // For aggregates like _count(books: {}), compute the value from joined data
        let results =
            self.compute_relation_aggregates(results, select, &aggregate_internal_keys)?;

        // Strip fields from relation data that were added for filter evaluation
        // but not explicitly requested in the selection set.
        let results = Self::clean_filter_only_relation_fields(results, select);

        // Apply deferred limit/offset to relation fields.
        // TypeJoinMany stores ALL children (for aggregates to count), so we apply
        // the select's limit/offset here after aggregates have been computed.
        let results = Self::apply_relation_limits(results, select);

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
        // Build map of relation output_name → allowed sub-field names
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
    fn apply_relation_limits(mut results: Vec<JsonValue>, select: &Select) -> Vec<JsonValue> {
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
