use acp::Identity;
use identity::Did;
use schema::CollectionVersion;
use serde_json::{Map, Value as JsonValue};
use std::collections::HashSet;
use std::sync::Arc;

use crate::document::documents_to_plan_docs;
use crate::error::{QueryError, Result};
use crate::mapper::{Requestable, Select};
use crate::plan::PermissionFilterNode;
use crate::planner::index_selection::{
    can_be_ordered_by_index, can_or_filter_use_index, select_best_index,
};
use crate::planner::Planner;
use crate::query_parse::{parse_query_with_limits, ExplainType};
use crate::txn::TransactionRegistry;

use super::super::fetcher::FetcherWrapper;
use super::super::plan;
use super::super::plan_drive;
use super::super::{DocFetcher, QueryRunner};

impl<F: DocFetcher + 'static, R: TransactionRegistry> QueryRunner<F, R> {
    /// Execute the query with variables and return explain output with execution metrics.
    /// Format matches Go DefraDB's executeAndExplainRequest output.
    pub(crate) async fn execute_explain_with_vars(
        &self,
        query: &str,
        caller_identity: Option<Did>,
        variables: Option<&std::collections::HashMap<String, JsonValue>>,
    ) -> Result<JsonValue> {
        let mut selects = parse_query_with_limits(query, variables, self.query_limits)?;
        if query.contains("@exhaustive") {
            for s in &mut selects {
                s.exhaustive = true;
            }
        }

        let mut operation_children: Vec<JsonValue> = Vec::new();
        let mut execution_success = true;
        let mut execution_errors: Vec<String> = Vec::new();

        for select in selects {
            let is_top_level_aggregate = Self::is_top_level_aggregate(&select);

            // Execute the select and collect metrics
            match self
                .execute_select_with_metrics(&select, caller_identity.clone())
                .await
            {
                Ok((explanation, _doc_count, _exec_count)) => {
                    // Ensure selectNode wrapper
                    let select_node_content = Self::ensure_select_node_wrapper(
                        explanation,
                        &select,
                        ExplainType::Execute,
                    );

                    if is_top_level_aggregate {
                        // Top-level aggregates use topLevelNode wrapper
                        let top_level_node = self.build_top_level_aggregate_explain(
                            &select,
                            select_node_content,
                            ExplainType::Execute,
                        );
                        operation_children.push(top_level_node);
                    } else {
                        // Regular queries use selectTopNode wrapper
                        let select_top_node = serde_json::json!({
                            "selectTopNode": select_node_content
                        });
                        operation_children.push(select_top_node);
                    }
                }
                Err(e) => {
                    // Parse errors should propagate directly — in Go, GraphQL schema
                    // validation errors happen before the explain handler runs, so they
                    // are returned as top-level GQL errors, not wrapped in executionErrors.
                    if matches!(&e, QueryError::Parse(_)) {
                        return Err(e);
                    }
                    execution_success = false;
                    execution_errors.push(e.to_string());
                }
            }
        }

        // Go's executeAndExplainRequest calls Next() on the top-level operationNode.
        // Each Next()=true yields one query result. After all queries, Next()=false.
        // So planExecutions = number_of_queries + 1, sizeOfResult = number_of_queries.
        let num_queries = operation_children.len() as i64;
        let plan_executions = num_queries + 1;
        let size_of_result = num_queries;

        // Build explain result with operationNode and execution metrics
        let mut explain_result = Map::new();
        explain_result.insert(
            "operationNode".to_string(),
            JsonValue::Array(operation_children),
        );
        explain_result.insert(
            "executionSuccess".to_string(),
            serde_json::json!(execution_success),
        );
        explain_result.insert(
            "planExecutions".to_string(),
            serde_json::json!(plan_executions),
        );
        explain_result.insert(
            "sizeOfResult".to_string(),
            serde_json::json!(size_of_result),
        );

        if !execution_errors.is_empty() {
            explain_result.insert(
                "executionErrors".to_string(),
                serde_json::json!(execution_errors),
            );
        }

        Ok(serde_json::json!({ "explain": JsonValue::Object(explain_result) }))
    }

    /// Execute a select and return the explain output with metrics.
    /// Format matches Go DefraDB's collectExecuteExplainInfo output.
    pub(crate) async fn execute_select_with_metrics(
        &self,
        select: &Select,
        caller_identity: Option<Did>,
    ) -> Result<(JsonValue, usize, u64)> {
        // Handle _commits system collection (no real collection exists)
        if select.collection_name == "_commits" {
            // Actually execute the commits query to get real metrics
            let results = self
                .execute_commits_query(select, self.fetcher.as_ref(), caller_identity)
                .await?;
            let doc_count = results.as_array().map(|a| a.len()).unwrap_or(0);

            // Build execute explain with real metrics matching Go's format:
            // dagScanNode.iterations = doc_count + 1 (includes terminal Next()=false)
            // selectNode.iterations = doc_count + 1
            // selectNode.filterMatches = doc_count
            let iterations = (doc_count as u64) + 1;
            let explanation = serde_json::json!({
                "selectNode": {
                    "filterMatches": doc_count as u64,
                    "iterations": iterations,
                    "dagScanNode": {
                        "iterations": iterations,
                    }
                }
            });
            return Ok((explanation, doc_count, 1));
        }

        // Get collection schema
        let collection = self
            .effective_provider()
            .get_collection(&select.collection_name)
            .await?
            .ok_or_else(|| QueryError::collection_not_found(&select.collection_name))?;

        // Embedded-only types (interface types from view SDL) are not root-queryable
        if collection.is_embedded_only {
            return Err(QueryError::parse(format!(
                "Cannot query field \"{}\" on type \"Query\".",
                select.collection_name
            )));
        }

        let fetcher = self.fetcher.as_ref();

        // Check if we can use an index (filter-based or ordering-based)
        let can_use_filter_index = select.doc_ids.is_none()
            && select.filter.is_some()
            && !collection.indexes.is_empty()
            && fetcher.supports_index_queries()
            && select
                .filter
                .as_ref()
                .map(|f| select_best_index(f, &collection.indexes).is_some())
                .unwrap_or(false);

        // Also use index when it can provide ordering (even without a filter)
        let can_use_ordering_index = select.doc_ids.is_none()
            && select.order_by.is_some()
            && !collection.indexes.is_empty()
            && fetcher.supports_index_queries()
            && select
                .order_by
                .as_ref()
                .map(|o| {
                    collection
                        .indexes
                        .iter()
                        .any(|idx| can_be_ordered_by_index(o, idx).0)
                })
                .unwrap_or(false);

        let can_use_or_filter_index = select.doc_ids.is_none()
            && select.filter.is_some()
            && !collection.indexes.is_empty()
            && fetcher.supports_index_queries()
            && select
                .filter
                .as_ref()
                .map(|f| can_or_filter_use_index(f, &collection.indexes))
                .unwrap_or(false);

        let can_use_index =
            can_use_filter_index || can_use_ordering_index || can_use_or_filter_index;

        // Check if any aggregates reference relations (e.g., _sum(articles: {field: pages}))
        // Relation aggregates need the Planner to create TypeJoinMany nodes
        let has_relation_aggregates = select.fields.iter().any(|f| {
            if let Requestable::Aggregate(agg) = f {
                agg.targets
                    .iter()
                    .any(|t| !t.host_name.is_empty() && t.host_name != select.collection_name)
            } else {
                false
            }
        });

        // Check if this query has nested selections (relations, _group, etc.)
        // These require the Planner to construct proper join/group nodes.
        let has_nested = select
            .fields
            .iter()
            .any(|f| matches!(f, Requestable::Select(_)));

        let filter_has_relations = select
            .filter
            .as_ref()
            .map(|f| f.has_relation_filters())
            .unwrap_or(false);

        let order_has_relations = select
            .order_by
            .as_ref()
            .map(|o| o.has_relation_order())
            .unwrap_or(false);

        let has_similarity = select
            .fields
            .iter()
            .any(|f| matches!(f, Requestable::Similarity(_)));

        if can_use_index
            || has_relation_aggregates
            || has_nested
            || filter_has_relations
            || order_has_relations
            || has_similarity
        {
            // Use Planner path for index-based queries, relation aggregates,
            // relation filters/ordering, or similarity
            let fetcher_arc = FetcherWrapper::new(fetcher);
            let collections_map = self.collections_map().await?;
            let collections: Vec<CollectionVersion> =
                collections_map.values().map(|c| (**c).clone()).collect();

            let mut planner = Planner::new(collections)
                .with_query_limits(self.query_limits)
                .with_fetcher(Arc::new(fetcher_arc))
                .with_acp(self.acp.clone(), caller_identity.clone());
            if let Some(ref lens_store) = self.lens_store {
                planner = planner.with_lens_store(lens_store.clone());
            }
            let plan_result = planner.plan_with_index_info(select)?;
            let mut plan = plan_result.plan;

            // Wrap with permission filter if needed (explain path)
            if let Some(ref policy) = collection.policy {
                plan = Box::new(PermissionFilterNode::new(
                    plan,
                    self.acp.clone(),
                    Identity::from(caller_identity),
                    &policy.id,
                    &policy.resource_name,
                ));
            }

            // Execute the plan and count iterations
            let outcome = async {
                plan.init().await?;
                plan.start().await?;

                // Go counts ALL next() calls (including the final false) for planExecutions
                let mut plan_executions: u64 = 0;
                let mut result_count: usize = 0;

                loop {
                    plan_executions += 1;
                    if !plan.next().await? {
                        break;
                    }
                    result_count += 1;
                }

                Ok((result_count, plan_executions))
            }
            .await;

            let (result_count, plan_executions) =
                plan_drive::close_after(plan.as_mut(), outcome).await?;

            // Use explain_execute to get metrics from each node
            let explanation = plan.explain_execute();

            Ok((explanation, result_count, plan_executions))
        } else {
            // Standard path: fetch all docs and build scan-based plan
            let mapping = plan::build_mapping(select, &collection)?;

            // A fetcher-backed scan streams documents one at a time, so it honours
            // show_deleted, the scan filter, and a downstream limit without ever
            // materializing the collection. doc_ids fetches specific documents
            // rather than scanning, so it stays materialized.
            let source = if select.show_deleted {
                plan::ScanSource::Fetcher(Arc::new(FetcherWrapper::new(fetcher)))
            } else if let Some(ref doc_ids) = select.doc_ids {
                // Deduplicate doc_ids while preserving order (Go compatibility)
                let mut seen = HashSet::new();
                let unique_ids: Vec<String> = doc_ids
                    .iter()
                    .filter(|id| seen.insert((*id).clone()))
                    .cloned()
                    .collect();
                let result = fetcher
                    .get_by_ids(&select.collection_name, &unique_ids)
                    .await?;
                plan::ScanSource::Docs(documents_to_plan_docs(&result.into_docs(), &mapping)?)
            } else {
                plan::ScanSource::Fetcher(Arc::new(FetcherWrapper::new(fetcher)))
            };

            // Build ACP filter config if collection has policy and ACP is configured
            let acp_filter = collection.policy.as_ref().map(|policy| plan::AcpFilter {
                acp: self.acp.clone(),
                identity: Identity::from(caller_identity),
                policy_id: policy.id.clone(),
                resource_name: policy.resource_name.clone(),
            });

            // Build the plan (ACP filter is inserted inside, after Select but before aggregates)
            let mut plan = plan::build_plan(
                select,
                source,
                mapping.clone(),
                &collection,
                acp_filter,
                self.query_limits,
            )?;

            // Execute the plan and count iterations
            let outcome = async {
                plan.init().await?;
                plan.start().await?;

                // Go counts ALL next() calls (including the final false) for planExecutions
                let mut plan_executions: u64 = 0;
                let mut result_count: usize = 0;

                loop {
                    plan_executions += 1;
                    if !plan.next().await? {
                        break;
                    }
                    result_count += 1;
                }

                Ok((result_count, plan_executions))
            }
            .await;

            let (result_count, plan_executions) =
                plan_drive::close_after(plan.as_mut(), outcome).await?;

            // Use explain_execute to get metrics from each node
            let explanation = plan.explain_execute();

            Ok((explanation, result_count, plan_executions))
        }
    }

    /// Add execution metrics to the explain output (Go format).
    /// Metrics are added to the scanNode, not the selectNode.
    #[allow(dead_code)]
    pub(crate) fn add_iterations_to_explain(
        mut explanation: JsonValue,
        iterations: u64,
        doc_fetches: usize,
        field_count: usize,
    ) -> JsonValue {
        // The explanation is { "selectNode": { "scanNode": { ... } } }
        // We need to add metrics to the scanNode
        Self::add_metrics_to_scan_node(&mut explanation, iterations, doc_fetches, field_count);
        explanation
    }

    /// Recursively find and add metrics to scanNode.
    /// If the scanNode has an indexName field, it's an index scan and gets indexFetches.
    #[allow(dead_code)]
    pub(crate) fn add_metrics_to_scan_node(
        value: &mut JsonValue,
        iterations: u64,
        doc_fetches: usize,
        field_count: usize,
    ) {
        if let Some(obj) = value.as_object_mut() {
            // Check if this object contains scanNode
            if let Some(scan_node) = obj.get_mut("scanNode") {
                if let Some(scan_obj) = scan_node.as_object_mut() {
                    scan_obj.insert("iterations".to_string(), serde_json::json!(iterations));
                    scan_obj.insert(
                        "docFetches".to_string(),
                        serde_json::json!(doc_fetches as u64),
                    );
                    // fieldFetches = number of fields per doc * number of docs fetched
                    let field_fetches = (field_count * doc_fetches) as u64;
                    scan_obj.insert("fieldFetches".to_string(), serde_json::json!(field_fetches));

                    // indexFetches is set by IndexScanNode::explain_inner() with
                    // the actual index key lookup count. For regular scans without an
                    // index, default to 0 (Go always includes this property).
                    if !scan_obj.contains_key("indexFetches") {
                        scan_obj.insert("indexFetches".to_string(), serde_json::json!(0u64));
                    }
                    return;
                }
            }

            // Recurse into child objects
            for (_, child) in obj.iter_mut() {
                Self::add_metrics_to_scan_node(child, iterations, doc_fetches, field_count);
            }
        }
    }
}
