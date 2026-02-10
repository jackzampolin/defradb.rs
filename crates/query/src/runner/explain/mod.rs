//! Query explain functionality
//!
//! Contains methods for generating query plan explanations:
//! - `explain_query_with_identity()` / `explain_query_with_identity_and_vars()`
//! - `explain_mutation_with_identity()` / `execute_mutation_explain()`
//! - `explain_select()` / `explain_nested_select()` / `explain_simple_select()`
//! - `explain_commits_select()` / `ensure_select_node_wrapper()`
//! - `execute_explain_with_vars()` / `execute_select_with_metrics()`
//! - `is_top_level_aggregate()` / `strip_aggregate_wrappers()`
//! - `build_top_level_aggregate_explain()`
//! - `merge_execute_metrics()` / `add_iterations_to_explain()` / `add_metrics_to_scan_node()`
//! - `execute_single_mutation_with_metrics()`

use acp::Identity;
use identity::Did;
use schema::CollectionVersion;
use serde_json::{Map, Value as JsonValue};
use std::collections::HashSet;
use std::sync::Arc;

use crate::document::{documents_to_plan_docs, documents_with_status_to_plan_docs};
use crate::error::{QueryError, Result};
use crate::mapper::{Requestable, Select};
use crate::plan::PermissionFilterNode;
use crate::planner::index_selection::{
    can_be_ordered_by_index, can_or_filter_use_index, select_best_index,
};
use crate::planner::Planner;
use crate::query_parse::{parse_query_with_variables, ExplainType};
use crate::txn::TransactionRegistry;

use super::fetcher::FetcherWrapper;
use super::plan;
use super::{DocFetcher, QueryRunner};

impl<F: DocFetcher + 'static, R: TransactionRegistry> QueryRunner<F, R> {
    /// Generate an explanation of the query plan.
    ///
    /// Used when queries include the @explain directive.
    /// Supports three modes:
    /// - Simple: Query plan structure without execution
    /// - Execute: Run the query and return plan structure with execution metrics
    /// - Debug: All plan nodes including internal ones
    ///
    /// Output format matches Go DefraDB:
    /// ```json
    /// {
    ///   "explain": {
    ///     "operationNode": [
    ///       {
    ///         "selectTopNode": {
    ///           "selectNode": { ... "scanNode": { ... } }
    ///         }
    ///       }
    ///     ]
    ///   }
    /// }
    /// ```
    pub async fn explain_query_with_identity(
        &self,
        query: &str,
        caller_identity: Option<Did>,
        explain_type: ExplainType,
    ) -> Result<JsonValue> {
        self.explain_query_with_identity_and_vars(query, caller_identity, explain_type, None)
            .await
    }

    /// Generate an explanation of the query plan with variable support.
    pub async fn explain_query_with_identity_and_vars(
        &self,
        query: &str,
        caller_identity: Option<Did>,
        explain_type: ExplainType,
        variables: Option<&std::collections::HashMap<String, JsonValue>>,
    ) -> Result<JsonValue> {
        match explain_type {
            ExplainType::Simple | ExplainType::Debug => {
                // Simple and Debug modes: explain without execution
                let selects = parse_query_with_variables(query, variables)?;
                let mut operation_children: Vec<JsonValue> = Vec::new();

                for select in selects {
                    // Check if this is a top-level aggregate query (e.g., _avg, _count, _sum)
                    let is_top_level_aggregate = Self::is_top_level_aggregate(&select);

                    // Build the plan explanation for this select
                    let select_node_content = self.explain_select(&select, explain_type).await?;

                    if is_top_level_aggregate {
                        // Top-level aggregates use topLevelNode wrapper
                        let top_level_node = self.build_top_level_aggregate_explain(
                            &select,
                            select_node_content,
                            explain_type,
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

                // Wrap all selects in operationNode array (Go's MultiNode pattern)
                Ok(serde_json::json!({
                    "explain": {
                        "operationNode": operation_children
                    }
                }))
            }
            ExplainType::Execute => {
                // Execute mode: run the query and collect metrics
                self.execute_explain_with_vars(query, caller_identity, variables)
                    .await
            }
        }
    }

    /// Generate an explanation of the mutation plan.
    ///
    /// Used when mutations include the @explain directive.
    /// Output format matches Go DefraDB with createNode/deleteNode/updateNode/upsertNode.
    pub async fn explain_mutation_with_identity(
        &self,
        mutation_str: &str,
        caller_identity: Option<Did>,
        explain_type: ExplainType,
    ) -> Result<JsonValue> {
        use crate::query_parse::parse_mutations;

        match explain_type {
            ExplainType::Simple | ExplainType::Debug => {
                // Simple and Debug modes: explain without execution
                let mutations = parse_mutations(mutation_str)?;
                let mut operation_children: Vec<JsonValue> = Vec::new();

                for mutation in mutations {
                    let mutation_explain = self
                        .explain_single_mutation(&mutation, explain_type)
                        .await?;
                    operation_children.push(mutation_explain);
                }

                // Wrap all mutations in operationNode array (Go's MultiNode pattern)
                Ok(serde_json::json!({
                    "explain": {
                        "operationNode": operation_children
                    }
                }))
            }
            ExplainType::Execute => {
                // Execute mode: run the mutation and collect metrics
                self.execute_mutation_explain(mutation_str, caller_identity)
                    .await
            }
        }
    }

    /// Execute mutation and return explain output with execution metrics.
    async fn execute_mutation_explain(
        &self,
        mutation_str: &str,
        caller_identity: Option<Did>,
    ) -> Result<JsonValue> {
        use crate::query_parse::parse_mutations;

        let mutations = parse_mutations(mutation_str)?;
        let mut operation_children: Vec<JsonValue> = Vec::new();
        let mut execution_success = true;
        let mut execution_errors: Vec<String> = Vec::new();

        for mutation in mutations {
            match self
                .execute_single_mutation_with_metrics(&mutation, caller_identity.clone())
                .await
            {
                Ok((mutation_explain, _doc_count, _exec_count)) => {
                    operation_children.push(mutation_explain);
                }
                Err(e) => {
                    if matches!(&e, QueryError::Parse(_)) {
                        return Err(e);
                    }
                    execution_success = false;
                    execution_errors.push(e.to_string());
                }
            }
        }

        // Go's executeAndExplainRequest calls Next() on the top-level operationNode.
        // Each Next()=true yields one mutation result. After all mutations, Next()=false.
        // So planExecutions = number_of_mutations + 1, sizeOfResult = number_of_mutations.
        let num_mutations = operation_children.len() as u64;
        let plan_executions = num_mutations + 1;
        let size_of_result = num_mutations;

        // Build explain result with execution metrics
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

    /// Execute a single mutation and return explain with metrics.
    ///
    /// Go's mutation plan runs as: mutationNode → selectTopNode → selectNode → scanNode.
    /// The filter lives at the scanNode level (not selectNode). Metrics accumulate:
    /// - Create: single pass AFTER creation
    /// - Delete: single pass BEFORE deletion
    /// - Update/Upsert: two passes (Phase 1: scan+mutate, Phase 2: scan+return), metrics sum
    async fn execute_single_mutation_with_metrics(
        &self,
        mutation: &crate::mapper::Mutation,
        caller_identity: Option<Did>,
    ) -> Result<(JsonValue, usize, u64)> {
        use crate::mapper::MutationType;

        let node_kind = match mutation.mutation_type {
            MutationType::Create => "createNode",
            MutationType::Update => "updateNode",
            MutationType::Delete => "deleteNode",
            MutationType::Upsert => "upsertNode",
        };

        // Build a select with the mutation's filter/doc_ids for metric collection.
        // Go puts the mutation filter on the scanNode; build_plan places the filter
        // on both scanNode (for iteration/docFetch counting) and selectNode (for
        // filterMatches counting), which matches Go's behavior.
        let mut metric_select = Select::new(&mutation.collection_name);
        if let Some(ref filter) = mutation.filter {
            metric_select.filter = Some(filter.clone());
        }
        if let Some(ref doc_ids) = mutation.doc_ids {
            metric_select.doc_ids = Some(doc_ids.clone());
        }

        // Phase 1: Collect metrics BEFORE mutation (delete, update, upsert).
        // For delete: this is the only scan (single pass over original data).
        // For update/upsert: Phase 1 captures the "find + mutate" scan metrics.
        let phase1 = if mutation.mutation_type != MutationType::Create {
            Some(
                self.execute_select_with_metrics(&metric_select, caller_identity.clone())
                    .await?,
            )
        } else {
            None
        };

        // Execute the actual mutation
        if let Some(ref mutator) = self.mutator {
            use chrono::{FixedOffset, Utc};
            let utc_offset = FixedOffset::east_opt(0).unwrap();
            let request_time = Utc::now().with_timezone(&utc_offset);

            let collection = self.get_collection(&mutation.collection_name).await?;
            let mapping = self.build_mutation_mapping(mutation)?;
            let resolved_doc_ids = self.resolve_filter_to_doc_ids(mutation).await?;

            let mut plan: Box<dyn crate::planner::PlanNode> = match mutation.mutation_type {
                MutationType::Create => {
                    let inputs = self.build_create_inputs(mutation)?;
                    Box::new(
                        crate::plan::CreateNode::new(
                            &mutation.collection_name,
                            mutator.clone(),
                            mapping.clone(),
                        )
                        .with_collection(collection.clone())
                        .with_request_time(request_time)
                        .with_inputs(inputs),
                    )
                }
                MutationType::Update => {
                    let input = self.build_update_input(mutation)?;
                    let fetcher: Arc<dyn crate::fetcher::DocFetcher> = self.fetcher.clone();
                    let mut node = crate::plan::UpdateNode::new(
                        &mutation.collection_name,
                        mutator.clone(),
                        fetcher,
                        mapping.clone(),
                    )
                    .with_collection(collection.clone())
                    .with_request_time(request_time)
                    .with_input(input);

                    if let Some(ref doc_ids) = resolved_doc_ids {
                        node = node.with_doc_ids(doc_ids.clone());
                    } else if let Some(ref doc_ids) = mutation.doc_ids {
                        node = node.with_doc_ids(doc_ids.clone());
                    }
                    if let Some(ref filter) = mutation.filter {
                        node = node.with_filter(filter.clone());
                    }
                    Box::new(node)
                }
                MutationType::Delete => {
                    let fetcher: Arc<dyn crate::fetcher::DocFetcher> = self.fetcher.clone();
                    let mut node = crate::plan::DeleteNode::new(
                        &mutation.collection_name,
                        mutator.clone(),
                        fetcher,
                        mapping.clone(),
                    );

                    if let Some(ref doc_ids) = resolved_doc_ids {
                        node = node.with_doc_ids(doc_ids.clone());
                    } else if let Some(ref doc_ids) = mutation.doc_ids {
                        node = node.with_doc_ids(doc_ids.clone());
                    }
                    if mutation.filter.is_some()
                        && resolved_doc_ids.is_none()
                        && mutation.doc_ids.is_none()
                    {
                        node = node.with_filter(mutation.filter.clone().unwrap());
                    }
                    Box::new(node)
                }
                MutationType::Upsert => {
                    let mut node = crate::plan::UpsertNode::new(
                        &mutation.collection_name,
                        mutator.clone(),
                        mapping.clone(),
                    )
                    .with_collection(collection.clone())
                    .with_request_time(request_time);

                    if !mutation.create_input.is_empty() {
                        let create_input =
                            self.build_upsert_input_from_map(&mutation.create_input[0])?;
                        node = node.with_create_input(create_input);
                    }
                    if !mutation.update_input.is_empty() {
                        let update_input =
                            self.build_upsert_input_from_map(&mutation.update_input)?;
                        node = node.with_update_input(update_input);
                    }
                    if let Some(ref doc_ids) = resolved_doc_ids {
                        node = node.with_doc_ids(doc_ids.clone());
                    } else if let Some(ref doc_ids) = mutation.doc_ids {
                        node = node.with_doc_ids(doc_ids.clone());
                    }
                    Box::new(node)
                }
            };

            // Execute the mutation plan (ignore results, we just need the side effects)
            plan.init().await?;
            plan.start().await?;
            while plan.next().await? {}
            plan.close().await?;
        }

        // Phase 2: Collect metrics AFTER mutation (create, update, upsert).
        // For create: this is the only scan (single pass over created data).
        // For update/upsert: Phase 2 captures the "return results" scan metrics.
        let phase2 = if mutation.mutation_type != MutationType::Delete {
            Some(
                self.execute_select_with_metrics(&metric_select, caller_identity.clone())
                    .await?,
            )
        } else {
            None
        };

        // Combine metrics from phases
        let combined_explain = match (&phase1, &phase2) {
            (Some((p1, _, _)), Some((p2, _, _))) => {
                // Two-pass (update/upsert): merge by summing all numeric values
                Self::merge_execute_metrics(p1, p2)
            }
            (Some((p1, _, _)), None) => p1.clone(),
            (None, Some((p2, _, _))) => p2.clone(),
            _ => unreachable!(),
        };

        // Wrap in selectTopNode
        let select_node_content = Self::ensure_select_node_wrapper(
            combined_explain,
            &metric_select,
            ExplainType::Execute,
        );

        // Build mutation node
        let mut mutation_inner = serde_json::Map::new();

        // Mutation-specific fields
        match mutation.mutation_type {
            MutationType::Create => {
                let (_, _, plan_execs) = phase2.as_ref().unwrap();
                mutation_inner.insert("iterations".to_string(), serde_json::json!(*plan_execs));
            }
            MutationType::Delete => {
                let (_, _, plan_execs) = phase1.as_ref().unwrap();
                mutation_inner.insert("iterations".to_string(), serde_json::json!(*plan_execs));
            }
            MutationType::Update => {
                let (_, result_count, plan_execs) = phase1.as_ref().unwrap();
                mutation_inner.insert("iterations".to_string(), serde_json::json!(*plan_execs));
                mutation_inner.insert(
                    "updates".to_string(),
                    serde_json::json!(*result_count as u64),
                );
            }
            MutationType::Upsert => {
                // Go's upsertNode returns empty map for execute explain (no iterations)
            }
        }

        mutation_inner.insert("selectTopNode".to_string(), select_node_content);

        let mutation_node = serde_json::json!({
            node_kind: mutation_inner
        });

        let doc_count = match (&phase1, &phase2) {
            (_, Some((_, count, _))) => *count,
            (Some((_, count, _)), None) => *count,
            _ => 0,
        };

        Ok((mutation_node, doc_count, 1))
    }

    /// Recursively merge two execute explain JSON trees by summing numeric values.
    /// Used to combine Phase 1 and Phase 2 metrics for update/upsert mutations.
    fn merge_execute_metrics(phase1: &JsonValue, phase2: &JsonValue) -> JsonValue {
        match (phase1, phase2) {
            (JsonValue::Object(a), JsonValue::Object(b)) => {
                let mut merged = serde_json::Map::new();
                for (key, val_a) in a {
                    if let Some(val_b) = b.get(key) {
                        merged.insert(key.clone(), Self::merge_execute_metrics(val_a, val_b));
                    } else {
                        merged.insert(key.clone(), val_a.clone());
                    }
                }
                for (key, val_b) in b {
                    if !a.contains_key(key) {
                        merged.insert(key.clone(), val_b.clone());
                    }
                }
                JsonValue::Object(merged)
            }
            (JsonValue::Number(a), JsonValue::Number(b)) => {
                let sum = a.as_u64().unwrap_or(0) + b.as_u64().unwrap_or(0);
                serde_json::json!(sum)
            }
            _ => phase2.clone(),
        }
    }

    /// Generate an explanation for a single mutation operation.
    async fn explain_single_mutation(
        &self,
        mutation: &crate::mapper::Mutation,
        explain_type: ExplainType,
    ) -> Result<JsonValue> {
        use crate::mapper::MutationType;

        // Get the mutation node kind name
        let node_kind = match mutation.mutation_type {
            MutationType::Create => "createNode",
            MutationType::Update => "updateNode",
            MutationType::Delete => "deleteNode",
            MutationType::Upsert => "upsertNode",
        };

        // Build the inner select plan explanation
        // Mutations in Go have: mutationNode -> selectTopNode -> selectNode -> scanNode
        let collection = self
            .collection_provider
            .get_collection(&mutation.collection_name)
            .await?
            .ok_or_else(|| QueryError::collection_not_found(&mutation.collection_name))?;

        // Build a select for the mutation's result fields, including filter and docIDs
        // These are passed through to the scanNode for proper explain output
        let mut select = crate::mapper::Select::new(&mutation.collection_name);
        if let Some(ref filter) = mutation.filter {
            select = select.with_filter(filter.clone());
        }
        if let Some(ref doc_ids) = mutation.doc_ids {
            select = select.with_doc_ids(doc_ids.clone());
        }
        let inner_explain = self.explain_simple_select(&select, &collection, explain_type)?;

        // Build mutation-specific attributes
        let mut mutation_attrs = serde_json::Map::new();

        match mutation.mutation_type {
            MutationType::Create => {
                // input: array of input objects
                let input_array: Vec<JsonValue> = mutation
                    .create_input
                    .iter()
                    .map(|input| {
                        let mut input_obj = serde_json::Map::new();
                        for (field_name, value) in input {
                            input_obj.insert(field_name.clone(), value.clone());
                        }
                        JsonValue::Object(input_obj)
                    })
                    .collect();
                mutation_attrs.insert("input".to_string(), JsonValue::Array(input_array));
            }
            MutationType::Update => {
                // docID: array of doc IDs (or null)
                if let Some(ref doc_ids) = mutation.doc_ids {
                    mutation_attrs.insert(
                        "docID".to_string(),
                        JsonValue::Array(
                            doc_ids
                                .iter()
                                .map(|id| JsonValue::String(id.clone()))
                                .collect(),
                        ),
                    );
                } else {
                    mutation_attrs.insert("docID".to_string(), JsonValue::Null);
                }

                // filter: filter expression (or null, including empty filter)
                if let Some(ref filter) = mutation.filter {
                    let conditions = filter.conditions();
                    if conditions.is_empty() {
                        mutation_attrs.insert("filter".to_string(), JsonValue::Null);
                    } else {
                        mutation_attrs.insert("filter".to_string(), serde_json::json!(conditions));
                    }
                } else {
                    mutation_attrs.insert("filter".to_string(), JsonValue::Null);
                }

                // input: map of update values
                let mut input_obj = serde_json::Map::new();
                for (field_name, value) in &mutation.update_input {
                    input_obj.insert(field_name.clone(), value.clone());
                }
                mutation_attrs.insert("input".to_string(), JsonValue::Object(input_obj));
            }
            MutationType::Delete => {
                // docID: array of doc IDs (or null)
                if let Some(ref doc_ids) = mutation.doc_ids {
                    mutation_attrs.insert(
                        "docID".to_string(),
                        JsonValue::Array(
                            doc_ids
                                .iter()
                                .map(|id| JsonValue::String(id.clone()))
                                .collect(),
                        ),
                    );
                } else {
                    mutation_attrs.insert("docID".to_string(), JsonValue::Null);
                }

                // filter: filter expression (or null, including empty filter)
                if let Some(ref filter) = mutation.filter {
                    let conditions = filter.conditions();
                    if conditions.is_empty() {
                        mutation_attrs.insert("filter".to_string(), JsonValue::Null);
                    } else {
                        mutation_attrs.insert("filter".to_string(), serde_json::json!(conditions));
                    }
                } else {
                    mutation_attrs.insert("filter".to_string(), JsonValue::Null);
                }
            }
            MutationType::Upsert => {
                // Go format: separate create, filter, and update fields
                // create: map of fields for new document creation
                if !mutation.create_input.is_empty() {
                    let mut create_obj = serde_json::Map::new();
                    for (field_name, value) in &mutation.create_input[0] {
                        create_obj.insert(field_name.clone(), value.clone());
                    }
                    mutation_attrs.insert("create".to_string(), JsonValue::Object(create_obj));
                }

                // filter: filter expression used to find existing documents
                if let Some(ref filter) = mutation.filter {
                    let conditions = filter.conditions();
                    if conditions.is_empty() {
                        mutation_attrs.insert("filter".to_string(), JsonValue::Null);
                    } else {
                        mutation_attrs.insert("filter".to_string(), serde_json::json!(conditions));
                    }
                } else {
                    mutation_attrs.insert("filter".to_string(), JsonValue::Null);
                }

                // update: map of fields for updating existing document
                if !mutation.update_input.is_empty() {
                    let mut update_obj = serde_json::Map::new();
                    for (field_name, value) in &mutation.update_input {
                        update_obj.insert(field_name.clone(), value.clone());
                    }
                    mutation_attrs.insert("update".to_string(), JsonValue::Object(update_obj));
                }
            }
        }

        // Add selectTopNode containing the inner select explanation
        mutation_attrs.insert("selectTopNode".to_string(), inner_explain);

        // Wrap in the mutation node type
        let mutation_node = serde_json::json!({
            node_kind: JsonValue::Object(mutation_attrs)
        });

        Ok(mutation_node)
    }

    /// Execute the query with variables and return explain output with execution metrics.
    /// Format matches Go DefraDB's executeAndExplainRequest output.
    async fn execute_explain_with_vars(
        &self,
        query: &str,
        caller_identity: Option<Did>,
        variables: Option<&std::collections::HashMap<String, JsonValue>>,
    ) -> Result<JsonValue> {
        let selects = parse_query_with_variables(query, variables)?;

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
        let num_queries = operation_children.len() as u64;
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
    async fn execute_select_with_metrics(
        &self,
        select: &Select,
        caller_identity: Option<Did>,
    ) -> Result<(JsonValue, usize, u64)> {
        // Handle _commits system collection (no real collection exists)
        if select.collection_name == "_commits" {
            // Actually execute the commits query to get real metrics
            let results = self.execute_commits_query(select).await?;
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
            .collection_provider
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

            let mut planner = Planner::new(collections).with_fetcher(Arc::new(fetcher_arc));
            if let Some(ref lens_store) = self.lens_store {
                planner = planner.with_lens_store(lens_store.clone());
            }
            let plan_result = planner.plan_with_index_info(select)?;
            let mut plan = plan_result.plan;

            // Wrap with permission filter if needed (explain path)
            if let (Some(ref acp), Some(ref policy)) = (&self.acp, &collection.policy) {
                plan = Box::new(PermissionFilterNode::new(
                    plan,
                    acp.clone(),
                    Identity::from(caller_identity),
                    &policy.id,
                    &policy.resource_name,
                ));
            }

            // Execute the plan and count iterations
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

            plan.close().await?;

            // Use explain_execute to get metrics from each node
            let explanation = plan.explain_execute();

            Ok((explanation, result_count, plan_executions))
        } else {
            // Standard path: fetch all docs and build scan-based plan
            let mapping = plan::build_mapping(select, &collection)?;

            // When show_deleted is true, we need to use get_all_with_deleted to include
            // logically deleted documents. The doc_ids filter will be applied by the SelectNode.
            let plan_docs = if select.show_deleted {
                let docs_with_status = fetcher
                    .get_all_with_deleted(&select.collection_name, true)
                    .await?;
                documents_with_status_to_plan_docs(&docs_with_status, &mapping)?
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
                documents_to_plan_docs(&result.into_docs(), &mapping)?
            } else {
                let docs = fetcher.get_all(&select.collection_name).await?;
                documents_to_plan_docs(&docs, &mapping)?
            };
            let _doc_count = plan_docs.len();

            // Build ACP filter config if collection has policy and ACP is configured
            let acp_filter =
                if let (Some(ref acp), Some(ref policy)) = (&self.acp, &collection.policy) {
                    Some(plan::AcpFilter {
                        acp: acp.clone(),
                        identity: Identity::from(caller_identity),
                        policy_id: policy.id.clone(),
                        resource_name: policy.resource_name.clone(),
                    })
                } else {
                    None
                };

            // Build the plan (ACP filter is inserted inside, after Select but before aggregates)
            let mut plan = plan::build_plan(
                select,
                plan_docs.clone(),
                mapping.clone(),
                &collection,
                acp_filter,
            )?;

            // Execute the plan and count iterations
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

            plan.close().await?;

            // Use explain_execute to get metrics from each node
            let explanation = plan.explain_execute();

            Ok((explanation, result_count, plan_executions))
        }
    }

    /// Add execution metrics to the explain output (Go format).
    /// Metrics are added to the scanNode, not the selectNode.
    #[allow(dead_code)]
    fn add_iterations_to_explain(
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
    fn add_metrics_to_scan_node(
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

    /// Generate an explanation of a single Select operation.
    async fn explain_select(
        &self,
        select: &Select,
        explain_type: ExplainType,
    ) -> Result<JsonValue> {
        // Handle encrypted search queries - return a simple seScanNode explanation
        if select.is_encrypted {
            return Ok(serde_json::json!({
                "selectNode": {
                    "seScanNode": {
                        "collection": select.collection_name,
                        "filter": select.filter.as_ref().map(|f| f.conditions())
                    }
                }
            }));
        }

        // Handle _commits system collection specially
        if select.collection_name == "_commits" {
            return self.explain_commits_select(select, explain_type);
        }

        // Get collection schema
        let collection = self
            .collection_provider
            .get_collection(&select.collection_name)
            .await?
            .ok_or_else(|| QueryError::collection_not_found(&select.collection_name))?;

        // Check if this query has nested selections (relations)
        let has_nested = select
            .fields
            .iter()
            .any(|f| matches!(f, Requestable::Select(_)));

        // Check if an ordering-only index can be used (planner needed for IndexScanNode)
        let has_ordering_index = !has_nested
            && select.order_by.is_some()
            && !collection.indexes.is_empty()
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

        // Check if a filter-based index can be used
        let has_filter_index = !has_nested
            && select.filter.is_some()
            && !collection.indexes.is_empty()
            && select
                .filter
                .as_ref()
                .map(|f| select_best_index(f, &collection.indexes).is_some())
                .unwrap_or(false);

        // Check if any aggregates reference relations (e.g., _sum(articles: {field: pages}))
        let has_relation_aggregates = select.fields.iter().any(|f| {
            if let Requestable::Aggregate(agg) = f {
                agg.targets
                    .iter()
                    .any(|t| !t.host_name.is_empty() && t.host_name != select.collection_name)
            } else {
                false
            }
        });

        // Check if the filter references relation fields (e.g., {author: {verified: true}})
        let filter_has_relations = select
            .filter
            .as_ref()
            .map(|f| f.has_relation_filters())
            .unwrap_or(false);

        // Check if the order references relation fields (e.g., {author: {age: DESC}})
        let order_has_relations = select
            .order_by
            .as_ref()
            .map(|o| o.has_relation_order())
            .unwrap_or(false);

        // Check if any similarity fields are present (require SimilarityNode in planner)
        let has_similarity = select
            .fields
            .iter()
            .any(|f| matches!(f, Requestable::Similarity(_)));

        // Check if any secondary relation ID fields are selected (e.g., `_authorID`)
        let has_secondary_relation_id = select.fields.iter().any(|f| {
            if let Requestable::Field(field) = f {
                let field_name = &field.name;
                if field_name.starts_with('_') && field_name.ends_with("ID") && field_name.len() > 3
                {
                    let relation_name = &field_name[1..field_name.len() - 2];
                    if let Some(relation_field) = collection.field_by_name(relation_name) {
                        return relation_field.kind.is_relation() && !relation_field.is_primary;
                    }
                }
            }
            false
        });

        let is_view = collection.query.is_some();

        if is_view
            || has_nested
            || has_ordering_index
            || has_filter_index
            || has_relation_aggregates
            || filter_has_relations
            || order_has_relations
            || has_similarity
            || has_secondary_relation_id
        {
            // Use the Planner for views, nested selections, index usage, relation aggregates,
            // relation filters/ordering, similarity, or secondary relation IDs
            self.explain_nested_select(select, explain_type).await
        } else {
            // Explain simple query plan
            self.explain_simple_select(select, &collection, explain_type)
        }
    }

    /// Generate an explanation for a query with nested selections.
    async fn explain_nested_select(
        &self,
        select: &Select,
        explain_type: ExplainType,
    ) -> Result<JsonValue> {
        // Build the plan using the Planner
        let collection_names = self.collection_provider.list_collections().await?;
        let mut collections = Vec::new();
        for name in collection_names {
            if let Some(coll) = self.collection_provider.get_collection(&name).await? {
                collections.push((*coll).clone());
            }
        }

        let mut planner = Planner::new(collections);
        if let Some(ref lens_store) = self.lens_store {
            planner = planner.with_lens_store(lens_store.clone());
        }
        let plan_result = planner.plan_with_index_info(select)?;
        let plan = plan_result.plan;

        // Get the plan explanation based on type
        let explain = match explain_type {
            ExplainType::Debug => plan.explain_debug(),
            _ => plan.explain(),
        };

        // Ensure result is wrapped in selectNode (Go format)
        Ok(Self::ensure_select_node_wrapper(
            explain,
            select,
            explain_type,
        ))
    }

    /// Generate an explanation for a simple query without nested selections.
    fn explain_simple_select(
        &self,
        select: &Select,
        collection: &CollectionVersion,
        explain_type: ExplainType,
    ) -> Result<JsonValue> {
        // Build document mapping and plan
        let mapping = plan::build_mapping(select, collection)?;

        // Create an empty plan with no documents for explanation purposes
        let plan = plan::build_plan(select, vec![], mapping, collection, None)?;

        // Get the plan explanation based on type
        let explain = match explain_type {
            ExplainType::Debug => plan.explain_debug(),
            _ => plan.explain(),
        };

        // Ensure result is wrapped in selectNode (Go format)
        Ok(Self::ensure_select_node_wrapper(
            explain,
            select,
            explain_type,
        ))
    }

    /// Process explain output for Go format compatibility.
    ///
    /// Since we now always create SelectNode in the plan, this function handles:
    /// - For Simple mode: ensures docID and filter attributes are in selectNode
    /// - For Debug mode: returns as-is (no additional attributes)
    /// - For Execute mode: returns as-is (attributes added during execution)
    fn ensure_select_node_wrapper(
        explain: JsonValue,
        _select: &Select,
        explain_type: ExplainType,
    ) -> JsonValue {
        // For Debug mode, return as-is (Go debug doesn't add attributes)
        if matches!(explain_type, ExplainType::Debug) {
            return explain;
        }

        // For Simple/Execute mode, the SelectNode already has the attributes
        // from its explain_inner method, so return as-is
        explain
    }

    /// Generate an explanation for a _commits system collection query.
    ///
    /// Returns a dagScanNode structure matching Go's explain output for commits queries.
    fn explain_commits_select(
        &self,
        select: &Select,
        explain_type: ExplainType,
    ) -> Result<JsonValue> {
        // For Debug mode, return empty inner objects
        if matches!(explain_type, ExplainType::Debug) {
            return Ok(serde_json::json!({
                "selectNode": {
                    "dagScanNode": {}
                }
            }));
        }

        // Build the dagScanNode attributes for Simple/Execute mode
        let mut dag_scan_attrs = serde_json::Map::new();

        // cid: the specific commit CID if provided, else null
        if let Some(ref cid) = select.cid {
            dag_scan_attrs.insert("cid".to_string(), serde_json::json!(cid));
        } else {
            dag_scan_attrs.insert("cid".to_string(), serde_json::Value::Null);
        }

        // prefixes: array of storage prefixes being scanned
        // Format: /d/<docID> for document-specific commits
        let prefixes: Vec<String> = if let Some(ref doc_ids) = select.doc_ids {
            doc_ids.iter().map(|id| format!("/d/{}", id)).collect()
        } else {
            vec![]
        };
        dag_scan_attrs.insert("prefixes".to_string(), serde_json::json!(prefixes));

        // Build the selectNode wrapper (Go structure: selectNode -> dagScanNode)
        let dag_scan_node = serde_json::json!({ "dagScanNode": dag_scan_attrs });

        Ok(serde_json::json!({
            "selectNode": dag_scan_node
        }))
    }

    /// Check if a select represents a top-level aggregate query (e.g., _avg, _count, _sum).
    ///
    /// Top-level aggregates are queries like `_count(Author)` where the aggregate function
    /// is the root query field name itself.
    ///
    /// This does NOT include queries like `Author { _count(books) }` - those are regular
    /// queries with aggregate sub-fields, not top-level aggregates.
    fn is_top_level_aggregate(select: &Select) -> bool {
        // Only true when the query field name itself is an aggregate function
        let field_name = select.field.name.as_str();
        field_name.starts_with('_')
            && ["_count", "_sum", "_avg", "_min", "_max"].contains(&field_name)
    }

    /// Aggregate node kind names that can wrap a selectNode in the plan explain.
    const AGGREGATE_NODE_KINDS: &'static [&'static str] =
        &["countNode", "sumNode", "averageNode", "minNode", "maxNode"];

    /// Aggregate-specific explain fields that should be stripped when unwrapping aggregate nodes.
    /// "sources" appears in default explain, "iterations" in execute explain.
    const AGGREGATE_EXPLAIN_FIELDS: [&'static str; 2] = ["sources", "iterations"];

    /// Strip aggregate wrapper nodes from explain output for top-level aggregate queries.
    ///
    /// The Rust planner wraps the plan in aggregate nodes (e.g., CountNode → SelectNode → ScanNode),
    /// but Go's explain format puts aggregates as siblings in topLevelNode, not as wrappers.
    /// This function peels off any top-level aggregate wrappers to expose the inner selectNode.
    ///
    /// Example: `{ "countNode": { "sources": [...], "selectNode": { "scanNode": {...} } } }`
    /// becomes: `{ "selectNode": { "scanNode": {...} } }`
    fn strip_aggregate_wrappers(mut explain: JsonValue) -> JsonValue {
        loop {
            // Check if this is an aggregate wrapper node
            let is_aggregate_wrapper = if let Some(obj) = explain.as_object() {
                // An aggregate wrapper has the aggregate node kind as the only top-level key
                obj.len() == 1
                    && obj
                        .keys()
                        .next()
                        .map(|k| Self::AGGREGATE_NODE_KINDS.contains(&k.as_str()))
                        .unwrap_or(false)
            } else {
                false
            };

            if is_aggregate_wrapper {
                // Unwrap: take the inner value from the aggregate node
                let obj = explain.as_object_mut().unwrap();
                let key = obj.keys().next().unwrap().clone();
                explain = obj.remove(&key).unwrap();

                // Remove aggregate-specific fields from the inner content
                if let Some(inner_obj) = explain.as_object_mut() {
                    for field in &Self::AGGREGATE_EXPLAIN_FIELDS {
                        inner_obj.remove(*field);
                    }
                }
            } else {
                break;
            }
        }
        explain
    }

    /// Build the explain output for a top-level aggregate query.
    ///
    /// Go's format: { "topLevelNode": [ {selectTopNode: ...}, {sumNode: {}}, {countNode: {}}, ... ] }
    fn build_top_level_aggregate_explain(
        &self,
        select: &Select,
        select_explain: JsonValue,
        explain_type: ExplainType,
    ) -> JsonValue {
        use crate::mapper::AggregateType;

        // Strip aggregate wrappers from the plan explain to get the inner selectNode content.
        // The Rust planner wraps aggregates around the plan, but Go puts them as siblings.
        let inner_explain = Self::strip_aggregate_wrappers(select_explain);

        let mut top_level_children: Vec<JsonValue> = Vec::new();

        // First element: the data source (selectTopNode)
        top_level_children.push(serde_json::json!({
            "selectTopNode": inner_explain
        }));

        // Add aggregate nodes based on what's in the fields
        for field in &select.fields {
            if let Requestable::Aggregate(agg) = field {
                let node_name = match agg.aggregate_type {
                    AggregateType::Sum => "sumNode",
                    AggregateType::Count => "countNode",
                    AggregateType::Average => "averageNode",
                    AggregateType::Min => "minNode",
                    AggregateType::Max => "maxNode",
                };

                // For execute explain, aggregate nodes show iterations instead of sources
                if explain_type == ExplainType::Execute {
                    if agg.aggregate_type == AggregateType::Average {
                        // Go decomposes average into sumNode + countNode + averageNode
                        // Each shows iterations: 1 in execute mode
                        top_level_children.push(serde_json::json!({
                            "sumNode": { "iterations": 1u64 }
                        }));
                        top_level_children.push(serde_json::json!({
                            "countNode": { "iterations": 1u64 }
                        }));
                        top_level_children.push(serde_json::json!({
                            "averageNode": { "iterations": 1u64 }
                        }));
                    } else {
                        top_level_children.push(serde_json::json!({
                            node_name: { "iterations": 1u64 }
                        }));
                    }
                    continue;
                }

                // Default/Debug explain: show sources metadata
                let target_filter = if !agg.targets.is_empty() {
                    agg.targets[0].filter.as_ref()
                } else {
                    None
                };

                let filter_value = if let Some(filter) = target_filter {
                    let conditions = filter.conditions();
                    if conditions.is_empty() {
                        JsonValue::Null
                    } else {
                        serde_json::json!(conditions)
                    }
                } else {
                    JsonValue::Null
                };

                // For aggregates that operate on a field (sum, min, max, avg), include childFieldName
                let child_field_name = if !agg.targets.is_empty() {
                    agg.targets[0].field_name.as_ref()
                } else {
                    None
                };

                // Go decomposes average into sumNode + countNode + averageNode
                if agg.aggregate_type == AggregateType::Average {
                    // Go adds {field: {_neq: null}} for both sum and count source filters,
                    // but only for regular fields (not aggregate refs like _avg).
                    let avg_filter = if let Some(field_name) = child_field_name {
                        if field_name.starts_with('_') {
                            // Aggregate field refs don't get neq filter
                            filter_value.clone()
                        } else if filter_value.is_null() {
                            serde_json::json!({field_name: {"_neq": serde_json::Value::Null}})
                        } else if let Some(obj) = filter_value.as_object() {
                            // Merge {field: {_neq: null}} into existing filter conditions
                            let mut merged = obj.clone();
                            merged
                                .entry(field_name.to_string())
                                .and_modify(|v| {
                                    if let JsonValue::Object(ref mut ops) = v {
                                        ops.insert("_neq".to_string(), serde_json::Value::Null);
                                    }
                                })
                                .or_insert(serde_json::json!({"_neq": serde_json::Value::Null}));
                            JsonValue::Object(merged)
                        } else {
                            serde_json::json!({field_name: {"_neq": serde_json::Value::Null}})
                        }
                    } else {
                        filter_value.clone()
                    };

                    // 1. sumNode with sources (includes childFieldName)
                    let sum_source = if let Some(field_name) = child_field_name {
                        serde_json::json!({
                            "fieldName": select.collection_name,
                            "childFieldName": field_name,
                            "filter": avg_filter
                        })
                    } else {
                        serde_json::json!({
                            "fieldName": select.collection_name,
                            "filter": avg_filter
                        })
                    };
                    top_level_children.push(serde_json::json!({
                        "sumNode": {
                            "sources": [sum_source]
                        }
                    }));

                    // 2. countNode with sources (no childFieldName, same filter as sum)
                    let count_source = serde_json::json!({
                        "fieldName": select.collection_name,
                        "filter": avg_filter
                    });
                    top_level_children.push(serde_json::json!({
                        "countNode": {
                            "sources": [count_source]
                        }
                    }));

                    // 3. averageNode (empty)
                    top_level_children.push(serde_json::json!({
                        "averageNode": {}
                    }));
                    continue;
                }

                let source = if let Some(field_name) = child_field_name {
                    serde_json::json!({
                        "fieldName": select.collection_name,
                        "childFieldName": field_name,
                        "filter": filter_value
                    })
                } else {
                    serde_json::json!({
                        "fieldName": select.collection_name,
                        "filter": filter_value
                    })
                };

                top_level_children.push(serde_json::json!({
                    node_name: {
                        "sources": [source]
                    }
                }));
            }
        }

        serde_json::json!({
            "topLevelNode": top_level_children
        })
    }
}
