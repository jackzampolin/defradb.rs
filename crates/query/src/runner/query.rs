//! Query execution methods for QueryRunner.

use acp::{DocumentPermission, Identity};
use document::Document;
use identity::Did;
use schema::CollectionVersion;
use serde_json::{Map, Value as JsonValue};
use std::collections::HashSet;
use std::sync::Arc;
use tracing::{debug, warn};

use crate::document::{documents_to_plan_docs, documents_with_status_to_plan_docs};
use crate::error::{QueryError, Result};
use crate::mapper::{Requestable, Select};
use crate::plan::PermissionFilterNode;
use crate::planner::index_selection::{
    can_be_ordered_by_index, filter_to_index_scan, select_best_index,
};
use crate::planner::Planner;
use crate::query_parse::{parse_query_with_variables, ExplainType};
use crate::txn::TransactionRegistry;

use super::fetcher::FetcherWrapper;
use super::plan;
use super::{DocFetcher, QueryRunner};

/// Return a GraphQL-style error when ordering by a relation field.
///
/// Go rejects `order: {articles: {name: ASC}}` at the GraphQL schema level because
/// relation fields are not valid order input fields. This reproduces the same error.
fn reject_relation_order(order_by: &crate::mapper::OrderBy) -> QueryError {
    for condition in &order_by.conditions {
        if condition.fields.len() > 1 {
            let relation_field = &condition.fields[0];
            let child_field = &condition.fields[1];
            let direction = match condition.direction {
                crate::mapper::OrderDirection::Asc => "ASC",
                crate::mapper::OrderDirection::Desc => "DESC",
            };
            return QueryError::parse(format!(
                "Argument \"order\" has invalid value {{{}: {{{}: {}}}}}.\nIn field \"{}\": Unknown field.",
                relation_field, child_field, direction, relation_field
            ));
        }
    }
    QueryError::parse("Argument \"order\" has invalid value.\nUnknown field.")
}

impl<F: DocFetcher + 'static, R: TransactionRegistry> QueryRunner<F, R> {
    /// Execute a GraphQL query and return JSON results.
    pub async fn execute_query(&self, query: &str) -> Result<JsonValue> {
        self.execute_query_internal(query, self.fetcher.as_ref(), None)
            .await
    }

    /// Execute a GraphQL query with identity for ACP permission checks.
    pub async fn execute_query_with_identity(
        &self,
        query: &str,
        caller_identity: Option<Did>,
    ) -> Result<JsonValue> {
        self.execute_query_internal(query, self.fetcher.as_ref(), caller_identity)
            .await
    }

    /// Execute a GraphQL query with identity and variables.
    pub async fn execute_query_with_identity_and_vars(
        &self,
        query: &str,
        caller_identity: Option<Did>,
        variables: Option<&std::collections::HashMap<String, JsonValue>>,
    ) -> Result<JsonValue> {
        self.execute_query_internal_with_vars(
            query,
            self.fetcher.as_ref(),
            caller_identity,
            variables,
        )
        .await
    }

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
                mutation_inner
                    .insert("iterations".to_string(), serde_json::json!(*plan_execs));
            }
            MutationType::Delete => {
                let (_, _, plan_execs) = phase1.as_ref().unwrap();
                mutation_inner
                    .insert("iterations".to_string(), serde_json::json!(*plan_execs));
            }
            MutationType::Update => {
                let (_, result_count, plan_execs) = phase1.as_ref().unwrap();
                mutation_inner
                    .insert("iterations".to_string(), serde_json::json!(*plan_execs));
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
                    mutation_attrs
                        .insert("create".to_string(), JsonValue::Object(create_obj));
                }

                // filter: filter expression used to find existing documents
                if let Some(ref filter) = mutation.filter {
                    let conditions = filter.conditions();
                    if conditions.is_empty() {
                        mutation_attrs.insert("filter".to_string(), JsonValue::Null);
                    } else {
                        mutation_attrs
                            .insert("filter".to_string(), serde_json::json!(conditions));
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
                    mutation_attrs
                        .insert("update".to_string(), JsonValue::Object(update_obj));
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
            // Go rejects ordering by relation fields at the GraphQL schema level.
            // Check here so the error propagates as a top-level error, not executionErrors.
            let order_has_relations = select
                .order_by
                .as_ref()
                .map(|o| o.has_relation_order())
                .unwrap_or(false);
            if order_has_relations {
                if let Some(ref order_by) = select.order_by {
                    return Err(reject_relation_order(order_by));
                }
            }

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

        let can_use_index = can_use_filter_index || can_use_ordering_index;

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

        // Go rejects ordering by relation fields at the GraphQL schema level.
        if order_has_relations {
            if let Some(ref order_by) = select.order_by {
                return Err(reject_relation_order(order_by));
            }
        }

        let has_similarity = select
            .fields
            .iter()
            .any(|f| matches!(f, Requestable::Similarity(_)));

        if can_use_index
            || has_relation_aggregates
            || has_nested
            || filter_has_relations
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
            let doc_count = plan_docs.len();

            // Build ACP filter config if collection has policy and ACP is configured
            let acp_filter = if let (Some(ref acp), Some(ref policy)) = (&self.acp, &collection.policy) {
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
            let mut plan =
                plan::build_plan(select, plan_docs.clone(), mapping.clone(), &collection, acp_filter)?;

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
                    scan_obj.insert(
                        "iterations".to_string(),
                        serde_json::json!(iterations as u64),
                    );
                    scan_obj.insert(
                        "docFetches".to_string(),
                        serde_json::json!(doc_fetches as u64),
                    );
                    // fieldFetches = number of fields per doc * number of docs fetched
                    let field_fetches = (field_count * doc_fetches) as u64;
                    scan_obj.insert(
                        "fieldFetches".to_string(),
                        serde_json::json!(field_fetches),
                    );

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

        // Go rejects ordering by relation fields at the GraphQL schema level.
        // Reproduce the same error for compatibility.
        if order_has_relations {
            if let Some(ref order_by) = select.order_by {
                return Err(reject_relation_order(order_by));
            }
        }

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
                                        ops.insert(
                                            "_neq".to_string(),
                                            serde_json::Value::Null,
                                        );
                                    }
                                })
                                .or_insert(
                                    serde_json::json!({"_neq": serde_json::Value::Null}),
                                );
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

    /// Execute a GraphQL query with a specific fetcher and identity.
    ///
    /// This is used internally for both regular queries (using the default fetcher)
    /// and transactional queries (using a transaction-scoped fetcher).
    pub(crate) async fn execute_query_internal(
        &self,
        query: &str,
        fetcher: &dyn DocFetcher,
        caller_identity: Option<Did>,
    ) -> Result<JsonValue> {
        self.execute_query_internal_with_vars(query, fetcher, caller_identity, None)
            .await
    }

    /// Execute a GraphQL query with a specific fetcher, identity, and variables.
    pub(crate) async fn execute_query_internal_with_vars(
        &self,
        query: &str,
        fetcher: &dyn DocFetcher,
        caller_identity: Option<Did>,
        variables: Option<&std::collections::HashMap<String, JsonValue>>,
    ) -> Result<JsonValue> {
        let selects = parse_query_with_variables(query, variables)?;

        let mut results = Map::new();

        for select in selects {
            let result = self
                .execute_select_internal(&select, fetcher, caller_identity.clone())
                .await?;
            let key = select.field.output_name();
            results.insert(key.to_string(), result);
        }

        Ok(JsonValue::Object(results))
    }

    /// Execute already-parsed Select operations with a specific fetcher and identity.
    pub(crate) async fn execute_selects_internal(
        &self,
        selects: Vec<Select>,
        fetcher: &dyn DocFetcher,
        caller_identity: Option<Did>,
    ) -> Result<JsonValue> {
        let mut results = Map::new();

        for select in selects {
            let result = self
                .execute_select_internal(&select, fetcher, caller_identity.clone())
                .await?;
            let key = select.field.output_name();
            results.insert(key.to_string(), result);
        }

        Ok(JsonValue::Object(results))
    }

    /// Execute a single Select operation with a specific fetcher and identity.
    async fn execute_select_internal(
        &self,
        select: &Select,
        fetcher: &dyn DocFetcher,
        caller_identity: Option<Did>,
    ) -> Result<JsonValue> {
        // Handle encrypted search queries (encrypted_<Collection>)
        if select.is_encrypted {
            return self.execute_encrypted_select(select, fetcher).await;
        }

        // Handle _commits system collection specially
        if select.collection_name == "_commits" {
            return self.execute_commits_query(select).await;
        }

        // Check if _version is selected - it needs special handling since it's commit data
        // not a schema field. Extract the _version Select for later use.
        let version_selection: Option<&Select> = select.fields.iter().find_map(|f| {
            if let Requestable::Select(s) = f {
                if s.field.name == "_version" {
                    return Some(s.as_ref());
                }
            }
            None
        });

        // Handle CID-based time-travel queries
        if select.cid.is_some() {
            return self
                .execute_cid_query_with_version(select, fetcher, caller_identity, version_selection)
                .await;
        }

        // For queries with _version, execute documents first then add version data
        if version_selection.is_some() {
            return self
                .execute_query_with_version(select, fetcher, caller_identity, version_selection)
                .await;
        }

        // Get collection schema on-demand from provider
        let collection = self.get_collection(&select.collection_name).await?;

        // Embedded-only types (interface types from view SDL) are not root-queryable
        if collection.is_embedded_only {
            return Err(QueryError::parse(format!(
                "Cannot query field \"{}\" on type \"Query\".",
                select.collection_name
            )));
        }

        // Validate unsupported features and field references
        plan::validate_select(select, &collection)?;

        // Check if this query has nested selections (relations)
        let has_nested = select
            .fields
            .iter()
            .any(|f| matches!(f, Requestable::Select(_)));

        // Check if the filter references relation fields (e.g., {author: {verified: true}})
        // If so, we need to use the Planner to join the relation for filtering even if
        // the relation field is not in the selection set.
        let filter_has_relations = select
            .filter
            .as_ref()
            .map(|f| f.has_relation_filters())
            .unwrap_or(false);

        // Check if the order references relation fields (e.g., {author: {age: DESC}})
        // If so, we need to use the Planner to join the relation for ordering.
        let order_has_relations = select
            .order_by
            .as_ref()
            .map(|o| o.has_relation_order())
            .unwrap_or(false);

        // Check if this is a top-level aggregate query (e.g., { _avg(Users: {field: Age}) })
        // Top-level aggregates have: only aggregate fields, and all targets have host_name == collection_name
        let is_top_level_aggregate = !select.fields.is_empty()
            && select
                .fields
                .iter()
                .all(|f| matches!(f, Requestable::Aggregate(_)))
            && select.fields.iter().all(|f| {
                if let Requestable::Aggregate(agg) = f {
                    agg.targets
                        .iter()
                        .all(|t| t.host_name == select.collection_name)
                } else {
                    true
                }
            });

        // Check if any aggregates reference relations (e.g., _count(books: {}))
        // Relation-based aggregates have a non-empty host_name that differs from collection_name.
        // Exclude top-level aggregates where host_name == collection_name.
        let aggregates_have_relations = select.fields.iter().any(|f| {
            if let Requestable::Aggregate(agg) = f {
                agg.targets
                    .iter()
                    .any(|t| !t.host_name.is_empty() && t.host_name != select.collection_name)
            } else {
                false
            }
        });

        // Handle top-level aggregates specially - return single value, not array
        if is_top_level_aggregate {
            return self
                .execute_top_level_aggregate(select, fetcher, &collection, caller_identity)
                .await;
        }

        // Check if any secondary relation ID fields are selected (e.g., `_authorID` for a secondary `author` relation).
        // These require a TypeJoin to compute the ID via reverse lookup.
        let has_secondary_relation_id = select.fields.iter().any(|f| {
            if let Requestable::Field(field) = f {
                let field_name = &field.name;
                // Check pattern: _<relationName>ID
                if field_name.starts_with('_') && field_name.ends_with("ID") && field_name.len() > 3
                {
                    let relation_name = &field_name[1..field_name.len() - 2];
                    if let Some(relation_field) = collection.field_by_name(relation_name) {
                        // Only secondary relations need a join to compute the ID
                        return relation_field.kind.is_relation() && !relation_field.is_primary;
                    }
                }
            }
            false
        });

        // Check if an ordering-only index can be used (planner needed for IndexScanNode)
        let has_ordering_index = select.order_by.is_some()
            && fetcher.supports_index_queries()
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

        // Check if any similarity fields are present (require SimilarityNode in planner)
        let has_similarity = select
            .fields
            .iter()
            .any(|f| matches!(f, Requestable::Similarity(_)));

        // Validate similarity fields against the collection schema
        if has_similarity {
            for field in &select.fields {
                if let Requestable::Similarity(sim) = field {
                    let target = &sim.target_field;
                    let schema_field = collection.field_by_name(target);
                    // Check that the target field exists and is a numeric array
                    let element_kind = schema_field.and_then(|f| {
                        if let schema::FieldKind::ScalarArray(arr) = &f.kind {
                            let ek = arr.element_kind();
                            match ek {
                                schema::ScalarKind::Int
                                | schema::ScalarKind::Float32
                                | schema::ScalarKind::Float64 => Some(ek),
                                _ => None,
                            }
                        } else {
                            None
                        }
                    });

                    let element_kind = match element_kind {
                        Some(ek) => ek,
                        None => {
                            return Err(QueryError::execution(format!(
                                "Unknown argument \"{}\" on field \"_similarity\" of type \"{}\".",
                                target, collection.name
                            )));
                        }
                    };

                    // For Int fields, validate that vector values are integers
                    if element_kind == schema::ScalarKind::Int {
                        let non_int_values: Vec<String> = sim
                            .vector
                            .iter()
                            .filter(|v| v.fract() != 0.0)
                            .map(|v| format!("{}", v))
                            .collect();
                        if !non_int_values.is_empty() {
                            let vector_repr = format!(
                                "[{}]",
                                sim.vector
                                    .iter()
                                    .map(|v| format!("{}", v))
                                    .collect::<Vec<_>>()
                                    .join(", ")
                            );
                            let mut msg = format!(
                                "Argument \"{}\" has invalid value {{vector: {}}}.",
                                target, vector_repr
                            );
                            for v in &non_int_values {
                                msg.push_str(&format!(
                                    "\nIn field \"vector\": In element #1: Expected type \"Int\", found {}.",
                                    v
                                ));
                            }
                            return Err(QueryError::execution(msg));
                        }
                    }
                }
            }
        }

        // Views must always use the planner because they don't store data directly -
        // the planner creates a ViewNode that executes the underlying query.
        let is_view = collection.query.is_some();

        // Use Planner if there are nested selections, filter through relations,
        // order through relations, aggregates on relations, secondary relation ID fields,
        // similarity computations, when an index can provide ordering, or when querying a view
        let needs_planner = is_view
            || has_nested
            || filter_has_relations
            || order_has_relations
            || aggregates_have_relations
            || has_secondary_relation_id
            || has_ordering_index
            || has_similarity;

        if needs_planner {
            // Use the Planner for queries with nested selections (joins) or relation filters.
            self.execute_nested_select_with_planner(select, fetcher, caller_identity)
                .await
        } else {
            // Use the optimized path for simple queries
            self.execute_simple_select(select, fetcher, &collection, caller_identity)
                .await
        }
    }

    /// Execute an encrypted search query (`encrypted_<Collection>`).
    ///
    /// Validates encrypted index exists, then fetches documents, applies _eq filter
    /// conditions, and returns Go-compatible `[{"docIDs": [...]}]` format.
    async fn execute_encrypted_select(
        &self,
        select: &Select,
        fetcher: &dyn DocFetcher,
    ) -> Result<JsonValue> {
        // Get collection to check for encrypted indexes
        let collection = self.get_collection(&select.collection_name).await?;

        // Validate collection has encrypted indexes (Go-compatible error)
        if collection.encrypted_indexes.is_empty() {
            return Err(QueryError::internal(
                "collection has no encrypted indexes",
            ));
        }

        // Extract filtered field names and validate they have encrypted indexes
        if let Some(ref filter) = select.filter {
            let filtered_fields = filter.referenced_fields();
            for field_name in &filtered_fields {
                let has_index = collection
                    .encrypted_indexes
                    .iter()
                    .any(|idx| idx.field_name == *field_name);
                if !has_index {
                    return Err(QueryError::internal(format!(
                        "no encrypted index found for field: {}",
                        field_name
                    )));
                }
            }
        }

        let docs = fetcher.get_all(&select.collection_name).await?;

        let matching_ids: Vec<String> = if let Some(ref filter) = select.filter {
            let mut ids = Vec::new();
            for doc in &docs {
                let json_map = doc
                    .to_map()
                    .map_err(|e| QueryError::internal(e.to_string()))?;
                let json_obj = JsonValue::Object(
                    json_map
                        .into_iter()
                        .collect::<Map<String, JsonValue>>(),
                );
                if filter.matches_json_object(&json_obj)? {
                    if let Some(id) = doc.id() {
                        ids.push(id.to_string());
                    }
                }
            }
            ids
        } else {
            docs.iter()
                .filter_map(|doc| doc.id().map(|id| id.to_string()))
                .collect()
        };

        Ok(serde_json::json!([{"docIDs": matching_ids}]))
    }

    /// Execute a query with nested selections using the Planner.
    ///
    /// The Planner builds a proper join plan with TypeJoinOne/TypeJoinMany nodes.
    /// ScanNodes fetch their own data via the attached fetcher.
    /// ACP permission filtering is applied per-collection via PermissionFilterNode in the plan.
    async fn execute_nested_select_with_planner(
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

        let mut planner = Planner::new(collections).with_fetcher(Arc::new(fetcher_arc));
        if let Some(ref acp) = self.acp {
            planner = planner.with_acp(acp.clone(), identity);
        }
        if let Some(ref lens_store) = self.lens_store {
            planner = planner.with_lens_store(lens_store.clone());
        }
        let plan_result = planner.plan_with_index_info(select)?;
        let mut plan = plan_result.plan;
        let ordering_only_fields = plan_result.ordering_only_fields;

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
        let results = self.compute_relation_aggregates(results, select)?;

        // Strip fields from relation data that were added for filter evaluation
        // but not explicitly requested in the selection set.
        let results = Self::clean_filter_only_relation_fields(results, select);

        // Apply deferred limit/offset to relation fields.
        // TypeJoinMany stores ALL children (for aggregates to count), so we apply
        // the select's limit/offset here after aggregates have been computed.
        let results = Self::apply_relation_limits(results, select);

        Ok(JsonValue::Array(results))
    }

    /// Compute aggregate values from joined relation data.
    ///
    /// For each relation-based aggregate (e.g., _count(books: {})), this function:
    /// 1. Finds the joined relation data (stored under the relation field name)
    /// 2. Computes the aggregate (count, sum, avg, etc.)
    /// 3. Stores the result under the aggregate's output name
    fn compute_relation_aggregates(
        &self,
        mut results: Vec<JsonValue>,
        select: &Select,
    ) -> Result<Vec<JsonValue>> {
        use crate::mapper::AggregateType;

        // Collect info about relation aggregates with full target references
        let mut aggregates_info: Vec<(
            String,
            AggregateType,
            Vec<&crate::mapper::AggregateTarget>,
        )> = Vec::new();

        for requestable in &select.fields {
            if let Requestable::Aggregate(agg) = requestable {
                let mut relation_targets = Vec::new();
                for target in &agg.targets {
                    // Skip _group targets - they're handled by GroupByNode and aggregate nodes
                    if !target.host_name.is_empty() && target.host_name != "_group" {
                        relation_targets.push(target);
                    }
                }
                if !relation_targets.is_empty() {
                    aggregates_info.push((
                        agg.output_name().to_string(),
                        agg.aggregate_type.clone(),
                        relation_targets,
                    ));
                }
            }
        }

        if aggregates_info.is_empty() {
            return Ok(results);
        }

        // Collect which relation fields are explicitly selected and their requested fields (for cleanup later)
        let selected_relations: std::collections::HashSet<String> = select
            .fields
            .iter()
            .filter_map(|f| {
                if let Requestable::Select(s) = f {
                    Some(s.field.name.clone())
                } else {
                    None
                }
            })
            .collect();

        // For each selected relation, collect the fields that were explicitly requested.
        // Any fields NOT in this set were added for aggregate filter evaluation and should be cleaned up.
        let selected_relation_fields: std::collections::HashMap<
            String,
            std::collections::HashSet<String>,
        > = select
            .fields
            .iter()
            .filter_map(|f| {
                if let Requestable::Select(s) = f {
                    let mut fields = std::collections::HashSet::new();
                    // Always include _docID as it's implicit
                    fields.insert("_docID".to_string());
                    for requestable in &s.fields {
                        match requestable {
                            Requestable::Field(f) => {
                                fields.insert(f.output_name().to_string());
                            }
                            Requestable::Select(nested) => {
                                fields.insert(nested.field.output_name().to_string());
                            }
                            Requestable::Aggregate(agg) => {
                                fields.insert(agg.output_name().to_string());
                            }
                            Requestable::Similarity(sim) => {
                                fields.insert(sim.output_name().to_string());
                            }
                        }
                    }
                    Some((s.field.output_name().to_string(), fields))
                } else {
                    None
                }
            })
            .collect();

        // Collect all relation names used by aggregates (for deferred cleanup)
        let mut aggregate_relation_names: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        for (_, _, targets) in &aggregates_info {
            for target in targets {
                aggregate_relation_names.insert(target.host_name.clone());
            }
        }

        // Build a mapping from relation field name → output name for aliased relation selections.
        // When a query uses `NewestPublishersBook: book(...)`, the JSON key is "NewestPublishersBook"
        // but the aggregate target references "book". We need to resolve these aliases.
        let relation_alias_map: std::collections::HashMap<&str, &str> = select
            .fields
            .iter()
            .filter_map(|f| {
                if let Requestable::Select(s) = f {
                    let name = s.field.name.as_str();
                    let output = s.field.output_name();
                    if name != output {
                        Some((name, output))
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();

        // Process each result
        for result in &mut results {
            if let JsonValue::Object(ref mut obj) = result {
                for (output_name, agg_type, targets) in &aggregates_info {
                    let mut total_value: f64 = 0.0;
                    let mut total_count: i64 = 0;

                    for target in targets {
                        let relation_name = &target.host_name;
                        let field_name = target.field_name.as_deref();

                        // Try the direct relation name first, then fall back to alias
                        let relation_data = obj
                            .get(relation_name.as_str())
                            .or_else(|| {
                                relation_alias_map
                                    .get(relation_name.as_str())
                                    .and_then(|alias| obj.get(*alias))
                            });
                        if let Some(relation_data) = relation_data {
                            if let JsonValue::Array(items) = relation_data {
                                // Array data: relation or inline array aggregate
                                // Step 1: Apply filter to array elements
                                let filtered_items: Vec<&JsonValue> = if let Some(ref filter) =
                                    target.filter
                                {
                                    // Check if the filter has field-based conditions
                                    // (keys that are not operators like _gt, _eq, etc.)
                                    let has_field_conditions = filter.has_field_conditions();

                                    items
                                        .iter()
                                        .filter(|item| {
                                            if has_field_conditions {
                                                // Field-based filter like {rating: {_gt: 4.8}}
                                                // Match against the entire item object
                                                filter.matches_json_object(item).unwrap_or(false)
                                            } else {
                                                // Operator-only filter like {_gt: 4.8}
                                                // Extract the field value and match against it
                                                let val = match field_name {
                                                    Some(f) => item
                                                        .as_object()
                                                        .and_then(|o| o.get(f))
                                                        .unwrap_or(&JsonValue::Null),
                                                    None => *item,
                                                };
                                                filter.matches_scalar_value(val).unwrap_or(false)
                                            }
                                        })
                                        .collect()
                                } else {
                                    items.iter().collect()
                                };

                                // Step 2: Apply order (sort array elements before limit/offset)
                                // The order field may differ from the aggregate field (e.g., order by "name", sum "rating")
                                // Supports nested paths (e.g., order: {publisher: {yearOpened: ASC}})
                                let mut ordered_items = filtered_items;
                                if let Some(ref order) = target.order {
                                    if let Some(condition) = order.conditions.first() {
                                        let fields = &condition.fields;
                                        let desc = matches!(
                                            condition.direction,
                                            crate::mapper::OrderDirection::Desc
                                        );
                                        ordered_items.sort_by(|a, b| {
                                            let resolve_value =
                                                |item: &&JsonValue| -> Option<JsonValue> {
                                                    if fields.is_empty() {
                                                        return Some((*item).clone());
                                                    }
                                                    // Start with the first field
                                                    let first = &fields[0];
                                                    let mut current = item
                                                        .as_object()
                                                        .and_then(|o| o.get(first.as_str()))
                                                        .cloned()?;
                                                    // Resolve remaining nested fields
                                                    for key in &fields[1..] {
                                                        current = match current {
                                                            JsonValue::Object(ref obj) => {
                                                                obj.get(key.as_str())?.clone()
                                                            }
                                                            _ => return None,
                                                        };
                                                    }
                                                    Some(current)
                                                };
                                            let a_val = resolve_value(a);
                                            let b_val = resolve_value(b);
                                            let cmp = crate::plan::compare_json_values(
                                                a_val.as_ref(),
                                                b_val.as_ref(),
                                            );
                                            if desc {
                                                cmp.reverse()
                                            } else {
                                                cmp
                                            }
                                        });
                                    }
                                }

                                // Step 3: Apply limit/offset
                                let final_items: Vec<&JsonValue> =
                                    if let Some(ref limit) = target.limit {
                                        let offset = limit.offset as usize;
                                        let sliced = if offset < ordered_items.len() {
                                            &ordered_items[offset..]
                                        } else {
                                            &[][..]
                                        };
                                        match limit.limit {
                                            Some(l) => {
                                                sliced.iter().take(l as usize).copied().collect()
                                            }
                                            None => sliced.to_vec(),
                                        }
                                    } else {
                                        ordered_items
                                    };

                                // Step 4: Compute aggregate over final items
                                match agg_type {
                                    AggregateType::Count => {
                                        total_count += final_items.len() as i64;
                                    }
                                    AggregateType::Sum | AggregateType::Average => {
                                        for item in &final_items {
                                            if let Some(n) = extract_numeric(item, field_name) {
                                                total_value += n;
                                                total_count += 1;
                                            }
                                        }
                                    }
                                    AggregateType::Min => {
                                        for item in &final_items {
                                            if let Some(n) = extract_numeric(item, field_name) {
                                                if total_count == 0 || n < total_value {
                                                    total_value = n;
                                                }
                                                total_count += 1;
                                            }
                                        }
                                    }
                                    AggregateType::Max => {
                                        for item in &final_items {
                                            if let Some(n) = extract_numeric(item, field_name) {
                                                if total_count == 0 || n > total_value {
                                                    total_value = n;
                                                }
                                                total_count += 1;
                                            }
                                        }
                                    }
                                }
                            } else {
                                // Scalar data: multi-field per-document aggregate
                                // e.g., _avg(HeightM: {}, Age: {}) where HeightM is a scalar
                                if let Some(n) = relation_data.as_f64() {
                                    match agg_type {
                                        AggregateType::Count => {
                                            total_count += 1;
                                        }
                                        AggregateType::Sum | AggregateType::Average => {
                                            total_value += n;
                                            total_count += 1;
                                        }
                                        AggregateType::Min => {
                                            if total_count == 0 || n < total_value {
                                                total_value = n;
                                            }
                                            total_count += 1;
                                        }
                                        AggregateType::Max => {
                                            if total_count == 0 || n > total_value {
                                                total_value = n;
                                            }
                                            total_count += 1;
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // Store the computed aggregate value
                    let computed_value = match agg_type {
                        AggregateType::Count => JsonValue::Number(total_count.into()),
                        AggregateType::Sum => {
                            if total_value == total_value.floor()
                                && total_value.abs() < i64::MAX as f64
                            {
                                JsonValue::Number((total_value as i64).into())
                            } else {
                                JsonValue::Number(
                                    serde_json::Number::from_f64(total_value)
                                        .unwrap_or_else(|| 0.into()),
                                )
                            }
                        }
                        AggregateType::Average => {
                            if total_count > 0 {
                                let avg = total_value / total_count as f64;
                                if avg == avg.floor() && avg.abs() < i64::MAX as f64 {
                                    JsonValue::Number((avg as i64).into())
                                } else {
                                    JsonValue::Number(
                                        serde_json::Number::from_f64(avg)
                                            .unwrap_or_else(|| 0.into()),
                                    )
                                }
                            } else {
                                // Go DefraDB returns 0 for average of empty/null arrays
                                JsonValue::Number(0.into())
                            }
                        }
                        AggregateType::Min | AggregateType::Max => {
                            if total_count > 0 {
                                if total_value == total_value.floor()
                                    && total_value.abs() < i64::MAX as f64
                                {
                                    JsonValue::Number((total_value as i64).into())
                                } else {
                                    JsonValue::Number(
                                        serde_json::Number::from_f64(total_value)
                                            .unwrap_or_else(|| 0.into()),
                                    )
                                }
                            } else {
                                JsonValue::Null
                            }
                        }
                    };

                    obj.insert(output_name.clone(), computed_value);
                }

                // Deferred cleanup: remove relation data only used for aggregation.
                // When a selection uses an alias (e.g., `books2020: book(...)`), the
                // aggregate's raw relation data ("book") must also be removed since
                // the display data is at the alias key ("books2020").
                for relation_name in &aggregate_relation_names {
                    let selected_with_same_key = select.fields.iter().any(|f| {
                        if let Requestable::Select(s) = f {
                            s.field.name == *relation_name
                                && s.field.output_name() == relation_name
                        } else {
                            false
                        }
                    });
                    if !selected_with_same_key {
                        obj.remove(relation_name.as_str());
                    }
                }

                // Clean up extra fields from relation data that were added for aggregate filter evaluation
                // but weren't in the original selection. For example, if the selection was
                // `published { name }` but the aggregate filter needed `rating`, we need to remove
                // `rating` from each item in `published` after aggregate computation.
                for (relation_name, allowed_fields) in &selected_relation_fields {
                    if let Some(relation_data) = obj.get_mut(relation_name) {
                        if let JsonValue::Array(items) = relation_data {
                            for item in items.iter_mut() {
                                if let JsonValue::Object(item_obj) = item {
                                    // Remove fields that weren't in the original selection
                                    item_obj.retain(|k, _| allowed_fields.contains(k));
                                }
                            }
                        }
                    }
                }
            }
        }

        // Apply post-aggregate filtering if needed
        // When filter uses _alias to reference computed aggregates, the SelectNode can't
        // filter during plan execution since aggregate values don't exist yet.
        // Example: filter: {_alias: {publishedCount: {_gt: 0}}}
        if let Some(ref filter) = select.filter {
            let aggregate_output_names: std::collections::HashSet<&str> = aggregates_info
                .iter()
                .map(|(name, _, _)| name.as_str())
                .collect();

            // Check if filter has _alias conditions referencing aggregate names
            if let Some(alias_conditions) = filter.conditions().get("_alias") {
                if let Some(alias_obj) = alias_conditions.as_object() {
                    let needs_post_filter = alias_obj
                        .keys()
                        .any(|k| aggregate_output_names.contains(k.as_str()));

                    if needs_post_filter {
                        results.retain(|result| {
                            if let Some(obj) = result.as_object() {
                                // Evaluate each alias condition
                                for (alias_name, condition) in alias_obj {
                                    if let Some(value) = obj.get(alias_name) {
                                        // Parse and evaluate the operator conditions
                                        if let Some(cond_obj) = condition.as_object() {
                                            for (op_str, expected) in cond_obj {
                                                if let Some(op) =
                                                    crate::mapper::FilterOp::parse(op_str)
                                                {
                                                    match op {
                                                        crate::mapper::FilterOp::Eq => {
                                                            if value != expected {
                                                                return false;
                                                            }
                                                        }
                                                        crate::mapper::FilterOp::Ne => {
                                                            if value == expected {
                                                                return false;
                                                            }
                                                        }
                                                        crate::mapper::FilterOp::Gt => {
                                                            let v = value.as_f64().unwrap_or(0.0);
                                                            let e =
                                                                expected.as_f64().unwrap_or(0.0);
                                                            if v <= e {
                                                                return false;
                                                            }
                                                        }
                                                        crate::mapper::FilterOp::Gte => {
                                                            let v = value.as_f64().unwrap_or(0.0);
                                                            let e =
                                                                expected.as_f64().unwrap_or(0.0);
                                                            if v < e {
                                                                return false;
                                                            }
                                                        }
                                                        crate::mapper::FilterOp::Lt => {
                                                            let v = value.as_f64().unwrap_or(0.0);
                                                            let e =
                                                                expected.as_f64().unwrap_or(0.0);
                                                            if v >= e {
                                                                return false;
                                                            }
                                                        }
                                                        crate::mapper::FilterOp::Lte => {
                                                            let v = value.as_f64().unwrap_or(0.0);
                                                            let e =
                                                                expected.as_f64().unwrap_or(0.0);
                                                            if v > e {
                                                                return false;
                                                            }
                                                        }
                                                        _ => {}
                                                    }
                                                }
                                            }
                                        }
                                    } else {
                                        // Alias field not found in result, filter it out
                                        return false;
                                    }
                                }
                            }
                            true
                        });
                    }
                }
            }
        }

        // Apply post-aggregate ordering if needed
        // When order references aggregate aliases (e.g., order: {_alias: {total: DESC}}),
        // the OrderByNode can't sort during plan execution since values don't exist yet.
        if let Some(ref order_by) = select.order_by {
            let aggregate_output_names: std::collections::HashSet<&str> = aggregates_info
                .iter()
                .map(|(name, _, _)| name.as_str())
                .collect();

            let needs_post_sort = order_by.conditions.iter().any(|c| {
                c.fields.len() == 1 && aggregate_output_names.contains(c.fields[0].as_str())
            });

            if needs_post_sort {
                results.sort_by(|a, b| {
                    for condition in &order_by.conditions {
                        if condition.fields.len() != 1 {
                            continue;
                        }
                        let field = &condition.fields[0];
                        let a_val = a.as_object().and_then(|o| o.get(field));
                        let b_val = b.as_object().and_then(|o| o.get(field));
                        let a_f = a_val.and_then(|v| v.as_f64()).unwrap_or(0.0);
                        let b_f = b_val.and_then(|v| v.as_f64()).unwrap_or(0.0);
                        let ord = a_f.partial_cmp(&b_f).unwrap_or(std::cmp::Ordering::Equal);
                        let ord =
                            if matches!(condition.direction, crate::mapper::OrderDirection::Desc) {
                                ord.reverse()
                            } else {
                                ord
                            };
                        if ord != std::cmp::Ordering::Equal {
                            return ord;
                        }
                    }
                    std::cmp::Ordering::Equal
                });
            }
        }

        Ok(results)
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
                if nested_select.field.name == "_group" {
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
                if nested_select.field.name == "_group" {
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
                    if let Some(relation_data) = obj.get_mut(field_name) {
                        if let JsonValue::Array(items) = relation_data {
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
        }

        results
    }

    /// Execute a simple query without nested selections.
    ///
    /// This is the optimized path that supports aggregations and grouping.
    async fn execute_simple_select(
        &self,
        select: &Select,
        fetcher: &dyn DocFetcher,
        collection: &Arc<CollectionVersion>,
        identity: Option<Did>,
    ) -> Result<JsonValue> {
        // Build document mapping first (needed for both paths)
        let mapping = plan::build_mapping(select, collection)?;

        // When show_deleted is true, we need to use get_all_with_deleted to include
        // logically deleted documents. The doc_ids filter will be applied by the plan.
        let plan_docs = if select.show_deleted {
            let docs_with_status = fetcher
                .get_all_with_deleted(&select.collection_name, true)
                .await?;
            documents_with_status_to_plan_docs(&docs_with_status, &mapping)?
        } else {
            // Fetch documents from storage
            let docs = if let Some(ref doc_ids) = select.doc_ids {
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
                let missing = result.missing_ids();
                if !missing.is_empty() {
                    warn!(
                        collection = %select.collection_name,
                        missing_ids = ?missing,
                        requested_count = unique_ids.len(),
                        found_count = result.docs().len(),
                        "Some requested documents were not found"
                    );
                }
                result.into_docs()
            } else if let Some(ref filter) = select.filter {
                // Try to use an index if available
                if fetcher.supports_index_queries() && !collection.indexes.is_empty() {
                    if let Some(best_index) = select_best_index(filter, &collection.indexes) {
                        if let Some(params) =
                            filter_to_index_scan(filter, best_index, select.order_by.as_ref(), &collection.fields)
                        {
                            debug!(
                                collection = %select.collection_name,
                                index = %params.index_name,
                                "Using index for query"
                            );
                            // Get doc IDs from index
                            let doc_ids = fetcher
                                .get_by_index_scan(&select.collection_name, &params)
                                .await?;
                            // Fetch the actual documents by ID
                            let result = fetcher
                                .get_by_ids(&select.collection_name, &doc_ids)
                                .await?;
                            result.into_docs()
                        } else {
                            // Filter doesn't translate to index scan, fallback to full scan
                            fetcher.get_all(&select.collection_name).await?
                        }
                    } else {
                        // No suitable index found, fallback to full scan
                        fetcher.get_all(&select.collection_name).await?
                    }
                } else {
                    // Fetcher doesn't support index queries or no indexes, fallback to full scan
                    fetcher.get_all(&select.collection_name).await?
                }
            } else {
                fetcher.get_all(&select.collection_name).await?
            };
            documents_to_plan_docs(&docs, &mapping)?
        };

        // Build ACP filter config if collection has policy and ACP is configured
        let acp_filter = if let (Some(ref acp), Some(ref policy)) = (&self.acp, &collection.policy) {
            Some(plan::AcpFilter {
                acp: acp.clone(),
                identity: Identity::from(identity),
                policy_id: policy.id.clone(),
                resource_name: policy.resource_name.clone(),
            })
        } else {
            if collection.policy.is_some() && self.acp.is_none() {
                tracing::warn!(
                    collection = %collection.name,
                    "Collection has ACP policy but QueryRunner has no ACP configured - ACP enforcement is DISABLED"
                );
            }
            None
        };

        // Build and execute the plan (ACP filter is inserted inside, after Select but before aggregates)
        let mut plan = plan::build_plan(select, plan_docs, mapping.clone(), collection, acp_filter)?;

        // Execute the plan and collect results
        plan.init().await?;
        plan.start().await?;

        let mut results = Vec::new();

        while plan.next().await? {
            let doc = plan.value();
            let json = self.doc_to_json(doc, &mapping)?;
            results.push(json);
        }

        plan.close().await?;

        Ok(JsonValue::Array(results))
    }

    /// Execute a CID-based time-travel query with optional _version support.
    ///
    /// Reconstructs the document as it existed at the specified commit CID
    /// by walking the merkle DAG backwards and replaying CRDT deltas.
    ///
    /// CID queries require `cid` argument and optionally `docID` for validation.
    /// For document CIDs, returns a single-element array. For collection CIDs
    /// (branchable collections), returns all documents visible at that state.
    async fn execute_cid_query_with_version(
        &self,
        select: &Select,
        fetcher: &dyn DocFetcher,
        _caller_identity: Option<Did>,
        version_selection: Option<&Select>,
    ) -> Result<JsonValue> {
        let cid = select.cid.as_ref().ok_or_else(|| {
            QueryError::internal("execute_cid_query called without CID - this is a bug")
        })?;

        // Get expected docID from select.doc_ids (optional validation)
        let expected_doc_id = select.doc_ids.as_ref().and_then(|ids| ids.first());

        // Fetch document(s) at the specified CID.
        // For collection-level CIDs (branchable), this returns multiple documents.
        // For document-level CIDs, this returns a single document.
        let documents = match fetcher
            .get_documents_at_cid(cid, expected_doc_id.map(|s| s.as_str()))
            .await
        {
            Ok(docs) => docs,
            Err(e) => {
                let err_msg = e.to_string();
                // docID mismatch: Go returns empty results
                if err_msg.contains("cid either does not exist or belong to document") {
                    return Ok(JsonValue::Array(vec![]));
                }
                // Block not found in blockstore: propagate as error (Go does the same)
                return Err(e);
            }
        };

        // Get collection schema for building the mapping
        let collection = self.get_collection(&select.collection_name).await?;

        // Separate nested selects (relation fields) from scalar fields.
        // build_mapping can't handle nested selects, so we strip them and resolve relations separately.
        let mut nested_selects: Vec<&Select> = Vec::new();
        let scalar_fields: Vec<Requestable> = select
            .fields
            .iter()
            .filter(|f| {
                if let Requestable::Select(s) = f {
                    if s.field.name == "_version" {
                        return false;
                    }
                    nested_selects.push(s);
                    return false;
                }
                true
            })
            .cloned()
            .collect();

        let select_for_mapping = Select {
            fields: scalar_fields,
            ..select.clone()
        };

        // Build mapping for scalar fields only
        let mapping = plan::build_mapping(&select_for_mapping, &collection)?;

        // Process each document into a JSON object
        let mut result_array = Vec::new();

        for document in &documents {
            // Convert the document to JSON with only the requested scalar fields
            let mut obj = serde_json::Map::new();

            for render_key in &mapping.render_keys {
                let field_name = mapping
                    .try_find_name_from_index(render_key.index)
                    .unwrap_or("");

                let value = if field_name == "__typename" {
                    JsonValue::String(select.collection_name.clone())
                } else if field_name == "_docID" {
                    document
                        .id()
                        .map(|id| JsonValue::String(id.to_string()))
                        .unwrap_or(JsonValue::Null)
                } else if field_name == "_deleted" {
                    JsonValue::Bool(document.is_deleted())
                } else if let Some(nv) = document.get(field_name) {
                    crate::json_convert::normal_value_to_json(nv).unwrap_or(JsonValue::Null)
                } else {
                    JsonValue::Null
                };

                obj.insert(render_key.key.clone(), value);
            }

            // Resolve nested selects (relation fields like `author { name }`)
            for nested_select in &nested_selects {
                let relation_name = &nested_select.field.name;
                let output_name = nested_select.field.output_name();
                let related_collection = &nested_select.collection_name;

                // Many-to-one: parent has FK field (e.g., Book._authorID → Author)
                let fk_field_name = CollectionVersion::relation_id_field_name(relation_name);
                if let Some(fk_value) = document.get(&fk_field_name) {
                    let fk_doc_id = crate::json_convert::normal_value_to_json(fk_value)
                        .ok()
                        .and_then(|v| v.as_str().map(|s| s.to_string()))
                        .unwrap_or_default();

                    if !fk_doc_id.is_empty() {
                        let result = fetcher.get_by_ids(related_collection, &[fk_doc_id]).await?;

                        if let Some(related_doc) = result.docs().first() {
                            let related_obj =
                                self.render_document_fields(related_doc, nested_select);
                            obj.insert(output_name.to_string(), JsonValue::Object(related_obj));
                        } else {
                            obj.insert(output_name.to_string(), JsonValue::Null);
                        }
                    } else {
                        obj.insert(output_name.to_string(), JsonValue::Null);
                    }
                } else {
                    // One-to-many or no FK found: return null for now
                    obj.insert(output_name.to_string(), JsonValue::Null);
                }
            }

            // Add _version data if requested
            if let Some(version_select) = version_selection {
                let doc_id = document.id().map(|id| id.to_string());
                if let Some(doc_id_str) = doc_id {
                    let version_data = self
                        .fetch_version_data(fetcher, &doc_id_str, version_select, Some(cid))
                        .await?;
                    let output_name = version_select.field.output_name();
                    obj.insert(output_name.to_string(), version_data);
                }
            }

            result_array.push(JsonValue::Object(obj));
        }

        Ok(JsonValue::Array(result_array))
    }

    /// Execute a regular query with _version field support.
    ///
    /// This handles queries that include _version selection but don't have a CID argument.
    /// For each document result, fetches the commit history and adds _version data.
    async fn execute_query_with_version(
        &self,
        select: &Select,
        fetcher: &dyn DocFetcher,
        caller_identity: Option<Did>,
        version_selection: Option<&Select>,
    ) -> Result<JsonValue> {
        // Get collection schema
        let collection = self.get_collection(&select.collection_name).await?;

        // Check if _docID is already in the selection (we need it to fetch version data)
        let has_doc_id = select.fields.iter().any(|f| {
            if let Requestable::Field(field) = f {
                field.name == "_docID"
            } else {
                false
            }
        });

        // Build a modified select without _version for the regular query
        // (We'll add _version data after fetching documents)
        // Also add _docID if not already present (needed to fetch version data)
        let mut fields_without_version: Vec<Requestable> = select
            .fields
            .iter()
            .filter(|f| {
                if let Requestable::Select(s) = f {
                    s.field.name != "_version"
                } else {
                    true
                }
            })
            .cloned()
            .collect();

        // Add _docID field if not already present
        if !has_doc_id {
            fields_without_version.push(Requestable::Field(crate::mapper::Field {
                name: "_docID".to_string(),
                alias: None,
            }));
        }

        let select_without_version = Select {
            fields: fields_without_version,
            ..select.clone()
        };

        // Check if remaining fields need the Planner (has real nested selections)
        let has_nested = select_without_version
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

        // Views always need the planner (they execute queries, not storage reads)
        let is_view = collection.query.is_some();

        // Execute the query for document data (without _version)
        let result = if is_view || has_nested || filter_has_relations || order_has_relations {
            self.execute_nested_select_with_planner(
                &select_without_version,
                fetcher,
                caller_identity,
            )
            .await?
        } else {
            self.execute_simple_select(
                &select_without_version,
                fetcher,
                &collection,
                caller_identity,
            )
            .await?
        };

        // If no _version selection, return as-is
        let version_select = match version_selection {
            Some(v) => v,
            None => return Ok(result),
        };

        // Add _version data to each document result
        let results = result
            .as_array()
            .ok_or_else(|| QueryError::internal("Expected array result"))?;

        let mut enriched_results = Vec::new();
        for doc_json in results {
            let mut doc_obj = doc_json
                .as_object()
                .ok_or_else(|| QueryError::internal("Expected object in result"))?
                .clone();

            // Get document ID from the result
            let doc_id = doc_obj
                .get("_docID")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());

            // Remove _docID if it wasn't originally requested
            if !has_doc_id {
                doc_obj.remove("_docID");
            }

            if let Some(doc_id_str) = doc_id {
                let version_data = self
                    .fetch_version_data(fetcher, &doc_id_str, version_select, None)
                    .await?;
                let output_name = version_select.field.output_name();
                doc_obj.insert(output_name.to_string(), version_data);
            } else {
                // No docID available - return empty version array
                let output_name = version_select.field.output_name();
                doc_obj.insert(output_name.to_string(), JsonValue::Array(vec![]));
            }

            enriched_results.push(JsonValue::Object(doc_obj));
        }

        Ok(JsonValue::Array(enriched_results))
    }

    /// Render a Document's fields as a JSON object using only the fields requested by a Select.
    fn render_document_fields(
        &self,
        doc: &Document,
        select: &Select,
    ) -> serde_json::Map<String, JsonValue> {
        let mut obj = serde_json::Map::new();
        for field in &select.fields {
            if let Requestable::Field(f) = field {
                let fname = &f.name;
                let output = f.output_name();
                if fname == "_docID" {
                    if let Some(id) = doc.id() {
                        obj.insert(output.to_string(), JsonValue::String(id.to_string()));
                    } else {
                        obj.insert(output.to_string(), JsonValue::Null);
                    }
                } else if fname == "__typename" {
                    obj.insert(
                        output.to_string(),
                        JsonValue::String(select.collection_name.clone()),
                    );
                } else if let Some(nv) = doc.get(fname) {
                    let json_val =
                        crate::json_convert::normal_value_to_json(nv).unwrap_or(JsonValue::Null);
                    obj.insert(output.to_string(), json_val);
                } else {
                    obj.insert(output.to_string(), JsonValue::Null);
                }
            }
        }
        obj
    }

    /// Fetch version (commit) data for a document.
    ///
    /// Returns an array of commit objects filtered to composite commits (fieldName = "_C")
    /// and rendered with the requested fields from the _version selection.
    pub(crate) async fn fetch_version_data(
        &self,
        fetcher: &dyn DocFetcher,
        doc_id: &str,
        version_select: &Select,
        target_cid: Option<&str>,
    ) -> Result<JsonValue> {
        use crate::fetcher::CommitsQueryOptions;

        // Fetch commits for this document
        // When we have a target CID, we need to traverse all commits back to genesis
        // by setting a large depth. Without a target CID (regular query), depth=None
        // traverses all heads to genesis anyway.
        let depth = if target_cid.is_some() {
            Some(1000) // Reasonable max depth for version history traversal
        } else {
            None
        };

        let options = CommitsQueryOptions {
            doc_id: Some(doc_id.to_string()),
            cid: target_cid.map(|s| s.to_string()),
            depth,
            field_name: None,
        };

        let commits = fetcher.get_commits(&options).await?;

        // Filter to composite commits only (fieldName = "_C")
        // and render the requested fields
        let mut version_results: Vec<JsonValue> = Vec::new();

        for commit in commits {
            // Filter to composite commits
            let field_name = commit
                .get("fieldName")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if field_name != "_C" {
                continue;
            }

            let commit_json = self.render_commit(&commit, version_select)?;
            version_results.push(commit_json);
        }

        // Sort by height descending (newest first)
        version_results.sort_by(|a, b| {
            let h_a = a.get("height").and_then(|v| v.as_i64()).unwrap_or(0);
            let h_b = b.get("height").and_then(|v| v.as_i64()).unwrap_or(0);
            h_b.cmp(&h_a)
        });

        Ok(JsonValue::Array(version_results))
    }

    /// Render a commit document according to the _version selection fields.
    fn render_commit(&self, commit: &Document, version_select: &Select) -> Result<JsonValue> {
        let mut obj = serde_json::Map::new();

        for requestable in &version_select.fields {
            match requestable {
                Requestable::Field(f) => {
                    let field_name = &f.name;
                    let output_name = f.output_name();

                    if let Some(value) = commit.get(field_name) {
                        let json_value = crate::json_convert::normal_value_to_json(value)
                            .unwrap_or(JsonValue::Null);
                        obj.insert(output_name.to_string(), json_value);
                    } else {
                        obj.insert(output_name.to_string(), JsonValue::Null);
                    }
                }
                Requestable::Select(nested) => {
                    let field_name = &nested.field.name;
                    let output_name = nested.field.output_name();

                    // Handle nested selections (links, heads) with optional filter
                    if let Some(value) = commit.get(field_name) {
                        if let Ok(json_val) = crate::json_convert::normal_value_to_json(value) {
                            if let Some(arr) = json_val.as_array() {
                                // Apply filter if present on the nested selection
                                let filtered_items: Vec<&JsonValue> =
                                    if let Some(ref filter) = nested.filter {
                                        arr.iter()
                                            .filter(|item| {
                                                // Check each filter condition against the item
                                                self.json_item_matches_filter(item, filter)
                                            })
                                            .collect()
                                    } else {
                                        arr.iter().collect()
                                    };

                                let nested_results: Vec<JsonValue> = filtered_items
                                    .into_iter()
                                    .map(|item| {
                                        let mut nested_obj = serde_json::Map::new();
                                        for nested_field in &nested.fields {
                                            if let Requestable::Field(nf) = nested_field {
                                                let nf_name = &nf.name;
                                                let nf_output = nf.output_name();
                                                if let Some(nv) = item.get(nf_name) {
                                                    nested_obj
                                                        .insert(nf_output.to_string(), nv.clone());
                                                } else {
                                                    nested_obj.insert(
                                                        nf_output.to_string(),
                                                        JsonValue::Null,
                                                    );
                                                }
                                            }
                                        }
                                        JsonValue::Object(nested_obj)
                                    })
                                    .collect();
                                obj.insert(
                                    output_name.to_string(),
                                    JsonValue::Array(nested_results),
                                );
                            } else {
                                obj.insert(output_name.to_string(), JsonValue::Null);
                            }
                        } else {
                            obj.insert(output_name.to_string(), JsonValue::Null);
                        }
                    } else {
                        obj.insert(output_name.to_string(), JsonValue::Array(vec![]));
                    }
                }
                Requestable::Aggregate(agg) => {
                    // Handle aggregates on commit fields (e.g., _count(links: {}))
                    let output_name = agg.output_name();
                    if let Some(target) = agg.targets.first() {
                        let target_field = target
                            .field_name
                            .as_deref()
                            .filter(|s| !s.is_empty())
                            .or_else(|| Some(target.host_name.as_str()).filter(|s| !s.is_empty()));

                        if let Some(field) = target_field {
                            if let Some(val) = commit.get(field) {
                                if let Ok(json_val) = crate::json_convert::normal_value_to_json(val)
                                {
                                    if let Some(arr) = json_val.as_array() {
                                        obj.insert(
                                            output_name.to_string(),
                                            JsonValue::Number((arr.len() as i64).into()),
                                        );
                                    } else {
                                        obj.insert(
                                            output_name.to_string(),
                                            JsonValue::Number(0.into()),
                                        );
                                    }
                                } else {
                                    obj.insert(
                                        output_name.to_string(),
                                        JsonValue::Number(0.into()),
                                    );
                                }
                            } else {
                                obj.insert(output_name.to_string(), JsonValue::Number(0.into()));
                            }
                        } else {
                            obj.insert(output_name.to_string(), JsonValue::Number(1.into()));
                        }
                    } else {
                        obj.insert(output_name.to_string(), JsonValue::Number(1.into()));
                    }
                }
                Requestable::Similarity(_) => {
                    // Similarity is not applicable in commit context
                }
            }
        }

        Ok(JsonValue::Object(obj))
    }

    /// Check if a JSON object matches a filter for nested commit selections.
    ///
    /// This is a simplified filter matcher for nested selections like `links(filter: {fieldName: {_eq: "Age"}})`.
    /// The filter conditions are stored as `{field_name: {_op: value}}`.
    fn json_item_matches_filter(&self, item: &JsonValue, filter: &crate::mapper::Filter) -> bool {
        use crate::mapper::FilterOp;

        // Get the filter conditions - a map of field_name -> operator conditions
        let conditions = filter.conditions();

        for (field_name, condition_value) in conditions {
            // Check if this is a logical operator (_and, _or, _not)
            if let Some(op) = FilterOp::parse(field_name) {
                match op {
                    FilterOp::And => {
                        if let JsonValue::Array(arr) = condition_value {
                            for sub_cond in arr {
                                if let JsonValue::Object(obj) = sub_cond {
                                    let sub_map: std::collections::HashMap<String, JsonValue> =
                                        obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                                    let sub_filter =
                                        crate::mapper::Filter::from_conditions(sub_map);
                                    if !self.json_item_matches_filter(item, &sub_filter) {
                                        return false;
                                    }
                                }
                            }
                        }
                    }
                    FilterOp::Or => {
                        if let JsonValue::Array(arr) = condition_value {
                            let mut any_match = false;
                            for sub_cond in arr {
                                if let JsonValue::Object(obj) = sub_cond {
                                    let sub_map: std::collections::HashMap<String, JsonValue> =
                                        obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                                    let sub_filter =
                                        crate::mapper::Filter::from_conditions(sub_map);
                                    if self.json_item_matches_filter(item, &sub_filter) {
                                        any_match = true;
                                        break;
                                    }
                                }
                            }
                            if !any_match {
                                return false;
                            }
                        }
                    }
                    FilterOp::Not => {
                        if let JsonValue::Object(obj) = condition_value {
                            let sub_map: std::collections::HashMap<String, JsonValue> =
                                obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                            let sub_filter = crate::mapper::Filter::from_conditions(sub_map);
                            if self.json_item_matches_filter(item, &sub_filter) {
                                return false;
                            }
                        }
                    }
                    _ => {}
                }
                continue;
            }

            // This is a field condition: field_name -> {_op: value}
            let item_value = item.get(field_name);

            // The condition_value should be an object like {"_eq": "Age"}
            if let JsonValue::Object(ops) = condition_value {
                for (op_name, expected_value) in ops {
                    if let Some(op) = FilterOp::parse(op_name) {
                        let matches = self.check_filter_op(item_value, op, expected_value);
                        if !matches {
                            return false;
                        }
                    }
                }
            }
        }

        true
    }

    /// Check if an item value matches a filter operator condition.
    fn check_filter_op(
        &self,
        item_value: Option<&JsonValue>,
        op: crate::mapper::FilterOp,
        expected: &JsonValue,
    ) -> bool {
        use crate::mapper::FilterOp;

        match op {
            FilterOp::Eq => match (item_value, expected) {
                (Some(JsonValue::String(a)), JsonValue::String(b)) => a == b,
                (Some(JsonValue::Number(a)), JsonValue::Number(b)) => a == b,
                (Some(JsonValue::Bool(a)), JsonValue::Bool(b)) => a == b,
                (Some(JsonValue::Null), JsonValue::Null) => true,
                (None, JsonValue::Null) => true,
                _ => false,
            },
            FilterOp::Ne => match (item_value, expected) {
                (Some(JsonValue::String(a)), JsonValue::String(b)) => a != b,
                (Some(JsonValue::Number(a)), JsonValue::Number(b)) => a != b,
                (Some(JsonValue::Bool(a)), JsonValue::Bool(b)) => a != b,
                (Some(JsonValue::Null), JsonValue::Null) => false,
                (None, _) => true,
                _ => true,
            },
            FilterOp::Gt => match (item_value.and_then(|v| v.as_f64()), expected.as_f64()) {
                (Some(a), Some(b)) => a > b,
                _ => false,
            },
            FilterOp::Gte => match (item_value.and_then(|v| v.as_f64()), expected.as_f64()) {
                (Some(a), Some(b)) => a >= b,
                _ => false,
            },
            FilterOp::Lt => match (item_value.and_then(|v| v.as_f64()), expected.as_f64()) {
                (Some(a), Some(b)) => a < b,
                _ => false,
            },
            FilterOp::Lte => match (item_value.and_then(|v| v.as_f64()), expected.as_f64()) {
                (Some(a), Some(b)) => a <= b,
                _ => false,
            },
            FilterOp::In => {
                if let JsonValue::Array(values) = expected {
                    item_value.map(|v| values.contains(v)).unwrap_or(false)
                } else {
                    false
                }
            }
            FilterOp::Nin => {
                if let JsonValue::Array(values) = expected {
                    item_value.map(|v| !values.contains(v)).unwrap_or(true)
                } else {
                    true
                }
            }
            _ => true, // For unsupported operators, default to matching
        }
    }

    /// Execute a top-level aggregate query (e.g., `{ _avg(Users: {field: Age}) }`).
    ///
    /// Top-level aggregates compute a single value over all documents in a collection.
    /// Unlike regular collection queries that return an array, top-level aggregates
    /// return a single value (the computed aggregate).
    ///
    /// Returns 0 for empty collections (Go DefraDB semantics).
    async fn execute_top_level_aggregate(
        &self,
        select: &Select,
        fetcher: &dyn DocFetcher,
        collection: &Arc<CollectionVersion>,
        identity: Option<Did>,
    ) -> Result<JsonValue> {
        // Fetch all documents from the collection
        let docs = fetcher.get_all(&select.collection_name).await?;

        // Build document mapping for field access
        let mapping = plan::build_mapping(select, collection)?;

        // Convert storage documents to values for aggregation
        let mut plan_docs = documents_to_plan_docs(&docs, &mapping)?;

        // Apply ACP filtering if collection has a policy and ACP is configured
        if let (Some(ref acp), Some(ref policy)) = (&self.acp, &collection.policy) {
            let acp_identity = Identity::from(identity);
            let mut filtered = Vec::with_capacity(plan_docs.len());
            for doc in plan_docs {
                if let Some(doc_id_val) = doc.get(0) {
                    if let Some(doc_id) = doc_id_val.as_str() {
                        let has_access = acp
                            .check_doc_access(
                                &acp_identity,
                                DocumentPermission::Read,
                                &policy.id,
                                &policy.resource_name,
                                doc_id,
                            )
                            .await
                            .unwrap_or(false);
                        if has_access {
                            filtered.push(doc);
                        }
                    }
                }
            }
            plan_docs = filtered;
        }

        // For each aggregate in the select, compute its value
        // For top-level aggregates, we return a single object with aggregate results
        let mut result = serde_json::Map::new();

        for requestable in &select.fields {
            if let Requestable::Aggregate(agg) = requestable {
                let output_name = agg.output_name().to_string();

                // Get the target info
                let target = agg.targets.first();
                let field_name = target.and_then(|t| t.field_name.as_ref());
                let target_filter = target.and_then(|t| t.filter.as_ref());

                // Find field index in mapping
                let field_index = field_name.and_then(|name| mapping.first_index_of_name(name));

                // Apply filter if present
                let filtered_docs: Vec<&crate::planner::Doc> = if let Some(filter) = target_filter {
                    plan_docs
                        .iter()
                        .filter(|doc| {
                            // Convert Doc to fields Vec for filter evaluation
                            let fields: Vec<Option<JsonValue>> = (0..mapping.next_index())
                                .map(|i| doc.get(i).cloned())
                                .collect();
                            filter.matches(&fields, &mapping).unwrap_or(false)
                        })
                        .collect()
                } else {
                    plan_docs.iter().collect()
                };

                // Compute the aggregate value
                let value = match agg.aggregate_type {
                    crate::mapper::AggregateType::Count => {
                        // Count documents (optionally filtered)
                        let count = filtered_docs.len() as i64;
                        JsonValue::Number(count.into())
                    }
                    crate::mapper::AggregateType::Sum => {
                        if let Some(idx) = field_index {
                            let sum: f64 = filtered_docs
                                .iter()
                                .filter_map(|doc| doc.get(idx))
                                .filter_map(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
                                .sum();
                            if sum == sum.floor() {
                                JsonValue::Number((sum as i64).into())
                            } else {
                                JsonValue::Number(
                                    serde_json::Number::from_f64(sum).unwrap_or_else(|| 0.into()),
                                )
                            }
                        } else {
                            JsonValue::Number(0.into())
                        }
                    }
                    crate::mapper::AggregateType::Average => {
                        if let Some(idx) = field_index {
                            let values: Vec<f64> = filtered_docs
                                .iter()
                                .filter_map(|doc| doc.get(idx))
                                .filter_map(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
                                .collect();
                            if values.is_empty() {
                                // Go DefraDB returns 0 for AVG of empty set
                                JsonValue::Number(0.into())
                            } else {
                                let avg = values.iter().sum::<f64>() / values.len() as f64;
                                JsonValue::Number(
                                    serde_json::Number::from_f64(avg).unwrap_or_else(|| 0.into()),
                                )
                            }
                        } else {
                            JsonValue::Number(0.into())
                        }
                    }
                    crate::mapper::AggregateType::Min => {
                        if let Some(idx) = field_index {
                            let min: Option<f64> = filtered_docs
                                .iter()
                                .filter_map(|doc| doc.get(idx))
                                .filter_map(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
                                .min_by(|a, b| {
                                    a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
                                });
                            match min {
                                Some(v) if v == v.floor() => JsonValue::Number((v as i64).into()),
                                Some(v) => JsonValue::Number(
                                    serde_json::Number::from_f64(v).unwrap_or_else(|| 0.into()),
                                ),
                                None => JsonValue::Null,
                            }
                        } else {
                            JsonValue::Null
                        }
                    }
                    crate::mapper::AggregateType::Max => {
                        if let Some(idx) = field_index {
                            let max: Option<f64> = filtered_docs
                                .iter()
                                .filter_map(|doc| doc.get(idx))
                                .filter_map(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
                                .max_by(|a, b| {
                                    a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
                                });
                            match max {
                                Some(v) if v == v.floor() => JsonValue::Number((v as i64).into()),
                                Some(v) => JsonValue::Number(
                                    serde_json::Number::from_f64(v).unwrap_or_else(|| 0.into()),
                                ),
                                None => JsonValue::Null,
                            }
                        } else {
                            JsonValue::Null
                        }
                    }
                };

                result.insert(output_name, value);
            }
        }

        // Return the result object directly (not wrapped in an array)
        // The caller will insert this into the response with the aggregate name as key
        // But for top-level aggregates, we need the caller to extract the value
        // Actually, looking at execute_query_internal, it inserts result with select.field.output_name()
        // For { _avg(...) }, output_name is "_avg", and we're returning {"_avg": value}
        // So we'd get {"_avg": {"_avg": value}} which is wrong.
        // We need to return just the value.

        // For top-level aggregates, return the single aggregate value
        // (assumes single aggregate in top-level query)
        if let Some((_, value)) = result.into_iter().next() {
            Ok(value)
        } else {
            Ok(JsonValue::Null)
        }
    }

    /// Execute a _commits system collection query.
    ///
    /// This handles queries to the special _commits collection which fetches
    /// commit history from the headstore and blockstore.
    async fn execute_commits_query(&self, select: &Select) -> Result<JsonValue> {
        use crate::fetcher::CommitsQueryOptions;
        use crate::mapper::{AggregateType, OrderDirection};

        // Build options from the select
        let options = CommitsQueryOptions {
            doc_id: select.doc_ids.as_ref().and_then(|ids| ids.first().cloned()),
            cid: select.cid.clone(),
            depth: select.depth,
            field_name: None,
        };

        // Fetch commits using the fetcher
        let mut commits = self.fetcher.get_commits(&options).await?;

        // Build a mapping for commit fields (needed for filter evaluation)
        let mapping = Self::build_commits_mapping();

        // Apply filter if present
        if let Some(ref filter) = select.filter {
            commits.retain(|commit| {
                let fields = Self::commit_to_fields(commit, &mapping);
                filter.matches(&fields, &mapping).unwrap_or(true)
            });
        }

        // Apply groupBy if present - this changes how we process commits
        // Each entry is (representative_commit, all_commits_in_group)
        let grouped: Option<Vec<(document::Document, Vec<document::Document>)>> =
            if let Some(ref group_by) = select.group_by {
                if !group_by.fields.is_empty() {
                    let mut groups: Vec<(String, document::Document, Vec<document::Document>)> =
                        Vec::new();
                    let mut group_map: std::collections::HashMap<String, usize> =
                        std::collections::HashMap::new();

                    for commit in commits.drain(..) {
                        let key = Self::generate_commit_group_key(&commit, &group_by.fields);
                        if let Some(&idx) = group_map.get(&key) {
                            groups[idx].2.push(commit);
                        } else {
                            let idx = groups.len();
                            group_map.insert(key.clone(), idx);
                            groups.push((key, commit.clone(), vec![commit]));
                        }
                    }

                    Some(
                        groups
                            .into_iter()
                            .map(|(_, rep, docs)| (rep, docs))
                            .collect(),
                    )
                } else {
                    None
                }
            } else {
                None
            };

        // Get the list of commits to process (either grouped representatives or all commits)
        let mut work_items: Vec<(document::Document, Option<Vec<document::Document>>)> =
            if let Some(grouped) = grouped {
                grouped
                    .into_iter()
                    .map(|(rep, all)| (rep, Some(all)))
                    .collect()
            } else {
                commits.into_iter().map(|c| (c, None)).collect()
            };

        // Apply ordering if present
        if let Some(ref order_by) = select.order_by {
            for condition in order_by.conditions.iter().rev() {
                if let Some(field_name) = condition.fields.first() {
                    let desc = matches!(condition.direction, OrderDirection::Desc);
                    work_items.sort_by(|(a, _), (b, _)| {
                        let val_a = a.get(field_name);
                        let val_b = b.get(field_name);
                        let cmp = Self::compare_json_values(val_a, val_b);
                        if desc {
                            cmp.reverse()
                        } else {
                            cmp
                        }
                    });
                }
            }
        }

        // Apply limit and offset if present
        if let Some(ref limit_spec) = select.limit {
            let offset = limit_spec.offset as usize;
            if offset > 0 && offset < work_items.len() {
                work_items = work_items.split_off(offset);
            } else if offset >= work_items.len() {
                work_items.clear();
            }
            if let Some(limit) = limit_spec.limit {
                work_items.truncate(limit as usize);
            }
        }

        // Build results
        let mut results = Vec::new();
        for (commit, group_docs) in &work_items {
            let mut obj = serde_json::Map::new();

            // Map requested fields from the commit document
            for field in &select.fields {
                match field {
                    Requestable::Field(f) => {
                        let field_name = &f.name;
                        let output_name = f.output_name();

                        if let Some(value) = commit.get(field_name) {
                            let json_value = crate::json_convert::normal_value_to_json(value)
                                .unwrap_or(JsonValue::Null);
                            obj.insert(output_name.to_string(), json_value);
                        } else {
                            obj.insert(output_name.to_string(), JsonValue::Null);
                        }
                    }
                    Requestable::Aggregate(agg) => {
                        // Handle aggregates like _count on links/heads
                        let output_name = agg.output_name();
                        let count = match agg.aggregate_type {
                            AggregateType::Count => {
                                // Count on a target field (e.g., links, heads)
                                if let Some(target) = agg.targets.first() {
                                    // Get the target field name - check field_name first (from
                                    // `field:` arg), then host_name (from relation syntax)
                                    let target_field = target
                                        .field_name
                                        .as_deref()
                                        .filter(|s| !s.is_empty())
                                        .or_else(|| {
                                            Some(target.host_name.as_str())
                                                .filter(|s| !s.is_empty())
                                        });

                                    if let Some(field) = target_field {
                                        if let Some(val) = commit.get(field) {
                                            // Convert NormalValue to JSON to check array
                                            if let Ok(json_val) =
                                                crate::json_convert::normal_value_to_json(val)
                                            {
                                                if let Some(arr) = json_val.as_array() {
                                                    arr.len() as i64
                                                } else {
                                                    0
                                                }
                                            } else {
                                                0
                                            }
                                        } else {
                                            0
                                        }
                                    } else {
                                        1 // Count without target = count this commit
                                    }
                                } else {
                                    1 // Count without target = count this commit
                                }
                            }
                            _ => 0, // Other aggregates not supported for commits
                        };
                        obj.insert(output_name.to_string(), JsonValue::Number(count.into()));
                    }
                    Requestable::Select(nested) => {
                        let field_name = &nested.field.name;
                        let output_name = nested.field.output_name();

                        // Handle _group special field for grouped results
                        if field_name == "_group" {
                            if let Some(docs) = group_docs {
                                // Build array of group documents with requested fields
                                let group_array: Vec<JsonValue> = docs
                                    .iter()
                                    .map(|doc: &document::Document| {
                                        let mut nested_obj = serde_json::Map::new();
                                        for nested_field in &nested.fields {
                                            if let Requestable::Field(nf) = nested_field {
                                                let nf_name = &nf.name;
                                                let nf_output = nf.output_name();
                                                if let Some(val) = doc.get(nf_name) {
                                                    let json_val =
                                                        crate::json_convert::normal_value_to_json(
                                                            val,
                                                        )
                                                        .unwrap_or(JsonValue::Null);
                                                    nested_obj
                                                        .insert(nf_output.to_string(), json_val);
                                                } else {
                                                    nested_obj.insert(
                                                        nf_output.to_string(),
                                                        JsonValue::Null,
                                                    );
                                                }
                                            }
                                        }
                                        JsonValue::Object(nested_obj)
                                    })
                                    .collect();
                                obj.insert(output_name.to_string(), JsonValue::Array(group_array));
                            } else {
                                // Not grouped, _group is empty
                                obj.insert(output_name.to_string(), JsonValue::Array(vec![]));
                            }
                        } else if let Some(value) = commit.get(field_name) {
                            // Handle nested selects (e.g., links { cid })
                            if let Ok(json_val) = crate::json_convert::normal_value_to_json(value) {
                                if let Some(arr) = json_val.as_array() {
                                    // Handle array nested selects (e.g., links { cid }, heads { cid })
                                    let nested_results: Vec<JsonValue> = arr
                                        .iter()
                                        .map(|item: &JsonValue| {
                                            let mut nested_obj = serde_json::Map::new();
                                            for nested_field in &nested.fields {
                                                if let Requestable::Field(nf) = nested_field {
                                                    let nf_name = &nf.name;
                                                    let nf_output = nf.output_name();
                                                    if let Some(nv) = item.get(nf_name) {
                                                        nested_obj.insert(
                                                            nf_output.to_string(),
                                                            nv.clone(),
                                                        );
                                                    } else {
                                                        nested_obj.insert(
                                                            nf_output.to_string(),
                                                            JsonValue::Null,
                                                        );
                                                    }
                                                }
                                            }
                                            JsonValue::Object(nested_obj)
                                        })
                                        .collect();
                                    obj.insert(
                                        output_name.to_string(),
                                        JsonValue::Array(nested_results),
                                    );
                                } else if json_val.is_object() {
                                    // Handle object nested selects (e.g., signature { type identity value })
                                    let mut nested_obj = serde_json::Map::new();
                                    for nested_field in &nested.fields {
                                        if let Requestable::Field(nf) = nested_field {
                                            let nf_name = &nf.name;
                                            let nf_output = nf.output_name();
                                            if let Some(nv) = json_val.get(nf_name) {
                                                nested_obj.insert(
                                                    nf_output.to_string(),
                                                    nv.clone(),
                                                );
                                            } else {
                                                nested_obj.insert(
                                                    nf_output.to_string(),
                                                    JsonValue::Null,
                                                );
                                            }
                                        }
                                    }
                                    obj.insert(
                                        output_name.to_string(),
                                        JsonValue::Object(nested_obj),
                                    );
                                } else {
                                    obj.insert(output_name.to_string(), JsonValue::Null);
                                }
                            } else {
                                obj.insert(output_name.to_string(), JsonValue::Null);
                            }
                        } else {
                            obj.insert(output_name.to_string(), JsonValue::Array(vec![]));
                        }
                    }
                    Requestable::Similarity(_) => {
                        // Similarity is not applicable in commit context
                    }
                }
            }

            results.push(JsonValue::Object(obj));
        }

        Ok(JsonValue::Array(results))
    }

    /// Generate a group key from commit field values.
    /// Format matches Go DefraDB: `{index}_{value}_` for each field.
    fn generate_commit_group_key(commit: &document::Document, fields: &[String]) -> String {
        let mut key = String::new();
        for (i, field_name) in fields.iter().enumerate() {
            key.push_str(&format!("{}_", i));
            if let Some(value) = commit.get(field_name) {
                if let Ok(json_val) = crate::json_convert::normal_value_to_json(value) {
                    key.push_str(&Self::json_value_to_key(&json_val));
                } else {
                    key.push_str("null");
                }
            } else {
                key.push_str("null");
            }
            key.push('_');
        }
        key
    }

    /// Convert a JSON value to a string for use in group key.
    fn json_value_to_key(value: &JsonValue) -> String {
        match value {
            JsonValue::Null => "null".to_string(),
            JsonValue::Bool(b) => b.to_string(),
            JsonValue::Number(n) => n.to_string(),
            JsonValue::String(s) => s.clone(),
            JsonValue::Array(arr) => {
                format!(
                    "[{}]",
                    arr.iter()
                        .map(Self::json_value_to_key)
                        .collect::<Vec<_>>()
                        .join(",")
                )
            }
            JsonValue::Object(obj) => {
                format!(
                    "{{{}}}",
                    obj.iter()
                        .map(|(k, v)| format!("{}:{}", k, Self::json_value_to_key(v)))
                        .collect::<Vec<_>>()
                        .join(",")
                )
            }
        }
    }

    /// Build a DocumentMapping for commit fields.
    fn build_commits_mapping() -> crate::document::DocumentMapping {
        let mut mapping = crate::document::DocumentMapping::new();
        mapping.add(0, "cid");
        mapping.add(1, "height");
        mapping.add(2, "fieldName");
        mapping.add(3, "docID");
        mapping.add(4, "delta");
        mapping.add(5, "collectionVersionId");
        mapping.add(6, "links");
        mapping.add(7, "heads");
        mapping.add(8, "signature");
        mapping
    }

    /// Convert a commit document to a fields array for filter evaluation.
    fn commit_to_fields(
        commit: &document::Document,
        _mapping: &crate::document::DocumentMapping,
    ) -> Vec<Option<JsonValue>> {
        let field_names = [
            "cid",
            "height",
            "fieldName",
            "docID",
            "delta",
            "collectionVersionId",
            "links",
            "heads",
            "signature",
        ];
        field_names
            .iter()
            .map(|name| {
                commit
                    .get(name)
                    .and_then(|v| crate::json_convert::normal_value_to_json(v).ok())
            })
            .collect()
    }

    /// Compare two JSON values for ordering.
    fn compare_json_values(
        a: Option<&document::NormalValue>,
        b: Option<&document::NormalValue>,
    ) -> std::cmp::Ordering {
        use std::cmp::Ordering;

        match (a, b) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) => Ordering::Less,
            (Some(_), None) => Ordering::Greater,
            (Some(va), Some(vb)) => {
                // Convert to JSON for comparison
                let ja = crate::json_convert::normal_value_to_json(va).ok();
                let jb = crate::json_convert::normal_value_to_json(vb).ok();

                match (ja, jb) {
                    (Some(JsonValue::Number(na)), Some(JsonValue::Number(nb))) => {
                        let fa = na.as_f64().unwrap_or(0.0);
                        let fb = nb.as_f64().unwrap_or(0.0);
                        fa.partial_cmp(&fb).unwrap_or(Ordering::Equal)
                    }
                    (Some(JsonValue::String(sa)), Some(JsonValue::String(sb))) => sa.cmp(&sb),
                    (Some(a), Some(b)) => a.to_string().cmp(&b.to_string()),
                    _ => Ordering::Equal,
                }
            }
        }
    }
}

/// Extract a numeric value from a JSON item for aggregate computation.
///
/// Handles two cases:
/// - Inline array items: raw values like `JsonValue::Number(5)` — field_name is None
/// - Relation items: objects like `{"score": 5}` — field_name specifies which key
fn extract_numeric(item: &JsonValue, field_name: Option<&str>) -> Option<f64> {
    let val = match field_name {
        Some(field) => {
            // Relation aggregate: extract field from object
            item.as_object()?.get(field)?
        }
        None => {
            // Inline array aggregate: item is the value itself
            item
        }
    };
    val.as_f64().or_else(|| val.as_i64().map(|n| n as f64))
}
