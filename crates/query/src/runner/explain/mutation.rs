use identity::Did;
use serde_json::{Map, Value as JsonValue};
use std::sync::Arc;

use crate::error::{QueryError, Result};
use crate::mapper::{Mutation, Select};
use crate::query_parse::ExplainType;
use crate::txn::TransactionRegistry;

use super::super::plan_drive;
use super::super::{DocFetcher, QueryRunner};

impl<F: DocFetcher + 'static, R: TransactionRegistry> QueryRunner<F, R> {
    /// Execute mutation and return explain output with execution metrics.
    pub(crate) async fn execute_mutation_explain(
        &self,
        mutation_str: &str,
        caller_identity: Option<Did>,
    ) -> Result<JsonValue> {
        use crate::query_parse::parse_mutations_with_limits;

        let mutations = parse_mutations_with_limits(mutation_str, None, self.query_limits)?;
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
    pub(crate) async fn execute_single_mutation_with_metrics(
        &self,
        mutation: &Mutation,
        caller_identity: Option<Did>,
    ) -> Result<(JsonValue, usize, u64)> {
        use crate::mapper::MutationType;

        let node_kind = match mutation.mutation_type {
            MutationType::Create => "addNode",
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
            let resolved_doc_ids = self
                .resolve_filter_to_doc_ids(mutation, self.fetcher.as_ref())
                .await?;

            let mut plan: Box<dyn crate::planner::PlanNode> = match mutation.mutation_type {
                MutationType::Create => {
                    let inputs = self.build_create_inputs(mutation, &collection)?;
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
                    let input = self.build_update_input(mutation, &collection)?;
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
                    if resolved_doc_ids.is_none() && mutation.doc_ids.is_none() {
                        if let Some(ref filter) = mutation.filter {
                            node = node.with_filter(filter.clone());
                        }
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
                        let create_input = self
                            .build_upsert_input_from_map(&collection, &mutation.create_input[0])?;
                        node = node.with_create_input(create_input);
                    }
                    if !mutation.update_input.is_empty() {
                        let update_input =
                            self.build_upsert_input_from_map(&collection, &mutation.update_input)?;
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
            let outcome = async {
                plan.init().await?;
                plan.start().await?;
                while plan.next().await? {}
                Ok(())
            }
            .await;

            plan_drive::close_after(plan.as_mut(), outcome).await?;
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
        let mut combined_explain = match (&phase1, &phase2) {
            (Some((p1, _, _)), Some((p2, _, _))) => {
                // Two-pass (update/upsert): merge by summing all numeric values
                Self::merge_execute_metrics(p1, p2)
            }
            (Some((p1, _, _)), None) => p1.clone(),
            (None, Some((p2, _, _))) => p2.clone(),
            _ => unreachable!(),
        };

        // Go's upsert replaces the scanNode with a non-explainable valuesNode
        // when the filter matches (update path). When no match is found (create path),
        // the scanNode remains in the plan and appears in the explain output.
        let upsert_matched = phase1
            .as_ref()
            .map(|(_, doc_count, _)| *doc_count > 0)
            .unwrap_or(false);
        if mutation.mutation_type == MutationType::Upsert && upsert_matched {
            Self::strip_scan_node(&mut combined_explain);
        }

        // For update mutations, Go uses phase1 metrics for both outer and inner selectNode
        // (the pre-mutation scan), NOT the combined sum.
        let inner_explain = if mutation.mutation_type == MutationType::Update {
            if let Some((p1, _, _)) = &phase1 {
                p1.clone()
            } else {
                combined_explain
            }
        } else {
            combined_explain
        };

        // Wrap in selectTopNode
        let select_node_content =
            Self::ensure_select_node_wrapper(inner_explain, &metric_select, ExplainType::Execute);

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

        // Go wraps updateNode inside selectTopNode -> selectNode for execute explain too.
        // The outer selectNode gets phase1 metrics (pre-mutation scan iterations/filterMatches).
        let wrapped_node = if mutation.mutation_type == MutationType::Update {
            if let Some((p1_explain, _, p1_execs)) = &phase1 {
                let filter_matches = p1_explain
                    .as_object()
                    .and_then(|o| o.get("selectNode"))
                    .and_then(|sn| sn.as_object())
                    .and_then(|o| o.get("filterMatches"))
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0);
                serde_json::json!({
                    "selectTopNode": {
                        "selectNode": {
                            "filterMatches": filter_matches,
                            "iterations": *p1_execs,
                            node_kind: mutation_inner
                        }
                    }
                })
            } else {
                mutation_node
            }
        } else {
            mutation_node
        };

        let doc_count = match (&phase1, &phase2) {
            (_, Some((_, count, _))) => *count,
            (Some((_, count, _)), None) => *count,
            _ => 0,
        };

        Ok((wrapped_node, doc_count, 1))
    }

    /// Recursively merge two execute explain JSON trees by summing numeric values.
    /// Used to combine Phase 1 and Phase 2 metrics for update/upsert mutations.
    pub(crate) fn merge_execute_metrics(phase1: &JsonValue, phase2: &JsonValue) -> JsonValue {
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

    /// Recursively remove `scanNode` from the explain JSON tree.
    ///
    /// Go's upsert replaces the scanNode with a non-explainable valuesNode
    /// during execution, so the scanNode is absent from its explain output.
    fn strip_scan_node(value: &mut JsonValue) {
        if let Some(obj) = value.as_object_mut() {
            obj.remove("scanNode");
            for child in obj.values_mut() {
                Self::strip_scan_node(child);
            }
        }
    }

    /// Generate an explanation for a single mutation operation.
    pub(crate) async fn explain_single_mutation(
        &self,
        mutation: &Mutation,
        explain_type: ExplainType,
    ) -> Result<JsonValue> {
        use crate::mapper::MutationType;

        // Get the mutation node kind name
        let node_kind = match mutation.mutation_type {
            MutationType::Create => "addNode",
            MutationType::Update => "updateNode",
            MutationType::Delete => "deleteNode",
            MutationType::Upsert => "upsertNode",
        };

        // Build the inner select plan explanation
        // Mutations in Go have: mutationNode -> selectTopNode -> selectNode -> scanNode
        let collection = self
            .effective_provider()
            .get_collection(&mutation.collection_name)
            .await?
            .ok_or_else(|| QueryError::collection_not_found(&mutation.collection_name))?;

        // Build a select for the mutation's result fields, including filter and docIDs
        // These are passed through to the scanNode for proper explain output
        let mut select = Select::new(&mutation.collection_name);
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

        // Go wraps updateNode inside selectTopNode -> selectNode because
        // the update plan reads results through a selectNode pipeline.
        // Other mutations (create, delete, upsert) are top-level operation nodes.
        if mutation.mutation_type == MutationType::Update {
            Ok(serde_json::json!({
                "selectTopNode": {
                    "selectNode": mutation_node
                }
            }))
        } else {
            Ok(mutation_node)
        }
    }
}
