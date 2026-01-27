//! Query execution methods for QueryRunner.

use acp::Identity;
use identity::Did;
use schema::CollectionVersion;
use serde_json::{Map, Value as JsonValue};
use std::collections::HashSet;
use std::sync::Arc;
use tracing::warn;

use crate::document::documents_to_plan_docs;
use crate::error::{QueryError, Result};
use crate::mapper::{Requestable, Select};
use crate::plan::PermissionFilterNode;
use crate::planner::Planner;
use crate::query_parse::{parse_query, ExplainType};
use crate::txn::TransactionRegistry;

use super::fetcher::FetcherWrapper;
use super::plan;
use super::{DocFetcher, QueryRunner};

impl<F: DocFetcher, R: TransactionRegistry> QueryRunner<F, R> {
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

    /// Generate an explanation of the query plan.
    ///
    /// Used when queries include the @explain directive.
    /// Supports three modes:
    /// - Simple: Query plan structure without execution
    /// - Execute: Run the query and return plan structure with execution metrics
    /// - Debug: All plan nodes including internal ones
    pub async fn explain_query_with_identity(
        &self,
        query: &str,
        caller_identity: Option<Did>,
        explain_type: ExplainType,
    ) -> Result<JsonValue> {
        match explain_type {
            ExplainType::Simple | ExplainType::Debug => {
                // Simple and Debug modes: explain without execution
                let selects = parse_query(query)?;
                let mut results = Map::new();

                for select in selects {
                    let explanation = self.explain_select(&select, explain_type).await?;
                    let key = select.field.output_name();
                    results.insert(key.to_string(), explanation);
                }

                Ok(serde_json::json!({ "explain": results }))
            }
            ExplainType::Execute => {
                // Execute mode: run the query and collect metrics
                self.execute_explain(query, caller_identity).await
            }
        }
    }

    /// Execute the query and return explain output with execution metrics.
    /// Format matches Go DefraDB's executeAndExplainRequest output.
    async fn execute_explain(
        &self,
        query: &str,
        caller_identity: Option<Did>,
    ) -> Result<JsonValue> {
        let selects = parse_query(query)?;

        let mut explain_result = Map::new();
        let mut total_executions: u64 = 0;
        let mut total_docs: usize = 0;
        let mut execution_success = true;
        let mut execution_errors: Vec<String> = Vec::new();

        for select in selects {
            // Execute the select and collect metrics
            match self
                .execute_select_with_metrics(&select, caller_identity.clone())
                .await
            {
                Ok((explanation, doc_count, exec_count)) => {
                    // Merge the plan tree into explain result (Go format)
                    if let Some(obj) = explanation.as_object() {
                        for (key, value) in obj {
                            explain_result.insert(key.clone(), value.clone());
                        }
                    }
                    total_docs += doc_count;
                    total_executions += exec_count;
                }
                Err(e) => {
                    execution_success = false;
                    execution_errors.push(e.to_string());
                }
            }
        }

        // Add execution metrics (Go format)
        explain_result.insert(
            "executionSuccess".to_string(),
            serde_json::json!(execution_success),
        );
        explain_result.insert(
            "planExecutions".to_string(),
            serde_json::json!(total_executions),
        );
        explain_result.insert("sizeOfResult".to_string(), serde_json::json!(total_docs));

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
        // Get collection schema
        let collection = self
            .collection_provider
            .get_collection(&select.collection_name)
            .await?
            .ok_or_else(|| QueryError::collection_not_found(&select.collection_name))?;

        // Build document mapping and plan
        let mapping = plan::build_mapping(select, &collection)?;

        // Fetch documents
        let fetcher = self.fetcher.as_ref();
        let docs = if let Some(ref doc_ids) = select.doc_ids {
            // Deduplicate doc_ids while preserving order (Go compatibility)
            let mut seen = HashSet::new();
            let unique_ids: Vec<String> = doc_ids
                .iter()
                .filter(|id| seen.insert((*id).clone()))
                .cloned()
                .collect();
            let result = fetcher.get_by_ids(&select.collection_name, &unique_ids).await?;
            result.into_docs()
        } else {
            fetcher.get_all(&select.collection_name).await?
        };

        // Convert to plan docs
        let plan_docs = documents_to_plan_docs(&docs, &mapping)?;
        let doc_count = plan_docs.len();

        // Build the plan
        let mut plan = plan::build_plan(select, plan_docs.clone(), mapping.clone(), &collection)?;

        // Wrap with permission filter if needed
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

        let mut iterations: u64 = 0;
        let mut result_count = 0;

        while plan.next().await? {
            iterations += 1;
            result_count += 1;
        }

        plan.close().await?;

        // Get the execution explain from the plan (Go format: { "nodeKind": { ... } })
        let explanation = plan.explain();

        // Go adds iterations to each node during execute explain
        // For now, we add it to the outermost node
        let explanation = Self::add_iterations_to_explain(explanation, iterations, doc_count);

        Ok((explanation, result_count, iterations))
    }

    /// Add execution metrics to the explain output (Go format).
    fn add_iterations_to_explain(
        mut explanation: JsonValue,
        iterations: u64,
        doc_fetches: usize,
    ) -> JsonValue {
        // The explanation is { "nodeKind": { ... } }
        // We need to add iterations to the inner object
        if let Some(obj) = explanation.as_object_mut() {
            if let Some((_, inner)) = obj.iter_mut().next() {
                if let Some(inner_obj) = inner.as_object_mut() {
                    inner_obj.insert("iterations".to_string(), serde_json::json!(iterations));
                    inner_obj.insert("docFetches".to_string(), serde_json::json!(doc_fetches));
                }
            }
        }
        explanation
    }

    /// Generate an explanation of a single Select operation.
    async fn explain_select(
        &self,
        select: &Select,
        explain_type: ExplainType,
    ) -> Result<JsonValue> {
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

        if has_nested {
            // Use the Planner for queries with nested selections
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

        let planner = Planner::new(collections);
        let plan_result = planner.plan_with_index_info(select)?;
        let plan = plan_result.plan;

        // Return the plan explanation based on type
        match explain_type {
            ExplainType::Debug => Ok(plan.explain_debug()),
            _ => Ok(plan.explain()),
        }
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
        let plan = plan::build_plan(select, vec![], mapping, collection)?;

        // Return the plan explanation based on type
        match explain_type {
            ExplainType::Debug => Ok(plan.explain_debug()),
            _ => Ok(plan.explain()),
        }
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
        let selects = parse_query(query)?;

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
        // Get collection schema on-demand from provider
        let collection = self.get_collection(&select.collection_name).await?;

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
            && select.fields.iter().all(|f| matches!(f, Requestable::Aggregate(_)))
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
                .execute_top_level_aggregate(select, fetcher, &collection)
                .await;
        }

        // Use Planner if there are nested selections, filter through relations,
        // order through relations, or aggregates on relations
        let needs_planner =
            has_nested || filter_has_relations || order_has_relations || aggregates_have_relations;

        // SECURITY: Block nested queries on ACP-protected collections until Planner ACP is implemented.
        // See issue #114 for tracking the full fix.
        if needs_planner && self.acp.is_some() {
            // Check root collection for ACP
            if collection.policy.is_some() {
                return Err(QueryError::execution(format!(
                    "Nested queries on ACP-protected collections are not yet supported. \
                     Collection '{}' has an ACP policy. Remove nested selections or use \
                     separate queries. See issue #114 for tracking.",
                    collection.name
                )));
            }

            // Check nested collections by resolving relation targets from parent collection
            if let Some(acp_coll) = self
                .find_acp_collection_in_nested(select, &collection)
                .await?
            {
                return Err(QueryError::execution(format!(
                    "Nested queries on ACP-protected collections are not yet supported. \
                     Collection '{}' has an ACP policy. Remove nested selections or use \
                     separate queries. See issue #114 for tracking.",
                    acp_coll
                )));
            }
        }

        if needs_planner {
            // Use the Planner for queries with nested selections (joins) or relation filters.
            // Note: ACP filtering for nested queries is not yet implemented.
            // Queries on ACP-protected collections are blocked above.
            self.execute_nested_select_with_planner(select, fetcher, caller_identity)
                .await
        } else {
            // Use the optimized path for simple queries
            self.execute_simple_select(select, fetcher, &collection, caller_identity)
                .await
        }
    }

    /// Execute a query with nested selections using the Planner.
    ///
    /// The Planner builds a proper join plan with TypeJoinOne/TypeJoinMany nodes.
    /// ScanNodes fetch their own data via the attached fetcher.
    ///
    /// Note: This path does NOT enforce ACP permissions. Queries involving
    /// ACP-protected collections are blocked at the caller level (execute_select_internal).
    /// The identity parameter is accepted for future use when Planner ACP is implemented.
    async fn execute_nested_select_with_planner(
        &self,
        select: &Select,
        fetcher: &dyn DocFetcher,
        _identity: Option<Did>,
    ) -> Result<JsonValue> {
        // Create a fetcher wrapper that can be shared across plan nodes
        // We need to wrap the reference in an Arc-compatible struct
        let fetcher_arc = FetcherWrapper::new(fetcher);

        // Build the plan using the Planner with fetcher support
        // Get all collections from provider for join planning
        let collections_map = self.collections_map().await?;
        let collections: Vec<CollectionVersion> =
            collections_map.values().map(|c| (**c).clone()).collect();

        let planner = Planner::new(collections).with_fetcher(Arc::new(fetcher_arc));
        let plan_result = planner.plan_with_index_info(select)?;
        let mut plan = plan_result.plan;

        // Get the mapping from the plan
        let mapping = plan.document_map().clone();

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

        // Post-process relation-based aggregates
        // For aggregates like _count(books: {}), compute the value from joined data
        let results = self.compute_relation_aggregates(results, select)?;

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
        // Collect info about relation aggregates
        let mut aggregates_info: Vec<(String, crate::mapper::AggregateType, Vec<(String, Option<String>)>)> =
            Vec::new();

        for requestable in &select.fields {
            if let Requestable::Aggregate(agg) = requestable {
                let mut relation_targets = Vec::new();
                for target in &agg.targets {
                    if !target.host_name.is_empty() {
                        relation_targets
                            .push((target.host_name.clone(), target.field_name.clone()));
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

        // If no relation aggregates, return as-is
        if aggregates_info.is_empty() {
            return Ok(results);
        }

        // Process each result
        for result in &mut results {
            if let JsonValue::Object(ref mut obj) = result {
                for (output_name, agg_type, targets) in &aggregates_info {
                    let mut total_value: f64 = 0.0;
                    let mut total_count: i64 = 0;

                    for (relation_name, field_name) in targets {
                        // Get the joined relation data
                        if let Some(relation_data) = obj.get(relation_name) {
                            if let JsonValue::Array(items) = relation_data {
                                match agg_type {
                                    crate::mapper::AggregateType::Count => {
                                        total_count += items.len() as i64;
                                    }
                                    crate::mapper::AggregateType::Sum
                                    | crate::mapper::AggregateType::Average => {
                                        if let Some(field) = field_name {
                                            for item in items {
                                                if let JsonValue::Object(item_obj) = item {
                                                    if let Some(val) = item_obj.get(field) {
                                                        if let Some(n) = val.as_f64() {
                                                            total_value += n;
                                                            total_count += 1;
                                                        } else if let Some(n) = val.as_i64() {
                                                            total_value += n as f64;
                                                            total_count += 1;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    crate::mapper::AggregateType::Min => {
                                        if let Some(field) = field_name {
                                            for item in items {
                                                if let JsonValue::Object(item_obj) = item {
                                                    if let Some(val) = item_obj.get(field) {
                                                        if let Some(n) = val.as_f64() {
                                                            if total_count == 0
                                                                || n < total_value
                                                            {
                                                                total_value = n;
                                                            }
                                                            total_count += 1;
                                                        } else if let Some(n) = val.as_i64() {
                                                            let n = n as f64;
                                                            if total_count == 0
                                                                || n < total_value
                                                            {
                                                                total_value = n;
                                                            }
                                                            total_count += 1;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    crate::mapper::AggregateType::Max => {
                                        if let Some(field) = field_name {
                                            for item in items {
                                                if let JsonValue::Object(item_obj) = item {
                                                    if let Some(val) = item_obj.get(field) {
                                                        if let Some(n) = val.as_f64() {
                                                            if total_count == 0
                                                                || n > total_value
                                                            {
                                                                total_value = n;
                                                            }
                                                            total_count += 1;
                                                        } else if let Some(n) = val.as_i64() {
                                                            let n = n as f64;
                                                            if total_count == 0
                                                                || n > total_value
                                                            {
                                                                total_value = n;
                                                            }
                                                            total_count += 1;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        // Remove the joined relation data (it was only for aggregation)
                        // But only if the relation wasn't explicitly selected
                        let relation_selected = select.fields.iter().any(|f| {
                            if let Requestable::Select(s) = f {
                                s.field.name == *relation_name
                            } else {
                                false
                            }
                        });
                        if !relation_selected {
                            obj.remove(relation_name);
                        }
                    }

                    // Store the computed aggregate value
                    let computed_value = match agg_type {
                        crate::mapper::AggregateType::Count => {
                            JsonValue::Number(total_count.into())
                        }
                        crate::mapper::AggregateType::Sum => {
                            if total_value == total_value.floor() {
                                JsonValue::Number((total_value as i64).into())
                            } else {
                                JsonValue::Number(
                                    serde_json::Number::from_f64(total_value)
                                        .unwrap_or_else(|| 0.into()),
                                )
                            }
                        }
                        crate::mapper::AggregateType::Average => {
                            if total_count > 0 {
                                let avg = total_value / total_count as f64;
                                JsonValue::Number(
                                    serde_json::Number::from_f64(avg).unwrap_or_else(|| 0.into()),
                                )
                            } else {
                                JsonValue::Null
                            }
                        }
                        crate::mapper::AggregateType::Min | crate::mapper::AggregateType::Max => {
                            if total_count > 0 {
                                if total_value == total_value.floor() {
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
            }
        }

        Ok(results)
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
        // Fetch documents from storage
        let docs = if let Some(ref doc_ids) = select.doc_ids {
            // Deduplicate doc_ids while preserving order (Go compatibility)
            let mut seen = HashSet::new();
            let unique_ids: Vec<String> = doc_ids
                .iter()
                .filter(|id| seen.insert((*id).clone()))
                .cloned()
                .collect();
            let result = fetcher.get_by_ids(&select.collection_name, &unique_ids).await?;
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
        } else {
            fetcher.get_all(&select.collection_name).await?
        };

        // Build document mapping
        let mapping = plan::build_mapping(select, collection)?;

        // Convert storage documents to plan docs
        let plan_docs = documents_to_plan_docs(&docs, &mapping)?;

        // Build and execute the plan
        let mut plan = plan::build_plan(select, plan_docs, mapping.clone(), collection)?;

        // Wrap with permission filter if collection has ACP policy and ACP is configured
        if let (Some(ref acp), Some(ref policy)) = (&self.acp, &collection.policy) {
            plan = Box::new(PermissionFilterNode::new(
                plan,
                acp.clone(),
                Identity::from(identity),
                &policy.id,
                &policy.resource_name,
            ));
        } else if collection.policy.is_some() && self.acp.is_none() {
            // Collection has an ACP policy but ACP is not configured on this runner
            // This means ACP enforcement is disabled - all documents will be accessible
            tracing::warn!(
                collection = %collection.name,
                "Collection has ACP policy but QueryRunner has no ACP configured - ACP enforcement is DISABLED"
            );
        }

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
    ) -> Result<JsonValue> {
        // Fetch all documents from the collection
        let docs = fetcher.get_all(&select.collection_name).await?;

        // Build document mapping for field access
        let mapping = plan::build_mapping(select, collection)?;

        // Convert storage documents to values for aggregation
        let plan_docs = documents_to_plan_docs(&docs, &mapping)?;

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
                                .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
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
                                .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
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
}
