//! Query execution methods for QueryRunner.

use acp::Identity;
use identity::Did;
use schema::CollectionVersion;
use serde_json::{Map, Value as JsonValue};
use std::sync::Arc;
use tracing::warn;

use crate::document::documents_to_plan_docs;
use crate::error::{QueryError, Result};
use crate::mapper::{Requestable, Select};
use crate::plan::PermissionFilterNode;
use crate::planner::Planner;
use crate::query_parse::parse_query;
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

    /// Generate an explanation of the query plan without executing.
    ///
    /// Used when queries include the @explain directive.
    pub async fn explain_query_with_identity(
        &self,
        query: &str,
        _caller_identity: Option<Did>,
    ) -> Result<JsonValue> {
        let selects = parse_query(query)?;

        let mut results = Map::new();

        for select in selects {
            let explanation = self.explain_select(&select)?;
            let key = select.field.output_name();
            results.insert(key.to_string(), explanation);
        }

        Ok(serde_json::json!({ "explain": results }))
    }

    /// Generate an explanation of a single Select operation.
    fn explain_select(&self, select: &Select) -> Result<JsonValue> {
        // Get collection schema
        let collection = self
            .collections
            .get(&select.collection_name)
            .ok_or_else(|| QueryError::collection_not_found(&select.collection_name))?;

        // Check if this query has nested selections (relations)
        let has_nested = select
            .fields
            .iter()
            .any(|f| matches!(f, Requestable::Select(_)));

        if has_nested {
            // Use the Planner for queries with nested selections
            self.explain_nested_select(select)
        } else {
            // Explain simple query plan
            self.explain_simple_select(select, collection)
        }
    }

    /// Generate an explanation for a query with nested selections.
    fn explain_nested_select(&self, select: &Select) -> Result<JsonValue> {
        // Build the plan using the Planner
        let collections: Vec<CollectionVersion> =
            self.collections.values().map(|c| (**c).clone()).collect();

        let planner = Planner::new(collections);
        let plan_result = planner.plan_with_index_info(select)?;
        let plan = plan_result.plan;

        // Return the plan explanation
        Ok(plan.explain())
    }

    /// Generate an explanation for a simple query without nested selections.
    fn explain_simple_select(
        &self,
        select: &Select,
        collection: &Arc<CollectionVersion>,
    ) -> Result<JsonValue> {
        // Build document mapping and plan
        let mapping = plan::build_mapping(select, collection, &self.collections)?;

        // Create an empty plan with no documents for explanation purposes
        let plan = plan::build_plan(select, vec![], mapping, &self.collections)?;

        // Return the plan explanation
        Ok(plan.explain())
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
        // Get collection schema
        let collection = self
            .collections
            .get(&select.collection_name)
            .ok_or_else(|| QueryError::collection_not_found(&select.collection_name))?;

        // Validate unsupported features and field references
        plan::validate_select(select, collection)?;

        // Check if this query has nested selections (relations)
        let has_nested = select
            .fields
            .iter()
            .any(|f| matches!(f, Requestable::Select(_)));

        // SECURITY: Block nested queries on ACP-protected collections until Planner ACP is implemented.
        // See issue #114 for tracking the full fix.
        if has_nested && self.acp.is_some() {
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
            if let Some(acp_coll) = self.find_acp_collection_in_nested(select, collection) {
                return Err(QueryError::execution(format!(
                    "Nested queries on ACP-protected collections are not yet supported. \
                     Collection '{}' has an ACP policy. Remove nested selections or use \
                     separate queries. See issue #114 for tracking.",
                    acp_coll
                )));
            }
        }

        if has_nested {
            // Use the Planner for queries with nested selections (joins)
            // Note: ACP filtering for nested queries is not yet implemented.
            // Queries on ACP-protected collections are blocked above.
            self.execute_nested_select_with_planner(select, fetcher, caller_identity)
                .await
        } else {
            // Use the optimized path for simple queries
            self.execute_simple_select(select, fetcher, collection, caller_identity)
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
        let collections: Vec<CollectionVersion> =
            self.collections.values().map(|c| (**c).clone()).collect();

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

        Ok(JsonValue::Array(results))
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
            let result = fetcher.get_by_ids(&select.collection_name, doc_ids).await?;
            let missing = result.missing_ids();
            if !missing.is_empty() {
                warn!(
                    collection = %select.collection_name,
                    missing_ids = ?missing,
                    requested_count = doc_ids.len(),
                    found_count = result.docs().len(),
                    "Some requested documents were not found"
                );
            }
            result.into_docs()
        } else {
            fetcher.get_all(&select.collection_name).await?
        };

        // Build document mapping
        let mapping = plan::build_mapping(select, collection, &self.collections)?;

        // Convert storage documents to plan docs
        let plan_docs = documents_to_plan_docs(&docs, &mapping)?;

        // Build and execute the plan
        let mut plan = plan::build_plan(select, plan_docs, mapping.clone(), &self.collections)?;

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
}
