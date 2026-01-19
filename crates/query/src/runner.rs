//! Query runner - executes queries against storage
//!
//! This module provides the QueryRunner which bridges the query planner
//! with the storage layer, executing queries and returning JSON results.
//!
//! # Transaction Support
//!
//! The QueryRunner supports executing queries within transaction contexts via
//! a `TransactionRegistry`. The registry manages transaction lifecycle and provides
//! transaction-scoped document fetchers for query execution.

use acp::{DocumentACP, Identity};
use async_trait::async_trait;
use document::Document;
use identity::Did;
use schema::CollectionVersion;
use serde_json::{Map, Value as JsonValue};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;
use tracing::warn;

use crate::document::{document_to_plan_doc, documents_to_plan_docs, DocumentMapping};
use crate::error::{QueryError, Result, TransactionError};
use crate::executor::{QueryExecutor, QueryRequest, QueryResponse, QueryResponseError};
use crate::mapper::{AggregateType, Mutation, MutationType, Requestable, Select};
use crate::mutator::DocMutator;
use crate::plan::{
    AllDocsNode, AverageNode, CountNode, CreateInput, CreateNode, DeleteNode, GroupByNode,
    LimitNode, MaxNode, MinNode, OrderByNode, PermissionFilterNode, ScanNode, SelectNode, SumNode,
    UpdateInput, UpdateNode, UpsertInput, UpsertNode,
};
use crate::planner::{Doc, PlanNode, Planner};
use crate::query_parse::{parse_mutations, parse_query, parse_request, ParsedOperation};
use crate::txn::{
    GetTransactionResult, NoOpTransactionRegistry, TransactionHandle, TransactionRegistry,
};

// Re-export for backwards compatibility
pub use crate::fetcher::{DocFetcher, FetchByIdsResult};

/// Query runner that executes GraphQL queries against storage.
pub struct QueryRunner<F: DocFetcher, R: TransactionRegistry = NoOpTransactionRegistry> {
    /// Document fetcher for storage access (used for non-transactional queries)
    fetcher: Arc<F>,
    /// Collection schemas by name
    collections: HashMap<String, Arc<CollectionVersion>>,
    /// Transaction registry for transaction lifecycle management
    registry: Arc<R>,
    /// Document mutator for mutation operations (optional)
    mutator: Option<Arc<dyn DocMutator>>,
    /// Document ACP for permission checks (optional)
    acp: Option<Arc<dyn DocumentACP>>,
    /// Default identity for ACP permission checks.
    ///
    /// Used when a request doesn't include an explicit identity (e.g., no bearer token).
    /// Typically set from the `--identity` CLI flag.
    default_identity: Option<Did>,
}

impl<F: DocFetcher> QueryRunner<F, NoOpTransactionRegistry> {
    /// Create a new query runner with the given fetcher and collections.
    ///
    /// This creates a runner without transaction support. Use `with_registry`
    /// to enable transaction support.
    pub fn new(fetcher: F, collections: Vec<CollectionVersion>) -> Self {
        let collections_map = collections
            .iter()
            .map(|c| (c.name.clone(), Arc::new(c.clone())))
            .collect();
        Self {
            fetcher: Arc::new(fetcher),
            collections: collections_map,
            registry: Arc::new(NoOpTransactionRegistry),
            mutator: None,
            acp: None,
            default_identity: None,
        }
    }
}

impl<F: DocFetcher, R: TransactionRegistry> QueryRunner<F, R> {
    /// Create a new query runner with transaction support.
    pub fn with_registry(fetcher: F, collections: Vec<CollectionVersion>, registry: R) -> Self {
        let collections_map = collections
            .iter()
            .map(|c| (c.name.clone(), Arc::new(c.clone())))
            .collect();
        Self {
            fetcher: Arc::new(fetcher),
            collections: collections_map,
            registry: Arc::new(registry),
            mutator: None,
            acp: None,
            default_identity: None,
        }
    }

    /// Set the document mutator for mutation operations.
    ///
    /// This enables support for CREATE, UPDATE, and DELETE mutations.
    pub fn with_mutator(mut self, mutator: Arc<dyn DocMutator>) -> Self {
        self.mutator = Some(mutator);
        self
    }

    /// Set the document ACP for permission checks.
    ///
    /// When set, queries will filter results based on the identity's permissions.
    /// Collections with a policy will have ACP enforced; others are unaffected.
    pub fn with_acp(mut self, acp: Arc<dyn DocumentACP>) -> Self {
        self.acp = Some(acp);
        self
    }

    /// Set the default identity for ACP permission checks.
    ///
    /// This identity is used when a request doesn't include an explicit identity
    /// (e.g., no `Authorization: Bearer <token>` header). Typically set from
    /// the `--identity` CLI flag.
    ///
    /// When a request DOES include an identity, that identity takes precedence
    /// over the default.
    pub fn with_default_identity(mut self, identity: Did) -> Self {
        self.default_identity = Some(identity);
        self
    }

    /// Resolve the effective identity for a request.
    ///
    /// Priority:
    /// 1. Request-provided identity (from bearer token)
    /// 2. Default identity (from --identity CLI flag)
    /// 3. Anonymous (None)
    fn resolve_identity(&self, request_identity: Option<Did>) -> Option<Did> {
        request_identity.or_else(|| self.default_identity.clone())
    }

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

    /// Execute a GraphQL query with a specific fetcher and identity.
    ///
    /// This is used internally for both regular queries (using the default fetcher)
    /// and transactional queries (using a transaction-scoped fetcher).
    async fn execute_query_internal(
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
        self.validate_select(select, collection)?;

        // Check if this query has nested selections (relations)
        let has_nested = select
            .fields
            .iter()
            .any(|f| matches!(f, Requestable::Select(_)));

        if has_nested {
            // Use the Planner for queries with nested selections (joins)
            // Note: ACP filtering for nested queries is not yet implemented
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
    /// Note: ACP filtering for nested queries is not yet implemented.
    /// The identity parameter is accepted for future use.
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
        let mapping = self.build_mapping(select, collection)?;

        // Convert storage documents to plan docs
        let plan_docs = documents_to_plan_docs(&docs, &mapping)?;

        // Build and execute the plan
        let mut plan = self.build_plan(select, plan_docs, mapping.clone())?;

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

    /// Execute a GraphQL mutation and return JSON results.
    ///
    /// Requires a mutator to be configured via `with_mutator()`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let runner = QueryRunner::new(fetcher, collections)
    ///     .with_mutator(mutator);
    ///
    /// let result = runner.execute_mutation(r#"
    ///     mutation {
    ///         create_Users(input: [{name: "Alice", age: 30}]) {
    ///             _docID
    ///             name
    ///         }
    ///     }
    /// "#).await?;
    /// ```
    pub async fn execute_mutation(&self, mutation_str: &str) -> Result<JsonValue> {
        self.execute_mutation_with_identity(mutation_str, None)
            .await
    }

    /// Execute a GraphQL mutation with identity for ACP permission checks.
    ///
    /// For collections with ACP policies:
    /// - CREATE: Registers created documents with caller_identity as owner (if caller_identity provided)
    /// - UPDATE: Checks caller_identity has updater permission on each document
    /// - DELETE: Checks caller_identity has deleter permission on each document
    pub async fn execute_mutation_with_identity(
        &self,
        mutation_str: &str,
        caller_identity: Option<Did>,
    ) -> Result<JsonValue> {
        let mutator = self.mutator.as_ref().ok_or_else(|| {
            QueryError::execution("mutations require a mutator; call with_mutator() first")
        })?;

        self.execute_mutation_internal(mutation_str, mutator.clone(), caller_identity)
            .await
    }

    /// Execute a GraphQL mutation with a specific mutator and caller_identity.
    async fn execute_mutation_internal(
        &self,
        mutation_str: &str,
        mutator: Arc<dyn DocMutator>,
        caller_identity: Option<Did>,
    ) -> Result<JsonValue> {
        let mutations = parse_mutations(mutation_str)?;

        let mut results = Map::new();

        for mutation in mutations {
            let result = self
                .execute_single_mutation(&mutation, mutator.clone(), caller_identity.clone())
                .await?;
            // Use collection name as key (Go behavior)
            results.insert(mutation.collection_name.clone(), result);
        }

        Ok(JsonValue::Object(results))
    }

    /// Execute a single mutation operation with ACP enforcement.
    async fn execute_single_mutation(
        &self,
        mutation: &Mutation,
        mutator: Arc<dyn DocMutator>,
        caller_identity: Option<Did>,
    ) -> Result<JsonValue> {
        use acp::DocumentPermission;

        // Validate collection exists
        let collection = self
            .collections
            .get(&mutation.collection_name)
            .ok_or_else(|| QueryError::collection_not_found(&mutation.collection_name))?;

        // Build document mapping from requested fields
        let mapping = self.build_mutation_mapping(mutation)?;

        // Resolve filter to doc_ids if filter is provided without doc_ids
        let resolved_doc_ids = self.resolve_filter_to_doc_ids(mutation).await?;

        // Get doc_ids for permission checking (UPDATE/DELETE need this)
        let doc_ids_for_check = resolved_doc_ids
            .as_ref()
            .or(mutation.doc_ids.as_ref())
            .cloned();

        // Check ACP permissions for UPDATE/DELETE operations
        if let (Some(ref acp), Some(ref policy)) = (&self.acp, &collection.policy) {
            match mutation.mutation_type {
                MutationType::Update | MutationType::Upsert => {
                    // For UPDATE, check updater permission on each target doc
                    if let Some(ref doc_ids) = doc_ids_for_check {
                        let identity_for_acp = Identity::from(identity.as_ref());
                        for doc_id in doc_ids {
                            let has_permission = acp
                                .check_doc_access(
                                    &identity_for_acp,
                                    DocumentPermission::Update,
                                    &policy.id,
                                    &policy.resource_name,
                                    doc_id,
                                )
                                .await
                                .unwrap_or_else(|e| {
                                    tracing::warn!(
                                        doc_id = %doc_id,
                                        identity = %identity_for_acp,
                                        error = %e,
                                        "ACP permission check failed during UPDATE, denying access"
                                    );
                                    false // Fail-closed on errors
                                });

                            if !has_permission {
                                return Err(QueryError::permission_denied(format!(
                                    "caller_identity does not have update permission on document '{}'",
                                    doc_id
                                )));
                            }
                        }
                    } else {
                        // No doc_ids to check - this means the mutation has no targets
                        // Log this as it may indicate a logic issue
                        tracing::debug!(
                            collection = %mutation.collection_name,
                            "UPDATE mutation has no doc_ids for ACP permission check - no documents will be affected"
                        );
                    }
                }
                MutationType::Delete => {
                    // For DELETE, check deleter permission on each target doc
                    if let Some(ref doc_ids) = doc_ids_for_check {
                        let identity_for_acp = Identity::from(identity.as_ref());
                        for doc_id in doc_ids {
                            let has_permission = acp
                                .check_doc_access(
                                    &identity_for_acp,
                                    DocumentPermission::Delete,
                                    &policy.id,
                                    &policy.resource_name,
                                    doc_id,
                                )
                                .await
                                .unwrap_or_else(|e| {
                                    tracing::warn!(
                                        doc_id = %doc_id,
                                        identity = %identity_for_acp,
                                        error = %e,
                                        "ACP permission check failed during DELETE, denying access"
                                    );
                                    false // Fail-closed on errors
                                });

                            if !has_permission {
                                return Err(QueryError::permission_denied(format!(
                                    "caller_identity does not have delete permission on document '{}'",
                                    doc_id
                                )));
                            }
                        }
                    } else {
                        // No doc_ids to check - this means the mutation has no targets
                        tracing::debug!(
                            collection = %mutation.collection_name,
                            "DELETE mutation has no doc_ids for ACP permission check - no documents will be affected"
                        );
                    }
                }
                MutationType::Create => {
                    // CREATE permission is checked implicitly - anyone can create
                    // but ownership is established via registration
                }
            }
        }

        // Build and execute the appropriate mutation plan
        let mut plan: Box<dyn PlanNode> = match mutation.mutation_type {
            MutationType::Create => {
                let inputs = self.build_create_inputs(mutation)?;
                Box::new(
                    CreateNode::new(&mutation.collection_name, mutator, mapping.clone())
                        .with_inputs(inputs),
                )
            }
            MutationType::Update => {
                let input = self.build_update_input(mutation)?;
                let mut node = UpdateNode::new(&mutation.collection_name, mutator, mapping.clone())
                    .with_input(input);

                // Use resolved doc_ids (from filter) or original doc_ids
                if let Some(ref doc_ids) = resolved_doc_ids {
                    node = node.with_doc_ids(doc_ids.clone());
                } else if let Some(ref doc_ids) = mutation.doc_ids {
                    node = node.with_doc_ids(doc_ids.clone());
                }

                Box::new(node)
            }
            MutationType::Delete => {
                let mut node = DeleteNode::new(&mutation.collection_name, mutator, mapping.clone());

                // Use resolved doc_ids (from filter) or original doc_ids
                if let Some(ref doc_ids) = resolved_doc_ids {
                    node = node.with_doc_ids(doc_ids.clone());
                } else if let Some(ref doc_ids) = mutation.doc_ids {
                    node = node.with_doc_ids(doc_ids.clone());
                }

                Box::new(node)
            }
            MutationType::Upsert => {
                let input = self.build_upsert_input(mutation)?;
                let mut node = UpsertNode::new(&mutation.collection_name, mutator, mapping.clone())
                    .with_input(input);

                // Use resolved doc_ids (from filter) or original doc_ids
                if let Some(ref doc_ids) = resolved_doc_ids {
                    node = node.with_doc_ids(doc_ids.clone());
                } else if let Some(ref doc_ids) = mutation.doc_ids {
                    node = node.with_doc_ids(doc_ids.clone());
                }

                Box::new(node)
            }
        };

        // Execute the plan
        plan.init().await?;
        plan.start().await?;

        let mut results = Vec::new();

        while plan.next().await? {
            let doc = plan.value();
            let json = self.doc_to_json(doc, &mapping)?;
            results.push(json);
        }

        plan.close().await?;

        // For CREATE/UPSERT operations with caller_identity: register created docs with ACP
        if matches!(
            mutation.mutation_type,
            MutationType::Create | MutationType::Upsert
        ) {
            if let (Some(ref acp), Some(ref policy), Some(ref identity_did)) =
                (&self.acp, &collection.policy, &caller_identity)
            {
                for result in &results {
                    if let Some(doc_id) = result.get("_docID").and_then(|v| v.as_str()) {
                        // Check if document is already registered (for upsert of existing doc)
                        let is_registered = acp
                            .is_doc_registered(&policy.id, &policy.resource_name, doc_id)
                            .await
                            .unwrap_or_else(|e| {
                                tracing::warn!(
                                    doc_id = %doc_id,
                                    error = %e,
                                    "Failed to check document registration status - assuming unregistered"
                                );
                                false
                            });

                        // Only register if not already registered (new document)
                        if !is_registered {
                            // Register the document with the creator as owner
                            // CRITICAL: Registration failure must fail the mutation to prevent
                            // documents from being created without proper access control
                            acp.register_doc_object(
                                identity_did,
                                &policy.id,
                                &policy.resource_name,
                                doc_id,
                            )
                            .await
                            .map_err(|e| {
                                tracing::error!(
                                    doc_id = %doc_id,
                                    error = %e,
                                    "Failed to register document with ACP - aborting mutation"
                                );
                                QueryError::acp_registration_failed(doc_id, e)
                            })?;
                        }
                    }
                }
            }
        }

        Ok(JsonValue::Array(results))
    }

    /// Resolve a filter to document IDs by querying the collection.
    ///
    /// This is used for filter-based mutations where we need to first
    /// find matching documents, then perform the mutation on them.
    async fn resolve_filter_to_doc_ids(&self, mutation: &Mutation) -> Result<Option<Vec<String>>> {
        // Only resolve if there's a filter but no explicit doc_ids
        let filter = match (&mutation.filter, &mutation.doc_ids) {
            (Some(filter), None) => filter,
            _ => return Ok(None),
        };

        // Get the collection schema to build a mapping
        let collection = self
            .collections
            .get(&mutation.collection_name)
            .ok_or_else(|| QueryError::collection_not_found(&mutation.collection_name))?;

        // Build mapping from collection schema
        let mut mapping = DocumentMapping::new();
        for (i, field) in collection.fields.iter().enumerate() {
            mapping.add(i, &field.name);
        }

        // Get all documents from the collection
        let all_docs = self.fetcher.get_all(&mutation.collection_name).await?;

        // Apply filter to find matching documents
        let mut matching_ids = Vec::new();
        for doc in &all_docs {
            // Convert Document to fields array for filter matching
            let plan_doc = document_to_plan_doc(doc, &mapping)?;
            let fields = plan_doc.fields();

            if filter.matches(fields, &mapping)? {
                if let Some(id) = doc.id() {
                    matching_ids.push(id.to_string());
                }
            }
        }

        Ok(Some(matching_ids))
    }

    /// Build document mapping for mutation result fields.
    fn build_mutation_mapping(&self, mutation: &Mutation) -> Result<DocumentMapping> {
        let mut mapping = DocumentMapping::new();

        // Add requested fields
        for field in mutation.requested_fields() {
            let index = mapping.next_index();
            mapping.add(index, &field.name);
            mapping.add_render_key(index, field.output_name());
        }

        // If no fields specified, at minimum return _docID
        if mapping.next_index() == 0 {
            mapping.add(0, "_docID");
            mapping.add_render_key(0, "_docID");
        }

        Ok(mapping)
    }

    /// Build CreateInput objects from mutation input.
    fn build_create_inputs(&self, mutation: &Mutation) -> Result<Vec<CreateInput>> {
        let mut inputs = Vec::new();

        for doc_input in &mutation.create_input {
            let mut create_input = CreateInput::new();
            for (field_name, value) in doc_input {
                create_input = create_input.with_field(field_name.clone(), value.clone());
            }
            inputs.push(create_input);
        }

        Ok(inputs)
    }

    /// Build UpdateInput from mutation input.
    fn build_update_input(&self, mutation: &Mutation) -> Result<UpdateInput> {
        let mut update_input = UpdateInput::new();

        for (field_name, value) in &mutation.update_input {
            update_input = update_input.with_field(field_name.clone(), value.clone());
        }

        Ok(update_input)
    }

    /// Build UpsertInput from mutation input.
    fn build_upsert_input(&self, mutation: &Mutation) -> Result<UpsertInput> {
        let mut upsert_input = UpsertInput::new();

        // Upsert uses update_input for the field values
        for (field_name, value) in &mutation.update_input {
            upsert_input = upsert_input.with_field(field_name.clone(), value.clone());
        }

        Ok(upsert_input)
    }

    /// Validate that the select doesn't use unsupported features.
    fn validate_select(&self, select: &Select, collection: &CollectionVersion) -> Result<()> {
        if select.cid.is_some() {
            return Err(QueryError::execution(
                "CID-based queries are not yet implemented; remove the 'cid' argument",
            ));
        }

        // Note: Nested selections (relations) are now supported via the Planner

        // Helper to check if a field exists in the collection schema
        let field_exists = |name: &str| -> bool {
            name == "_docID" || collection.fields.iter().any(|f| f.name == name)
        };

        // Validate aggregate target fields exist in schema
        for requestable in &select.fields {
            if let Requestable::Aggregate(agg) = requestable {
                for target in &agg.targets {
                    if let Some(ref field_name) = target.field_name {
                        if !field_exists(field_name) {
                            return Err(QueryError::unknown_field(format!(
                                "aggregate target field '{}' not found in collection '{}'",
                                field_name, select.collection_name
                            )));
                        }
                    }
                }
            }
        }

        // Validate GROUP BY fields exist in schema
        if let Some(ref group_by) = select.group_by {
            for field_name in &group_by.fields {
                if !field_exists(field_name) {
                    return Err(QueryError::unknown_field(format!(
                        "GROUP BY field '{}' not found in collection '{}'",
                        field_name, select.collection_name
                    )));
                }
            }
        }

        Ok(())
    }

    /// Build the document mapping for a select operation.
    fn build_mapping(
        &self,
        select: &Select,
        collection: &CollectionVersion,
    ) -> Result<DocumentMapping> {
        let mut mapping = DocumentMapping::new();

        // Add requested fields and aggregates
        for requestable in &select.fields {
            match requestable {
                Requestable::Field(field) => {
                    let index = mapping.next_index();
                    mapping.add(index, &field.name);
                    mapping.add_render_key(index, field.output_name());
                }
                Requestable::Aggregate(agg) => {
                    let index = mapping.next_index();
                    let name = agg.aggregate_type.as_str();
                    mapping.add(index, name);
                    // Use alias if provided, otherwise use the aggregate name
                    mapping.add_render_key(index, agg.output_name());
                }
                Requestable::Select(nested) => {
                    // This code path should not be reached - nested selections should
                    // be routed to execute_nested_select_with_planner. If we get here,
                    // it indicates a bug in query routing.
                    return Err(QueryError::internal(format!(
                        "Unexpected nested select '{}' in simple query path - \
                         this indicates a bug in query routing",
                        nested.field.name
                    )));
                }
            }
        }

        // Add fields referenced by the filter (but not selected)
        // These are needed for filter evaluation but won't be rendered
        if let Some(ref filter) = select.filter {
            for field_name in filter.referenced_fields() {
                if mapping.first_index_of_name(&field_name).is_none() {
                    let index = mapping.next_index();
                    mapping.add(index, &field_name);
                    // Don't add render_key - we don't want to output these fields
                }
            }
        }

        // Add GROUP BY fields (they need to be in mapping for grouping)
        if let Some(ref group_by) = select.group_by {
            for field_name in &group_by.fields {
                if mapping.first_index_of_name(field_name).is_none() {
                    let index = mapping.next_index();
                    mapping.add(index, field_name);
                    // Don't add render_key - they may or may not be selected
                }
            }
        }

        // Add aggregate target fields (needed for aggregation but not rendered)
        for requestable in &select.fields {
            if let Requestable::Aggregate(agg) = requestable {
                for target in &agg.targets {
                    if let Some(ref field_name) = target.field_name {
                        if mapping.first_index_of_name(field_name).is_none() {
                            let index = mapping.next_index();
                            mapping.add(index, field_name);
                            // Don't add render_key - we don't want to output these fields
                        }
                    }
                }
            }
        }

        // If no fields specified, add all from collection
        if mapping.next_index() == 0 {
            for (i, field) in collection.fields.iter().enumerate() {
                mapping.add(i, &field.name);
                mapping.add_render_key(i, &field.name);
            }
        }

        Ok(mapping)
    }

    /// Build a plan tree from a Select operation and documents.
    fn build_plan(
        &self,
        select: &Select,
        docs: Vec<Doc>,
        mapping: DocumentMapping,
    ) -> Result<Box<dyn PlanNode>> {
        let collection = self
            .collections
            .get(&select.collection_name)
            .ok_or_else(|| QueryError::collection_not_found(&select.collection_name))?;

        // Create ScanNode with preloaded documents
        let scan = ScanNode::new((**collection).clone(), mapping.clone())
            .with_docs(docs)
            .with_show_deleted(select.show_deleted);

        let mut plan: Box<dyn PlanNode> = Box::new(scan);

        // Add SelectNode for filtering
        if let Some(ref filter) = select.filter {
            let select_node = SelectNode::new(plan, mapping.clone()).with_filter(filter.clone());
            plan = Box::new(select_node);
        }

        // Check if we have GROUP BY
        let has_group_by = select.group_by.is_some();

        if has_group_by {
            // WITH GROUP BY: GroupByNode → Aggregates → OrderBy → Limit

            // Add GroupByNode
            if let Some(ref group_by) = select.group_by {
                plan = Box::new(GroupByNode::new(plan, group_by.clone(), mapping.clone()));
            }

            // Add aggregate nodes
            plan = Self::add_aggregate_nodes(plan, select, &mapping)?;

            // Add OrderByNode for sorting (after grouping/aggregation)
            if let Some(ref order_by) = select.order_by {
                plan = Box::new(OrderByNode::new(plan, order_by.clone(), mapping.clone()));
            }

            // Add LimitNode
            if let Some(ref limit) = select.limit {
                plan = Box::new(LimitNode::new(plan, limit.limit, limit.offset));
            }
        } else {
            // WITHOUT GROUP BY: OrderBy → Limit → [AllDocs if multiple aggs] → Aggregates

            // Add OrderByNode for sorting (after filtering, before limit)
            if let Some(ref order_by) = select.order_by {
                plan = Box::new(OrderByNode::new(plan, order_by.clone(), mapping.clone()));
            }

            // Add LimitNode
            if let Some(ref limit) = select.limit {
                plan = Box::new(LimitNode::new(plan, limit.limit, limit.offset));
            }

            // Count aggregates to determine if we need AllDocsNode
            let aggregate_count = select
                .fields
                .iter()
                .filter(|f| matches!(f, Requestable::Aggregate(_)))
                .count();

            // If there are multiple aggregates, wrap in AllDocsNode so they all
            // can access the original documents via current_group_docs()
            if aggregate_count > 1 {
                plan = Box::new(AllDocsNode::new(plan, mapping.clone()));
            }

            // Add aggregate nodes
            // Without GROUP BY, aggregates return a single row for the entire result
            plan = Self::add_aggregate_nodes(plan, select, &mapping)?;
        }

        Ok(plan)
    }

    /// Add aggregate nodes to the plan based on the select's aggregate fields.
    fn add_aggregate_nodes(
        mut plan: Box<dyn PlanNode>,
        select: &Select,
        mapping: &DocumentMapping,
    ) -> Result<Box<dyn PlanNode>> {
        for field in &select.fields {
            if let Requestable::Aggregate(agg) = field {
                // Get the index where the aggregate result should be stored
                // Use the aggregate type name for lookup (that's how it's registered in mapping)
                let agg_type_name = agg.aggregate_type.as_str();
                let agg_index = mapping.first_index_of_name(agg_type_name).ok_or_else(|| {
                    QueryError::internal(format!(
                        "aggregate '{}' not found in document mapping - this is a bug",
                        agg_type_name
                    ))
                })?;

                // For aggregates that operate on a field, get the field index
                let field_index = if !agg.targets.is_empty() && agg.targets[0].field_name.is_some()
                {
                    let target_field = agg.targets[0].field_name.as_ref().unwrap();
                    mapping.first_index_of_name(target_field).ok_or_else(|| {
                        QueryError::execution(format!(
                            "aggregate target field '{}' not found in mapping",
                            target_field
                        ))
                    })?
                } else {
                    0 // Not used for count
                };

                match agg.aggregate_type {
                    AggregateType::Count => {
                        plan = Box::new(CountNode::new(plan, mapping.clone(), agg_index));
                    }
                    AggregateType::Sum => {
                        plan =
                            Box::new(SumNode::new(plan, mapping.clone(), field_index, agg_index));
                    }
                    AggregateType::Average => {
                        plan = Box::new(AverageNode::new(
                            plan,
                            mapping.clone(),
                            field_index,
                            agg_index,
                        ));
                    }
                    AggregateType::Min => {
                        plan =
                            Box::new(MinNode::new(plan, mapping.clone(), field_index, agg_index));
                    }
                    AggregateType::Max => {
                        plan =
                            Box::new(MaxNode::new(plan, mapping.clone(), field_index, agg_index));
                    }
                }
            }
        }
        Ok(plan)
    }

    /// Convert a plan Doc to JSON for output.
    fn doc_to_json(&self, doc: &Doc, mapping: &DocumentMapping) -> Result<JsonValue> {
        let mut obj = Map::new();

        for render_key in &mapping.render_keys {
            let value = doc
                .fields()
                .get(render_key.index)
                .cloned()
                .flatten()
                .unwrap_or(JsonValue::Null);
            obj.insert(render_key.key.clone(), value);
        }

        Ok(JsonValue::Object(obj))
    }

    /// Get the names of all collections.
    ///
    /// Returns a sorted list of collection names registered with this runner.
    pub fn collection_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.collections.keys().cloned().collect();
        names.sort();
        names
    }

    /// Check if a collection exists.
    pub fn has_collection(&self, name: &str) -> bool {
        self.collections.contains_key(name)
    }
}

#[async_trait]
impl<F: DocFetcher, R: TransactionRegistry> QueryExecutor for QueryRunner<F, R> {
    async fn execute(&self, request: QueryRequest) -> QueryResponse {
        // First, parse the request to determine if it's a query or mutation
        let parsed = match parse_request(&request.query) {
            Ok(p) => p,
            Err(e) => {
                return QueryResponse {
                    data: None,
                    errors: vec![QueryResponseError {
                        message: format!("parse error: {}", e),
                        path: None,
                        locations: None,
                    }],
                };
            }
        };

        // Resolve effective identity: request identity takes precedence over default
        let identity = self.resolve_identity(request.identity);

        // Route to appropriate handler based on operation type
        // Pass identity through for ACP permission checks
        let result = match parsed {
            ParsedOperation::Query(_) => {
                self.execute_query_with_identity(&request.query, identity)
                    .await
            }
            ParsedOperation::Mutation(_) => {
                self.execute_mutation_with_identity(&request.query, identity)
                    .await
            }
        };

        match result {
            Ok(data) => QueryResponse {
                data: Some(data),
                errors: vec![],
            },
            Err(e) => {
                tracing::error!(
                    query = %request.query,
                    error = %e,
                    "Query execution failed"
                );
                QueryResponse {
                    data: None,
                    errors: vec![QueryResponseError {
                        message: e.to_string(),
                        path: None,
                        locations: None,
                    }],
                }
            }
        }
    }

    async fn execute_in_txn(
        &self,
        request: QueryRequest,
        handle: &TransactionHandle,
    ) -> QueryResponse {
        // Look up the transaction in the registry
        let txn_ctx = match self.registry.get(handle) {
            GetTransactionResult::Found(ctx) => ctx,
            GetTransactionResult::NotFound => {
                return QueryResponse::error(format!(
                    "transaction '{}' not found or has been committed/rolled back",
                    handle
                ));
            }
            GetTransactionResult::LockPoisoned => {
                return QueryResponse::error(format!(
                    "transaction registry lock poisoned - system may be in corrupted state (transaction '{}')",
                    handle
                ));
            }
        };

        // Resolve effective identity: request identity takes precedence over default
        let identity = self.resolve_identity(request.identity);

        // Get the transaction-scoped fetcher and execute with identity for ACP
        let fetcher = txn_ctx.doc_fetcher();
        match self
            .execute_query_internal(&request.query, fetcher.as_ref(), identity)
            .await
        {
            Ok(data) => QueryResponse {
                data: Some(data),
                errors: vec![],
            },
            Err(e) => {
                tracing::error!(
                    query = %request.query,
                    txn_id = %handle,
                    error = %e,
                    "Query execution failed in transaction"
                );
                QueryResponse {
                    data: None,
                    errors: vec![QueryResponseError {
                        message: e.to_string(),
                        path: None,
                        locations: None,
                    }],
                }
            }
        }
    }

    async fn begin_txn(
        &self,
        readonly: bool,
    ) -> std::result::Result<TransactionHandle, TransactionError> {
        self.registry.begin(readonly).await
    }

    async fn commit_txn(
        &self,
        handle: &TransactionHandle,
    ) -> std::result::Result<(), TransactionError> {
        self.registry.commit(handle).await
    }

    async fn rollback_txn(
        &self,
        handle: &TransactionHandle,
    ) -> std::result::Result<(), TransactionError> {
        self.registry.rollback(handle).await
    }

    async fn schema(&self) -> Result<String> {
        let mut schema_str = String::new();
        for collection in self.collections.values() {
            schema_str.push_str(&format!("type {} {{\n", collection.name));
            for field in &collection.fields {
                let gql_type = field.kind.graphql_type_name();
                schema_str.push_str(&format!("  {}: {}\n", field.name, gql_type));
            }
            schema_str.push_str("}\n\n");
        }
        Ok(schema_str)
    }
}

/// Wrapper to convert a `&dyn DocFetcher` reference into an owned `DocFetcher`.
///
/// This allows passing a fetcher reference to the Planner, which requires
/// `Arc<dyn DocFetcher>`. The wrapper is only valid for the duration of the
/// query execution.
///
/// # Safety Invariants
///
/// 1. **Lifetime**: The original `&dyn DocFetcher` reference MUST outlive all uses
///    of this wrapper. The caller is responsible for ensuring this - currently
///    enforced by only creating and using the wrapper within `execute_nested_select_with_planner`.
///
/// 2. **Thread Safety**: The `Send + Sync` implementations are safe because
///    `DocFetcher: Send + Sync` (see fetcher.rs:65), meaning the underlying data
///    can be safely accessed from any thread. The wrapper merely holds a pointer
///    to data that is already thread-safe.
///
/// 3. **Fat Pointer Layout**: The transmute relies on the standard fat pointer layout
///    `(data_ptr, vtable)` for trait objects, which is stable in practice but not
///    formally guaranteed. Consider using `std::ptr::metadata` when it stabilizes
///    for a safer alternative.
struct FetcherWrapper {
    // Store data pointer and vtable separately to avoid lifetime issues with fat pointers
    data_ptr: *const (),
    vtable: *const (),
    // PhantomData to express the logical lifetime relationship, even though
    // we can't enforce it at compile time due to the pointer erasure
    _phantom: PhantomData<*const dyn DocFetcher>,
}

impl FetcherWrapper {
    fn new(fetcher: &dyn DocFetcher) -> Self {
        // Split the fat pointer into data and vtable components.
        // This avoids the lifetime issue with *const dyn Trait.
        let ptr = fetcher as *const dyn DocFetcher;
        let (data_ptr, vtable) =
            unsafe { std::mem::transmute::<*const dyn DocFetcher, (*const (), *const ())>(ptr) };
        Self {
            data_ptr,
            vtable,
            _phantom: PhantomData,
        }
    }

    fn get_fetcher(&self) -> &dyn DocFetcher {
        // Reconstruct the fat pointer from data and vtable
        let ptr = unsafe {
            std::mem::transmute::<(*const (), *const ()), *const dyn DocFetcher>((
                self.data_ptr,
                self.vtable,
            ))
        };
        // SAFETY: The caller guarantees the original reference outlives this wrapper
        unsafe { &*ptr }
    }
}

// SAFETY: These implementations are safe because:
// 1. DocFetcher: Send + Sync (the underlying data is thread-safe)
// 2. The wrapper only holds a pointer to already-thread-safe data
// 3. The lifetime invariant (original ref outlives wrapper) is maintained by the caller
unsafe impl Send for FetcherWrapper {}
unsafe impl Sync for FetcherWrapper {}

#[async_trait]
impl DocFetcher for FetcherWrapper {
    async fn get_all(&self, collection_name: &str) -> Result<Vec<Document>> {
        self.get_fetcher()
            .get_all(collection_name)
            .await
            .map_err(|e| {
                QueryError::execution(format!(
                    "fetcher error during planner execution for collection '{}': {}",
                    collection_name, e
                ))
            })
    }

    async fn get_by_ids(
        &self,
        collection_name: &str,
        doc_ids: &[String],
    ) -> Result<FetchByIdsResult> {
        self.get_fetcher()
            .get_by_ids(collection_name, doc_ids)
            .await
            .map_err(|e| {
                QueryError::execution(format!(
                    "fetcher error during planner execution for collection '{}' (fetching {} doc IDs): {}",
                    collection_name,
                    doc_ids.len(),
                    e
                ))
            })
    }

    async fn get_by_field_value(
        &self,
        collection_name: &str,
        field_name: &str,
        value: &str,
    ) -> Result<Vec<Document>> {
        self.get_fetcher()
            .get_by_field_value(collection_name, field_name, value)
            .await
            .map_err(|e| {
                QueryError::execution(format!(
                    "fetcher error during planner execution for collection '{}' (field lookup {}='{}'): {}",
                    collection_name, field_name, value, e
                ))
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{MockFetcher, MockTxnRegistry};
    use schema::{FieldDescription, FieldKind};

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

    #[tokio::test]
    async fn test_execute_simple_query() {
        let fetcher = MockFetcher::new();

        let mut doc = Document::new();
        doc.set("name", "Alice");
        doc.set("age", 30i64);
        doc.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc);

        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        let result = runner
            .execute_query("{ Users { name age } }")
            .await
            .unwrap();

        assert!(result.is_object());
        let users = result.get("Users").unwrap();
        assert!(users.is_array());
        let arr = users.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0].get("name").unwrap(), "Alice");
        assert_eq!(arr[0].get("age").unwrap(), 30);
    }

    #[tokio::test]
    async fn test_execute_query_with_docid() {
        let fetcher = MockFetcher::new();

        let mut doc = Document::new();
        doc.set("name", "Bob");
        doc.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc);

        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        let result = runner
            .execute_query("{ Users { _docID name } }")
            .await
            .unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 1);
        assert!(users[0].get("_docID").unwrap().is_string());
        assert_eq!(users[0].get("name").unwrap(), "Bob");
    }

    #[tokio::test]
    async fn test_execute_empty_collection() {
        let fetcher = MockFetcher::new();
        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        let result = runner.execute_query("{ Users { name } }").await.unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert!(users.is_empty());
    }

    #[tokio::test]
    async fn test_execute_unknown_collection() {
        let fetcher = MockFetcher::new();
        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        let result = runner.execute_query("{ Posts { title } }").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_execute_with_limit() {
        let fetcher = MockFetcher::new();

        for i in 0..5 {
            let mut doc = Document::new();
            doc.set("name", format!("User{}", i));
            doc.set("age", i as i64);
            doc.generate_and_set_doc_id().unwrap();
            fetcher.add_doc("Users", doc);
        }

        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        let result = runner
            .execute_query("{ Users(limit: 2) { name } }")
            .await
            .unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 2);
    }

    #[tokio::test]
    async fn test_query_executor_trait() {
        let fetcher = MockFetcher::new();

        let mut doc = Document::new();
        doc.set("name", "Alice");
        doc.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc);

        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        let request = QueryRequest::new("{ Users { name } }");

        let response = runner.execute(request).await;

        assert!(response.errors.is_empty());
        assert!(response.data.is_some());
    }

    /// Mock fetcher that returns errors
    struct FailingFetcher;

    #[async_trait]
    impl DocFetcher for FailingFetcher {
        async fn get_all(&self, _collection_name: &str) -> Result<Vec<Document>> {
            Err(QueryError::execution("storage failure"))
        }

        async fn get_by_ids(
            &self,
            _collection_name: &str,
            _doc_ids: &[String],
        ) -> Result<FetchByIdsResult> {
            Err(QueryError::execution("storage failure"))
        }

        async fn get_by_field_value(
            &self,
            _collection_name: &str,
            _field_name: &str,
            _value: &str,
        ) -> Result<Vec<Document>> {
            Err(QueryError::execution("storage failure"))
        }
    }

    #[tokio::test]
    async fn test_fetcher_error_propagates() {
        let runner = QueryRunner::new(FailingFetcher, vec![make_test_collection()]);

        let result = runner.execute_query("{ Users { name } }").await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("storage failure"));
    }

    #[tokio::test]
    async fn test_query_executor_error_response_format() {
        let fetcher = MockFetcher::new();
        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        let request = QueryRequest::new("{ InvalidCollection { name } }");

        let response = runner.execute(request).await;

        assert!(response.data.is_none());
        assert_eq!(response.errors.len(), 1);
        assert!(response.errors[0].message.contains("collection not found"));
    }

    #[tokio::test]
    async fn test_execute_with_offset() {
        let fetcher = MockFetcher::new();

        for i in 0..5 {
            let mut doc = Document::new();
            doc.set("name", format!("User{}", i));
            doc.set("age", i as i64);
            doc.generate_and_set_doc_id().unwrap();
            fetcher.add_doc("Users", doc);
        }

        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        let result = runner
            .execute_query("{ Users(offset: 2) { name } }")
            .await
            .unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 3);
    }

    #[tokio::test]
    async fn test_execute_with_limit_and_offset() {
        let fetcher = MockFetcher::new();

        for i in 0..10 {
            let mut doc = Document::new();
            doc.set("name", format!("User{}", i));
            doc.generate_and_set_doc_id().unwrap();
            fetcher.add_doc("Users", doc);
        }

        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        let result = runner
            .execute_query("{ Users(limit: 3, offset: 2) { name } }")
            .await
            .unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 3);
    }

    #[tokio::test]
    async fn test_execute_query_with_doc_ids() {
        let fetcher = MockFetcher::new();

        let mut doc1 = Document::new();
        doc1.set("name", "Alice");
        doc1.generate_and_set_doc_id().unwrap();
        let doc1_id = doc1.id().unwrap().to_string();
        fetcher.add_doc("Users", doc1);

        let mut doc2 = Document::new();
        doc2.set("name", "Bob");
        doc2.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc2);

        let mut doc3 = Document::new();
        doc3.set("name", "Charlie");
        doc3.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc3);

        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        let query = format!(r#"{{ Users(docIDs: ["{}"]) {{ name }} }}"#, doc1_id);
        let result = runner.execute_query(&query).await.unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].get("name").unwrap(), "Alice");
    }

    #[tokio::test]
    async fn test_unknown_collection_error() {
        let fetcher = MockFetcher::new();
        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        let result = runner.execute_query("{ Posts { title } }").await;

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("collection not found: Posts"));
    }

    #[tokio::test]
    async fn test_order_by_single_field_asc() {
        let fetcher = MockFetcher::new();

        let mut doc1 = Document::new();
        doc1.set("name", "Charlie");
        doc1.set("age", 35i64);
        doc1.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc1);

        let mut doc2 = Document::new();
        doc2.set("name", "Alice");
        doc2.set("age", 25i64);
        doc2.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc2);

        let mut doc3 = Document::new();
        doc3.set("name", "Bob");
        doc3.set("age", 30i64);
        doc3.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc3);

        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        let result = runner
            .execute_query("{ Users(order: {name: ASC}) { name } }")
            .await
            .unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 3);
        assert_eq!(users[0].get("name").unwrap(), "Alice");
        assert_eq!(users[1].get("name").unwrap(), "Bob");
        assert_eq!(users[2].get("name").unwrap(), "Charlie");
    }

    #[tokio::test]
    async fn test_order_by_single_field_desc() {
        let fetcher = MockFetcher::new();

        let mut doc1 = Document::new();
        doc1.set("name", "Alice");
        doc1.set("age", 25i64);
        doc1.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc1);

        let mut doc2 = Document::new();
        doc2.set("name", "Charlie");
        doc2.set("age", 35i64);
        doc2.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc2);

        let mut doc3 = Document::new();
        doc3.set("name", "Bob");
        doc3.set("age", 30i64);
        doc3.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc3);

        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        let result = runner
            .execute_query("{ Users(order: {name: DESC}) { name } }")
            .await
            .unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 3);
        assert_eq!(users[0].get("name").unwrap(), "Charlie");
        assert_eq!(users[1].get("name").unwrap(), "Bob");
        assert_eq!(users[2].get("name").unwrap(), "Alice");
    }

    #[tokio::test]
    async fn test_order_by_numeric_field() {
        let fetcher = MockFetcher::new();

        let mut doc1 = Document::new();
        doc1.set("name", "Alice");
        doc1.set("age", 30i64);
        doc1.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc1);

        let mut doc2 = Document::new();
        doc2.set("name", "Bob");
        doc2.set("age", 25i64);
        doc2.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc2);

        let mut doc3 = Document::new();
        doc3.set("name", "Charlie");
        doc3.set("age", 35i64);
        doc3.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc3);

        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        let result = runner
            .execute_query("{ Users(order: {age: ASC}) { name age } }")
            .await
            .unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 3);
        assert_eq!(users[0].get("name").unwrap(), "Bob"); // age 25
        assert_eq!(users[1].get("name").unwrap(), "Alice"); // age 30
        assert_eq!(users[2].get("name").unwrap(), "Charlie"); // age 35
    }

    #[tokio::test]
    async fn test_order_by_with_limit() {
        let fetcher = MockFetcher::new();

        for i in 0..10 {
            let mut doc = Document::new();
            doc.set("name", format!("User{}", i));
            doc.set("age", (100 - i) as i64); // age: 100, 99, 98, ...
            doc.generate_and_set_doc_id().unwrap();
            fetcher.add_doc("Users", doc);
        }

        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        // Order by age ASC (91, 92, ..., 100), then limit to 3
        let result = runner
            .execute_query("{ Users(order: {age: ASC}, limit: 3) { name age } }")
            .await
            .unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 3);
        // Lowest ages first
        assert_eq!(users[0].get("age").unwrap(), 91);
        assert_eq!(users[1].get("age").unwrap(), 92);
        assert_eq!(users[2].get("age").unwrap(), 93);
    }

    #[tokio::test]
    async fn test_order_by_with_filter() {
        let fetcher = MockFetcher::new();

        let mut doc1 = Document::new();
        doc1.set("name", "Alice");
        doc1.set("age", 25i64);
        doc1.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc1);

        let mut doc2 = Document::new();
        doc2.set("name", "Bob");
        doc2.set("age", 35i64);
        doc2.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc2);

        let mut doc3 = Document::new();
        doc3.set("name", "Charlie");
        doc3.set("age", 30i64);
        doc3.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc3);

        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        // Filter age >= 30, then order by name ASC
        let result = runner
            .execute_query(
                r#"{ Users(filter: {age: {_gte: 30}}, order: {name: ASC}) { name age } }"#,
            )
            .await
            .unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 2); // Only Bob and Charlie
        assert_eq!(users[0].get("name").unwrap(), "Bob"); // B before C
        assert_eq!(users[1].get("name").unwrap(), "Charlie");
    }

    #[tokio::test]
    async fn test_group_by_single_field() {
        let fetcher = MockFetcher::new();

        // Add test documents with same names (will group together)
        let mut doc1 = Document::new();
        doc1.set("name", "Alice");
        doc1.set("age", 30i64);
        doc1.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc1);

        let mut doc2 = Document::new();
        doc2.set("name", "Bob");
        doc2.set("age", 25i64);
        doc2.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc2);

        let mut doc3 = Document::new();
        doc3.set("name", "Alice");
        doc3.set("age", 35i64);
        doc3.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc3);

        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        let result = runner
            .execute_query("{ Users(groupBy: [name]) { name } }")
            .await
            .unwrap();

        let users = result["Users"].as_array().unwrap();
        // Should get 2 groups: Alice and Bob
        assert_eq!(users.len(), 2);

        let names: Vec<&str> = users.iter().map(|u| u["name"].as_str().unwrap()).collect();
        assert!(names.contains(&"Alice"));
        assert!(names.contains(&"Bob"));
    }

    #[tokio::test]
    async fn test_group_by_with_count() {
        let fetcher = MockFetcher::new();

        // Add test documents: 2 in Engineering, 2 in Sales
        let mut doc1 = Document::new();
        doc1.set("name", "Alice");
        doc1.set("age", 30i64);
        doc1.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc1);

        let mut doc2 = Document::new();
        doc2.set("name", "Bob");
        doc2.set("age", 25i64);
        doc2.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc2);

        let mut doc3 = Document::new();
        doc3.set("name", "Alice");
        doc3.set("age", 35i64);
        doc3.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc3);

        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        let result = runner
            .execute_query("{ Users(groupBy: [name]) { name _count } }")
            .await
            .unwrap();

        let users = result["Users"].as_array().unwrap();
        // Should get 2 groups: Alice (count 2) and Bob (count 1)
        assert_eq!(users.len(), 2);

        // Find Alice group and verify count
        let alice = users
            .iter()
            .find(|u| u["name"].as_str() == Some("Alice"))
            .unwrap();
        assert_eq!(alice["_count"].as_i64(), Some(2));

        // Find Bob group and verify count
        let bob = users
            .iter()
            .find(|u| u["name"].as_str() == Some("Bob"))
            .unwrap();
        assert_eq!(bob["_count"].as_i64(), Some(1));
    }

    #[tokio::test]
    async fn test_nested_selection_with_missing_relation_field() {
        // Nested selections are now supported via the Planner.
        // This test verifies that a query with nested selections fails gracefully
        // when the relation field doesn't exist in the schema.
        let fetcher = MockFetcher::new();
        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        let result = runner
            .execute_query("{ Users { name posts { title } } }")
            .await;

        // Should fail because 'posts' is not a field in the Users collection
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("unknown field") || err.contains("not found"),
            "Expected field-not-found error, got: {}",
            err
        );
    }

    #[tokio::test]
    async fn test_execute_in_txn_without_registry_returns_error() {
        let fetcher = MockFetcher::new();
        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        let request = QueryRequest::new("{ Users { name } }");
        let handle: TransactionHandle = "txn-123".parse().unwrap();
        let response = runner.execute_in_txn(request, &handle).await;

        // Without a proper registry, transactions are not found
        assert!(response.has_errors());
        assert!(response.errors[0].message.contains("txn-123"));
        assert!(response.errors[0].message.contains("not found"));
    }

    #[tokio::test]
    async fn test_execute_query_with_filter() {
        let fetcher = MockFetcher::new();

        let mut doc1 = Document::new();
        doc1.set("name", "Alice");
        doc1.set("age", 30i64);
        doc1.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc1);

        let mut doc2 = Document::new();
        doc2.set("name", "Bob");
        doc2.set("age", 25i64);
        doc2.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc2);

        let mut doc3 = Document::new();
        doc3.set("name", "Charlie");
        doc3.set("age", 35i64);
        doc3.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc3);

        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        let result = runner
            .execute_query(r#"{ Users(filter: {age: {_gte: 30}}) { name age } }"#)
            .await
            .unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 2);

        let names: Vec<&str> = users
            .iter()
            .map(|u| u.get("name").unwrap().as_str().unwrap())
            .collect();
        assert!(names.contains(&"Alice"));
        assert!(names.contains(&"Charlie"));
        assert!(!names.contains(&"Bob"));
    }

    #[tokio::test]
    async fn test_field_alias_in_output() {
        let fetcher = MockFetcher::new();

        let mut doc = Document::new();
        doc.set("name", "Alice");
        doc.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc);

        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        let result = runner
            .execute_query("{ Users { userName: name } }")
            .await
            .unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 1);
        assert!(users[0].get("userName").is_some());
        assert!(users[0].get("name").is_none());
        assert_eq!(users[0].get("userName").unwrap(), "Alice");
    }

    #[tokio::test]
    async fn test_collection_alias_in_output() {
        let fetcher = MockFetcher::new();

        let mut doc = Document::new();
        doc.set("name", "Alice");
        doc.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc);

        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        let result = runner
            .execute_query("{ allUsers: Users { name } }")
            .await
            .unwrap();

        assert!(result.get("allUsers").is_some());
        assert!(result.get("Users").is_none());
        let users = result.get("allUsers").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 1);
    }

    #[tokio::test]
    async fn test_schema_generation() {
        let fetcher = MockFetcher::new();
        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        let schema = runner.schema().await.unwrap();

        assert!(schema.contains("type Users"));
        assert!(schema.contains("_docID: ID"));
        assert!(schema.contains("name: String"));
        assert!(schema.contains("age: Int"));
    }

    // Transaction support tests

    #[tokio::test]
    async fn test_begin_txn() {
        let fetcher = MockFetcher::new();
        let registry = MockTxnRegistry::new(MockFetcher::new());
        let runner = QueryRunner::with_registry(fetcher, vec![make_test_collection()], registry);

        let txn_id = runner.begin_txn(false).await.unwrap();
        assert!(txn_id.starts_with("txn-"));
    }

    #[tokio::test]
    async fn test_begin_and_commit_txn() {
        let fetcher = MockFetcher::new();
        let registry = MockTxnRegistry::new(MockFetcher::new());
        let runner = QueryRunner::with_registry(fetcher, vec![make_test_collection()], registry);

        let txn_id = runner.begin_txn(false).await.unwrap();
        let result = runner.commit_txn(&txn_id).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_begin_and_rollback_txn() {
        let fetcher = MockFetcher::new();
        let registry = MockTxnRegistry::new(MockFetcher::new());
        let runner = QueryRunner::with_registry(fetcher, vec![make_test_collection()], registry);

        let txn_id = runner.begin_txn(false).await.unwrap();
        let result = runner.rollback_txn(&txn_id).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_commit_nonexistent_txn_returns_error() {
        let fetcher = MockFetcher::new();
        let registry = MockTxnRegistry::new(MockFetcher::new());
        let runner = QueryRunner::with_registry(fetcher, vec![make_test_collection()], registry);

        let nonexistent_handle: TransactionHandle = "nonexistent-txn".parse().unwrap();
        let result = runner.commit_txn(&nonexistent_handle).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_execute_in_txn_success() {
        let fetcher = MockFetcher::new();

        // Set up data in the registry's fetcher
        let registry_fetcher = MockFetcher::new();
        let mut doc = Document::new();
        doc.set("name", "TxnAlice");
        doc.set("age", 40i64);
        doc.generate_and_set_doc_id().unwrap();
        registry_fetcher.add_doc("Users", doc);

        let registry = MockTxnRegistry::new(registry_fetcher);
        let runner = QueryRunner::with_registry(fetcher, vec![make_test_collection()], registry);

        // Begin transaction
        let txn_id = runner.begin_txn(false).await.unwrap();

        // Execute query in transaction
        let request = QueryRequest::new("{ Users { name age } }");
        let response = runner.execute_in_txn(request, &txn_id).await;

        assert!(!response.has_errors());
        let data = response.data.unwrap();
        let users = data.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].get("name").unwrap(), "TxnAlice");
        assert_eq!(users[0].get("age").unwrap(), 40);

        // Commit
        runner.commit_txn(&txn_id).await.unwrap();
    }

    #[tokio::test]
    async fn test_execute_in_txn_after_commit_fails() {
        let fetcher = MockFetcher::new();
        let registry = MockTxnRegistry::new(MockFetcher::new());
        let runner = QueryRunner::with_registry(fetcher, vec![make_test_collection()], registry);

        let txn_id = runner.begin_txn(false).await.unwrap();
        runner.commit_txn(&txn_id).await.unwrap();

        // Try to execute after commit
        let request = QueryRequest::new("{ Users { name } }");
        let response = runner.execute_in_txn(request, &txn_id).await;

        assert!(response.has_errors());
        assert!(response.errors[0].message.contains("not found"));
    }

    #[tokio::test]
    async fn test_multiple_queries_in_same_transaction() {
        let registry_fetcher = MockFetcher::new();
        let mut doc1 = Document::new();
        doc1.set("name", "Alice");
        doc1.set("age", 30i64);
        doc1.generate_and_set_doc_id().unwrap();
        registry_fetcher.add_doc("Users", doc1);

        let mut doc2 = Document::new();
        doc2.set("name", "Bob");
        doc2.set("age", 25i64);
        doc2.generate_and_set_doc_id().unwrap();
        registry_fetcher.add_doc("Users", doc2);

        let registry = MockTxnRegistry::new(registry_fetcher);
        let runner =
            QueryRunner::with_registry(MockFetcher::new(), vec![make_test_collection()], registry);

        let txn_id = runner.begin_txn(false).await.unwrap();

        // First query
        let request1 = QueryRequest::new("{ Users { name } }");
        let response1 = runner.execute_in_txn(request1, &txn_id).await;
        assert!(!response1.has_errors());

        // Second query in same transaction
        let request2 = QueryRequest::new("{ Users { age } }");
        let response2 = runner.execute_in_txn(request2, &txn_id).await;
        assert!(!response2.has_errors());

        // Both should see the same data
        let users1 = response1
            .data
            .unwrap()
            .get("Users")
            .unwrap()
            .as_array()
            .unwrap()
            .len();
        let users2 = response2
            .data
            .unwrap()
            .get("Users")
            .unwrap()
            .as_array()
            .unwrap()
            .len();
        assert_eq!(users1, users2);
        assert_eq!(users1, 2);

        runner.commit_txn(&txn_id).await.unwrap();
    }

    #[tokio::test]
    async fn test_begin_txn_without_registry_returns_error() {
        let fetcher = MockFetcher::new();
        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        let result = runner.begin_txn(false).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not supported"));
    }

    #[tokio::test]
    async fn test_query_error_does_not_invalidate_transaction() {
        let registry_fetcher = MockFetcher::new();
        let mut doc = Document::new();
        doc.set("name", "Alice");
        doc.set("age", 30i64);
        doc.generate_and_set_doc_id().unwrap();
        registry_fetcher.add_doc("Users", doc);

        let registry = MockTxnRegistry::new(registry_fetcher);
        let runner =
            QueryRunner::with_registry(MockFetcher::new(), vec![make_test_collection()], registry);

        // Begin transaction
        let txn_id = runner.begin_txn(false).await.unwrap();

        // Execute an invalid query (unknown collection) - should return error response
        let bad_request = QueryRequest::new("{ NonExistentCollection { name } }");
        let bad_response = runner.execute_in_txn(bad_request, &txn_id).await;
        assert!(
            bad_response.has_errors(),
            "Query for unknown collection should fail"
        );
        assert!(
            bad_response.errors[0]
                .message
                .contains("collection not found"),
            "Error should mention collection not found"
        );

        // The transaction should still be valid - execute a good query
        let good_request = QueryRequest::new("{ Users { name } }");
        let good_response = runner.execute_in_txn(good_request, &txn_id).await;
        assert!(
            !good_response.has_errors(),
            "Valid query should succeed after failed query"
        );
        let data = good_response.data.unwrap();
        let users = data.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].get("name").unwrap(), "Alice");

        // Commit should succeed - transaction was not invalidated
        let commit_result = runner.commit_txn(&txn_id).await;
        assert!(
            commit_result.is_ok(),
            "Commit should succeed after query error"
        );
    }

    // Mutation tests

    /// Mock mutator for testing
    struct MockMutator {
        docs: std::sync::Mutex<Vec<(String, Document)>>,
    }

    impl MockMutator {
        fn new() -> Self {
            Self {
                docs: std::sync::Mutex::new(Vec::new()),
            }
        }

        fn created_docs(&self) -> Vec<(String, Document)> {
            self.docs.lock().unwrap().clone()
        }

        fn add_doc(&self, collection: &str, doc: Document) {
            self.docs
                .lock()
                .unwrap()
                .push((collection.to_string(), doc));
        }
    }

    #[async_trait]
    impl crate::mutator::DocMutator for MockMutator {
        async fn create(
            &self,
            collection_name: &str,
            mut doc: Document,
        ) -> Result<crate::mutator::CreateResult> {
            doc.generate_and_set_doc_id()
                .map_err(|e| QueryError::execution(format!("Failed to generate DocID: {}", e)))?;

            let doc_id = doc
                .id()
                .cloned()
                .ok_or_else(|| QueryError::execution("Document should have ID after generation"))?;

            self.docs
                .lock()
                .unwrap()
                .push((collection_name.to_string(), doc.clone()));

            Ok(crate::mutator::CreateResult::new(doc_id, doc))
        }

        async fn update(
            &self,
            _collection_name: &str,
            doc: Document,
        ) -> Result<crate::mutator::UpdateResult> {
            let modified = doc.values().len();
            Ok(crate::mutator::UpdateResult::new(doc, modified))
        }

        async fn delete(
            &self,
            _collection_name: &str,
            doc_id: &document::DocID,
        ) -> Result<crate::mutator::DeleteResult> {
            // Check if doc exists and remove it
            let mut docs = self.docs.lock().unwrap();
            let existed = docs
                .iter()
                .position(|(_, d)| d.id().map(|id| id.to_string()) == Some(doc_id.to_string()))
                .map(|i| docs.remove(i))
                .is_some();
            Ok(crate::mutator::DeleteResult::new(doc_id.clone(), existed))
        }

        async fn exists(&self, _collection_name: &str, doc_id: &document::DocID) -> Result<bool> {
            let docs = self.docs.lock().unwrap();
            Ok(docs
                .iter()
                .any(|(_, d)| d.id().map(|id| id.to_string()) == Some(doc_id.to_string())))
        }

        async fn get_for_update(
            &self,
            _collection_name: &str,
            doc_id: &document::DocID,
        ) -> Result<Option<Document>> {
            let docs = self.docs.lock().unwrap();
            Ok(docs
                .iter()
                .find(|(_, d)| d.id().map(|id| id.to_string()) == Some(doc_id.to_string()))
                .map(|(_, d)| d.clone()))
        }
    }

    #[tokio::test]
    async fn test_execute_mutation_without_mutator_returns_error() {
        let fetcher = MockFetcher::new();
        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        let result = runner
            .execute_mutation(r#"mutation { create_Users(input: [{name: "Alice"}]) { _docID } }"#)
            .await;

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("mutations require a mutator"));
    }

    #[tokio::test]
    async fn test_execute_create_mutation() {
        let fetcher = MockFetcher::new();
        let mutator = Arc::new(MockMutator::new());
        let runner =
            QueryRunner::new(fetcher, vec![make_test_collection()]).with_mutator(mutator.clone());

        let result = runner
            .execute_mutation(
                r#"mutation { create_Users(input: [{name: "Alice", age: 30}]) { _docID name } }"#,
            )
            .await
            .unwrap();

        // Check response structure
        assert!(result.is_object());
        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 1);
        assert!(users[0].get("_docID").is_some());
        assert_eq!(users[0].get("name").unwrap(), "Alice");

        // Verify document was created via mutator
        let created = mutator.created_docs();
        assert_eq!(created.len(), 1);
        assert_eq!(created[0].0, "Users");
    }

    #[tokio::test]
    async fn test_execute_create_multiple_documents() {
        let fetcher = MockFetcher::new();
        let mutator = Arc::new(MockMutator::new());
        let runner =
            QueryRunner::new(fetcher, vec![make_test_collection()]).with_mutator(mutator.clone());

        let result = runner
            .execute_mutation(
                r#"mutation {
                    create_Users(input: [
                        {name: "Alice", age: 30},
                        {name: "Bob", age: 25}
                    ]) { _docID name }
                }"#,
            )
            .await
            .unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 2);

        let names: Vec<&str> = users
            .iter()
            .map(|u| u.get("name").unwrap().as_str().unwrap())
            .collect();
        assert!(names.contains(&"Alice"));
        assert!(names.contains(&"Bob"));

        // Verify both documents were created
        let created = mutator.created_docs();
        assert_eq!(created.len(), 2);
    }

    #[tokio::test]
    async fn test_execute_create_with_partial_fields() {
        let fetcher = MockFetcher::new();
        let mutator = Arc::new(MockMutator::new());
        let runner =
            QueryRunner::new(fetcher, vec![make_test_collection()]).with_mutator(mutator.clone());

        // Create with only 'name' field, no 'age'
        let result = runner
            .execute_mutation(
                r#"mutation { create_Users(input: [{name: "Alice"}]) { _docID name } }"#,
            )
            .await
            .unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].get("name").unwrap(), "Alice");
        // _docID should be generated
        assert!(users[0].get("_docID").is_some());
    }

    #[tokio::test]
    async fn test_execute_create_returns_generated_doc_id() {
        let fetcher = MockFetcher::new();
        let mutator = Arc::new(MockMutator::new());
        let runner =
            QueryRunner::new(fetcher, vec![make_test_collection()]).with_mutator(mutator.clone());

        let result = runner
            .execute_mutation(
                r#"mutation { create_Users(input: [{name: "Alice", age: 30}]) { _docID } }"#,
            )
            .await
            .unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 1);

        let doc_id = users[0].get("_docID").unwrap().as_str().unwrap();
        // DocID should be a valid bae- prefixed string
        assert!(doc_id.starts_with("bae-"), "DocID should start with 'bae-'");
        assert!(doc_id.len() > 10, "DocID should be reasonably long");
    }

    #[tokio::test]
    async fn test_execute_create_with_all_field_types() {
        let fetcher = MockFetcher::new();
        let mutator = Arc::new(MockMutator::new());
        let runner =
            QueryRunner::new(fetcher, vec![make_test_collection()]).with_mutator(mutator.clone());

        // Create with string and integer fields
        let result = runner
            .execute_mutation(
                r#"mutation { create_Users(input: [{name: "Alice", age: 30}]) { _docID name age } }"#,
            )
            .await
            .unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].get("name").unwrap(), "Alice");
        assert_eq!(users[0].get("age").unwrap(), 30);
    }

    #[tokio::test]
    async fn test_execute_create_each_doc_gets_unique_id() {
        let fetcher = MockFetcher::new();
        let mutator = Arc::new(MockMutator::new());
        let runner =
            QueryRunner::new(fetcher, vec![make_test_collection()]).with_mutator(mutator.clone());

        // Create multiple documents with different content
        let result = runner
            .execute_mutation(
                r#"mutation {
                    create_Users(input: [
                        {name: "Alice", age: 30},
                        {name: "Bob", age: 25},
                        {name: "Charlie", age: 35}
                    ]) { _docID name }
                }"#,
            )
            .await
            .unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 3);

        // Collect all doc IDs
        let doc_ids: Vec<&str> = users
            .iter()
            .map(|u| u.get("_docID").unwrap().as_str().unwrap())
            .collect();

        // All IDs should be unique
        let mut unique_ids = doc_ids.clone();
        unique_ids.sort();
        unique_ids.dedup();
        assert_eq!(unique_ids.len(), 3, "Each document should have a unique ID");
    }

    #[tokio::test]
    async fn test_execute_mutation_unknown_collection_returns_error() {
        let fetcher = MockFetcher::new();
        let mutator = Arc::new(MockMutator::new());
        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]).with_mutator(mutator);

        let result = runner
            .execute_mutation(
                r#"mutation { create_NonExistent(input: [{name: "Alice"}]) { _docID } }"#,
            )
            .await;

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("collection not found"));
    }

    #[tokio::test]
    async fn test_execute_delete_mutation() {
        let fetcher = MockFetcher::new();
        let mutator = Arc::new(MockMutator::new());

        // Pre-populate with a document
        let mut doc = Document::new();
        doc.set("name", "Alice");
        doc.set("age", 30i64);
        doc.generate_and_set_doc_id().unwrap();
        let doc_id = doc.id().unwrap().to_string();
        mutator.add_doc("Users", doc);

        let runner =
            QueryRunner::new(fetcher, vec![make_test_collection()]).with_mutator(mutator.clone());

        let mutation = format!(
            r#"mutation {{ delete_Users(docIDs: ["{}"]) {{ _docID }} }}"#,
            doc_id
        );
        let result = runner.execute_mutation(&mutation).await.unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].get("_docID").unwrap().as_str().unwrap(), doc_id);

        // Verify document was deleted
        assert!(mutator.created_docs().is_empty());
    }

    // ==========================================================================
    // Update mutation tests
    // ==========================================================================

    #[tokio::test]
    async fn test_execute_update_mutation() {
        let fetcher = MockFetcher::new();
        let mutator = Arc::new(MockMutator::new());

        // Pre-populate with a document
        let mut doc = Document::new();
        doc.set("name", "Alice");
        doc.set("age", 25i64);
        doc.generate_and_set_doc_id().unwrap();
        let doc_id = doc.id().unwrap().to_string();
        mutator.add_doc("Users", doc);

        let runner =
            QueryRunner::new(fetcher, vec![make_test_collection()]).with_mutator(mutator.clone());

        let mutation = format!(
            r#"mutation {{ update_Users(docIDs: ["{}"], input: {{name: "Alice Updated", age: 30}}) {{ _docID name age }} }}"#,
            doc_id
        );
        let result = runner.execute_mutation(&mutation).await.unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].get("_docID").unwrap().as_str().unwrap(), doc_id);
        assert_eq!(users[0].get("name").unwrap(), "Alice Updated");
        assert_eq!(users[0].get("age").unwrap(), 30);
    }

    #[tokio::test]
    async fn test_execute_update_multiple_documents() {
        let fetcher = MockFetcher::new();
        let mutator = Arc::new(MockMutator::new());

        // Pre-populate with multiple documents
        let mut doc1 = Document::new();
        doc1.set("name", "Alice");
        doc1.set("age", 25i64);
        doc1.generate_and_set_doc_id().unwrap();
        let doc1_id = doc1.id().unwrap().to_string();
        mutator.add_doc("Users", doc1);

        let mut doc2 = Document::new();
        doc2.set("name", "Bob");
        doc2.set("age", 30i64);
        doc2.generate_and_set_doc_id().unwrap();
        let doc2_id = doc2.id().unwrap().to_string();
        mutator.add_doc("Users", doc2);

        let runner =
            QueryRunner::new(fetcher, vec![make_test_collection()]).with_mutator(mutator.clone());

        let mutation = format!(
            r#"mutation {{ update_Users(docIDs: ["{}", "{}"], input: {{age: 99}}) {{ _docID age }} }}"#,
            doc1_id, doc2_id
        );
        let result = runner.execute_mutation(&mutation).await.unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 2);

        // Both should have updated age
        for user in users {
            assert_eq!(user.get("age").unwrap(), 99);
        }
    }

    #[tokio::test]
    async fn test_execute_update_nonexistent_document_skipped() {
        let fetcher = MockFetcher::new();
        let mutator = Arc::new(MockMutator::new());

        // Pre-populate with one document
        let mut doc = Document::new();
        doc.set("name", "Alice");
        doc.generate_and_set_doc_id().unwrap();
        let existing_id = doc.id().unwrap().to_string();
        mutator.add_doc("Users", doc);

        // Generate a non-existent ID
        let mut template = Document::new();
        template.set("name", "NonExistent");
        template.generate_and_set_doc_id().unwrap();
        let nonexistent_id = template.id().unwrap().to_string();

        let runner =
            QueryRunner::new(fetcher, vec![make_test_collection()]).with_mutator(mutator.clone());

        // Try to update both existing and non-existent
        let mutation = format!(
            r#"mutation {{ update_Users(docIDs: ["{}", "{}"], input: {{name: "Updated"}}) {{ _docID name }} }}"#,
            existing_id, nonexistent_id
        );
        let result = runner.execute_mutation(&mutation).await.unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        // Only the existing document should be returned
        assert_eq!(users.len(), 1);
        assert_eq!(
            users[0].get("_docID").unwrap().as_str().unwrap(),
            existing_id
        );
    }

    #[tokio::test]
    async fn test_execute_delete_multiple_documents() {
        let fetcher = MockFetcher::new();
        let mutator = Arc::new(MockMutator::new());

        // Pre-populate with multiple documents
        let mut doc1 = Document::new();
        doc1.set("name", "Alice");
        doc1.generate_and_set_doc_id().unwrap();
        let doc1_id = doc1.id().unwrap().to_string();
        mutator.add_doc("Users", doc1);

        let mut doc2 = Document::new();
        doc2.set("name", "Bob");
        doc2.generate_and_set_doc_id().unwrap();
        let doc2_id = doc2.id().unwrap().to_string();
        mutator.add_doc("Users", doc2);

        let runner =
            QueryRunner::new(fetcher, vec![make_test_collection()]).with_mutator(mutator.clone());

        let mutation = format!(
            r#"mutation {{ delete_Users(docIDs: ["{}", "{}"]) {{ _docID }} }}"#,
            doc1_id, doc2_id
        );
        let result = runner.execute_mutation(&mutation).await.unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 2);

        let deleted_ids: Vec<&str> = users
            .iter()
            .map(|u| u.get("_docID").unwrap().as_str().unwrap())
            .collect();
        assert!(deleted_ids.contains(&doc1_id.as_str()));
        assert!(deleted_ids.contains(&doc2_id.as_str()));
    }

    #[tokio::test]
    async fn test_execute_delete_nonexistent_document_skipped() {
        let fetcher = MockFetcher::new();
        let mutator = Arc::new(MockMutator::new());

        // Pre-populate with one document
        let mut doc = Document::new();
        doc.set("name", "Alice");
        doc.generate_and_set_doc_id().unwrap();
        let existing_id = doc.id().unwrap().to_string();
        mutator.add_doc("Users", doc);

        // Generate a non-existent ID
        let mut template = Document::new();
        template.set("name", "NonExistent");
        template.generate_and_set_doc_id().unwrap();
        let nonexistent_id = template.id().unwrap().to_string();

        let runner =
            QueryRunner::new(fetcher, vec![make_test_collection()]).with_mutator(mutator.clone());

        // Try to delete both existing and non-existent
        let mutation = format!(
            r#"mutation {{ delete_Users(docIDs: ["{}", "{}"]) {{ _docID }} }}"#,
            existing_id, nonexistent_id
        );
        let result = runner.execute_mutation(&mutation).await.unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        // Only the existing document should be returned as deleted
        assert_eq!(users.len(), 1);
        assert_eq!(
            users[0].get("_docID").unwrap().as_str().unwrap(),
            existing_id
        );
    }

    // ==========================================================================
    // Upsert mutation tests
    // ==========================================================================

    #[tokio::test]
    async fn test_execute_upsert_creates_when_not_exists() {
        let fetcher = MockFetcher::new();
        let mutator = Arc::new(MockMutator::new());

        // Generate a valid docID that doesn't exist in the store
        let mut template = Document::new();
        template.set("name", "Template");
        template.generate_and_set_doc_id().unwrap();
        let new_doc_id = template.id().unwrap().to_string();

        let runner =
            QueryRunner::new(fetcher, vec![make_test_collection()]).with_mutator(mutator.clone());

        let mutation = format!(
            r#"mutation {{ upsert_Users(docIDs: ["{}"], input: {{name: "Alice", age: 30}}) {{ _docID name age }} }}"#,
            new_doc_id
        );
        let result = runner.execute_mutation(&mutation).await.unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].get("name").unwrap(), "Alice");
        assert_eq!(users[0].get("age").unwrap(), 30);

        // Verify document was created
        assert_eq!(mutator.created_docs().len(), 1);
    }

    #[tokio::test]
    async fn test_execute_upsert_updates_when_exists() {
        let fetcher = MockFetcher::new();
        let mutator = Arc::new(MockMutator::new());

        // Pre-populate with a document
        let mut existing_doc = Document::new();
        existing_doc.set("name", "Alice");
        existing_doc.set("age", 25i64);
        existing_doc.generate_and_set_doc_id().unwrap();
        let existing_id = existing_doc.id().unwrap().to_string();
        mutator.add_doc("Users", existing_doc);

        let runner =
            QueryRunner::new(fetcher, vec![make_test_collection()]).with_mutator(mutator.clone());

        let mutation = format!(
            r#"mutation {{ upsert_Users(docIDs: ["{}"], input: {{age: 30}}) {{ _docID name age }} }}"#,
            existing_id
        );
        let result = runner.execute_mutation(&mutation).await.unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 1);
        // Name should be preserved from existing doc
        assert_eq!(users[0].get("name").unwrap(), "Alice");
        // Age should be updated
        assert_eq!(users[0].get("age").unwrap(), 30);
    }

    #[tokio::test]
    async fn test_execute_upsert_mixed_create_and_update() {
        let fetcher = MockFetcher::new();
        let mutator = Arc::new(MockMutator::new());

        // Pre-populate with one document
        let mut existing_doc = Document::new();
        existing_doc.set("name", "Alice");
        existing_doc.set("age", 25i64);
        existing_doc.generate_and_set_doc_id().unwrap();
        let existing_id = existing_doc.id().unwrap().to_string();
        mutator.add_doc("Users", existing_doc);

        // Generate a new ID that doesn't exist
        let mut template = Document::new();
        template.set("name", "Template");
        template.generate_and_set_doc_id().unwrap();
        let new_id = template.id().unwrap().to_string();

        let runner =
            QueryRunner::new(fetcher, vec![make_test_collection()]).with_mutator(mutator.clone());

        let mutation = format!(
            r#"mutation {{ upsert_Users(docIDs: ["{}", "{}"], input: {{name: "Updated", age: 99}}) {{ _docID name age }} }}"#,
            existing_id, new_id
        );
        let result = runner.execute_mutation(&mutation).await.unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 2);

        // Both should have the upserted values
        for user in users {
            assert_eq!(user.get("name").unwrap(), "Updated");
            assert_eq!(user.get("age").unwrap(), 99);
        }
    }

    #[tokio::test]
    async fn test_execute_upsert_create_without_doc_id() {
        let fetcher = MockFetcher::new();
        let mutator = Arc::new(MockMutator::new());
        let runner =
            QueryRunner::new(fetcher, vec![make_test_collection()]).with_mutator(mutator.clone());

        // Upsert without docIDs creates a new document
        let result = runner
            .execute_mutation(
                r#"mutation { upsert_Users(input: {name: "NewUser", age: 42}) { _docID name age } }"#,
            )
            .await
            .unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 1);
        assert!(users[0].get("_docID").is_some());
        assert_eq!(users[0].get("name").unwrap(), "NewUser");
        assert_eq!(users[0].get("age").unwrap(), 42);

        // Verify document was created
        assert_eq!(mutator.created_docs().len(), 1);
    }

    // ==========================================================================
    // Filter-based mutation tests
    // ==========================================================================

    #[tokio::test]
    async fn test_execute_update_with_filter() {
        let fetcher = MockFetcher::new();
        let mutator = Arc::new(MockMutator::new());

        // Pre-populate with documents
        let mut doc1 = Document::new();
        doc1.set("name", "Alice");
        doc1.set("age", 20i64);
        doc1.generate_and_set_doc_id().unwrap();
        let doc1_id = doc1.id().unwrap().to_string();
        fetcher.add_doc("Users", doc1.clone());
        mutator.add_doc("Users", doc1);

        let mut doc2 = Document::new();
        doc2.set("name", "Bob");
        doc2.set("age", 30i64);
        doc2.generate_and_set_doc_id().unwrap();
        let doc2_id = doc2.id().unwrap().to_string();
        fetcher.add_doc("Users", doc2.clone());
        mutator.add_doc("Users", doc2);

        let mut doc3 = Document::new();
        doc3.set("name", "Charlie");
        doc3.set("age", 40i64);
        doc3.generate_and_set_doc_id().unwrap();
        let doc3_id = doc3.id().unwrap().to_string();
        fetcher.add_doc("Users", doc3.clone());
        mutator.add_doc("Users", doc3);

        let runner =
            QueryRunner::new(fetcher, vec![make_test_collection()]).with_mutator(mutator.clone());

        // Update only users with age >= 30
        let result = runner
            .execute_mutation(
                r#"mutation { update_Users(filter: {age: {_gte: 30}}, input: {name: "Updated"}) { _docID name } }"#,
            )
            .await
            .unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        // Only Bob (30) and Charlie (40) should be updated
        assert_eq!(users.len(), 2);

        let updated_ids: Vec<&str> = users
            .iter()
            .map(|u| u.get("_docID").unwrap().as_str().unwrap())
            .collect();
        assert!(!updated_ids.contains(&doc1_id.as_str())); // Alice (20) not updated
        assert!(updated_ids.contains(&doc2_id.as_str())); // Bob (30) updated
        assert!(updated_ids.contains(&doc3_id.as_str())); // Charlie (40) updated
    }

    #[tokio::test]
    async fn test_execute_delete_with_filter() {
        let fetcher = MockFetcher::new();
        let mutator = Arc::new(MockMutator::new());

        // Pre-populate with documents
        let mut doc1 = Document::new();
        doc1.set("name", "Alice");
        doc1.set("age", 25i64);
        doc1.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc1.clone());
        mutator.add_doc("Users", doc1);

        let mut doc2 = Document::new();
        doc2.set("name", "Bob");
        doc2.set("age", 35i64);
        doc2.generate_and_set_doc_id().unwrap();
        let doc2_id = doc2.id().unwrap().to_string();
        fetcher.add_doc("Users", doc2.clone());
        mutator.add_doc("Users", doc2);

        let runner =
            QueryRunner::new(fetcher, vec![make_test_collection()]).with_mutator(mutator.clone());

        // Delete only users with age > 30
        let result = runner
            .execute_mutation(r#"mutation { delete_Users(filter: {age: {_gt: 30}}) { _docID } }"#)
            .await
            .unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        // Only Bob should be deleted
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].get("_docID").unwrap().as_str().unwrap(), doc2_id);
    }

    #[tokio::test]
    async fn test_execute_upsert_with_filter() {
        let fetcher = MockFetcher::new();
        let mutator = Arc::new(MockMutator::new());

        // Pre-populate with documents
        let mut doc1 = Document::new();
        doc1.set("name", "Alice");
        doc1.set("age", 25i64);
        doc1.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc1.clone());
        mutator.add_doc("Users", doc1);

        let mut doc2 = Document::new();
        doc2.set("name", "Bob");
        doc2.set("age", 35i64);
        doc2.generate_and_set_doc_id().unwrap();
        let doc2_id = doc2.id().unwrap().to_string();
        fetcher.add_doc("Users", doc2.clone());
        mutator.add_doc("Users", doc2);

        let runner =
            QueryRunner::new(fetcher, vec![make_test_collection()]).with_mutator(mutator.clone());

        // Upsert users with age > 30 (should update Bob)
        let result = runner
            .execute_mutation(
                r#"mutation { upsert_Users(filter: {age: {_gt: 30}}, input: {name: "Updated"}) { _docID name } }"#,
            )
            .await
            .unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].get("_docID").unwrap().as_str().unwrap(), doc2_id);
        assert_eq!(users[0].get("name").unwrap(), "Updated");
    }

    #[tokio::test]
    async fn test_filter_mutation_no_matches_returns_empty() {
        let fetcher = MockFetcher::new();
        let mutator = Arc::new(MockMutator::new());

        // Pre-populate with a document
        let mut doc = Document::new();
        doc.set("name", "Alice");
        doc.set("age", 25i64);
        doc.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc.clone());
        mutator.add_doc("Users", doc);

        let runner =
            QueryRunner::new(fetcher, vec![make_test_collection()]).with_mutator(mutator.clone());

        // Filter matches nothing (no users with age > 100)
        let result = runner
            .execute_mutation(
                r#"mutation { update_Users(filter: {age: {_gt: 100}}, input: {name: "Updated"}) { _docID } }"#,
            )
            .await
            .unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        // Should return empty array, not an error
        assert!(users.is_empty());
    }

    #[tokio::test]
    async fn test_filter_delete_no_matches_returns_empty() {
        let fetcher = MockFetcher::new();
        let mutator = Arc::new(MockMutator::new());

        // Pre-populate with a document
        let mut doc = Document::new();
        doc.set("name", "Alice");
        doc.set("age", 25i64);
        doc.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc.clone());
        mutator.add_doc("Users", doc);

        let runner =
            QueryRunner::new(fetcher, vec![make_test_collection()]).with_mutator(mutator.clone());

        // Filter matches nothing
        let result = runner
            .execute_mutation(
                r#"mutation { delete_Users(filter: {name: {_eq: "NonExistent"}}) { _docID } }"#,
            )
            .await
            .unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert!(users.is_empty());

        // Original document should still exist
        assert_eq!(mutator.created_docs().len(), 1);
    }

    #[tokio::test]
    async fn test_doc_ids_takes_priority_over_filter() {
        let fetcher = MockFetcher::new();
        let mutator = Arc::new(MockMutator::new());

        // Pre-populate with documents
        let mut doc1 = Document::new();
        doc1.set("name", "Alice");
        doc1.set("age", 50i64);
        doc1.generate_and_set_doc_id().unwrap();
        let doc1_id = doc1.id().unwrap().to_string();
        fetcher.add_doc("Users", doc1.clone());
        mutator.add_doc("Users", doc1);

        let mut doc2 = Document::new();
        doc2.set("name", "Bob");
        doc2.set("age", 60i64);
        doc2.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc2.clone());
        mutator.add_doc("Users", doc2);

        let runner =
            QueryRunner::new(fetcher, vec![make_test_collection()]).with_mutator(mutator.clone());

        // Provide both docIDs and filter - docIDs should take priority
        // Filter would match both, but docIDs only specifies doc1
        let mutation = format!(
            r#"mutation {{ update_Users(docIDs: ["{}"], filter: {{age: {{_gte: 50}}}}, input: {{name: "Updated"}}) {{ _docID name }} }}"#,
            doc1_id
        );
        let result = runner.execute_mutation(&mutation).await.unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        // Only doc1 should be updated (docIDs takes priority)
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].get("_docID").unwrap().as_str().unwrap(), doc1_id);
    }

    // =============================================================================
    // Aggregation Tests
    // =============================================================================

    #[tokio::test]
    async fn test_count_all_documents() {
        let fetcher = MockFetcher::new();

        let mut doc1 = Document::new();
        doc1.set("name", "Alice");
        doc1.set("age", 30i64);
        doc1.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc1);

        let mut doc2 = Document::new();
        doc2.set("name", "Bob");
        doc2.set("age", 25i64);
        doc2.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc2);

        let mut doc3 = Document::new();
        doc3.set("name", "Charlie");
        doc3.set("age", 35i64);
        doc3.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc3);

        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        let result = runner.execute_query("{ Users { _count } }").await.unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].get("_count").unwrap(), 3);
    }

    #[tokio::test]
    async fn test_count_with_alias() {
        let fetcher = MockFetcher::new();

        let mut doc1 = Document::new();
        doc1.set("name", "Alice");
        doc1.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc1);

        let mut doc2 = Document::new();
        doc2.set("name", "Bob");
        doc2.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc2);

        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        let result = runner
            .execute_query("{ Users { total: _count } }")
            .await
            .unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].get("total").unwrap(), 2);
    }

    #[tokio::test]
    async fn test_count_with_filter() {
        let fetcher = MockFetcher::new();

        let mut doc1 = Document::new();
        doc1.set("name", "Alice");
        doc1.set("age", 30i64);
        doc1.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc1);

        let mut doc2 = Document::new();
        doc2.set("name", "Bob");
        doc2.set("age", 25i64);
        doc2.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc2);

        let mut doc3 = Document::new();
        doc3.set("name", "Charlie");
        doc3.set("age", 35i64);
        doc3.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc3);

        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        let result = runner
            .execute_query(r#"{ Users(filter: {age: {_gte: 30}}) { _count } }"#)
            .await
            .unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 1);
        // Should count only Alice and Charlie (age >= 30)
        assert_eq!(users[0].get("_count").unwrap(), 2);
    }

    #[tokio::test]
    async fn test_count_empty_collection() {
        let fetcher = MockFetcher::new();
        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        let result = runner.execute_query("{ Users { _count } }").await.unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].get("_count").unwrap(), 0);
    }

    #[tokio::test]
    async fn test_sum_aggregate() {
        let fetcher = MockFetcher::new();

        let mut doc1 = Document::new();
        doc1.set("name", "Alice");
        doc1.set("age", 30i64);
        doc1.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc1);

        let mut doc2 = Document::new();
        doc2.set("name", "Bob");
        doc2.set("age", 25i64);
        doc2.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc2);

        let mut doc3 = Document::new();
        doc3.set("name", "Charlie");
        doc3.set("age", 35i64);
        doc3.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc3);

        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        let result = runner
            .execute_query(r#"{ Users { _sum(field: "age") } }"#)
            .await
            .unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 1);
        // 30 + 25 + 35 = 90
        assert_eq!(users[0].get("_sum").unwrap(), 90);
    }

    #[tokio::test]
    async fn test_avg_aggregate() {
        let fetcher = MockFetcher::new();

        let mut doc1 = Document::new();
        doc1.set("name", "Alice");
        doc1.set("age", 30i64);
        doc1.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc1);

        let mut doc2 = Document::new();
        doc2.set("name", "Bob");
        doc2.set("age", 20i64);
        doc2.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc2);

        let mut doc3 = Document::new();
        doc3.set("name", "Charlie");
        doc3.set("age", 40i64);
        doc3.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc3);

        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        let result = runner
            .execute_query(r#"{ Users { _avg(field: "age") } }"#)
            .await
            .unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 1);
        // (30 + 20 + 40) / 3 = 30
        let avg = users[0].get("_avg").unwrap().as_f64().unwrap();
        assert!((avg - 30.0).abs() < 0.001);
    }

    #[tokio::test]
    async fn test_min_max_aggregate() {
        let fetcher = MockFetcher::new();

        let mut doc1 = Document::new();
        doc1.set("name", "Alice");
        doc1.set("age", 30i64);
        doc1.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc1);

        let mut doc2 = Document::new();
        doc2.set("name", "Bob");
        doc2.set("age", 25i64);
        doc2.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc2);

        let mut doc3 = Document::new();
        doc3.set("name", "Charlie");
        doc3.set("age", 35i64);
        doc3.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc3);

        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        // Test min
        let result = runner
            .execute_query(r#"{ Users { _min(field: "age") } }"#)
            .await
            .unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].get("_min").unwrap(), 25);

        // Test max
        let result = runner
            .execute_query(r#"{ Users { _max(field: "age") } }"#)
            .await
            .unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].get("_max").unwrap(), 35);
    }

    #[tokio::test]
    async fn test_multiple_aggregates_with_groupby() {
        let fetcher = MockFetcher::new();

        // Create docs in two groups
        let mut doc1 = Document::new();
        doc1.set("name", "Alice");
        doc1.set("age", 30i64);
        doc1.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc1);

        let mut doc2 = Document::new();
        doc2.set("name", "Alice");
        doc2.set("age", 20i64);
        doc2.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc2);

        let mut doc3 = Document::new();
        doc3.set("name", "Bob");
        doc3.set("age", 25i64);
        doc3.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc3);

        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        // Use GROUP BY so multiple aggregates work correctly
        let result = runner
            .execute_query(
                r#"{ Users(groupBy: [name]) { name _count _sum(field: "age") _avg(field: "age") } }"#,
            )
            .await
            .unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 2);

        // Find Alice group: 2 docs, sum=50, avg=25
        let alice = users
            .iter()
            .find(|u| u["name"].as_str() == Some("Alice"))
            .unwrap();
        assert_eq!(alice.get("_count").unwrap(), 2);
        assert_eq!(alice.get("_sum").unwrap(), 50);
        let alice_avg = alice.get("_avg").unwrap().as_f64().unwrap();
        assert!((alice_avg - 25.0).abs() < 0.001);

        // Find Bob group: 1 doc, sum=25, avg=25
        let bob = users
            .iter()
            .find(|u| u["name"].as_str() == Some("Bob"))
            .unwrap();
        assert_eq!(bob.get("_count").unwrap(), 1);
        assert_eq!(bob.get("_sum").unwrap(), 25);
        let bob_avg = bob.get("_avg").unwrap().as_f64().unwrap();
        assert!((bob_avg - 25.0).abs() < 0.001);
    }

    // ==========================================================================
    // Edge Case Tests
    // ==========================================================================

    #[tokio::test]
    async fn test_aggregate_on_nonexistent_field_returns_error() {
        let fetcher = MockFetcher::new();
        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        let result = runner
            .execute_query(r#"{ Users { _sum(field: "nonexistent_field") } }"#)
            .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("not found"));
    }

    #[tokio::test]
    async fn test_groupby_unknown_field_returns_error() {
        let fetcher = MockFetcher::new();

        let mut doc1 = Document::new();
        doc1.set("name", "Alice");
        doc1.set("age", 30i64);
        doc1.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc1);

        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        let result = runner
            .execute_query(r#"{ Users(groupBy: [unknown_field]) { name } }"#)
            .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("unknown field"));
    }

    #[tokio::test]
    async fn test_aggregate_with_negative_numbers() {
        let fetcher = MockFetcher::new();

        let mut doc1 = Document::new();
        doc1.set("name", "Alice");
        doc1.set("age", -50i64);
        doc1.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc1);

        let mut doc2 = Document::new();
        doc2.set("name", "Bob");
        doc2.set("age", 100i64);
        doc2.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc2);

        let mut doc3 = Document::new();
        doc3.set("name", "Charlie");
        doc3.set("age", -25i64);
        doc3.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc3);

        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        // Sum: -50 + 100 + (-25) = 25
        let result = runner
            .execute_query(r#"{ Users { _sum(field: "age") } }"#)
            .await
            .unwrap();
        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users[0].get("_sum").unwrap(), 25);

        // Min: -50
        let result = runner
            .execute_query(r#"{ Users { _min(field: "age") } }"#)
            .await
            .unwrap();
        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users[0].get("_min").unwrap(), -50);

        // Max: 100
        let result = runner
            .execute_query(r#"{ Users { _max(field: "age") } }"#)
            .await
            .unwrap();
        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users[0].get("_max").unwrap(), 100);

        // Avg: 25 / 3 ≈ 8.33
        let result = runner
            .execute_query(r#"{ Users { _avg(field: "age") } }"#)
            .await
            .unwrap();
        let users = result.get("Users").unwrap().as_array().unwrap();
        let avg = users[0].get("_avg").unwrap().as_f64().unwrap();
        assert!((avg - 8.333).abs() < 0.01);
    }

    #[tokio::test]
    async fn test_aggregate_mixed_int_and_float() {
        let fetcher = MockFetcher::new();

        let mut doc1 = Document::new();
        doc1.set("name", "Alice");
        doc1.set("age", 10i64); // int
        doc1.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc1);

        let mut doc2 = Document::new();
        doc2.set("name", "Bob");
        doc2.set("age", 20.5f64); // float
        doc2.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc2);

        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        // Sum should return float since one value is float: 10 + 20.5 = 30.5
        let result = runner
            .execute_query(r#"{ Users { _sum(field: "age") } }"#)
            .await
            .unwrap();
        let users = result.get("Users").unwrap().as_array().unwrap();
        let sum = users[0].get("_sum").unwrap().as_f64().unwrap();
        assert!((sum - 30.5).abs() < 0.001);
    }

    #[tokio::test]
    async fn test_multiple_aggregates_without_groupby() {
        let fetcher = MockFetcher::new();

        let mut doc1 = Document::new();
        doc1.set("name", "Alice");
        doc1.set("age", 30i64);
        doc1.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc1);

        let mut doc2 = Document::new();
        doc2.set("name", "Bob");
        doc2.set("age", 20i64);
        doc2.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc2);

        let mut doc3 = Document::new();
        doc3.set("name", "Charlie");
        doc3.set("age", 40i64);
        doc3.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc3);

        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        let result = runner
            .execute_query(
                r#"{ Users { _count _sum(field: "age") _min(field: "age") _max(field: "age") } }"#,
            )
            .await
            .unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 1); // Single aggregated result

        assert_eq!(users[0].get("_count").unwrap(), 3);
        assert_eq!(users[0].get("_sum").unwrap(), 90);
        assert_eq!(users[0].get("_min").unwrap(), 20);
        assert_eq!(users[0].get("_max").unwrap(), 40);
    }

    #[tokio::test]
    async fn test_aggregate_on_string_field_returns_zero() {
        let fetcher = MockFetcher::new();

        let mut doc1 = Document::new();
        doc1.set("name", "Alice");
        doc1.set("age", 30i64);
        doc1.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc1);

        let mut doc2 = Document::new();
        doc2.set("name", "Bob");
        doc2.set("age", 25i64);
        doc2.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc2);

        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        // SUM on string field returns 0 (skips non-numeric values)
        let result = runner
            .execute_query(r#"{ Users { _sum(field: "name") } }"#)
            .await
            .unwrap();
        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users[0].get("_sum").unwrap(), 0);

        // MIN on string field returns null
        let result = runner
            .execute_query(r#"{ Users { _min(field: "name") } }"#)
            .await
            .unwrap();
        let users = result.get("Users").unwrap().as_array().unwrap();
        assert!(users[0].get("_min").unwrap().is_null());

        // AVG on string field returns null (SQL semantics for empty set)
        let result = runner
            .execute_query(r#"{ Users { _avg(field: "name") } }"#)
            .await
            .unwrap();
        let users = result.get("Users").unwrap().as_array().unwrap();
        assert!(users[0].get("_avg").unwrap().is_null());
    }

    #[tokio::test]
    async fn test_aggregate_empty_collection_semantics() {
        let fetcher = MockFetcher::new();
        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        // COUNT on empty: 0
        let result = runner
            .execute_query(r#"{ Users { _count } }"#)
            .await
            .unwrap();
        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 1);
        assert_eq!(users[0].get("_count").unwrap(), 0);

        // SUM on empty: 0 (no values to sum)
        let result = runner
            .execute_query(r#"{ Users { _sum(field: "age") } }"#)
            .await
            .unwrap();
        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users[0].get("_sum").unwrap(), 0);

        // AVG on empty: null (SQL semantics)
        let result = runner
            .execute_query(r#"{ Users { _avg(field: "age") } }"#)
            .await
            .unwrap();
        let users = result.get("Users").unwrap().as_array().unwrap();
        assert!(users[0].get("_avg").unwrap().is_null());

        // MIN on empty: null
        let result = runner
            .execute_query(r#"{ Users { _min(field: "age") } }"#)
            .await
            .unwrap();
        let users = result.get("Users").unwrap().as_array().unwrap();
        assert!(users[0].get("_min").unwrap().is_null());

        // MAX on empty: null
        let result = runner
            .execute_query(r#"{ Users { _max(field: "age") } }"#)
            .await
            .unwrap();
        let users = result.get("Users").unwrap().as_array().unwrap();
        assert!(users[0].get("_max").unwrap().is_null());
    }

    #[tokio::test]
    async fn test_aggregate_with_filter() {
        let fetcher = MockFetcher::new();

        let mut doc1 = Document::new();
        doc1.set("name", "Alice");
        doc1.set("age", 30i64);
        doc1.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc1);

        let mut doc2 = Document::new();
        doc2.set("name", "Bob");
        doc2.set("age", 20i64);
        doc2.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc2);

        let mut doc3 = Document::new();
        doc3.set("name", "Charlie");
        doc3.set("age", 40i64);
        doc3.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc3);

        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        // SUM with filter: only age >= 30 (30 + 40 = 70)
        let result = runner
            .execute_query(r#"{ Users(filter: {age: {_gte: 30}}) { _sum(field: "age") } }"#)
            .await
            .unwrap();
        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users[0].get("_sum").unwrap(), 70);

        // AVG with filter: (30 + 40) / 2 = 35
        let result = runner
            .execute_query(r#"{ Users(filter: {age: {_gte: 30}}) { _avg(field: "age") } }"#)
            .await
            .unwrap();
        let users = result.get("Users").unwrap().as_array().unwrap();
        let avg = users[0].get("_avg").unwrap().as_f64().unwrap();
        assert!((avg - 35.0).abs() < 0.001);
    }

    #[tokio::test]
    async fn test_groupby_all_null_values_in_field() {
        let fetcher = MockFetcher::new();

        // Both docs have null age
        let mut doc1 = Document::new();
        doc1.set("name", "Alice");
        // age is null (not set)
        doc1.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc1);

        let mut doc2 = Document::new();
        doc2.set("name", "Alice");
        // age is null (not set)
        doc2.generate_and_set_doc_id().unwrap();
        fetcher.add_doc("Users", doc2);

        let runner = QueryRunner::new(fetcher, vec![make_test_collection()]);

        let result = runner
            .execute_query(
                r#"{ Users(groupBy: [name]) { name _sum(field: "age") _avg(field: "age") } }"#,
            )
            .await
            .unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 1);

        let alice = &users[0];
        // SUM of no values is 0
        assert_eq!(alice.get("_sum").unwrap(), 0);
        // AVG of no values is null (SQL semantics)
        assert!(alice.get("_avg").unwrap().is_null());
    }

    // ========================================================================
    // ACP Integration Tests
    // ========================================================================

    fn make_acp_collection() -> CollectionVersion {
        use schema::PolicyDescription;

        CollectionVersion::new(
            "Users",
            "v1",
            "coll-acp",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "name", FieldKind::string()),
                FieldDescription::new("3", "age", FieldKind::int()),
            ],
        )
        .with_policy(PolicyDescription::new("policy-acp", "Users"))
    }

    fn test_acp_did() -> Did {
        Did::new("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK").unwrap()
    }

    fn test_acp_did2() -> Did {
        Did::new("did:key:z6MkfXG2FkNy3u7Eg3jm8e2YQpGz7Z1JqWgHDAP1hLk9r2bR").unwrap()
    }

    #[tokio::test]
    async fn test_acp_owner_sees_registered_docs() {
        use acp::{LocalDocumentACP, MemoryAcpStore};

        let fetcher = MockFetcher::new();
        let store = Arc::new(MemoryAcpStore::new());
        let acp = Arc::new(LocalDocumentACP::new(store));

        let owner = test_acp_did();

        // Create docs with known IDs
        let mut doc1 = Document::new();
        doc1.set("_docID", "doc1");
        doc1.set("name", "Alice");
        doc1.set("age", 30i64);
        fetcher.add_doc("Users", doc1);

        let mut doc2 = Document::new();
        doc2.set("_docID", "doc2");
        doc2.set("name", "Bob");
        doc2.set("age", 25i64);
        fetcher.add_doc("Users", doc2);

        // Register both docs with owner
        acp.register_doc_object(&owner, "policy-acp", "Users", "doc1")
            .await
            .unwrap();
        acp.register_doc_object(&owner, "policy-acp", "Users", "doc2")
            .await
            .unwrap();

        let runner = QueryRunner::new(fetcher, vec![make_acp_collection()]).with_acp(acp);

        // Owner should see both docs (include _docID for ACP to work with projection)
        let result = runner
            .execute_query_with_identity("{ Users { _docID name } }", Some(owner))
            .await
            .unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 2, "owner should see all registered docs");
    }

    #[tokio::test]
    async fn test_acp_non_owner_sees_nothing() {
        use acp::{LocalDocumentACP, MemoryAcpStore};

        let fetcher = MockFetcher::new();
        let store = Arc::new(MemoryAcpStore::new());
        let acp = Arc::new(LocalDocumentACP::new(store));

        let owner = test_acp_did();
        let other = test_acp_did2();

        // Create doc with known ID
        let mut doc1 = Document::new();
        doc1.set("_docID", "doc1");
        doc1.set("name", "Alice");
        fetcher.add_doc("Users", doc1);

        // Register doc with owner
        acp.register_doc_object(&owner, "policy-acp", "Users", "doc1")
            .await
            .unwrap();

        let runner = QueryRunner::new(fetcher, vec![make_acp_collection()]).with_acp(acp);

        // Non-owner without permissions should see nothing
        let result = runner
            .execute_query_with_identity("{ Users { _docID name } }", Some(other))
            .await
            .unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(
            users.len(),
            0,
            "non-owner without permissions should see no docs"
        );
    }

    #[tokio::test]
    async fn test_acp_reader_sees_shared_doc() {
        use acp::{LocalDocumentACP, MemoryAcpStore, READER_RELATION};

        let fetcher = MockFetcher::new();
        let store = Arc::new(MemoryAcpStore::new());
        let acp = Arc::new(LocalDocumentACP::new(store));

        let owner = test_acp_did();
        let reader = test_acp_did2();

        // Create doc
        let mut doc1 = Document::new();
        doc1.set("_docID", "doc1");
        doc1.set("name", "Alice");
        fetcher.add_doc("Users", doc1);

        // Register and share with reader
        acp.register_doc_object(&owner, "policy-acp", "Users", "doc1")
            .await
            .unwrap();
        acp.add_actor_relationship(&owner, &reader, "Users", "doc1", READER_RELATION)
            .await
            .unwrap();

        let runner = QueryRunner::new(fetcher, vec![make_acp_collection()]).with_acp(acp);

        // Reader should see the shared doc
        let result = runner
            .execute_query_with_identity("{ Users { _docID name } }", Some(reader))
            .await
            .unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 1, "reader should see shared doc");
        assert_eq!(users[0].get("name").unwrap(), "Alice");
    }

    #[tokio::test]
    async fn test_acp_anonymous_cannot_see_registered_doc() {
        use acp::{LocalDocumentACP, MemoryAcpStore};

        let fetcher = MockFetcher::new();
        let store = Arc::new(MemoryAcpStore::new());
        let acp = Arc::new(LocalDocumentACP::new(store));

        let owner = test_acp_did();

        // Create doc
        let mut doc1 = Document::new();
        doc1.set("_docID", "doc1");
        doc1.set("name", "Alice");
        fetcher.add_doc("Users", doc1);

        // Register with owner
        acp.register_doc_object(&owner, "policy-acp", "Users", "doc1")
            .await
            .unwrap();

        let runner = QueryRunner::new(fetcher, vec![make_acp_collection()]).with_acp(acp);

        // Anonymous (None identity) should see nothing
        let result = runner
            .execute_query_with_identity("{ Users { _docID name } }", None)
            .await
            .unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 0, "anonymous should not see registered docs");
    }

    #[tokio::test]
    async fn test_acp_public_docs_visible_to_all() {
        use acp::{LocalDocumentACP, MemoryAcpStore};

        let fetcher = MockFetcher::new();
        let store = Arc::new(MemoryAcpStore::new());
        let acp = Arc::new(LocalDocumentACP::new(store));

        let owner = test_acp_did();
        let other = test_acp_did2();

        // Create two docs - one registered, one public
        let mut doc1 = Document::new();
        doc1.set("_docID", "doc1");
        doc1.set("name", "Alice");
        fetcher.add_doc("Users", doc1);

        let mut doc2 = Document::new();
        doc2.set("_docID", "doc2");
        doc2.set("name", "Bob");
        fetcher.add_doc("Users", doc2);

        // Only register doc1 with owner
        acp.register_doc_object(&owner, "policy-acp", "Users", "doc1")
            .await
            .unwrap();

        let runner = QueryRunner::new(fetcher, vec![make_acp_collection()]).with_acp(acp);

        // Non-owner should see only the unregistered (public) doc
        let result = runner
            .execute_query_with_identity("{ Users { _docID name } }", Some(other))
            .await
            .unwrap();

        let users = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(users.len(), 1, "non-owner should see only public doc");
        assert_eq!(users[0].get("name").unwrap(), "Bob");
    }

    #[tokio::test]
    async fn test_identity_passed_through_execute_request() {
        use acp::{LocalDocumentACP, MemoryAcpStore};

        let fetcher = MockFetcher::new();
        let store = Arc::new(MemoryAcpStore::new());
        let acp = Arc::new(LocalDocumentACP::new(store));

        let owner = test_acp_did();

        // Create doc
        let mut doc1 = Document::new();
        doc1.set("_docID", "doc1");
        doc1.set("name", "Alice");
        fetcher.add_doc("Users", doc1);

        // Register with owner
        acp.register_doc_object(&owner, "policy-acp", "Users", "doc1")
            .await
            .unwrap();

        let runner = QueryRunner::new(fetcher, vec![make_acp_collection()]).with_acp(acp);

        // Use the QueryRequest with identity (simulating HTTP flow)
        let request = QueryRequest::new("{ Users { _docID name } }").with_identity(Some(owner));
        let response = runner.execute(request).await;

        assert!(response.errors.is_empty());
        let users = response
            .data
            .unwrap()
            .get("Users")
            .unwrap()
            .as_array()
            .unwrap()
            .to_vec();
        assert_eq!(
            users.len(),
            1,
            "owner should see registered doc via execute()"
        );
    }

    // ========================================================================
    // Mutation ACP Integration Tests
    // ========================================================================

    #[tokio::test]
    async fn test_acp_create_with_identity_registers_doc() {
        use acp::{LocalDocumentACP, MemoryAcpStore};

        let fetcher = MockFetcher::new();
        let store = Arc::new(MemoryAcpStore::new());
        let acp = Arc::new(LocalDocumentACP::new(store.clone()));
        let mutator = Arc::new(MockMutator::new());

        let owner = test_acp_did();

        let runner = QueryRunner::new(fetcher, vec![make_acp_collection()])
            .with_acp(acp.clone())
            .with_mutator(mutator);

        // Create a document with identity
        let result = runner
            .execute_mutation_with_identity(
                r#"mutation { create_Users(input: [{name: "Alice"}]) { _docID name } }"#,
                Some(owner.clone()),
            )
            .await
            .unwrap();

        // Get the created doc ID
        let created = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(created.len(), 1);
        let doc_id = created[0].get("_docID").unwrap().as_str().unwrap();

        // Verify the document is registered with ACP
        let is_registered = acp
            .is_doc_registered("policy-acp", "Users", doc_id)
            .await
            .unwrap();
        assert!(is_registered, "created doc should be registered with ACP");

        // Verify owner has access
        let has_access = acp
            .check_doc_access(
                &Identity::Authenticated(owner),
                acp::DocumentPermission::Read,
                "policy-acp",
                "Users",
                doc_id,
            )
            .await
            .unwrap();
        assert!(has_access, "owner should have access to created doc");
    }

    #[tokio::test]
    async fn test_acp_create_without_identity_doc_is_public() {
        use acp::{LocalDocumentACP, MemoryAcpStore};

        let fetcher = MockFetcher::new();
        let store = Arc::new(MemoryAcpStore::new());
        let acp = Arc::new(LocalDocumentACP::new(store.clone()));
        let mutator = Arc::new(MockMutator::new());

        let runner = QueryRunner::new(fetcher, vec![make_acp_collection()])
            .with_acp(acp.clone())
            .with_mutator(mutator);

        // Create a document without identity (anonymous)
        let result = runner
            .execute_mutation_with_identity(
                r#"mutation { create_Users(input: [{name: "Bob"}]) { _docID name } }"#,
                None,
            )
            .await
            .unwrap();

        let created = result.get("Users").unwrap().as_array().unwrap();
        assert_eq!(created.len(), 1);
        let doc_id = created[0].get("_docID").unwrap().as_str().unwrap();

        // Document should NOT be registered (public)
        let is_registered = acp
            .is_doc_registered("policy-acp", "Users", doc_id)
            .await
            .unwrap();
        assert!(
            !is_registered,
            "anonymous create should not register doc with ACP"
        );
    }

    #[tokio::test]
    async fn test_acp_update_by_owner_succeeds() {
        use acp::{LocalDocumentACP, MemoryAcpStore};

        let fetcher = MockFetcher::new();
        let store = Arc::new(MemoryAcpStore::new());
        let acp = Arc::new(LocalDocumentACP::new(store));
        let mutator = Arc::new(MockMutator::new());

        let owner = test_acp_did();

        // Create a document with proper DocID
        let mut doc = Document::new();
        doc.set("name", "Alice");
        doc.set("age", 30i64);
        doc.generate_and_set_doc_id().unwrap();
        let doc_id = doc.id().unwrap().to_string();
        mutator.add_doc("Users", doc);

        // Register doc with ACP
        acp.register_doc_object(&owner, "policy-acp", "Users", &doc_id)
            .await
            .unwrap();

        let runner = QueryRunner::new(fetcher, vec![make_acp_collection()])
            .with_acp(acp)
            .with_mutator(mutator);

        // Owner should be able to update
        let mutation = format!(
            r#"mutation {{ update_Users(docIDs: ["{}"], input: {{name: "Alice Updated"}}) {{ _docID name }} }}"#,
            doc_id
        );
        let result = runner
            .execute_mutation_with_identity(&mutation, Some(owner))
            .await;

        assert!(
            result.is_ok(),
            "owner should be able to update their doc: {:?}",
            result.as_ref().err()
        );
    }

    #[tokio::test]
    async fn test_acp_update_without_permission_fails() {
        use acp::{LocalDocumentACP, MemoryAcpStore};

        let fetcher = MockFetcher::new();
        let store = Arc::new(MemoryAcpStore::new());
        let acp = Arc::new(LocalDocumentACP::new(store));
        let mutator = Arc::new(MockMutator::new());

        let owner = test_acp_did();
        let other = test_acp_did2();

        // Create a document with proper DocID
        let mut doc = Document::new();
        doc.set("name", "Alice");
        doc.set("age", 30i64);
        doc.generate_and_set_doc_id().unwrap();
        let doc_id = doc.id().unwrap().to_string();
        mutator.add_doc("Users", doc);

        // Register doc with owner
        acp.register_doc_object(&owner, "policy-acp", "Users", &doc_id)
            .await
            .unwrap();

        let runner = QueryRunner::new(fetcher, vec![make_acp_collection()])
            .with_acp(acp)
            .with_mutator(mutator);

        // Non-owner without updater permission should fail
        let mutation = format!(
            r#"mutation {{ update_Users(docIDs: ["{}"], input: {{name: "Hacked"}}) {{ _docID }} }}"#,
            doc_id
        );
        let result = runner
            .execute_mutation_with_identity(&mutation, Some(other))
            .await;

        assert!(
            result.is_err(),
            "non-owner without updater permission should not be able to update"
        );
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("permission denied"),
            "error should indicate permission denied"
        );
    }

    #[tokio::test]
    async fn test_acp_update_with_updater_relation_succeeds() {
        use acp::{LocalDocumentACP, MemoryAcpStore, UPDATER_RELATION};

        let fetcher = MockFetcher::new();
        let store = Arc::new(MemoryAcpStore::new());
        let acp = Arc::new(LocalDocumentACP::new(store));
        let mutator = Arc::new(MockMutator::new());

        let owner = test_acp_did();
        let updater = test_acp_did2();

        // Create a document with proper DocID
        let mut doc = Document::new();
        doc.set("name", "Alice");
        doc.set("age", 30i64);
        doc.generate_and_set_doc_id().unwrap();
        let doc_id = doc.id().unwrap().to_string();
        mutator.add_doc("Users", doc);

        // Register doc and grant updater permission
        acp.register_doc_object(&owner, "policy-acp", "Users", &doc_id)
            .await
            .unwrap();
        acp.add_actor_relationship(&owner, &updater, "Users", &doc_id, UPDATER_RELATION)
            .await
            .unwrap();

        let runner = QueryRunner::new(fetcher, vec![make_acp_collection()])
            .with_acp(acp)
            .with_mutator(mutator);

        // Updater should be able to update
        let mutation = format!(
            r#"mutation {{ update_Users(docIDs: ["{}"], input: {{name: "Updated by updater"}}) {{ _docID }} }}"#,
            doc_id
        );
        let result = runner
            .execute_mutation_with_identity(&mutation, Some(updater))
            .await;

        assert!(
            result.is_ok(),
            "identity with updater relation should be able to update: {:?}",
            result.as_ref().err()
        );
    }

    #[tokio::test]
    async fn test_acp_delete_by_owner_succeeds() {
        use acp::{LocalDocumentACP, MemoryAcpStore};

        let fetcher = MockFetcher::new();
        let store = Arc::new(MemoryAcpStore::new());
        let acp = Arc::new(LocalDocumentACP::new(store));
        let mutator = Arc::new(MockMutator::new());

        let owner = test_acp_did();

        // Create a document with proper DocID
        let mut doc = Document::new();
        doc.set("name", "Alice");
        doc.generate_and_set_doc_id().unwrap();
        let doc_id = doc.id().unwrap().to_string();
        mutator.add_doc("Users", doc);

        // Register doc with ACP
        acp.register_doc_object(&owner, "policy-acp", "Users", &doc_id)
            .await
            .unwrap();

        let runner = QueryRunner::new(fetcher, vec![make_acp_collection()])
            .with_acp(acp)
            .with_mutator(mutator);

        // Owner should be able to delete
        let mutation = format!(
            r#"mutation {{ delete_Users(docIDs: ["{}"]) {{ _docID }} }}"#,
            doc_id
        );
        let result = runner
            .execute_mutation_with_identity(&mutation, Some(owner))
            .await;

        assert!(
            result.is_ok(),
            "owner should be able to delete their doc: {:?}",
            result.as_ref().err()
        );
    }

    #[tokio::test]
    async fn test_acp_delete_without_permission_fails() {
        use acp::{LocalDocumentACP, MemoryAcpStore};

        let fetcher = MockFetcher::new();
        let store = Arc::new(MemoryAcpStore::new());
        let acp = Arc::new(LocalDocumentACP::new(store));
        let mutator = Arc::new(MockMutator::new());

        let owner = test_acp_did();
        let other = test_acp_did2();

        // Create a document with proper DocID
        let mut doc = Document::new();
        doc.set("name", "Alice");
        doc.generate_and_set_doc_id().unwrap();
        let doc_id = doc.id().unwrap().to_string();
        mutator.add_doc("Users", doc);

        // Register doc with owner
        acp.register_doc_object(&owner, "policy-acp", "Users", &doc_id)
            .await
            .unwrap();

        let runner = QueryRunner::new(fetcher, vec![make_acp_collection()])
            .with_acp(acp)
            .with_mutator(mutator);

        // Non-owner without deleter permission should fail
        let mutation = format!(
            r#"mutation {{ delete_Users(docIDs: ["{}"]) {{ _docID }} }}"#,
            doc_id
        );
        let result = runner
            .execute_mutation_with_identity(&mutation, Some(other))
            .await;

        assert!(
            result.is_err(),
            "non-owner without deleter permission should not be able to delete"
        );
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("permission denied"),
            "error should indicate permission denied"
        );
    }

    #[tokio::test]
    async fn test_acp_delete_with_deleter_relation_succeeds() {
        use acp::{LocalDocumentACP, MemoryAcpStore, DELETER_RELATION};

        let fetcher = MockFetcher::new();
        let store = Arc::new(MemoryAcpStore::new());
        let acp = Arc::new(LocalDocumentACP::new(store));
        let mutator = Arc::new(MockMutator::new());

        let owner = test_acp_did();
        let deleter = test_acp_did2();

        // Create a document with proper DocID
        let mut doc = Document::new();
        doc.set("name", "Alice");
        doc.generate_and_set_doc_id().unwrap();
        let doc_id = doc.id().unwrap().to_string();
        mutator.add_doc("Users", doc);

        // Register doc and grant deleter permission
        acp.register_doc_object(&owner, "policy-acp", "Users", &doc_id)
            .await
            .unwrap();
        acp.add_actor_relationship(&owner, &deleter, "Users", &doc_id, DELETER_RELATION)
            .await
            .unwrap();

        let runner = QueryRunner::new(fetcher, vec![make_acp_collection()])
            .with_acp(acp)
            .with_mutator(mutator);

        // Deleter should be able to delete
        let mutation = format!(
            r#"mutation {{ delete_Users(docIDs: ["{}"]) {{ _docID }} }}"#,
            doc_id
        );
        let result = runner
            .execute_mutation_with_identity(&mutation, Some(deleter))
            .await;

        assert!(
            result.is_ok(),
            "identity with deleter relation should be able to delete: {:?}",
            result.as_ref().err()
        );
    }

    #[tokio::test]
    async fn test_acp_anonymous_update_on_registered_doc_fails() {
        use acp::{LocalDocumentACP, MemoryAcpStore};

        let fetcher = MockFetcher::new();
        let store = Arc::new(MemoryAcpStore::new());
        let acp = Arc::new(LocalDocumentACP::new(store));
        let mutator = Arc::new(MockMutator::new());

        let owner = test_acp_did();

        // Create a document with proper DocID
        let mut doc = Document::new();
        doc.set("name", "Alice");
        doc.generate_and_set_doc_id().unwrap();
        let doc_id = doc.id().unwrap().to_string();
        mutator.add_doc("Users", doc);

        // Register doc with owner
        acp.register_doc_object(&owner, "policy-acp", "Users", &doc_id)
            .await
            .unwrap();

        let runner = QueryRunner::new(fetcher, vec![make_acp_collection()])
            .with_acp(acp)
            .with_mutator(mutator);

        // Anonymous (None identity) should fail to update registered doc
        let mutation = format!(
            r#"mutation {{ update_Users(docIDs: ["{}"], input: {{name: "Hacked"}}) {{ _docID }} }}"#,
            doc_id
        );
        let result = runner.execute_mutation_with_identity(&mutation, None).await;

        assert!(
            result.is_err(),
            "anonymous should not be able to update registered doc"
        );
    }

    #[tokio::test]
    async fn test_acp_mutation_on_public_doc_succeeds() {
        use acp::{LocalDocumentACP, MemoryAcpStore};

        let fetcher = MockFetcher::new();
        let store = Arc::new(MemoryAcpStore::new());
        let acp = Arc::new(LocalDocumentACP::new(store));
        let mutator = Arc::new(MockMutator::new());

        let other = test_acp_did2();

        // Create a document with proper DocID but DON'T register it (public)
        let mut doc = Document::new();
        doc.set("name", "PublicDoc");
        doc.generate_and_set_doc_id().unwrap();
        let doc_id = doc.id().unwrap().to_string();
        mutator.add_doc("Users", doc);

        let runner = QueryRunner::new(fetcher, vec![make_acp_collection()])
            .with_acp(acp)
            .with_mutator(mutator);

        // Anyone can update public (unregistered) documents
        let mutation = format!(
            r#"mutation {{ update_Users(docIDs: ["{}"], input: {{name: "Updated"}}) {{ _docID }} }}"#,
            doc_id
        );
        let result = runner
            .execute_mutation_with_identity(&mutation, Some(other))
            .await;

        assert!(
            result.is_ok(),
            "anyone should be able to update public (unregistered) doc: {:?}",
            result.as_ref().err()
        );
    }
}
