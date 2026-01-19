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
                        let identity_for_acp = Identity::from(caller_identity.as_ref());
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
                        let identity_for_acp = Identity::from(caller_identity.as_ref());
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

    /// Find the first ACP-protected collection in nested selections.
    ///
    /// This resolves relation field names to actual collection names by looking up
    /// the relation definition in the parent collection's schema.
    ///
    /// Returns Some(collection_name) if an ACP-protected collection is found, None otherwise.
    fn find_acp_collection_in_nested(
        &self,
        select: &Select,
        parent_collection: &CollectionVersion,
    ) -> Option<String> {
        for field in &select.fields {
            if let Requestable::Select(nested) = field {
                // The nested select's collection_name is the field name in the query
                // We need to resolve it to the actual target collection via the relation
                let field_name = &nested.collection_name;

                // Find the relation field in the parent collection
                if let Some(relation_field) = parent_collection.fields.iter().find(|f| &f.name == field_name) {
                    // Get the target collection name from the relation field's kind
                    if let Some(target_coll_name) = relation_field.kind.relation_collection_id() {
                        // Check if target collection has ACP
                        if let Some(target_coll) = self.collections.get(target_coll_name) {
                            if target_coll.policy.is_some() {
                                return Some(target_coll.name.clone());
                            }

                            // Recursively check deeper nested selections
                            if let Some(deep_acp) = self.find_acp_collection_in_nested(nested, target_coll) {
                                return Some(deep_acp);
                            }
                        }
                    }
                }
            }
        }
        None
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

// Tests extracted to crates/query/tests/runner_tests.rs
