//! Mutation execution methods for QueryRunner.

use acp::DocumentPermission;
use identity::Did;
use serde_json::{Map, Value as JsonValue};
use std::sync::Arc;

use crate::document::{document_to_plan_doc, DocumentMapping};
use crate::error::{QueryError, Result};
use crate::mapper::{Mutation, MutationType};
use crate::mutator::DocMutator;
use crate::plan::{
    CreateInput, CreateNode, DeleteNode, UpdateInput, UpdateNode, UpsertInput, UpsertNode,
};
use crate::planner::PlanNode;
use crate::query_parse::parse_mutations;
use crate::txn::TransactionRegistry;

use super::{DocFetcher, QueryRunner};

impl<F: DocFetcher, R: TransactionRegistry> QueryRunner<F, R> {
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
        use acp::Identity;

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
                                .map_err(|e| {
                                    tracing::warn!(
                                        doc_id = %doc_id,
                                        identity = %identity_for_acp,
                                        error = %e,
                                        "ACP permission check failed during UPDATE - propagating error"
                                    );
                                    QueryError::acp_check_failed("update", doc_id, e)
                                })?;

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
                                .map_err(|e| {
                                    tracing::warn!(
                                        doc_id = %doc_id,
                                        identity = %identity_for_acp,
                                        error = %e,
                                        "ACP permission check failed during DELETE - propagating error"
                                    );
                                    QueryError::acp_check_failed("delete", doc_id, e)
                                })?;

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
                            .map_err(|e| {
                                tracing::warn!(
                                    doc_id = %doc_id,
                                    policy_id = %policy.id,
                                    error = %e,
                                    "Failed to check ACP registration status - propagating error"
                                );
                                QueryError::acp_registration_check_failed(doc_id, e)
                            })?;

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
}
