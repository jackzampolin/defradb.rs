//! Mutation execution methods for QueryRunner.

use acp::DocumentPermission;
use chrono::{DateTime, FixedOffset, Utc};
use identity::Did;
use serde_json::{Map, Value as JsonValue};
use std::sync::Arc;

use crate::document::{document_to_plan_doc, DocumentMapping};
use crate::error::{QueryError, Result};
use crate::mapper::{Mutation, MutationType, Requestable};
use crate::mutator::DocMutator;
use crate::plan::{
    CreateInput, CreateNode, DeleteNode, UpdateInput, UpdateNode, UpsertInput, UpsertNode,
};
use crate::planner::PlanNode;
use crate::query_parse::parse_mutations_with_variables;
use crate::txn::TransactionRegistry;

use super::{DocFetcher, QueryRunner};

impl<F: DocFetcher + 'static, R: TransactionRegistry> QueryRunner<F, R> {
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
        self.execute_mutation_with_identity_and_vars(mutation_str, caller_identity, None)
            .await
    }

    /// Execute a GraphQL mutation with identity and variables.
    pub async fn execute_mutation_with_identity_and_vars(
        &self,
        mutation_str: &str,
        caller_identity: Option<Did>,
        variables: Option<&std::collections::HashMap<String, JsonValue>>,
    ) -> Result<JsonValue> {
        let mutator = self.mutator.as_ref().ok_or_else(|| {
            QueryError::execution("mutations require a mutator; call with_mutator() first")
        })?;

        self.execute_mutation_internal_with_vars(
            mutation_str,
            mutator.clone(),
            caller_identity,
            variables,
        )
        .await
    }

    /// Execute a GraphQL mutation with a specific mutator, caller_identity, and variables.
    pub(crate) async fn execute_mutation_internal_with_vars(
        &self,
        mutation_str: &str,
        mutator: Arc<dyn DocMutator>,
        caller_identity: Option<Did>,
        variables: Option<&std::collections::HashMap<String, JsonValue>>,
    ) -> Result<JsonValue> {
        let mutations = parse_mutations_with_variables(mutation_str, variables)?;

        // Compute request time once for all mutations in this request.
        // This ensures UTC_NOW resolves to the same timestamp across all mutations,
        // matching Go DefraDB's behavior.
        let utc_offset = FixedOffset::east_opt(0).unwrap();
        let request_time = Utc::now().with_timezone(&utc_offset);

        let mut results = Map::new();

        for mutation in mutations {
            let result = self
                .execute_single_mutation(
                    &mutation,
                    mutator.clone(),
                    caller_identity.clone(),
                    request_time,
                )
                .await?;
            // Use alias if provided, otherwise full mutation name (e.g., "create_Users")
            let key = mutation.output_name();
            results.insert(key, result);
        }

        Ok(JsonValue::Object(results))
    }

    /// Execute a single mutation operation with ACP enforcement.
    async fn execute_single_mutation(
        &self,
        mutation: &Mutation,
        mutator: Arc<dyn DocMutator>,
        caller_identity: Option<Did>,
        request_time: DateTime<FixedOffset>,
    ) -> Result<JsonValue> {
        use acp::Identity;

        // Validate collection exists - resolve on-demand from provider
        let collection = self.get_collection(&mutation.collection_name).await?;

        // Validate encryptFields if present
        // Match Go's validation order: check existence first, then builtin prefix.
        // _docID is in the field list (passes existence, hits builtin check).
        // _version is NOT in the field list (fails existence check).
        for field_name in &mutation.encrypt_fields {
            let exists = collection.fields.iter().any(|f| f.name == *field_name);
            if !exists {
                return Err(QueryError::execution(format!(
                    "the given field does not exist. Name: {}",
                    field_name
                )));
            }
            if field_name.starts_with('_') {
                return Err(QueryError::execution(format!(
                    "can not encrypt build-in field. Name: {}",
                    field_name
                )));
            }
        }

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
                                return Err(QueryError::document_not_found(
                                    "document not found or not authorized to access",
                                ));
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
                                return Err(QueryError::document_not_found(
                                    "document not found or not authorized to access",
                                ));
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

        // Set encryption config for this mutation (thread-local, read by AutoCommitMutator)
        if mutation.encrypt_doc || !mutation.encrypt_fields.is_empty() {
            if let Some(ref key) = self.encryption_key {
                defra_core::encryption::set_encryption_config(Some(
                    defra_core::encryption::EncryptionConfig {
                        encrypt_doc: mutation.encrypt_doc,
                        encrypt_fields: mutation.encrypt_fields.clone(),
                        encryption_key: key.clone(),
                    },
                ));
            }
        } else {
            defra_core::encryption::set_encryption_config(None);
        }

        // Build and execute the appropriate mutation plan
        let mut plan: Box<dyn PlanNode> = match mutation.mutation_type {
            MutationType::Create => {
                let inputs = self.build_create_inputs(mutation)?;
                Box::new(
                    CreateNode::new(&mutation.collection_name, mutator, mapping.clone())
                        .with_collection(collection.clone())
                        .with_request_time(request_time)
                        .with_inputs(inputs),
                )
            }
            MutationType::Update => {
                let input = self.build_update_input(mutation)?;
                let fetcher: Arc<dyn crate::fetcher::DocFetcher> = self.fetcher.clone();
                let mut node =
                    UpdateNode::new(&mutation.collection_name, mutator, fetcher, mapping.clone())
                        .with_collection(collection.clone())
                        .with_request_time(request_time)
                        .with_input(input);

                // Use resolved doc_ids (from filter) or original doc_ids
                if let Some(ref doc_ids) = resolved_doc_ids {
                    node = node.with_doc_ids(doc_ids.clone());
                } else if let Some(ref doc_ids) = mutation.doc_ids {
                    node = node.with_doc_ids(doc_ids.clone());
                }

                // Always pass filter: for node-level resolution when no doc_ids,
                // and for re-filtering results after update (Go compatibility)
                if let Some(ref filter) = mutation.filter {
                    node = node.with_filter(filter.clone());
                }

                Box::new(node)
            }
            MutationType::Delete => {
                let fetcher: Arc<dyn crate::fetcher::DocFetcher> = self.fetcher.clone();
                let mut node =
                    DeleteNode::new(&mutation.collection_name, mutator, fetcher, mapping.clone());

                // Use resolved doc_ids (from filter) or original doc_ids
                if let Some(ref doc_ids) = resolved_doc_ids {
                    node = node.with_doc_ids(doc_ids.clone());
                } else if let Some(ref doc_ids) = mutation.doc_ids {
                    node = node.with_doc_ids(doc_ids.clone());
                }

                // Pass through filter for node-level resolution when no doc_ids resolved
                if mutation.filter.is_some()
                    && resolved_doc_ids.is_none()
                    && mutation.doc_ids.is_none()
                {
                    node = node.with_filter(mutation.filter.clone().unwrap());
                }

                Box::new(node)
            }
            MutationType::Upsert => {
                let mut node = UpsertNode::new(&mutation.collection_name, mutator, mapping.clone())
                    .with_collection(collection.clone())
                    .with_request_time(request_time);

                // Set create_input (from Go's 'create' argument)
                if !mutation.create_input.is_empty() {
                    let create_input =
                        self.build_upsert_input_from_map(&mutation.create_input[0])?;
                    node = node.with_create_input(create_input);
                }

                // Set update_input (from Go's 'update' argument)
                if !mutation.update_input.is_empty() {
                    let update_input = self.build_upsert_input_from_map(&mutation.update_input)?;
                    node = node.with_update_input(update_input);
                }

                // Use resolved doc_ids (from filter) or original doc_ids
                if let Some(ref doc_ids) = resolved_doc_ids {
                    node = node.with_doc_ids(doc_ids.clone());
                } else if let Some(ref doc_ids) = mutation.doc_ids {
                    node = node.with_doc_ids(doc_ids.clone());
                }

                Box::new(node)
            }
        };

        // Execute the plan.
        // When ACP is active, wrap DocumentNotFound errors to match Go's generic message
        // (security best practice: don't reveal whether document exists vs unauthorized).
        let has_acp = self.acp.is_some() && collection.policy.is_some();
        let map_doc_not_found = |e: QueryError| -> QueryError {
            if has_acp {
                match &e {
                    QueryError::DocumentNotFound(_) => {
                        return QueryError::document_not_found(
                            "document not found or not authorized to access",
                        );
                    }
                    QueryError::Execution(msg) if msg.contains("document not found:") => {
                        return QueryError::document_not_found(
                            "document not found or not authorized to access",
                        );
                    }
                    _ => {}
                }
            }
            e
        };

        plan.init().await.map_err(&map_doc_not_found)?;
        plan.start().await.map_err(&map_doc_not_found)?;

        let mut results = Vec::new();

        while plan.next().await.map_err(&map_doc_not_found)? {
            let doc = plan.value();
            let json = self.doc_to_json(doc, &mapping)?;
            results.push(json);
        }

        // Clear encryption config after plan execution
        defra_core::encryption::set_encryption_config(None);

        plan.close().await.map_err(&map_doc_not_found)?;

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

        // For DELETE operations: unregister deleted docs from ACP
        // This cleans up ACP state to prevent orphaned registrations
        if matches!(mutation.mutation_type, MutationType::Delete) {
            if let (Some(ref acp), Some(ref policy)) = (&self.acp, &collection.policy) {
                for result in &results {
                    if let Some(doc_id) = result.get("_docID").and_then(|v| v.as_str()) {
                        // Best-effort unregistration - log failures but don't fail the delete
                        // The document is already deleted from storage; ACP cleanup failure
                        // leaves orphaned metadata but doesn't affect data integrity
                        if let Err(e) = acp
                            .unregister_doc_object(&policy.id, &policy.resource_name, doc_id)
                            .await
                        {
                            tracing::warn!(
                                doc_id = %doc_id,
                                policy_id = %policy.id,
                                error = %e,
                                "Failed to unregister deleted document from ACP - orphaned registration may remain"
                            );
                        } else {
                            tracing::debug!(
                                doc_id = %doc_id,
                                policy_id = %policy.id,
                                "Document unregistered from ACP after deletion"
                            );
                        }
                    }
                }
            }
        }

        // Enrich results with _version data if requested.
        // _version is a nested select (Requestable::Select) that returns commit history.
        // This must happen after ACP blocks which also need _docID.
        let version_select = mutation.fields.iter().find_map(|r| {
            if let Requestable::Select(s) = r {
                if s.field.name == "_version" {
                    return Some(s.as_ref());
                }
            }
            None
        });

        if let Some(version_sel) = version_select {
            let fetcher: &dyn crate::fetcher::DocFetcher = self.fetcher.as_ref();
            let output_name = version_sel.field.output_name().to_string();
            let docid_explicitly_requested = mutation
                .requested_fields()
                .iter()
                .any(|f| f.name == "_docID");

            for result in &mut results {
                if let JsonValue::Object(ref mut obj) = result {
                    if let Some(doc_id) =
                        obj.get("_docID").and_then(|v| v.as_str()).map(String::from)
                    {
                        let version_data = self
                            .fetch_version_data(fetcher, &doc_id, version_sel, None)
                            .await?;
                        obj.insert(output_name.clone(), version_data);
                    }

                    // Remove _docID from output if it was only added internally for version lookup
                    if !docid_explicitly_requested {
                        obj.remove("_docID");
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
    pub(crate) async fn resolve_filter_to_doc_ids(&self, mutation: &Mutation) -> Result<Option<Vec<String>>> {
        // Only resolve if there's a filter but no explicit doc_ids
        let filter = match (&mutation.filter, &mutation.doc_ids) {
            (Some(filter), None) => filter,
            _ => return Ok(None),
        };

        // Get the collection schema on-demand from provider
        let collection = self.get_collection(&mutation.collection_name).await?;

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
    pub(crate) fn build_mutation_mapping(&self, mutation: &Mutation) -> Result<DocumentMapping> {
        let mut mapping = DocumentMapping::new();

        // Always reserve index 0 for _docID (matches Go DefraDB DocumentMapping pattern).
        // This ensures set_doc_id() at index 0 doesn't collide with requested field values.
        mapping.add(0, "_docID");

        // Add requested fields (starting at index 1+ since 0 is reserved for _docID)
        let mut has_docid_render = false;
        for field in mutation.requested_fields() {
            if field.name == "_docID" {
                // _docID is already at index 0, just add render key
                mapping.add_render_key(0, field.output_name());
                has_docid_render = true;
                continue;
            }
            let index = mapping.next_index();
            mapping.add(index, &field.name);
            mapping.add_render_key(index, field.output_name());
        }

        // When _version is requested, ensure _docID is always rendered
        // (needed to look up version/commit data for each document)
        let has_version = mutation.fields.iter().any(|r| {
            matches!(r, Requestable::Select(s) if s.field.name == "_version")
        });
        if has_version && !has_docid_render {
            mapping.add_render_key(0, "_docID");
        }

        // If no fields explicitly requested, render _docID by default
        if mapping.render_keys.is_empty() {
            mapping.add_render_key(0, "_docID");
        }

        Ok(mapping)
    }

    /// Build CreateInput objects from mutation input.
    pub(crate) fn build_create_inputs(&self, mutation: &Mutation) -> Result<Vec<CreateInput>> {
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
    pub(crate) fn build_update_input(&self, mutation: &Mutation) -> Result<UpdateInput> {
        let mut update_input = UpdateInput::new();

        for (field_name, value) in &mutation.update_input {
            update_input = update_input.with_field(field_name.clone(), value.clone());
        }

        Ok(update_input)
    }

    /// Build UpsertInput from a field-value map.
    pub(crate) fn build_upsert_input_from_map(
        &self,
        input: &std::collections::HashMap<String, JsonValue>,
    ) -> Result<UpsertInput> {
        let mut upsert_input = UpsertInput::new();

        for (field_name, value) in input {
            upsert_input = upsert_input.with_field(field_name.clone(), value.clone());
        }

        Ok(upsert_input)
    }
}
