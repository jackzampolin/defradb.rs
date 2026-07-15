//! Mutation execution methods for QueryRunner.

use acp::DocumentPermission;
use chrono::{DateTime, FixedOffset, Utc};
use identity::Did;
use serde_json::{Map, Value as JsonValue};
use std::{sync::Arc, time::Instant};

use crate::error::{QueryError, Result};
use crate::mapper::{Mutation, MutationType, Requestable};
use crate::mutator::DocMutator;
use crate::plan::{CreateNode, DeleteNode, UpdateNode, UpsertNode};
use crate::planner::PlanNode;
use crate::query_parse::parse_mutations_with_limits;
use crate::txn::TransactionRegistry;

use super::{DocFetcher, QueryRunner};

/// RAII guard that clears the `encryption_config` thread-local on Drop.
///
/// `mutation.rs` sets the thread-local during execution. Without this
/// guard, an early-return via `?` (or a panic unwind) leaves stale
/// state on the blocking-pool worker thread, which the next
/// `spawn_blocking` closure inherits — silent encryption-key confusion
/// across requests. See #757.
///
/// Always clears to None on Drop, regardless of entry state. The
/// function "owns" the mutation context for the duration of the call
/// and leaves the worker thread in a clean state on exit (happy path,
/// early-return via `?`, or panic unwind). This also self-heals against
/// any previous task that may have leaked state.
struct EncryptionConfigGuard;

impl Drop for EncryptionConfigGuard {
    fn drop(&mut self) {
        defra_core::encryption::set_encryption_config(None);
    }
}

/// RAII guard that clears the `broadcast_creator_did` thread-local on Drop.
///
/// Critical for P2P identity correctness — without this, a panicking
/// mutation can leave the previous caller's DID on the blocking-pool
/// worker, and the next anonymous mutation would silently broadcast
/// with someone else's identity (registered as that identity on the
/// receiving peer via `acp_merge_handler`). See #757.
struct BroadcastCreatorDidGuard;

impl Drop for BroadcastCreatorDidGuard {
    fn drop(&mut self) {
        defra_core::signing::set_broadcast_creator_did(None);
    }
}

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
            None,
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
        fetcher_override: Option<Arc<dyn crate::fetcher::DocFetcher>>,
    ) -> Result<JsonValue> {
        let mutations = parse_mutations_with_limits(mutation_str, variables, self.query_limits)?;
        self.execute_parsed_mutations(mutations, mutator, caller_identity, fetcher_override)
            .await
    }

    /// Execute pre-parsed mutations, skipping redundant GraphQL parsing.
    pub(crate) async fn execute_parsed_mutations(
        &self,
        mutations: Vec<Mutation>,
        mutator: Arc<dyn DocMutator>,
        caller_identity: Option<Did>,
        fetcher_override: Option<Arc<dyn crate::fetcher::DocFetcher>>,
    ) -> Result<JsonValue> {
        // Compute request time once for all mutations in this request.
        // This ensures UTC_NOW resolves to the same timestamp across all mutations,
        // matching Go DefraDB's behavior.
        let utc_offset = FixedOffset::east_opt(0).unwrap();
        let request_time = Utc::now().with_timezone(&utc_offset);

        // Batch implicit multi-mutation requests when the mutator supports it.
        //
        // Explicit `/tx` requests already provide their own shared transaction
        // through `fetcher_override`. Requests touching policy-backed collections
        // still use the legacy per-mutation path because ACP post-write side
        // effects are applied outside the storage transaction.
        let has_policy_backed_mutation = if fetcher_override.is_none() {
            let mut has_policy = false;
            for mutation in &mutations {
                match self.get_collection(&mutation.collection_name).await {
                    Ok(collection) => {
                        if collection.policy.is_some() {
                            has_policy = true;
                            break;
                        }
                    }
                    Err(QueryError::CollectionNotFound(_)) => {}
                    Err(err) => return Err(err),
                }
            }
            has_policy
        } else {
            false
        };

        if mutations.len() > 1 && fetcher_override.is_none() && !has_policy_backed_mutation {
            if let Some(batch) = mutator.begin_batch().await? {
                let batch_mutator = batch.mutator();
                let batch_fetcher = batch.fetcher();
                let mut results = Map::new();

                for mutation in mutations {
                    let result = match self
                        .execute_single_mutation(
                            &mutation,
                            batch_mutator.clone(),
                            caller_identity.clone(),
                            request_time,
                            Some(batch_fetcher.clone()),
                        )
                        .await
                    {
                        Ok(result) => result,
                        Err(err) => {
                            if let Err(rollback_err) = batch.rollback().await {
                                tracing::warn!(
                                    error = %rollback_err,
                                    "Failed to rollback implicit mutation batch"
                                );
                            }
                            return Err(err);
                        }
                    };

                    results.insert(mutation.output_name(), result);
                }

                batch.commit().await?;
                return Ok(JsonValue::Object(results));
            }
        }

        let mut results = Map::new();

        for mutation in mutations {
            let result = self
                .execute_single_mutation(
                    &mutation,
                    mutator.clone(),
                    caller_identity.clone(),
                    request_time,
                    fetcher_override.clone(),
                )
                .await?;
            // Use alias if provided, otherwise full mutation name (e.g., "add_Users")
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
        fetcher_override: Option<Arc<dyn crate::fetcher::DocFetcher>>,
    ) -> Result<JsonValue> {
        struct SigningConfigReset(Option<defra_core::signing::SigningConfig>);

        impl Drop for SigningConfigReset {
            fn drop(&mut self) {
                defra_core::signing::set_signing_config(self.0.clone());
            }
        }

        let fetcher: Arc<dyn crate::fetcher::DocFetcher> =
            fetcher_override.unwrap_or_else(|| self.fetcher.clone());
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
        let resolved_doc_ids = self
            .resolve_filter_to_doc_ids(mutation, fetcher.as_ref())
            .await?;

        // Get doc_ids for permission checking (UPDATE/DELETE need this)
        let doc_ids_for_check = resolved_doc_ids
            .as_ref()
            .or(mutation.doc_ids.as_ref())
            .cloned();

        // Two-phase ACP permission check for UPDATE/DELETE operations.
        //
        // Phase 1: Check READ permission. If denied, the document is invisible to
        // this identity -- silently remove it from the target set (empty results, no error).
        // Phase 2: Check UPDATE/DELETE permission. If denied but readable, the document
        // is visible but unauthorized -- return an error.
        //
        // This matches Go's behavior where GQL mutations on invisible documents return
        // empty results, while mutations on visible-but-unauthorized documents return errors.
        let mut acp_filtered_doc_ids: Option<Vec<String>> = None;
        if let Some(ref policy) = collection.policy {
            match mutation.mutation_type {
                MutationType::Update | MutationType::Upsert => {
                    if let Some(ref doc_ids) = doc_ids_for_check {
                        let identity_for_acp = Identity::from(caller_identity.as_ref());
                        let mut visible_doc_ids = Vec::new();
                        for doc_id in doc_ids {
                            // Phase 1: Check if the identity can read the document
                            let can_read = crate::txn::check_doc_access_with_overlay(
                                self.acp.as_ref(),
                                &identity_for_acp,
                                DocumentPermission::Read,
                                &policy.id,
                                &policy.resource_name,
                                doc_id,
                                None,
                            )
                            .await
                            .unwrap_or(false);

                            if !can_read {
                                // Document is invisible to this identity -- skip silently
                                continue;
                            }

                            // Phase 2: Check if the identity can update the document
                            let can_update = crate::txn::check_doc_access_with_overlay(
                                self.acp.as_ref(),
                                &identity_for_acp,
                                DocumentPermission::Update,
                                &policy.id,
                                &policy.resource_name,
                                doc_id,
                                None,
                            )
                            .await
                            .map_err(|e| QueryError::acp_check_failed("update", doc_id, e))?;

                            if !can_update {
                                return Err(QueryError::document_not_found(
                                    "document not found or not authorized to access",
                                ));
                            }
                            visible_doc_ids.push(doc_id.clone());
                        }
                        acp_filtered_doc_ids = Some(visible_doc_ids);
                    }
                }
                MutationType::Delete => {
                    if let Some(ref doc_ids) = doc_ids_for_check {
                        let identity_for_acp = Identity::from(caller_identity.as_ref());
                        let mut visible_doc_ids = Vec::new();
                        for doc_id in doc_ids {
                            // Phase 1: Check if the identity can read the document
                            let can_read = crate::txn::check_doc_access_with_overlay(
                                self.acp.as_ref(),
                                &identity_for_acp,
                                DocumentPermission::Read,
                                &policy.id,
                                &policy.resource_name,
                                doc_id,
                                None,
                            )
                            .await
                            .unwrap_or(false);

                            if !can_read {
                                // Document is invisible to this identity -- skip silently
                                continue;
                            }

                            // Phase 2: Check if the identity can delete the document
                            let can_delete = crate::txn::check_doc_access_with_overlay(
                                self.acp.as_ref(),
                                &identity_for_acp,
                                DocumentPermission::Delete,
                                &policy.id,
                                &policy.resource_name,
                                doc_id,
                                None,
                            )
                            .await
                            .map_err(|e| QueryError::acp_check_failed("delete", doc_id, e))?;

                            if !can_delete {
                                return Err(QueryError::document_not_found(
                                    "document not found or not authorized to access",
                                ));
                            }
                            visible_doc_ids.push(doc_id.clone());
                        }
                        acp_filtered_doc_ids = Some(visible_doc_ids);
                    }
                }
                MutationType::Create => {
                    // CREATE permission is checked implicitly -- anyone can create
                    // but ownership is established via registration after the write.
                }
            }
        }

        // Bind a RAII guard so the thread-local is cleared on every exit
        // path (including `?`, panic unwind, and the happy path). See #757.
        let _encryption_config_guard = EncryptionConfigGuard;

        // Set encryption config for this mutation (thread-local, read by AutoCommitMutator)
        if mutation.encrypt_doc || !mutation.encrypt_fields.is_empty() {
            defra_core::encryption::set_encryption_config(Some(
                defra_core::encryption::EncryptionConfig {
                    encrypt_doc: mutation.encrypt_doc,
                    encrypt_fields: mutation.encrypt_fields.clone(),
                },
            ));
        } else {
            defra_core::encryption::set_encryption_config(None);
        }

        // Bind a RAII guard for broadcast_creator_did. Critical for P2P
        // identity correctness — without this, a panicking mutation can
        // leave the previous caller's DID on the blocking-pool worker,
        // and the next anonymous mutation would silently broadcast with
        // someone else's identity. See #757.
        let _broadcast_creator_did_guard = BroadcastCreatorDidGuard;

        // Scope the ambient acting identity to the caller for the duration of
        // this mutation so DB-layer NAC checks can resolve who is acting. Only
        // override when there IS a caller identity; when absent, leave the
        // ambient untouched so an embedded node's outer node DID survives across
        // multiple mutations in one execute(). Restores the prior value on drop.
        let _current_identity_guard = caller_identity.as_ref().map(|did| {
            defra_core::current_identity::scoped_current_identity(Some(did.to_string()))
        });

        // Set broadcast identity for P2P: PushLog Creator field will carry this
        // DID instead of the node PeerId, enabling ACP owner registration on the
        // receiving node during merge.
        if let Some(ref did) = caller_identity {
            defra_core::signing::set_broadcast_creator_did(Some(did.to_string()));
        }

        let _signing_config_reset = if matches!(mutation.mutation_type, MutationType::Create) {
            let current_signing_config = defra_core::signing::get_signing_config();
            match current_signing_config.clone() {
                Some(mut config) if config.remote_signer.is_some() => {
                    tracing::info!(
                        collection = %mutation.collection_name,
                        caller_identity = ?caller_identity.as_ref().map(|d| d.to_string()),
                        policy_id = ?collection.policy.as_ref().map(|p| p.id.clone()),
                        resource = ?collection.policy.as_ref().map(|p| p.resource_name.clone()),
                        "create mutation entering remote signer authorization path"
                    );
                    if let (Some(identity_did), Some(policy)) =
                        (caller_identity.as_ref(), collection.policy.as_ref())
                    {
                        tracing::info!(
                            actor_did = %identity_did,
                            policy_id = %policy.id,
                            resource = %policy.resource_name,
                            object_id = %policy.resource_name,
                            permission = "writer",
                            "create mutation requesting access decision"
                        );
                        let decision_id = self
                            .acp
                            .create_access_decision(
                                &Identity::from(identity_did.clone()),
                                &policy.id,
                                &policy.resource_name,
                                &policy.resource_name,
                                "writer",
                            )
                            .await
                            .map_err(|e| {
                                QueryError::permission_denied(format!(
                                    "create authorization failed: {}",
                                    e
                                ))
                            })?;

                        tracing::info!(
                            actor_did = %identity_did,
                            policy_id = %policy.id,
                            resource = %policy.resource_name,
                            object_id = %policy.resource_name,
                            permission = "writer",
                            decision_id = ?decision_id,
                            "create mutation access decision result"
                        );
                        if let Some(decision_id) = decision_id {
                            config.signing_authorization =
                                Some(defra_core::signing::SigningAuthorization::Decision {
                                    decision_id,
                                });
                            defra_core::signing::set_signing_config(Some(config));
                            Some(SigningConfigReset(current_signing_config))
                        } else {
                            tracing::warn!(
                                collection = %mutation.collection_name,
                                "create mutation received no access decision for remote signer"
                            );
                            None
                        }
                    } else {
                        tracing::warn!(
                            collection = %mutation.collection_name,
                            has_acp = true,
                            has_caller_identity = caller_identity.is_some(),
                            has_policy = collection.policy.is_some(),
                            "create mutation skipped remote signer authorization due to missing prerequisites"
                        );
                        None
                    }
                }
                Some(_) => {
                    tracing::info!(
                        collection = %mutation.collection_name,
                        "create mutation using local signer; remote signer authorization skipped"
                    );
                    None
                }
                None => {
                    // No signing config is the normal case for nodes that don't
                    // run with `--signer-type=...`. There is nothing to authorize
                    // and nothing actionable for an operator to do, so this stays
                    // at debug. The two `warn!` arms above remain warnings because
                    // they signal a *partially* configured remote-signer setup.
                    tracing::debug!(
                        collection = %mutation.collection_name,
                        "create mutation has no signing config; remote signer authorization skipped"
                    );
                    None
                }
            }
        } else {
            None
        };

        // Build and execute the appropriate mutation plan
        let mut plan: Box<dyn PlanNode> = match mutation.mutation_type {
            MutationType::Create => {
                let inputs = self.build_create_inputs(mutation, &collection)?;
                Box::new(
                    CreateNode::new(&mutation.collection_name, mutator, mapping.clone())
                        .with_collection(collection.clone())
                        .with_request_time(request_time)
                        .with_inputs(inputs),
                )
            }
            MutationType::Update => {
                let input = self.build_update_input(mutation, &collection)?;
                let mut node = UpdateNode::new(
                    &mutation.collection_name,
                    mutator,
                    fetcher.clone(),
                    mapping.clone(),
                )
                .with_collection(collection.clone())
                .with_request_time(request_time)
                .with_input(input);

                // Use ACP-filtered doc_ids (invisible docs removed), or resolved/original
                if let Some(ref doc_ids) = acp_filtered_doc_ids {
                    node = node.with_doc_ids(doc_ids.clone());
                } else if let Some(ref doc_ids) = resolved_doc_ids {
                    node = node.with_doc_ids(doc_ids.clone());
                } else if let Some(ref doc_ids) = mutation.doc_ids {
                    node = node.with_doc_ids(doc_ids.clone());
                }

                // Pass the filter only when the node still needs to resolve it.
                // If it was already resolved to doc IDs above, the filter must
                // not be applied again to post-update document state.
                if resolved_doc_ids.is_none() && mutation.doc_ids.is_none() {
                    if let Some(ref filter) = mutation.filter {
                        node = node.with_filter(filter.clone());
                    }
                }

                Box::new(node)
            }
            MutationType::Delete => {
                let mut node = DeleteNode::new(
                    &mutation.collection_name,
                    mutator,
                    fetcher.clone(),
                    mapping.clone(),
                );

                // Use ACP-filtered doc_ids (invisible docs removed), or resolved/original
                if let Some(ref doc_ids) = acp_filtered_doc_ids {
                    node = node.with_doc_ids(doc_ids.clone());
                } else if let Some(ref doc_ids) = resolved_doc_ids {
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

                // Set create_input (from Go's 'add' argument)
                if !mutation.create_input.is_empty() {
                    let create_input =
                        self.build_upsert_input_from_map(&collection, &mutation.create_input[0])?;
                    node = node.with_create_input(create_input);
                }

                // Set update_input (from Go's 'update' argument)
                if !mutation.update_input.is_empty() {
                    let update_input =
                        self.build_upsert_input_from_map(&collection, &mutation.update_input)?;
                    node = node.with_update_input(update_input);
                }

                // Use ACP-filtered doc_ids (invisible docs removed), or resolved/original
                if let Some(ref doc_ids) = acp_filtered_doc_ids {
                    node = node.with_doc_ids(doc_ids.clone());
                } else if let Some(ref doc_ids) = resolved_doc_ids {
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
        let has_acp = collection.policy.is_some();
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

        let plan_execution_start = Instant::now();
        plan.init().await.map_err(&map_doc_not_found)?;
        plan.start().await.map_err(&map_doc_not_found)?;

        let mut results = Vec::new();
        let mut result_doc_ids = Vec::new();

        while plan.next().await.map_err(&map_doc_not_found)? {
            let doc = plan.value();
            if let Some(doc_id) = doc.doc_id() {
                result_doc_ids.push(doc_id.to_string());
            }
            let json = self.doc_to_json(doc, &mapping)?;
            results.push(json);
        }

        plan.close().await.map_err(&map_doc_not_found)?;

        // Note: encryption_config and broadcast_creator_did are cleared
        // automatically by the RAII guards declared above when this
        // function returns. The bearer token store stays alive — the
        // ACP registration block below needs it for hub.rs auth.

        let plan_execution_elapsed = plan_execution_start.elapsed();
        for doc_id in &result_doc_ids {
            tracing::info!(
                doc_id = %doc_id,
                elapsed = ?plan_execution_elapsed,
                "Mutation plan execution completed"
            );
        }

        // For CREATE/UPSERT operations with caller_identity: register created docs with ACP
        if matches!(
            mutation.mutation_type,
            MutationType::Create | MutationType::Upsert
        ) {
            if let (Some(ref policy), Some(ref identity_did)) =
                (&collection.policy, &caller_identity)
            {
                for result in &results {
                    if let Some(doc_id) = result.get("_docID").and_then(|v| v.as_str()) {
                        // Check if document is already registered (for upsert of existing doc)
                        let is_registered = crate::txn::is_doc_registered_with_overlay(
                            self.acp.as_ref(),
                            &policy.id,
                            &policy.resource_name,
                            doc_id,
                        )
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
                            if let Some(deferred_acp_mutations) =
                                crate::txn::current_deferred_acp_mutations()
                            {
                                let request_bearer_token =
                                    defra_core::signing::get_request_bearer_token(
                                        identity_did.as_str(),
                                    );
                                deferred_acp_mutations
                                    .schedule_register_doc_object(
                                        self.acp.clone(),
                                        identity_did.clone(),
                                        policy.id.clone(),
                                        policy.resource_name.clone(),
                                        doc_id.to_string(),
                                        request_bearer_token,
                                    )
                                    .map_err(QueryError::execution)?;
                            } else {
                                let acp_registration_start = Instant::now();
                                self.acp
                                    .register_doc_object(
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
                                tracing::info!(
                                    doc_id = %doc_id,
                                    elapsed = ?acp_registration_start.elapsed(),
                                    "ACP document registration completed"
                                );
                            }
                        }
                    }
                }
            }
        }

        // For DELETE operations: unregister deleted docs from ACP
        // This cleans up ACP state to prevent orphaned registrations
        if matches!(mutation.mutation_type, MutationType::Delete) {
            if let Some(ref policy) = collection.policy {
                let request_bearer_token = caller_identity
                    .as_ref()
                    .and_then(|did| defra_core::signing::get_request_bearer_token(did.as_str()));
                for result in &results {
                    if let Some(doc_id) = result.get("_docID").and_then(|v| v.as_str()) {
                        // Best-effort unregistration - log failures but don't fail the delete
                        // The document is already deleted from storage; ACP cleanup failure
                        // leaves orphaned metadata but doesn't affect data integrity
                        if let Some(deferred_acp_mutations) =
                            crate::txn::current_deferred_acp_mutations()
                        {
                            if let Err(err) = deferred_acp_mutations.schedule_unregister_doc_object(
                                self.acp.clone(),
                                policy.id.clone(),
                                policy.resource_name.clone(),
                                doc_id.to_string(),
                                caller_identity.clone(),
                                request_bearer_token.clone(),
                            ) {
                                tracing::warn!(
                                    doc_id = %doc_id,
                                    policy_id = %policy.id,
                                    error = %err,
                                    "Failed to defer ACP unregister for deleted document"
                                );
                            }
                        } else if let Err(e) = self
                            .acp
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

        // Clear request bearer token after all ACP post-write work is queued or applied.
        if let Some(ref identity_did) = caller_identity {
            defra_core::signing::clear_request_bearer_token(identity_did.as_str());
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
            let fetcher: &dyn crate::fetcher::DocFetcher = fetcher.as_ref();
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
                            .fetch_version_data(
                                fetcher,
                                &doc_id,
                                version_sel,
                                &collection.collection_id,
                                None,
                            )
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

        // Enrich results with relation sub-select data if requested.
        //
        // Mutation plan nodes only return flat document fields. When the
        // mutation selection set includes relation sub-selects (e.g.,
        // `update_Author { name published { name } }`), we re-query each
        // result document through the query engine to resolve the joins.
        let relation_selects: Vec<&crate::mapper::Select> = mutation
            .fields
            .iter()
            .filter_map(|r| {
                if let Requestable::Select(s) = r {
                    if s.field.name != "_version" {
                        return Some(s.as_ref());
                    }
                }
                None
            })
            .collect();

        if !relation_selects.is_empty() {
            let docid_explicitly_requested = mutation
                .requested_fields()
                .iter()
                .any(|f| f.name == "_docID");

            for result in &mut results {
                if let JsonValue::Object(ref mut obj) = result {
                    if let Some(doc_id) =
                        obj.get("_docID").and_then(|v| v.as_str()).map(String::from)
                    {
                        // Build a GQL query to fetch relation data for this document
                        let mut relation_fields = String::new();
                        for sel in &relation_selects {
                            relation_fields.push(' ');
                            render_select_to_gql(sel, &mut relation_fields);
                        }

                        let query = format!(
                            "query {{ {}(docID: \"{}\") {{{} }} }}",
                            mutation.collection_name, doc_id, relation_fields
                        );

                        if let Ok(query_result) = self
                            .execute_query_internal(
                                &query,
                                fetcher.as_ref(),
                                caller_identity.clone(),
                            )
                            .await
                        {
                            if let Some(docs) = query_result
                                .get(&mutation.collection_name)
                                .and_then(|v| v.as_array())
                            {
                                if let Some(JsonValue::Object(fetched)) = docs.first() {
                                    for (key, value) in fetched {
                                        obj.insert(key.clone(), value.clone());
                                    }
                                }
                            }
                        }
                    }

                    if !docid_explicitly_requested {
                        obj.remove("_docID");
                    }
                }
            }
        }

        Ok(JsonValue::Array(results))
    }
}

/// Render a Select sub-select back to GQL selection syntax.
fn render_select_to_gql(select: &crate::mapper::Select, out: &mut String) {
    out.push_str(select.field.output_name());
    if !select.fields.is_empty() {
        out.push_str(" { ");
        for child in &select.fields {
            match child {
                Requestable::Field(f) => {
                    out.push_str(f.output_name());
                    out.push(' ');
                }
                Requestable::Select(s) => {
                    render_select_to_gql(s, out);
                    out.push(' ');
                }
                _ => {}
            }
        }
        out.push('}');
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use document::{DocID, Document};
    use schema::{CollectionVersion, FieldDescription, FieldKind};
    use std::collections::HashSet;
    use std::sync::Mutex;

    use crate::mutator::{CreateResult, DeleteResult, UpdateResult};
    use crate::test_utils::MockFetcher;
    use crate::{QueryExecutor, QueryRequest};

    struct CapturingMutator {
        created_docs: Mutex<Vec<Document>>,
    }

    impl CapturingMutator {
        fn new() -> Self {
            Self {
                created_docs: Mutex::new(Vec::new()),
            }
        }

        fn created_docs(&self) -> Vec<Document> {
            self.created_docs.lock().unwrap().clone()
        }
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
    impl crate::mutator::DocMutator for CapturingMutator {
        async fn create(&self, _collection_name: &str, mut doc: Document) -> Result<CreateResult> {
            if doc.id().is_none() {
                // Stand in for the genesis-CID identity a real create would
                // derive: seed a valid DocID from the capture order.
                let seq = self.created_docs.lock().unwrap().len();
                doc.set_id(document::DocID::new_v0_from_seed(&format!("test-doc-{seq}")));
            }
            self.created_docs.lock().unwrap().push(doc.clone());
            Ok(CreateResult::new(doc.id().unwrap().clone(), doc))
        }

        async fn update(
            &self,
            _collection_name: &str,
            doc: Document,
            modified_fields: HashSet<String>,
        ) -> Result<UpdateResult> {
            Ok(UpdateResult::new(doc, modified_fields.len()))
        }

        async fn delete(&self, _collection_name: &str, doc_id: &DocID) -> Result<DeleteResult> {
            Ok(DeleteResult::new(doc_id.clone(), true))
        }

        async fn exists(&self, _collection_name: &str, _doc_id: &DocID) -> Result<bool> {
            Ok(false)
        }

        async fn get_for_update(
            &self,
            _collection_name: &str,
            _doc_id: &DocID,
        ) -> Result<Option<Document>> {
            Ok(None)
        }
    }

    // =========================================================================
    // RAII guard tests for #757
    //
    // These tests verify that the EncryptionConfigGuard and
    // BroadcastCreatorDidGuard always clear their respective thread-locals
    // on Drop, regardless of how the owning function exits. They are NOT
    // #[tokio::test] because the entire point is to exercise thread-local
    // semantics on a single thread synchronously.
    // =========================================================================

    fn make_test_encryption_config() -> defra_core::encryption::EncryptionConfig {
        defra_core::encryption::EncryptionConfig {
            encrypt_doc: true,
            encrypt_fields: vec![],
        }
    }

    #[test]
    fn encryption_config_guard_clears_on_normal_drop() {
        // Pre-condition: ensure clean state
        defra_core::encryption::set_encryption_config(None);

        // Set state and bind guard
        defra_core::encryption::set_encryption_config(Some(make_test_encryption_config()));
        assert!(defra_core::encryption::get_encryption_config().is_some());

        {
            let _guard = EncryptionConfigGuard;
            // Inside the guard scope the state is still set.
            assert!(defra_core::encryption::get_encryption_config().is_some());
            // Guard drops at end of scope.
        }

        // After guard drop the thread-local is cleared.
        assert!(defra_core::encryption::get_encryption_config().is_none());
    }

    #[test]
    fn encryption_config_guard_clears_on_panic_unwind() {
        defra_core::encryption::set_encryption_config(None);
        defra_core::encryption::set_encryption_config(Some(make_test_encryption_config()));

        // Drive the guard's drop via a panic in a catch_unwind boundary,
        // simulating an early-return from a panicking mutation.
        let _ = std::panic::catch_unwind(|| {
            let _guard = EncryptionConfigGuard;
            panic!("simulated mutation panic");
        });

        // Even after a panic unwind, the guard cleared the thread-local.
        assert!(defra_core::encryption::get_encryption_config().is_none());
    }

    #[test]
    fn encryption_config_guard_self_heals_from_leaked_state() {
        // Simulate a previous task that leaked stale state on this thread.
        defra_core::encryption::set_encryption_config(Some(make_test_encryption_config()));
        assert!(defra_core::encryption::get_encryption_config().is_some());

        // Even if the guard is bound without us setting fresh state, on
        // Drop it clears the thread-local — self-healing.
        {
            let _guard = EncryptionConfigGuard;
        }

        assert!(defra_core::encryption::get_encryption_config().is_none());
    }

    #[test]
    fn broadcast_creator_did_guard_clears_on_normal_drop() {
        defra_core::signing::set_broadcast_creator_did(None);
        defra_core::signing::set_broadcast_creator_did(Some("did:key:test".to_string()));
        assert!(defra_core::signing::get_broadcast_creator_did().is_some());

        {
            let _guard = BroadcastCreatorDidGuard;
            assert!(defra_core::signing::get_broadcast_creator_did().is_some());
        }

        assert!(defra_core::signing::get_broadcast_creator_did().is_none());
    }

    #[test]
    fn broadcast_creator_did_guard_clears_on_panic_unwind() {
        defra_core::signing::set_broadcast_creator_did(None);
        defra_core::signing::set_broadcast_creator_did(Some("did:key:alice".to_string()));

        let _ = std::panic::catch_unwind(|| {
            let _guard = BroadcastCreatorDidGuard;
            panic!("simulated mutation panic");
        });

        // Critical: a panicking mutation must NOT leave Alice's DID on
        // this thread for the next anonymous mutation to broadcast as.
        assert!(defra_core::signing::get_broadcast_creator_did().is_none());
    }

    #[test]
    fn broadcast_creator_did_guard_self_heals_from_leaked_state() {
        defra_core::signing::set_broadcast_creator_did(Some("did:key:leaked".to_string()));

        {
            let _guard = BroadcastCreatorDidGuard;
        }

        assert!(defra_core::signing::get_broadcast_creator_did().is_none());
    }

    #[tokio::test]
    async fn create_mutation_maps_relation_doc_ids_to_fk_fields() {
        let company_collection = CollectionVersion::new(
            "Company",
            "v1",
            "coll-company",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "name", FieldKind::string()),
                FieldDescription::new("3", "employees", FieldKind::relation("Employee", true))
                    .with_relation_name("employee_company"),
            ],
        );

        let employee_collection = CollectionVersion::new(
            "Employee",
            "v1",
            "coll-employee",
            vec![
                FieldDescription::new("1", "_docID", FieldKind::doc_id()),
                FieldDescription::new("2", "name", FieldKind::string()),
                FieldDescription::new("3", "company", FieldKind::relation("Company", false))
                    .with_relation_name("employee_company")
                    .as_primary(),
                FieldDescription::new("4", "_companyID", FieldKind::doc_id())
                    .with_relation_name("employee_company")
                    .as_primary(),
            ],
        );

        let fetcher = MockFetcher::new();
        let mutator = Arc::new(CapturingMutator::new());
        let runner = QueryRunner::new(fetcher, vec![company_collection, employee_collection])
            .with_mutator(mutator.clone());

        let request = QueryRequest::new(
            r#"
            mutation {
                add_Employee(input: [{
                    name: "PubEmp in PubCompany"
                    company: "bae-7b649bba-3168-5c05-827c-514c0f8d56fd"
                }]) {
                    _docID
                }
            }
            "#,
        );

        let response = runner.execute(request).await;
        assert!(
            response.errors.is_empty(),
            "unexpected executor errors: {:?}",
            response.errors
        );
        assert!(response.data.is_some());

        // Also exercise the direct mutation path used by internal callers.
        runner
            .execute_mutation(
                r#"
                mutation {
                    add_Employee(input: [{
                        name: "PubEmp in PubCompany 2"
                        company: "bae-7b649bba-3168-5c05-827c-514c0f8d56fd"
                    }]) {
                        _docID
                    }
                }
                "#,
            )
            .await
            .unwrap();

        let created = mutator.created_docs();
        assert_eq!(created.len(), 2);
        let created_doc = &created[0];
        assert_eq!(
            created_doc
                .get("_companyID")
                .and_then(|value| value.as_str()),
            Some("bae-7b649bba-3168-5c05-827c-514c0f8d56fd")
        );
        assert!(created_doc.get("company").is_none());

        let created_doc = &created[1];
        assert_eq!(
            created_doc
                .get("_companyID")
                .and_then(|value| value.as_str()),
            Some("bae-7b649bba-3168-5c05-827c-514c0f8d56fd")
        );
        assert!(created_doc.get("company").is_none());
    }
}
