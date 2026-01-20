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

mod executor;
mod fetcher;
mod mutation;
mod plan;
mod query;

use acp::DocumentACP;
use identity::Did;
use schema::CollectionVersion;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::sync::Arc;

use crate::document::DocumentMapping;
use crate::error::Result;
use crate::mapper::Select;
use crate::mutator::DocMutator;
use crate::planner::Doc;
use crate::txn::{NoOpTransactionRegistry, TransactionRegistry};

// Re-export for backwards compatibility
pub use crate::fetcher::{DocFetcher, FetchByIdsResult};

/// Query runner that executes GraphQL queries against storage.
pub struct QueryRunner<F: DocFetcher, R: TransactionRegistry = NoOpTransactionRegistry> {
    /// Document fetcher for storage access (used for non-transactional queries)
    pub(crate) fetcher: Arc<F>,
    /// Collection schemas by name
    pub(crate) collections: HashMap<String, Arc<CollectionVersion>>,
    /// Transaction registry for transaction lifecycle management
    pub(crate) registry: Arc<R>,
    /// Document mutator for mutation operations (optional)
    pub(crate) mutator: Option<Arc<dyn DocMutator>>,
    /// Document ACP for permission checks (optional)
    pub(crate) acp: Option<Arc<dyn DocumentACP>>,
    /// Default identity for ACP permission checks.
    ///
    /// Used when a request doesn't include an explicit identity (e.g., no bearer token).
    /// Typically set from the `--identity` CLI flag.
    pub(crate) default_identity: Option<Did>,
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
    pub(crate) fn resolve_identity(&self, request_identity: Option<Did>) -> Option<Did> {
        request_identity.or_else(|| self.default_identity.clone())
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

    /// Convert a plan Doc to JSON for output.
    pub(crate) fn doc_to_json(&self, doc: &Doc, mapping: &DocumentMapping) -> Result<JsonValue> {
        plan::doc_to_json(doc, mapping)
    }

    /// Find the first ACP-protected collection in nested selections.
    ///
    /// This resolves relation field names to actual collection names by looking up
    /// the relation definition in the parent collection's schema.
    ///
    /// Returns Some(collection_name) if an ACP-protected collection is found, None otherwise.
    pub(crate) fn find_acp_collection_in_nested(
        &self,
        select: &Select,
        parent_collection: &CollectionVersion,
    ) -> Option<String> {
        use crate::mapper::Requestable;

        for field in &select.fields {
            if let Requestable::Select(nested) = field {
                // The nested select's collection_name is the field name in the query
                // We need to resolve it to the actual target collection via the relation
                let field_name = &nested.collection_name;

                // Find the relation field in the parent collection
                if let Some(relation_field) = parent_collection
                    .fields
                    .iter()
                    .find(|f| &f.name == field_name)
                {
                    // Get the target collection name from the relation field's kind
                    if let Some(target_coll_name) = relation_field.kind.relation_collection_id() {
                        // Check if target collection has ACP
                        if let Some(target_coll) = self.collections.get(target_coll_name) {
                            if target_coll.policy.is_some() {
                                return Some(target_coll.name.clone());
                            }

                            // Recursively check deeper nested selections
                            if let Some(deep_acp) =
                                self.find_acp_collection_in_nested(nested, target_coll)
                            {
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
