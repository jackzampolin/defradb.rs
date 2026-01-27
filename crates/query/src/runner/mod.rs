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
use crate::error::{QueryError, Result};
use crate::fetcher::{CollectionProvider, StaticCollectionProvider};
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
    /// Collection provider for on-demand schema resolution
    pub(crate) collection_provider: Arc<dyn CollectionProvider>,
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

impl<F: DocFetcher + 'static> QueryRunner<F, NoOpTransactionRegistry> {
    /// Create a new query runner with the given fetcher and collections.
    ///
    /// This creates a runner without transaction support. Use `with_registry`
    /// to enable transaction support.
    pub fn new(fetcher: F, collections: Vec<CollectionVersion>) -> Self {
        Self {
            fetcher: Arc::new(fetcher),
            collection_provider: Arc::new(StaticCollectionProvider::new(collections)),
            registry: Arc::new(NoOpTransactionRegistry),
            mutator: None,
            acp: None,
            default_identity: None,
        }
    }

    /// Create a new query runner with a collection provider.
    ///
    /// This creates a runner without transaction support. The provider is used
    /// to resolve collections on-demand, enabling dynamic schema updates.
    pub fn with_provider(fetcher: F, provider: Arc<dyn CollectionProvider>) -> Self {
        Self {
            fetcher: Arc::new(fetcher),
            collection_provider: provider,
            registry: Arc::new(NoOpTransactionRegistry),
            mutator: None,
            acp: None,
            default_identity: None,
        }
    }
}

impl<F: DocFetcher + 'static, R: TransactionRegistry> QueryRunner<F, R> {
    /// Create a new query runner with transaction support.
    ///
    /// This uses a static collection provider for backward compatibility.
    /// For dynamic schema updates, use `with_registry_and_provider`.
    pub fn with_registry(fetcher: F, collections: Vec<CollectionVersion>, registry: R) -> Self {
        Self {
            fetcher: Arc::new(fetcher),
            collection_provider: Arc::new(StaticCollectionProvider::new(collections)),
            registry: Arc::new(registry),
            mutator: None,
            acp: None,
            default_identity: None,
        }
    }

    /// Create a new query runner with transaction support and a collection provider.
    ///
    /// This is the recommended constructor for production use. The provider resolves
    /// collections on-demand from the database, ensuring newly added schemas are
    /// immediately available for queries.
    pub fn with_registry_and_provider(
        fetcher: F,
        provider: Arc<dyn CollectionProvider>,
        registry: R,
    ) -> Self {
        Self {
            fetcher: Arc::new(fetcher),
            collection_provider: provider,
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
    pub async fn collection_names(&self) -> Result<Vec<String>> {
        let mut names = self.collection_provider.list_collections().await?;
        names.sort();
        Ok(names)
    }

    /// Check if a collection exists.
    pub async fn has_collection(&self, name: &str) -> Result<bool> {
        Ok(self
            .collection_provider
            .get_collection(name)
            .await?
            .is_some())
    }

    /// Resolve a collection on-demand from the provider.
    ///
    /// Returns the collection schema or an error if not found.
    pub(crate) async fn get_collection(&self, name: &str) -> Result<Arc<CollectionVersion>> {
        self.collection_provider
            .get_collection(name)
            .await?
            .ok_or_else(|| QueryError::collection_not_found(name))
    }

    /// Get all collections as a HashMap for operations that need multiple collections.
    ///
    /// This is used internally for plan building which requires access to multiple
    /// collection schemas simultaneously (e.g., for joins).
    pub(crate) async fn collections_map(&self) -> Result<HashMap<String, Arc<CollectionVersion>>> {
        let names = self.collection_provider.list_collections().await?;
        let mut map = HashMap::new();
        for name in names {
            if let Some(coll) = self.collection_provider.get_collection(&name).await? {
                map.insert(name, coll);
            }
        }
        Ok(map)
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
    pub(crate) async fn find_acp_collection_in_nested(
        &self,
        select: &Select,
        parent_collection: &CollectionVersion,
    ) -> Result<Option<String>> {
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
                        if let Some(target_coll) = self
                            .collection_provider
                            .get_collection(target_coll_name)
                            .await?
                        {
                            if target_coll.policy.is_some() {
                                return Ok(Some(target_coll.name.clone()));
                            }

                            // Recursively check deeper nested selections
                            if let Some(deep_acp) =
                                Box::pin(self.find_acp_collection_in_nested(nested, &target_coll))
                                    .await?
                            {
                                return Ok(Some(deep_acp));
                            }
                        }
                    }
                }
            }
        }
        Ok(None)
    }
}
