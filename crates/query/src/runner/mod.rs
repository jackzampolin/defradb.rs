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
//!
//! # Module Organization
//!
//! - `executor`: QueryExecutor trait implementation
//! - `fetcher`: Document fetching utilities
//! - `introspection`: GraphQL introspection handling
//! - `mutation`: Mutation execution
//! - `plan`: Plan execution utilities
//! - `query`: Main query execution logic
//! - `explain`: Explain query execution
//! - `commits`: Commits query handling

mod commits;
mod commits_height;
mod commits_numeric;
mod executor;
mod explain;
mod fetcher;
mod introspection;
mod mutation;
mod mutation_inputs;
mod plan;
mod plan_aggregates;
mod plan_formatting;
mod plan_validation;
mod query;
mod se_transport;
mod version;

pub use se_transport::SeQueryTransport;

use acp::nac::NodePermission;
use acp::{DocumentACP, Identity as AcpIdentity};
use async_trait::async_trait;
use identity::Did;
use schema::CollectionVersion;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::sync::Arc;

use crate::document::DocumentMapping;
use crate::error::{QueryError, Result};
use crate::fetcher::{CollectionProvider, StaticCollectionProvider};
use crate::limits::QueryLimits;
use crate::mutator::DocMutator;
use crate::planner::Doc;
use crate::txn::{NoOpTransactionRegistry, TransactionRegistry};

tokio::task_local! {
    /// Per-request collection provider override for transaction-scoped schema resolution.
    ///
    /// When a query executes within a transaction that has uncommitted schema changes,
    /// this is set to a transaction-aware provider so `get_collection()` sees the new schemas.
    static TXN_COLLECTION_PROVIDER: Arc<dyn CollectionProvider>;
}

// Re-export for backwards compatibility
pub use crate::fetcher::{DocFetcher, FetchByIdsResult, IndexScanResult};

/// Minimal NAC checker for query-level enforcement.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait NacChecker: Send + Sync {
    async fn check_permission(&self, identity: &Did, permission: NodePermission) -> bool;
}

struct NoOpNacChecker;

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl NacChecker for NoOpNacChecker {
    async fn check_permission(&self, _identity: &Did, _permission: NodePermission) -> bool {
        true
    }
}

struct NoOpDocumentAcp;

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl DocumentACP for NoOpDocumentAcp {
    async fn register_doc_object(
        &self,
        _identity: &Did,
        _policy_id: &str,
        _resource_name: &str,
        _doc_id: &str,
    ) -> acp::Result<()> {
        Ok(())
    }

    async fn is_doc_registered(
        &self,
        _policy_id: &str,
        _resource_name: &str,
        _doc_id: &str,
    ) -> acp::Result<bool> {
        Ok(false)
    }

    async fn check_doc_access(
        &self,
        _identity: &AcpIdentity,
        _permission: acp::DocumentPermission,
        _policy_id: &str,
        _resource_name: &str,
        _doc_id: &str,
    ) -> acp::Result<bool> {
        Ok(true)
    }

    async fn add_actor_relationship(
        &self,
        _requestor: &Did,
        _target: &Did,
        _policy_id: &str,
        _collection_id: &str,
        _doc_id: &str,
        _relation: &str,
        _managing_relations: &[String],
    ) -> acp::Result<bool> {
        Ok(false)
    }

    async fn delete_actor_relationship(
        &self,
        _requestor: &Did,
        _target: &Did,
        _policy_id: &str,
        _collection_id: &str,
        _doc_id: &str,
        _relation: &str,
        _managing_relations: &[String],
    ) -> acp::Result<bool> {
        Ok(false)
    }

    async fn unregister_doc_object(
        &self,
        _policy_id: &str,
        _resource_name: &str,
        _doc_id: &str,
    ) -> acp::Result<()> {
        Ok(())
    }
}

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
    /// Document ACP for permission checks.
    ///
    /// Defaults to a no-op ACP implementation that allows access and treats
    /// documents as unregistered, matching the "ACP not configured" behavior.
    pub(crate) acp: Arc<dyn DocumentACP>,
    /// Default identity for ACP permission checks.
    ///
    /// Used when a request doesn't include an explicit identity (e.g., no bearer token).
    /// Typically set from the `--identity` CLI flag.
    pub(crate) default_identity: Option<Did>,
    /// Local node DID for Go-parity ACP node-identity shortcuts.
    pub(crate) node_did: Option<Did>,
    /// Optional lens transform store for view queries with transforms
    pub(crate) lens_store: Option<Arc<dyn lens::TransformStore>>,
    /// NAC checker for query-level enforcement.
    ///
    /// Defaults to a no-op checker that allows every request.
    pub(crate) nac: Arc<dyn NacChecker>,
    /// Query execution timeout in seconds (0 = no timeout). Default: 30.
    pub(crate) query_timeout: u64,
    /// Query parsing and filter evaluation guardrails.
    pub(crate) query_limits: QueryLimits,
    /// Optional searchable-encryption remote query transport.
    ///
    /// When set, `encrypted_<Collection>` queries fan search tags out to
    /// replicators (Go semantics) instead of resolving against local plaintext.
    /// `None` keeps the back-compat local-plaintext path (unit/FFI/non-P2P).
    pub(crate) se_transport: Option<Arc<dyn SeQueryTransport>>,
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
            acp: Arc::new(NoOpDocumentAcp),
            default_identity: None,
            node_did: None,
            lens_store: None,
            nac: Arc::new(NoOpNacChecker),
            query_timeout: 30,
            query_limits: QueryLimits::default(),
            se_transport: None,
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
            acp: Arc::new(NoOpDocumentAcp),
            default_identity: None,
            node_did: None,
            lens_store: None,
            nac: Arc::new(NoOpNacChecker),
            query_timeout: 30,
            query_limits: QueryLimits::default(),
            se_transport: None,
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
            acp: Arc::new(NoOpDocumentAcp),
            default_identity: None,
            node_did: None,
            lens_store: None,
            nac: Arc::new(NoOpNacChecker),
            query_timeout: 30,
            query_limits: QueryLimits::default(),
            se_transport: None,
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
            acp: Arc::new(NoOpDocumentAcp),
            default_identity: None,
            node_did: None,
            lens_store: None,
            nac: Arc::new(NoOpNacChecker),
            query_timeout: 30,
            query_limits: QueryLimits::default(),
            se_transport: None,
        }
    }

    /// Create a new query runner with a shared transaction registry.
    ///
    /// Use this when you need to share the registry with other components
    /// (e.g., for transaction-aware migration configuration).
    pub fn with_arc_registry_and_provider(
        fetcher: F,
        provider: Arc<dyn CollectionProvider>,
        registry: Arc<R>,
    ) -> Self {
        Self {
            fetcher: Arc::new(fetcher),
            collection_provider: provider,
            registry,
            mutator: None,
            acp: Arc::new(NoOpDocumentAcp),
            default_identity: None,
            node_did: None,
            lens_store: None,
            nac: Arc::new(NoOpNacChecker),
            query_timeout: 30,
            query_limits: QueryLimits::default(),
            se_transport: None,
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
        if let Some(mutator) = &self.mutator {
            mutator.set_document_acp(acp.clone());
        }
        self.acp = acp;
        self
    }

    /// Set the searchable-encryption remote query transport.
    ///
    /// When set, `encrypted_<Collection>` queries resolve by fanning search tags
    /// out to replicators (Go's owner-queries-replicator model) rather than
    /// matching local plaintext.
    pub fn with_se_transport(mut self, transport: Arc<dyn SeQueryTransport>) -> Self {
        self.se_transport = Some(transport);
        self
    }

    /// Set the lens transform store for view queries with transforms.
    pub fn with_lens_store(mut self, store: Arc<dyn lens::TransformStore>) -> Self {
        self.lens_store = Some(store);
        self
    }

    /// Set the NAC checker for query-level permission enforcement.
    ///
    /// When set, queries will be checked against NAC permissions before execution.
    /// Denied operations return a GraphQL error (HTTP 200) matching Go behavior.
    pub fn with_nac(mut self, nac: Arc<dyn NacChecker>) -> Self {
        self.nac = nac;
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

    /// Set the local node DID for ACP node-identity shortcuts.
    pub fn with_node_did(mut self, node_did: Option<Did>) -> Self {
        self.node_did = node_did;
        self
    }

    pub(crate) fn node_did(&self) -> Option<&Did> {
        self.node_did.as_ref()
    }

    /// Set query execution timeout in seconds (0 = no timeout).
    pub fn with_query_timeout(mut self, timeout_secs: u64) -> Self {
        self.query_timeout = timeout_secs;
        self
    }

    /// Set query parsing and filter evaluation limits.
    pub fn with_query_limits(mut self, limits: QueryLimits) -> Self {
        self.query_limits = limits;
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

    /// Get the effective collection provider.
    ///
    /// Returns the transaction-scoped provider if set (via task-local storage),
    /// otherwise returns the default process-wide provider.
    fn effective_provider(&self) -> Arc<dyn CollectionProvider> {
        TXN_COLLECTION_PROVIDER
            .try_with(|p| p.clone())
            .unwrap_or_else(|_| self.collection_provider.clone())
    }

    /// Get the names of all collections.
    ///
    /// Returns a sorted list of collection names registered with this runner.
    pub async fn collection_names(&self) -> Result<Vec<String>> {
        let mut names = self.effective_provider().list_collections().await?;
        names.sort();
        Ok(names)
    }

    /// Check if a collection exists.
    pub async fn has_collection(&self, name: &str) -> Result<bool> {
        Ok(self
            .effective_provider()
            .get_collection(name)
            .await?
            .is_some())
    }

    /// Resolve a collection on-demand from the provider.
    ///
    /// Returns the collection schema or an error if not found.
    pub(crate) async fn get_collection(&self, name: &str) -> Result<Arc<CollectionVersion>> {
        self.effective_provider()
            .get_collection(name)
            .await?
            .ok_or_else(|| QueryError::collection_not_found(name))
    }

    /// Get all collections as a HashMap for operations that need multiple collections.
    ///
    /// This is used internally for plan building which requires access to multiple
    /// collection schemas simultaneously (e.g., for joins).
    pub(crate) async fn collections_map(&self) -> Result<HashMap<String, Arc<CollectionVersion>>> {
        let provider = self.effective_provider();
        let names = provider.list_collections().await?;
        let mut map = HashMap::new();
        for name in names {
            if let Some(coll) = provider.get_collection(&name).await? {
                map.insert(name, coll);
            }
        }
        Ok(map)
    }

    /// Convert a plan Doc to JSON for output.
    pub(crate) fn doc_to_json(&self, doc: &Doc, mapping: &DocumentMapping) -> Result<JsonValue> {
        plan::doc_to_json(doc, mapping)
    }

    /// Execute an introspection query (__schema, __type).
    ///
    /// Introspection queries are executed against a dynamically generated GraphQL
    /// schema based on the current collections, rather than against document storage.
    pub(crate) async fn execute_introspection(&self, query: &str) -> Result<JsonValue> {
        let provider = self.effective_provider();
        let collections = provider.list_collections().await?;
        let mut collection_versions = Vec::new();
        for name in collections {
            if let Some(coll) = provider.get_collection(&name).await? {
                collection_versions.push((*coll).clone());
            }
        }

        // Execute introspection query
        introspection::execute_introspection(collection_versions, query).await
    }
}
