/// Database layer for DefraDB matching Go's internal/db/.
///
/// This crate provides the high-level database API including:
///
/// - `DB`: Main database struct for creating transactions and managing collections
/// - `DbTxn`: Transaction wrapper with explicit/implicit handling
/// - `Collection`: Document collection with CRUD operations
///
/// # Architecture
///
/// ```text
/// User Code
///     ↓
/// db crate (DB, DbTxn, Collection)
///     ↓
/// datastore crate (BasicTxn, namespace views)
///     ↓
/// storage crate (corekv, backends)
/// ```
///
/// # Example
///
/// ```ignore
/// use db::{DB, Collection};
/// use document::Document;
/// use storage::backends::MemoryStore;
///
/// // Create database
/// let store = MemoryStore::new();
/// let db = DB::new(store)?;
///
/// // Create a transaction
/// let txn = db.new_txn(false).await?;
///
/// // Create a collection
/// let col = Collection::new(schema);
///
/// // Create a document
/// let doc = Document::from_json_str(r#"{"name": "Alice"}"#)?;
/// col.create(&txn, &doc).await?;
///
/// // Commit
/// txn.commit().await?;
/// ```
pub(crate) mod auto_commit_fetcher;
pub mod auto_commit_mutator;
// Backup extracted to standalone db-backup crate (#789). Callers now
// import directly from `db_backup::*`.
// Block builder extracted to standalone db-blocks crate for parallel compilation.
pub(crate) use db_blocks as block_builder;
pub mod block_reader;
pub mod block_verify;
pub mod collection;
pub mod collection_acp;
pub(crate) mod collection_cache;
pub(crate) mod collection_loader;
pub(crate) mod collection_name;
pub(crate) mod collection_ops;
pub(crate) mod collection_provider;
pub(crate) mod collection_snapshot;
mod commit_priority_index;
pub(crate) mod commits_fetcher;
pub mod database;
// Definition validation moved to the `schema` crate (was #791 substitute
// slice) — see `schema::definition_validation`.
// dense_search and embedding extracted to standalone db-search crate (Phase 6 of #796).
pub use db_search as dense_search;
pub(crate) mod doc_fetcher;
pub(crate) mod doc_mutator;
pub mod downsample;
pub(crate) mod dump;
pub mod error;
pub mod event_emission;
pub use event_emission::{TxnBroadcastEvent, TxnBroadcaster};
// Index manager extracted to standalone db-index crate.
pub use db_index as index_manager;
pub(crate) mod json_patch;
#[allow(dead_code)]
pub(crate) mod lens_utils;
pub(crate) mod lensed_auto_commit_fetcher;
pub(crate) mod lensed_fetcher;
pub(crate) mod migration;
// NAC extracted to standalone db-nac crate.
pub(crate) use db_nac as nac;
pub(crate) mod patch;
pub mod schema_loader;
pub mod txn;
pub(crate) mod txn_context;
pub(crate) mod txn_lens_store;
pub(crate) mod txn_registry;
pub(crate) mod versioned_fetcher;
pub(crate) mod view_ops;

// Re-export commonly used types
pub use auto_commit_fetcher::AutoCommitFetcher;
pub use auto_commit_mutator::AutoCommitMutator;
#[allow(deprecated)]
pub use block_builder::build_block_from_document;
pub use block_builder::{build_blocks_from_document, BlockResult};
#[allow(deprecated)]
pub use collection::{collection_short_id, Collection};
pub use collection_acp::{
    block_unsafe_policy_transition, check_doc_permission, check_policy_transition,
    register_doc_if_needed, unregister_doc_if_needed, warn_on_unsafe_policy_transition, AcpContext,
    PolicyTransitionCheck,
};
pub use collection_cache::CollectionCache;
pub use collection_name::CollectionName;
pub use collection_provider::DbCollectionProvider;
pub use collection_snapshot::CollectionSnapshot;
pub use commits_fetcher::{CommitsFetcher, CommitsQueryOptions};
pub use database::{DbOptions, EmbeddingClientConfig, DB};
pub use defra_core::encryption::{set_encryption_config, EncryptionConfig};
// dense_search items re-exported transparently from db-search
pub use db_search::{
    embed_text, hybrid_search_dense, require_query_success, DenseHybridSearchHit,
    DenseHybridSearchRequest, DenseHybridSearchResponse,
};
pub use doc_fetcher::DbDocFetcher;
pub use doc_mutator::DbDocMutator;
pub use downsample::GcDownsampleHistoriesOptions;
pub use error::{Error, Result};
pub use index_manager::{BulkIndexResult, IndexManager};
pub use lensed_auto_commit_fetcher::LensedAutoCommitFetcher;
pub use lensed_fetcher::LensedDocFetcher;
pub use schema_loader::{
    get_collection_by_version_id, get_collection_version_ids, get_collections_by_collection_id,
    load_active_collections,
};
pub use txn::DbTxn;
pub use txn_context::DbTransactionContext;
pub use txn_registry::{
    CleanupResult, DbTransactionRegistry, DEFAULT_TRANSACTION_CLEANUP_INTERVAL,
    DEFAULT_TRANSACTION_IDLE_TIMEOUT,
};
pub use versioned_fetcher::VersionedFetcher;
pub use view_ops::RefreshViewsOptions;

// P2P merge/sync extracted to standalone db-merge crate.
// Consumer crates (cli, embedded, defra-node, ffi) should depend on db-merge directly.

// NAC exports
#[cfg(all(not(target_arch = "wasm32"), feature = "redb"))]
pub use nac::create_persistent_nac_manager;
pub use nac::{create_memory_nac_manager, NacConfig, NacInfo, NacManager, NacManagerApi};

// Re-export related crate types for convenience
pub use datastore::{BasicTxn, NamespaceView};
pub use document::{DocID, Document, NormalValue};
pub use schema::CollectionVersion;
