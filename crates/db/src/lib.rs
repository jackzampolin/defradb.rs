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
/// // Documents are written through the mutator APIs, which allocate the
/// // node-local short ID and derive the public DocID from the genesis
/// // composite block CID.
///
/// // Commit
/// txn.commit().await?;
/// ```
pub(crate) mod auto_commit_fetcher;
// Backup extracted to standalone db-backup crate (#789). Callers now
// import directly from `db_backup::*`.
// Block builder extracted to standalone db-blocks crate for parallel compilation.
pub(crate) use db_blocks as block_builder;
pub(crate) mod block_cleanup;
pub mod block_reader;
pub mod block_verify;
pub mod collection;
pub use collection::stream::BackfillSource;
mod commit_priority_index;
pub(crate) mod commits_fetcher;
#[cfg(test)]
mod counting_store;
pub mod database;
// Definition validation moved to the `schema` crate (was #791 substitute
// slice) — see `schema::definition_validation`.
// dense_search and embedding extracted to standalone db-search crate (Phase 6 of #796).
pub use db_search as dense_search;
pub(crate) mod doc_fetcher;
pub mod doc_id_map;
pub mod downsample;
pub mod error;
pub mod event;
pub use event::emission::{TxnBroadcastEvent, TxnBroadcaster};
// Index manager extracted to standalone db-index crate.
pub use db_index as index_manager;
pub(crate) mod index_seek;
pub(crate) mod json_patch;
pub mod kms_adapters;
#[allow(dead_code)]
pub(crate) mod lens_utils;
pub(crate) mod lensed_auto_commit_fetcher;
pub(crate) mod lensed_fetcher;
#[cfg(test)]
mod limit_pushdown_tests;
pub mod migration;
// NAC extracted to standalone db-nac crate.
pub(crate) use db_nac as nac;
mod nac_guard;
pub mod node_access_checker;
pub(crate) mod patch;
#[cfg(test)]
mod plan_close_tests;
pub mod schema_loader;
pub mod txn;
pub(crate) mod vector_search;
pub(crate) mod versioned_fetcher;
pub(crate) mod view;
pub mod write;

// Re-export commonly used types
pub use auto_commit_fetcher::AutoCommitFetcher;
pub use block_builder::{build_blocks_from_document, BlockResult};
pub use collection::acp::{
    block_unsafe_policy_transition, check_doc_permission, check_policy_transition,
    register_collection_if_needed, register_doc_if_needed, unregister_doc_if_needed,
    warn_on_unsafe_policy_transition, AcpContext, PolicyTransitionCheck,
};
pub use collection::cache::CollectionCache;
pub use collection::name::CollectionName;
pub use collection::provider::DbCollectionProvider;
pub use collection::retriever::{resolve_collection_from_doc_id, DocCollectionInfo};
pub use collection::snapshot::CollectionSnapshot;
#[allow(deprecated)]
pub use collection::{collection_short_id, Collection};
pub use commits_fetcher::{CommitsFetcher, CommitsQueryOptions};
pub use database::{
    DbOptions, EmbeddingClientConfig, DB, DEFAULT_MAX_TXN_RETRIES,
    DEFAULT_MIGRATION_WRITE_BACK_BATCH_SIZE,
};
pub use defra_core::encryption::{set_encryption_config, EncryptionConfig};
pub use defra_core::{Action, ActionExecution, ActionStatus};
pub use write::autocommit::AutoCommitMutator;
pub use write::queue::DocWriteQueue;
// dense_search items re-exported transparently from db-search
pub use db_search::{
    embed_text, hybrid_search_dense, require_query_success, DenseHybridSearchHit,
    DenseHybridSearchRequest, DenseHybridSearchResponse,
};
pub use doc_fetcher::DbDocFetcher;
pub use downsample::GcDownsampleHistoriesOptions;
pub use error::{Error, Result};
pub use index_manager::{BulkIndexResult, IndexManager};
pub use kms_adapters::{
    DbBlockDocIDResolver, DbDocCollectionLookup, DbEncBlockStore, DbNodeAcpRead,
};
pub use lensed_auto_commit_fetcher::LensedAutoCommitFetcher;
pub use lensed_fetcher::LensedDocFetcher;
pub use node_access_checker::{node_access_checker, NodeAccessChecker};
pub use schema_loader::{
    get_collection_by_version_id, get_collection_version_ids, get_collections_by_collection_id,
    load_active_collections,
};
pub use txn::context::DbTransactionContext;
pub use txn::registry::{
    CleanupResult, DbTransactionRegistry, DEFAULT_TRANSACTION_CLEANUP_INTERVAL,
    DEFAULT_TRANSACTION_IDLE_TIMEOUT,
};
pub use txn::DbTxn;
pub use versioned_fetcher::VersionedFetcher;
pub use view::ops::{is_refreshable_view, RefreshViewsOptions};
pub use write::doc::DbDocMutator;

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
