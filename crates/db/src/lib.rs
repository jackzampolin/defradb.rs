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
/// use storage::RegolithStore;
///
/// // Create database
/// let store = RegolithStore::in_memory().unwrap();
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
pub mod access;
pub mod backup;
pub mod block;
pub mod collection;
pub use collection::stream::BackfillSource;
pub mod database;
// Definition validation moved to the `schema` crate (was #791 substitute
// slice) — see `schema::definition_validation`.
pub mod definition;
pub mod docid;
pub mod downsample;
pub mod error;
pub mod event;
pub mod index;
pub use event::emission::{TxnBroadcastEvent, TxnBroadcaster};
pub mod merge;
pub mod nac;
pub mod read;
pub mod search;
pub mod txn;
pub(crate) mod view;
pub mod write;

// Re-export commonly used types
pub use crate::block::builder::{build_blocks_from_document, BlockResult};
pub use crate::search::{
    embed_text, hybrid_search_dense, require_query_success, DenseHybridSearchHit,
    DenseHybridSearchRequest, DenseHybridSearchResponse,
};
pub use access::kms::{
    DbBlockDocIDResolver, DbDocCollectionLookup, DbEncBlockStore, DbNodeAcpRead,
};
pub use access::node::{node_access_checker, NodeAccessChecker};
pub use collection::acp::{
    block_unsafe_policy_transition, check_doc_permission, check_policy_transition,
    register_collection_if_needed, register_doc_if_needed, unregister_doc_if_needed,
    warn_on_unsafe_policy_transition, AcpContext, PolicyTransitionCheck,
};
pub use collection::cache::CollectionCache;
pub use collection::name::CollectionName;
pub use collection::provider::DbCollectionProvider;
pub use collection::retriever::{resolve_collection_from_doc_id, DocCollectionInfo};
pub use collection::selector::CollectionSelector;
pub use collection::snapshot::CollectionSnapshot;
#[allow(deprecated)]
pub use collection::{collection_short_id, Collection, DbCollectionTruncator};
pub use database::{
    DbOptions, EmbeddingClientConfig, DB, DEFAULT_MAX_TXN_RETRIES,
    DEFAULT_MIGRATION_WRITE_BACK_BATCH_SIZE,
};
pub use definition::loader::{
    get_collection_by_version_id, get_collection_version_ids, get_collections_by_collection_id,
    load_active_collections,
};
pub use defra_core::encryption::{set_encryption_config, EncryptionConfig};
pub use defra_core::{Action, ActionExecution, ActionStatus};
pub use downsample::GcDownsampleHistoriesOptions;
pub use error::{Error, Result};
pub use index::{BulkIndexResult, IndexManager};
pub use read::autocommit::AutoCommitFetcher;
pub use read::commits::{CommitsFetcher, CommitsQueryOptions};
pub use read::doc::DbDocFetcher;
pub use read::lensed::autocommit::LensedAutoCommitFetcher;
pub use read::lensed::fetcher::LensedDocFetcher;
pub use read::versioned::VersionedFetcher;
pub use txn::context::DbTransactionContext;
pub use txn::registry::{
    CleanupResult, DbTransactionRegistry, DEFAULT_TRANSACTION_CLEANUP_INTERVAL,
    DEFAULT_TRANSACTION_IDLE_TIMEOUT,
};
pub use txn::DbTxn;
pub use view::ops::is_refreshable_view;
pub use write::autocommit::AutoCommitMutator;
pub use write::doc::DbDocMutator;
pub use write::queue::DocWriteQueue;

// NAC exports
#[cfg(not(target_arch = "wasm32"))]
pub use nac::create_persistent_nac_manager;
pub use nac::{create_memory_nac_manager, NacConfig, NacInfo, NacManager, NacManagerApi};

// Re-export related crate types for convenience
pub use datastore::{BasicTxn, NamespaceView};
pub use document::{DocID, Document, NormalValue};
pub use schema::CollectionVersion;
