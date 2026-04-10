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
#[cfg(feature = "p2p")]
pub(crate) mod acp_merge_handler;
pub(crate) mod auto_commit_fetcher;
pub mod auto_commit_mutator;
pub mod backup;
pub(crate) mod block_builder;
pub mod block_verify;
#[cfg(feature = "p2p")]
pub(crate) mod broadcast_mutator;
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
pub mod definition_validation;
pub mod dense_search;
pub(crate) mod doc_fetcher;
pub(crate) mod doc_mutator;
pub mod downsample;
pub(crate) mod dump;
pub(crate) mod embedding;
pub(crate) mod error;
#[cfg(feature = "p2p")]
pub(crate) mod head_provider;
pub mod index_manager;
pub(crate) mod json_patch;
#[allow(dead_code)]
pub(crate) mod lens_utils;
pub(crate) mod lensed_auto_commit_fetcher;
pub(crate) mod lensed_fetcher;
#[cfg(feature = "p2p")]
pub(crate) mod merge_handler;
pub(crate) mod migration;
pub(crate) mod nac;
pub(crate) mod patch;
#[cfg(feature = "p2p")]
pub(crate) mod peer_identity;
#[cfg(feature = "p2p")]
pub(crate) mod push_docs;
#[cfg(feature = "p2p")]
pub(crate) mod push_docs_common;
#[cfg(feature = "p2p")]
pub(crate) mod push_docs_transport;
pub mod schema_loader;
#[allow(dead_code)]
pub(crate) mod se;
pub mod txn;
pub(crate) mod txn_context;
pub(crate) mod txn_registry;
pub(crate) mod versioned_fetcher;
pub(crate) mod view_ops;

// Re-export commonly used types
#[cfg(feature = "p2p")]
pub use acp_merge_handler::{AcpMergeError, AcpMergeHandler};
pub use auto_commit_fetcher::AutoCommitFetcher;
pub use auto_commit_mutator::AutoCommitMutator;
#[allow(deprecated)]
pub use block_builder::build_block_from_document;
pub use block_builder::{build_blocks_from_document, BlockResult};
#[cfg(feature = "p2p")]
pub use broadcast_mutator::BroadcastMutator;
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
pub use dense_search::{
    hybrid_search_dense, require_query_success, DenseHybridSearchHit, DenseHybridSearchRequest,
    DenseHybridSearchResponse,
};
pub use doc_fetcher::DbDocFetcher;
pub use doc_mutator::DbDocMutator;
pub use downsample::GcDownsampleHistoriesOptions;
pub use embedding::embed_text;
pub use error::{Error, Result};
#[cfg(feature = "p2p")]
pub use head_provider::DbHeadProvider;
pub use index_manager::{BulkIndexResult, IndexManager};
pub use lensed_auto_commit_fetcher::LensedAutoCommitFetcher;
pub use lensed_fetcher::LensedDocFetcher;
#[cfg(feature = "p2p")]
pub use merge_handler::{DbMergeHandler, MergeError};
#[cfg(feature = "p2p")]
pub use peer_identity::{
    create_peer_to_did_mapper, peer_id_to_did, public_key_to_did, PeerIdentityError,
};
#[cfg(feature = "p2p")]
pub use push_docs::{push_existing_docs, retry_doc};
#[cfg(feature = "p2p")]
pub use push_docs_transport::{push_existing_docs_via_transport, retry_doc_via_transport};
pub use schema_loader::{
    get_collection_by_version_id, get_collection_version_ids, get_collections_by_collection_id,
    load_active_collections,
};
pub use txn::DbTxn;
pub use txn_context::DbTransactionContext;
pub use txn_registry::{CleanupResult, DbTransactionRegistry};
pub use versioned_fetcher::VersionedFetcher;
pub use view_ops::RefreshViewsOptions;

// NAC exports
#[cfg(not(target_arch = "wasm32"))]
#[cfg(all(not(target_arch = "wasm32"), feature = "redb"))]
pub use nac::create_persistent_nac_manager;
pub use nac::{create_memory_nac_manager, NacConfig, NacInfo, NacManager, NacManagerApi};

// SE (Searchable Encryption) exports
pub use se::{
    fetch_doc_ids, generate_doc_artifacts, generate_field_artifact, store_artifacts, FieldQuery,
    FieldValueQuery, SECoordinator,
};

// Re-export related crate types for convenience
pub use datastore::{BasicTxn, NamespaceView};
pub use document::{DocID, Document, NormalValue};
pub use schema::CollectionVersion;
