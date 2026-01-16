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
/// let db = DB::new(store);
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
pub mod auto_commit_fetcher;
pub mod auto_commit_mutator;
pub mod collection;
pub mod collection_acp;
pub mod collection_cache;
pub(crate) mod collection_loader;
pub mod collection_name;
pub mod collection_snapshot;
pub mod database;
pub mod doc_fetcher;
pub mod doc_mutator;
pub mod error;
pub mod index_manager;
pub mod schema_loader;
pub mod txn;
pub mod txn_context;
pub mod txn_registry;

// Re-export commonly used types
pub use auto_commit_fetcher::AutoCommitFetcher;
pub use auto_commit_mutator::AutoCommitMutator;
pub use collection::Collection;
pub use collection_acp::{
    check_doc_permission, register_doc_if_needed, unregister_doc_if_needed, AcpContext,
};
pub use collection_cache::CollectionCache;
pub use collection_name::CollectionName;
pub use collection_snapshot::CollectionSnapshot;
pub use database::{DbOptions, DB};
pub use doc_fetcher::DbDocFetcher;
pub use doc_mutator::DbDocMutator;
pub use error::{Error, Result};
pub use index_manager::{BulkIndexResult, IndexManager};
pub use schema_loader::load_active_collections;
pub use txn::DbTxn;
pub use txn_context::DbTransactionContext;
pub use txn_registry::{CleanupResult, DbTransactionRegistry};

// Re-export related crate types for convenience
pub use datastore::{BasicTxn, NamespaceView};
pub use document::{DocID, Document, NormalValue};
pub use schema::CollectionVersion;
