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
pub mod collection;
pub mod database;
pub mod error;
pub mod txn;
pub mod txn_registry;

// Re-export commonly used types
pub use collection::Collection;
pub use database::{DbOptions, DB};
pub use error::{Error, Result};
pub use txn::DbTxn;
pub use txn_registry::{DbDocFetcher, DbTransactionContext, DbTransactionRegistry};

// Re-export related crate types for convenience
pub use datastore::{BasicTxn, NamespaceView};
pub use document::{DocID, Document, NormalValue};
pub use schema::CollectionVersion;
