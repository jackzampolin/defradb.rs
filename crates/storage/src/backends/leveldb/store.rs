use async_trait::async_trait;
use parking_lot::Mutex;
use rusty_leveldb::{LdbIterator, Options, WriteBatch, DB};
use std::collections::BTreeMap;
use std::path::Path;
use std::rc::Rc;

use super::transaction::LevelDbTxn;
use crate::backends::opfs_env::OpfsEnv;
use crate::backends::shared::CallbackManager;
use crate::corekv::{Dropable, Error, Result, Store, Txn};

/// LevelDB-backed key-value store for WASM.
///
/// This store wraps rusty-leveldb's `DB` type and provides the CoreKV
/// `Store` interface for DefraDB compatibility.
///
/// For browser persistence, use `open_with_opfs()` which backs LevelDB
/// with the Origin Private File System (OPFS).
pub struct LevelDbStore {
    /// The underlying LevelDB database.
    /// Wrapped in Rc<RefCell> because rusty-leveldb::DB is not Send.
    db: Rc<std::cell::RefCell<Option<DB>>>,
    /// Path to the database (for error messages)
    #[allow(dead_code)]
    path: String,
    /// Whether the store is closed
    closed: Rc<std::cell::RefCell<bool>>,
    /// OPFS environment for persistence (None = in-memory only).
    /// Cloned from the same env given to rusty-leveldb, so they share state.
    opfs_env: Option<OpfsEnv>,
}

impl LevelDbStore {
    /// Open or create a LevelDB database at the given path.
    ///
    /// Uses the default in-memory environment. Data will not persist
    /// across page refreshes. For browser persistence, use `open_with_opfs()`.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path_str = path.as_ref().to_string_lossy().to_string();

        let options = Options {
            create_if_missing: true,
            ..Options::default()
        };

        let db = DB::open(&path_str, options).map_err(|e| Error::Backend(e.to_string()))?;

        Ok(Self {
            db: Rc::new(std::cell::RefCell::new(Some(db))),
            path: path_str,
            closed: Rc::new(std::cell::RefCell::new(false)),
            opfs_env: None,
        })
    }

    /// Open or create a LevelDB database with OPFS persistence.
    ///
    /// This loads existing data from the browser's Origin Private File System
    /// at startup and persists changes back on `persist()` and `close()`.
    pub async fn open_with_opfs(db_name: &str) -> Result<Self> {
        let env = OpfsEnv::new(db_name);

        // Load existing data from OPFS into the in-memory environment
        env.load()
            .await
            .map_err(|e| Error::Backend(format!("Failed to load from OPFS: {:?}", e)))?;

        // Clone gives rusty-leveldb a view of the same shared state
        let options = Options {
            create_if_missing: true,
            env: Rc::new(Box::new(env.clone())),
            ..Options::default()
        };

        let db = DB::open(db_name, options).map_err(|e| Error::Backend(e.to_string()))?;

        Ok(Self {
            db: Rc::new(std::cell::RefCell::new(Some(db))),
            path: db_name.to_string(),
            closed: Rc::new(std::cell::RefCell::new(false)),
            opfs_env: Some(env),
        })
    }

    /// Open or create a LevelDB database with custom options.
    ///
    /// This allows specifying a custom `Env` for OPFS or other storage backends.
    pub fn open_with_options<P: AsRef<Path>>(path: P, options: Options) -> Result<Self> {
        let path_str = path.as_ref().to_string_lossy().to_string();

        let db = DB::open(&path_str, options).map_err(|e| Error::Backend(e.to_string()))?;

        Ok(Self {
            db: Rc::new(std::cell::RefCell::new(Some(db))),
            path: path_str,
            closed: Rc::new(std::cell::RefCell::new(false)),
            opfs_env: None,
        })
    }

    /// Persist pending changes to OPFS.
    ///
    /// No-op if the store was not opened with OPFS.
    /// Call this after important transaction commits to ensure durability.
    pub async fn persist(&self) -> Result<()> {
        if let Some(env) = &self.opfs_env {
            // Flush LevelDB's internal buffers before persisting
            if let Ok(mut db_ref) = self.get_db_mut() {
                if let Some(db) = db_ref.as_mut() {
                    db.flush().map_err(|e| Error::Backend(e.to_string()))?;
                }
            }
            env.persist()
                .await
                .map_err(|e| Error::Backend(format!("OPFS persist failed: {:?}", e)))?;
        }
        Ok(())
    }

    /// Check if the store is closed.
    fn is_closed(&self) -> bool {
        *self.closed.borrow()
    }

    /// Get a mutable reference to the DB.
    fn get_db_mut(&self) -> Result<std::cell::RefMut<'_, Option<DB>>> {
        if self.is_closed() {
            return Err(Error::DBClosed);
        }
        Ok(self.db.borrow_mut())
    }
}

impl crate::corekv::private::Sealed for LevelDbStore {}

#[async_trait(?Send)]
impl Store for LevelDbStore {
    async fn new_txn(&self, readonly: bool) -> Result<Box<dyn Txn>> {
        if self.is_closed() {
            return Err(Error::DBClosed);
        }

        // Create a snapshot for read isolation
        let mut db_ref = self.get_db_mut()?;
        let db = db_ref.as_mut().ok_or(Error::DBClosed)?;

        // Read current state into snapshot
        let mut snapshot = BTreeMap::new();
        let mut iter = db.new_iter().map_err(|e| Error::Backend(e.to_string()))?;

        // Position iterator at first element and iterate through all keys
        iter.seek_to_first();
        while iter.valid() {
            if let Some((key, value)) = iter.current() {
                snapshot.insert(key.to_vec(), value.to_vec());
            }
            iter.advance();
        }

        drop(db_ref);

        Ok(Box::new(LevelDbTxn {
            store: self.db.clone(),
            snapshot,
            pending: Mutex::new(BTreeMap::new()),
            readonly,
            discarded: Mutex::new(false),
            committed: Mutex::new(false),
            callbacks: CallbackManager::new(),
        }))
    }

    async fn close(&self) -> Result<()> {
        // Flush and persist to OPFS before closing
        if let Some(env) = &self.opfs_env {
            if let Ok(mut db_ref) = self.get_db_mut() {
                if let Some(db) = db_ref.as_mut() {
                    let _ = db.flush();
                }
            }
            let _ = env.persist().await;
        }

        *self.closed.borrow_mut() = true;
        // Drop the DB to flush and close
        *self.db.borrow_mut() = None;
        Ok(())
    }
}

#[async_trait(?Send)]
impl Dropable for LevelDbStore {
    async fn drop_all(&self) -> Result<()> {
        if self.is_closed() {
            return Err(Error::DBClosed);
        }

        let mut db_ref = self.get_db_mut()?;
        let db = db_ref.as_mut().ok_or(Error::DBClosed)?;

        // Collect all keys
        let mut keys: Vec<Vec<u8>> = Vec::new();
        let mut iter = db.new_iter().map_err(|e| Error::Backend(e.to_string()))?;
        iter.seek_to_first();
        while iter.valid() {
            if let Some((key, _)) = iter.current() {
                keys.push(key.to_vec());
            }
            iter.advance();
        }

        // Delete all keys
        let mut batch = WriteBatch::default();
        for key in &keys {
            batch.delete(key);
        }

        db.write(batch, true)
            .map_err(|e| Error::Backend(e.to_string()))?;

        Ok(())
    }
}
