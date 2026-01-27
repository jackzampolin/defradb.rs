//! IndexedDB storage backend for browser persistence.
//!
//! Provides persistent key-value storage using the browser's IndexedDB API.
//! Data survives page refreshes and browser restarts.

use idb::{Database, DatabaseEvent, Factory, ObjectStoreParams, Query, TransactionMode};
use std::cell::RefCell;
use std::rc::Rc;
use wasm_bindgen::JsValue;

use crate::error::{Result, WasmError};

const DB_VERSION: u32 = 1;
const STORE_NAME: &str = "defra_kv";

/// IndexedDB-backed key-value store for browser persistence.
///
/// Uses the browser's IndexedDB API to provide persistent storage
/// that survives page refreshes and browser restarts.
#[derive(Clone)]
pub struct WasmIndexedDbStore {
    db: Rc<RefCell<Option<Database>>>,
    db_name: String,
    closed: Rc<RefCell<bool>>,
}

impl WasmIndexedDbStore {
    /// Open or create an IndexedDB database.
    ///
    /// # Arguments
    ///
    /// * `db_name` - The name of the database to open/create
    pub async fn open(db_name: &str) -> Result<Self> {
        let factory = Factory::new().map_err(|e| {
            WasmError::Storage(format!("Failed to get IndexedDB factory: {:?}", e))
        })?;

        let mut open_request = factory
            .open(db_name, Some(DB_VERSION))
            .map_err(|e| WasmError::Storage(format!("Failed to open database: {:?}", e)))?;

        // Handle database upgrade (create object store if needed)
        open_request.on_upgrade_needed(|event| {
            let db = event.database().expect("Database should exist on upgrade");

            // Create the key-value object store if it doesn't exist
            if !db.store_names().iter().any(|name| name == STORE_NAME) {
                // Use out-of-line keys (we pass the key separately)
                let params = ObjectStoreParams::new();

                db.create_object_store(STORE_NAME, params)
                    .expect("Failed to create object store");
            }
        });

        let db = open_request
            .await
            .map_err(|e| WasmError::Storage(format!("Failed to open database: {:?}", e)))?;

        Ok(Self {
            db: Rc::new(RefCell::new(Some(db))),
            db_name: db_name.to_string(),
            closed: Rc::new(RefCell::new(false)),
        })
    }

    /// Check if the store is closed.
    pub fn is_closed(&self) -> bool {
        *self.closed.borrow()
    }

    fn ensure_open(&self) -> Result<()> {
        if *self.closed.borrow() {
            return Err(WasmError::Closed);
        }
        if self.db.borrow().is_none() {
            return Err(WasmError::NotInitialized);
        }
        Ok(())
    }

    /// Close the database connection.
    pub async fn close(&self) -> Result<()> {
        if *self.closed.borrow() {
            return Ok(());
        }

        if let Some(db) = self.db.borrow_mut().take() {
            db.close();
        }

        *self.closed.borrow_mut() = true;
        Ok(())
    }

    /// Convert a byte slice to JsValue for use as IndexedDB key/value
    fn bytes_to_js(bytes: &[u8]) -> JsValue {
        js_sys::Uint8Array::from(bytes).into()
    }

    /// Convert JsValue back to bytes
    fn js_to_bytes(js_value: &JsValue) -> Vec<u8> {
        let array = js_sys::Uint8Array::new(js_value);
        let mut bytes = vec![0u8; array.length() as usize];
        array.copy_to(&mut bytes);
        bytes
    }

    /// Get a value by key.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.ensure_open()?;

        let db = self.db.borrow();
        let db = db.as_ref().ok_or(WasmError::NotInitialized)?;

        let tx = db
            .transaction(&[STORE_NAME], TransactionMode::ReadOnly)
            .map_err(|e| WasmError::Storage(format!("Failed to start transaction: {:?}", e)))?;

        let store = tx
            .object_store(STORE_NAME)
            .map_err(|e| WasmError::Storage(format!("Failed to get object store: {:?}", e)))?;

        let js_key: Query = Self::bytes_to_js(key).into();
        let result = store
            .get(js_key)
            .map_err(|e| WasmError::Storage(format!("Failed to get value: {:?}", e)))?
            .await
            .map_err(|e| WasmError::Storage(format!("Get request failed: {:?}", e)))?;

        match result {
            Some(js_value) => Ok(Some(Self::js_to_bytes(&js_value))),
            None => Ok(None),
        }
    }

    /// Set a value for a key.
    pub async fn set(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.ensure_open()?;

        let db = self.db.borrow();
        let db = db.as_ref().ok_or(WasmError::NotInitialized)?;

        let tx = db
            .transaction(&[STORE_NAME], TransactionMode::ReadWrite)
            .map_err(|e| WasmError::Storage(format!("Failed to start transaction: {:?}", e)))?;

        let store = tx
            .object_store(STORE_NAME)
            .map_err(|e| WasmError::Storage(format!("Failed to get object store: {:?}", e)))?;

        let js_key = Self::bytes_to_js(key);
        let js_value = Self::bytes_to_js(value);

        store
            .put(&js_value, Some(&js_key))
            .map_err(|e| WasmError::Storage(format!("Failed to put value: {:?}", e)))?
            .await
            .map_err(|e| WasmError::Storage(format!("Put request failed: {:?}", e)))?;

        tx.commit()
            .map_err(|e| WasmError::Storage(format!("Failed to commit transaction: {:?}", e)))?
            .await
            .map_err(|e| WasmError::Storage(format!("Commit failed: {:?}", e)))?;

        Ok(())
    }

    /// Delete a key.
    pub async fn delete(&self, key: &[u8]) -> Result<()> {
        self.ensure_open()?;

        let db = self.db.borrow();
        let db = db.as_ref().ok_or(WasmError::NotInitialized)?;

        let tx = db
            .transaction(&[STORE_NAME], TransactionMode::ReadWrite)
            .map_err(|e| WasmError::Storage(format!("Failed to start transaction: {:?}", e)))?;

        let store = tx
            .object_store(STORE_NAME)
            .map_err(|e| WasmError::Storage(format!("Failed to get object store: {:?}", e)))?;

        let js_key: Query = Self::bytes_to_js(key).into();

        store
            .delete(js_key)
            .map_err(|e| WasmError::Storage(format!("Failed to delete value: {:?}", e)))?
            .await
            .map_err(|e| WasmError::Storage(format!("Delete request failed: {:?}", e)))?;

        tx.commit()
            .map_err(|e| WasmError::Storage(format!("Failed to commit transaction: {:?}", e)))?
            .await
            .map_err(|e| WasmError::Storage(format!("Commit failed: {:?}", e)))?;

        Ok(())
    }

    /// Check if a key exists.
    pub async fn has(&self, key: &[u8]) -> Result<bool> {
        Ok(self.get(key).await?.is_some())
    }

    /// Get all keys with a given prefix.
    pub async fn keys_with_prefix(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>> {
        self.ensure_open()?;

        let db = self.db.borrow();
        let db = db.as_ref().ok_or(WasmError::NotInitialized)?;

        let tx = db
            .transaction(&[STORE_NAME], TransactionMode::ReadOnly)
            .map_err(|e| WasmError::Storage(format!("Failed to start transaction: {:?}", e)))?;

        let store = tx
            .object_store(STORE_NAME)
            .map_err(|e| WasmError::Storage(format!("Failed to get object store: {:?}", e)))?;

        // Create a key range for the prefix
        // Start: prefix, End: prefix with last byte incremented
        let start_key = Self::bytes_to_js(prefix);
        let mut end_prefix = prefix.to_vec();
        // Increment last byte or append 0xFF for upper bound
        if let Some(last) = end_prefix.last_mut() {
            if *last < 0xFF {
                *last += 1;
            } else {
                end_prefix.push(0x00);
            }
        } else {
            end_prefix.push(0xFF);
        }
        let end_key = Self::bytes_to_js(&end_prefix);

        let range = idb::KeyRange::bound(&start_key, &end_key, Some(false), Some(true))
            .map_err(|e| WasmError::Storage(format!("Failed to create key range: {:?}", e)))?;

        let query: Query = range.into();
        let keys = store
            .get_all_keys(Some(query), None)
            .map_err(|e| WasmError::Storage(format!("Failed to get keys: {:?}", e)))?
            .await
            .map_err(|e| WasmError::Storage(format!("Get keys request failed: {:?}", e)))?;

        let mut result = Vec::new();
        for key in keys {
            result.push(Self::js_to_bytes(&key));
        }

        Ok(result)
    }

    /// Clear all data in the store.
    pub async fn clear(&self) -> Result<()> {
        self.ensure_open()?;

        let db = self.db.borrow();
        let db = db.as_ref().ok_or(WasmError::NotInitialized)?;

        let tx = db
            .transaction(&[STORE_NAME], TransactionMode::ReadWrite)
            .map_err(|e| WasmError::Storage(format!("Failed to start transaction: {:?}", e)))?;

        let store = tx
            .object_store(STORE_NAME)
            .map_err(|e| WasmError::Storage(format!("Failed to get object store: {:?}", e)))?;

        store
            .clear()
            .map_err(|e| WasmError::Storage(format!("Failed to clear store: {:?}", e)))?
            .await
            .map_err(|e| WasmError::Storage(format!("Clear request failed: {:?}", e)))?;

        tx.commit()
            .map_err(|e| WasmError::Storage(format!("Failed to commit transaction: {:?}", e)))?
            .await
            .map_err(|e| WasmError::Storage(format!("Commit failed: {:?}", e)))?;

        Ok(())
    }

    /// Delete the entire database.
    ///
    /// This will close the database and delete all data.
    pub async fn delete_database(&self) -> Result<()> {
        // Close the database first
        self.close().await?;

        let factory = Factory::new().map_err(|e| {
            WasmError::Storage(format!("Failed to get IndexedDB factory: {:?}", e))
        })?;

        factory
            .delete(&self.db_name)
            .map_err(|e| WasmError::Storage(format!("Failed to delete database: {:?}", e)))?
            .await
            .map_err(|e| WasmError::Storage(format!("Delete database failed: {:?}", e)))?;

        Ok(())
    }
}
