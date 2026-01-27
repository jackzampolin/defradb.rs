//! JavaScript interop helpers.
//!
//! Utilities for converting between Rust types and JavaScript values.

use serde::{de::DeserializeOwned, Serialize};
use wasm_bindgen::prelude::*;

use crate::error::{Result, WasmError};

/// Convert a Rust value to a JavaScript value.
pub fn to_js<T: Serialize>(value: &T) -> Result<JsValue> {
    serde_wasm_bindgen::to_value(value).map_err(WasmError::from)
}

/// Convert a JavaScript value to a Rust type.
pub fn from_js<T: DeserializeOwned>(value: JsValue) -> Result<T> {
    serde_wasm_bindgen::from_value(value).map_err(WasmError::from)
}

/// Client configuration passed from JavaScript.
#[derive(Debug, Clone, serde::Deserialize)]
#[serde(default)]
pub struct ClientConfig {
    /// Storage type: "memory" or "indexeddb"
    pub storage: StorageType,

    /// Database name (for IndexedDB)
    pub db_name: Option<String>,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            storage: StorageType::Memory,
            db_name: None,
        }
    }
}

/// Storage backend type.
#[derive(Debug, Clone, Copy, Default, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StorageType {
    #[default]
    Memory,
    IndexedDb,
}

/// Collection info returned to JavaScript.
#[derive(Debug, Clone, serde::Serialize)]
pub struct CollectionInfo {
    pub name: String,
    pub schema_version_id: String,
    pub fields: Vec<FieldInfo>,
}

/// Field info within a collection.
#[derive(Debug, Clone, serde::Serialize)]
pub struct FieldInfo {
    pub name: String,
    pub kind: String,
    pub crdt_type: String,
}

impl From<&schema::FieldDescription> for FieldInfo {
    fn from(field: &schema::FieldDescription) -> Self {
        Self {
            name: field.name.clone(),
            kind: format!("{:?}", field.kind),
            crdt_type: format!("{:?}", field.crdt_type),
        }
    }
}
