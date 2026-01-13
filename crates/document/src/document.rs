// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! Document type for DefraDB

use std::collections::{BTreeMap, HashMap};

use cid::Cid;
use multihash::Multihash;
use schema::{CType, CollectionVersion};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// SHA2-256 multihash code
const SHA2_256_CODE: u64 = 0x12;

/// Raw codec for CID
const RAW_CODEC: u64 = 0x55;

use crate::error::{Error, Result};
use crate::field::special::DOC_ID;
use crate::{DocID, Field, FieldValue, NormalValue};

/// A document in DefraDB.
///
/// Documents are the core data type in DefraDB, representing JSON-like objects
/// with fields and values. Each document has:
/// - An optional ID (generated from content if not provided)
/// - A collection of fields with their values
/// - A head CID representing the current state
/// - Dirty tracking for unsaved changes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Document {
    /// The document ID (content-addressed)
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<DocID>,

    /// Field definitions (name -> Field)
    #[serde(skip)]
    fields: HashMap<String, Field>,

    /// Field values (field name -> FieldValue)
    values: HashMap<String, FieldValue>,

    /// Current head CID (after save)
    #[serde(skip)]
    head: Option<Cid>,

    /// Whether the document has unsaved changes
    #[serde(skip)]
    is_dirty: bool,

    /// The collection schema this document belongs to
    #[serde(skip)]
    collection: Option<CollectionVersion>,
}

impl Document {
    /// Create a new empty document.
    pub fn new() -> Self {
        Self {
            id: None,
            fields: HashMap::new(),
            values: HashMap::new(),
            head: None,
            is_dirty: true,
            collection: None,
        }
    }

    /// Create a new document with a specific collection schema.
    pub fn with_collection(collection: CollectionVersion) -> Self {
        Self {
            id: None,
            fields: HashMap::new(),
            values: HashMap::new(),
            head: None,
            is_dirty: true,
            collection: Some(collection),
        }
    }

    /// Create a new document with a specific ID.
    pub fn with_id(id: DocID) -> Self {
        Self {
            id: Some(id),
            fields: HashMap::new(),
            values: HashMap::new(),
            head: None,
            is_dirty: true,
            collection: None,
        }
    }

    /// Create a document from a JSON byte slice.
    pub fn from_json(json: &[u8]) -> Result<Self> {
        let map: HashMap<String, serde_json::Value> = serde_json::from_slice(json)?;
        Self::from_map(map)
    }

    /// Create a document from a JSON string.
    pub fn from_json_str(json: &str) -> Result<Self> {
        Self::from_json(json.as_bytes())
    }

    /// Create a document from a map of values.
    pub fn from_map(mut map: HashMap<String, serde_json::Value>) -> Result<Self> {
        let mut doc = Document::new();

        // Check for special _docID field
        if let Some(id_value) = map.remove(DOC_ID) {
            if let Some(id_str) = id_value.as_str() {
                doc.id = Some(DocID::from_string(id_str)?);
            }
        }

        // Convert remaining fields
        for (key, value) in map {
            let normal_value = json_to_normal_value(value);
            let field = Field::lww(&key);
            doc.fields.insert(key.clone(), field);
            doc.values
                .insert(key, FieldValue::new(CType::LwwRegister, normal_value));
        }

        // Generate DocID if not provided
        if doc.id.is_none() {
            doc.generate_and_set_doc_id()?;
        }

        Ok(doc)
    }

    /// Get the document ID.
    pub fn id(&self) -> Option<&DocID> {
        self.id.as_ref()
    }

    /// Set the document ID.
    pub fn set_id(&mut self, id: DocID) {
        self.id = Some(id);
    }

    /// Get the current head CID.
    pub fn head(&self) -> Option<&Cid> {
        self.head.as_ref()
    }

    /// Set the head CID.
    pub fn set_head(&mut self, cid: Cid) {
        self.head = Some(cid);
    }

    /// Get the collection schema.
    pub fn collection(&self) -> Option<&CollectionVersion> {
        self.collection.as_ref()
    }

    /// Set the collection schema.
    pub fn set_collection(&mut self, collection: CollectionVersion) {
        self.collection = Some(collection);
    }

    /// Check if the document has unsaved changes.
    pub fn is_dirty(&self) -> bool {
        self.is_dirty || self.values.values().any(|v| v.is_dirty())
    }

    /// Mark the document as clean (saved).
    pub fn clean(&mut self) {
        self.is_dirty = false;
        for value in self.values.values_mut() {
            value.clean();
        }
    }

    /// Mark the document as dirty.
    pub fn mark_dirty(&mut self) {
        self.is_dirty = true;
    }

    /// Get a field value by name.
    pub fn get(&self, field: &str) -> Option<&NormalValue> {
        self.values.get(field).map(|fv| fv.value())
    }

    /// Get a field value wrapper by name.
    pub fn get_field_value(&self, field: &str) -> Option<&FieldValue> {
        self.values.get(field)
    }

    /// Get a mutable field value by name.
    pub fn get_mut(&mut self, field: &str) -> Option<&mut FieldValue> {
        self.is_dirty = true;
        self.values.get_mut(field)
    }

    /// Set a field value.
    pub fn set(&mut self, field: impl Into<String>, value: impl Into<NormalValue>) {
        let field_name = field.into();
        let normal_value = value.into();

        // Create or update the field
        if !self.fields.contains_key(&field_name) {
            self.fields
                .insert(field_name.clone(), Field::lww(&field_name));
        }

        // Set the value
        if let Some(fv) = self.values.get_mut(&field_name) {
            fv.set_value(normal_value);
        } else {
            self.values.insert(
                field_name,
                FieldValue::new(CType::LwwRegister, normal_value),
            );
        }

        self.is_dirty = true;
    }

    /// Set a field value with a specific CRDT type.
    pub fn set_with_crdt(
        &mut self,
        field: impl Into<String>,
        crdt_type: CType,
        value: impl Into<NormalValue>,
    ) {
        let field_name = field.into();
        let normal_value = value.into();

        self.fields
            .insert(field_name.clone(), Field::new(&field_name, crdt_type));
        self.values
            .insert(field_name, FieldValue::new(crdt_type, normal_value));
        self.is_dirty = true;
    }

    /// Remove a field from the document.
    pub fn remove(&mut self, field: &str) -> Option<FieldValue> {
        self.fields.remove(field);
        let result = self.values.remove(field);
        if result.is_some() {
            self.is_dirty = true;
        }
        result
    }

    /// Get all field names.
    pub fn field_names(&self) -> impl Iterator<Item = &str> {
        self.values.keys().map(|s| s.as_str())
    }

    /// Get all fields.
    pub fn fields(&self) -> &HashMap<String, Field> {
        &self.fields
    }

    /// Get all values.
    pub fn values(&self) -> &HashMap<String, FieldValue> {
        &self.values
    }

    /// Get all dirty fields.
    pub fn dirty_fields(&self) -> impl Iterator<Item = (&str, &FieldValue)> {
        self.values
            .iter()
            .filter(|(_, v)| v.is_dirty())
            .map(|(k, v)| (k.as_str(), v))
    }

    /// Convert the document to a map of values.
    pub fn to_map(&self) -> HashMap<String, serde_json::Value> {
        let mut map = HashMap::new();

        if let Some(ref id) = self.id {
            map.insert(
                DOC_ID.to_string(),
                serde_json::Value::String(id.to_string()),
            );
        }

        for (key, field_value) in &self.values {
            map.insert(key.clone(), normal_value_to_json(field_value.value()));
        }

        map
    }

    /// Encode the document to CBOR bytes.
    ///
    /// This encodes only the values, not the metadata (id, head, dirty flag).
    /// The CBOR encoding uses sorted keys for deterministic output.
    pub fn to_cbor(&self) -> Result<Vec<u8>> {
        // Use BTreeMap for deterministic key ordering (lexicographic)
        let mut map: BTreeMap<&str, &NormalValue> = BTreeMap::new();
        for (key, fv) in &self.values {
            map.insert(key, fv.value());
        }

        let mut buf = Vec::new();
        ciborium::into_writer(&map, &mut buf).map_err(|e| Error::CborEncode(e.to_string()))?;
        Ok(buf)
    }

    /// Generate the document ID from its content.
    ///
    /// The DocID is generated from:
    /// 1. CBOR encoding of the document values
    /// 2. Collection ID (if available)
    /// 3. SHA-256 hash of the combined bytes
    /// 4. UUID v5 derived from the hash
    pub fn generate_doc_id(&self) -> Result<DocID> {
        let cbor_bytes = self.to_cbor()?;

        // If we have a collection, include its ID in the hash
        let mut hash_input = cbor_bytes;
        if let Some(ref collection) = self.collection {
            hash_input.extend_from_slice(collection.collection_id.as_bytes());
        }

        // Hash with SHA2-256
        let mut hasher = Sha256::new();
        hasher.update(&hash_input);
        let hash_bytes = hasher.finalize();

        // Create multihash
        let mh: Multihash<64> = Multihash::wrap(SHA2_256_CODE, &hash_bytes)
            .map_err(|e| Error::CborEncode(format!("Failed to create multihash: {}", e)))?;

        // Create CID from the hash
        let cid = Cid::new_v1(RAW_CODEC, mh);

        Ok(DocID::new_v0(cid))
    }

    /// Generate and set the document ID.
    pub fn generate_and_set_doc_id(&mut self) -> Result<()> {
        let doc_id = self.generate_doc_id()?;
        self.id = Some(doc_id);
        Ok(())
    }

    /// Check if the document has a specific field.
    pub fn has_field(&self, field: &str) -> bool {
        self.values.contains_key(field)
    }

    /// Get the number of fields in the document.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Check if the document has no fields.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

impl Default for Document {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for Document {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.values == other.values
    }
}

// === Helper functions ===

/// Convert a JSON value to a NormalValue.
fn json_to_normal_value(value: serde_json::Value) -> NormalValue {
    match value {
        serde_json::Value::Null => NormalValue::Null,
        serde_json::Value::Bool(b) => NormalValue::Bool(b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                NormalValue::Int(i)
            } else if let Some(f) = n.as_f64() {
                NormalValue::Float64(f)
            } else {
                NormalValue::Null
            }
        }
        serde_json::Value::String(s) => NormalValue::String(s),
        serde_json::Value::Array(arr) => {
            // Try to infer array type from first element
            if arr.is_empty() {
                return NormalValue::JsonArray(vec![]);
            }

            match &arr[0] {
                serde_json::Value::Bool(_) => {
                    let bools: Vec<bool> = arr.into_iter().filter_map(|v| v.as_bool()).collect();
                    NormalValue::BoolArray(bools)
                }
                serde_json::Value::Number(n) if n.is_i64() => {
                    let ints: Vec<i64> = arr.into_iter().filter_map(|v| v.as_i64()).collect();
                    NormalValue::IntArray(ints)
                }
                serde_json::Value::Number(_) => {
                    let floats: Vec<f64> = arr.into_iter().filter_map(|v| v.as_f64()).collect();
                    NormalValue::Float64Array(floats)
                }
                serde_json::Value::String(_) => {
                    let strings: Vec<String> = arr
                        .into_iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect();
                    NormalValue::StringArray(strings)
                }
                _ => {
                    // Fall back to JSON array for complex types
                    NormalValue::JsonArray(arr)
                }
            }
        }
        serde_json::Value::Object(_) => {
            // Store complex objects as JSON
            NormalValue::Json(value)
        }
    }
}

/// Convert a NormalValue to a JSON value.
fn normal_value_to_json(value: &NormalValue) -> serde_json::Value {
    match value {
        NormalValue::Null => serde_json::Value::Null,
        NormalValue::Bool(b) => serde_json::Value::Bool(*b),
        NormalValue::Int(i) => serde_json::Value::Number((*i).into()),
        NormalValue::Float64(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        NormalValue::Float32(f) => serde_json::Number::from_f64(*f as f64)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        NormalValue::String(s) => serde_json::Value::String(s.clone()),
        NormalValue::Bytes(b) => {
            // Encode bytes as base64
            serde_json::Value::String(base64_encode(b))
        }
        NormalValue::Time(t) => serde_json::Value::String(t.to_rfc3339()),
        NormalValue::Json(v) => v.clone(),
        NormalValue::IntArray(arr) => serde_json::Value::Array(
            arr.iter()
                .map(|i| serde_json::Value::Number((*i).into()))
                .collect(),
        ),
        NormalValue::StringArray(arr) => serde_json::Value::Array(
            arr.iter()
                .map(|s| serde_json::Value::String(s.clone()))
                .collect(),
        ),
        NormalValue::BoolArray(arr) => {
            serde_json::Value::Array(arr.iter().map(|b| serde_json::Value::Bool(*b)).collect())
        }
        NormalValue::Float64Array(arr) => serde_json::Value::Array(
            arr.iter()
                .filter_map(|f| serde_json::Number::from_f64(*f).map(serde_json::Value::Number))
                .collect(),
        ),
        NormalValue::JsonArray(arr) => serde_json::Value::Array(arr.clone()),
        // Nillable variants
        NormalValue::NillableBool(opt) => opt
            .map(serde_json::Value::Bool)
            .unwrap_or(serde_json::Value::Null),
        NormalValue::NillableInt(opt) => opt
            .map(|i| serde_json::Value::Number(i.into()))
            .unwrap_or(serde_json::Value::Null),
        NormalValue::NillableString(opt) => opt
            .as_ref()
            .map(|s| serde_json::Value::String(s.clone()))
            .unwrap_or(serde_json::Value::Null),
        // For other types, use JSON serialization
        _ => serde_json::to_value(value).unwrap_or(serde_json::Value::Null),
    }
}

fn base64_encode(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_document() {
        let doc = Document::new();
        assert!(doc.id().is_none());
        assert!(doc.is_empty());
        assert!(doc.is_dirty());
    }

    #[test]
    fn test_set_and_get() {
        let mut doc = Document::new();
        doc.set("name", "Alice");
        doc.set("age", 30i64);
        doc.set("active", true);

        assert_eq!(doc.get("name").and_then(|v| v.as_str()), Some("Alice"));
        assert_eq!(doc.get("age").and_then(|v| v.as_int()), Some(30));
        assert_eq!(doc.get("active").and_then(|v| v.as_bool()), Some(true));
        assert!(doc.get("missing").is_none());
    }

    #[test]
    fn test_from_json() {
        let json = r#"{"name": "Bob", "age": 25, "active": true}"#;
        let doc = Document::from_json_str(json).unwrap();

        assert_eq!(doc.get("name").and_then(|v| v.as_str()), Some("Bob"));
        assert_eq!(doc.get("age").and_then(|v| v.as_int()), Some(25));
        assert_eq!(doc.get("active").and_then(|v| v.as_bool()), Some(true));
        assert!(doc.id().is_some()); // DocID should be generated
    }

    #[test]
    fn test_from_json_with_doc_id() {
        // First create a document to get a valid DocID
        let doc1 = Document::from_json_str(r#"{"name": "Test"}"#).unwrap();
        let doc_id_str = doc1.id().unwrap().to_string();

        // Now create a document with that DocID
        let json = format!(r#"{{"_docID": "{}", "name": "Test2"}}"#, doc_id_str);
        let doc2 = Document::from_json_str(&json).unwrap();

        assert_eq!(doc2.id().unwrap().to_string(), doc_id_str);
    }

    #[test]
    fn test_to_map() {
        let mut doc = Document::new();
        doc.set("name", "Charlie");
        doc.set("count", 42i64);

        let map = doc.to_map();
        assert_eq!(
            map.get("name"),
            Some(&serde_json::Value::String("Charlie".into()))
        );
        assert_eq!(map.get("count"), Some(&serde_json::json!(42)));
    }

    #[test]
    fn test_dirty_tracking() {
        let mut doc = Document::new();
        assert!(doc.is_dirty());

        doc.clean();
        assert!(!doc.is_dirty());

        doc.set("field", "value");
        assert!(doc.is_dirty());
    }

    #[test]
    fn test_dirty_fields() {
        let mut doc = Document::new();
        doc.set("field1", "value1");
        doc.set("field2", "value2");
        doc.clean();

        doc.set("field1", "new_value");

        let dirty: Vec<_> = doc.dirty_fields().collect();
        assert_eq!(dirty.len(), 1);
        assert_eq!(dirty[0].0, "field1");
    }

    #[test]
    fn test_remove_field() {
        let mut doc = Document::new();
        doc.set("name", "Alice");
        assert!(doc.has_field("name"));

        doc.remove("name");
        assert!(!doc.has_field("name"));
    }

    #[test]
    fn test_len_and_is_empty() {
        let mut doc = Document::new();
        assert!(doc.is_empty());
        assert_eq!(doc.len(), 0);

        doc.set("field", "value");
        assert!(!doc.is_empty());
        assert_eq!(doc.len(), 1);
    }

    #[test]
    fn test_generate_doc_id_deterministic() {
        let mut doc1 = Document::new();
        doc1.set("name", "Test");
        doc1.set("value", 123i64);

        let mut doc2 = Document::new();
        doc2.set("name", "Test");
        doc2.set("value", 123i64);

        let id1 = doc1.generate_doc_id().unwrap();
        let id2 = doc2.generate_doc_id().unwrap();

        assert_eq!(id1, id2);
    }

    #[test]
    fn test_different_content_different_id() {
        let mut doc1 = Document::new();
        doc1.set("name", "Alice");

        let mut doc2 = Document::new();
        doc2.set("name", "Bob");

        let id1 = doc1.generate_doc_id().unwrap();
        let id2 = doc2.generate_doc_id().unwrap();

        assert_ne!(id1, id2);
    }

    #[test]
    fn test_cbor_encoding() {
        let mut doc = Document::new();
        doc.set("name", "Test");
        doc.set("count", 42i64);

        let cbor = doc.to_cbor().unwrap();
        assert!(!cbor.is_empty());

        // CBOR should be deterministic
        let cbor2 = doc.to_cbor().unwrap();
        assert_eq!(cbor, cbor2);
    }

    #[test]
    fn test_array_fields() {
        let json = r#"{"tags": ["a", "b", "c"], "numbers": [1, 2, 3]}"#;
        let doc = Document::from_json_str(json).unwrap();

        let tags = doc.get("tags");
        assert!(tags.is_some());
        assert!(tags.unwrap().is_array());

        let numbers = doc.get("numbers");
        assert!(numbers.is_some());
        assert!(numbers.unwrap().is_array());
    }
}
