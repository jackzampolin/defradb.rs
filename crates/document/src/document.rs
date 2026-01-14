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

use std::collections::HashMap;

use cid::Cid;
use multihash::Multihash;
use schema::{CType, CollectionVersion};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// SHA2-256 multihash code
const SHA2_256_CODE: u64 = 0x12;

/// Raw codec for CID
const RAW_CODEC: u64 = 0x55;

use crate::encoding::{
    canonical_cbor_key_order, cbor_to_normal_value, json_to_normal_value, normal_value_to_cbor,
    normal_value_to_json,
};
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
            } else {
                return Err(Error::InvalidFieldValue {
                    field: DOC_ID.to_string(),
                    message: format!("_docID must be a string, got: {}", id_value),
                });
            }
        }

        // Convert remaining fields
        for (key, value) in map {
            let normal_value = json_to_normal_value(value)?;
            let field = Field::lww(&key)?;
            doc.fields.insert(key.clone(), field);
            doc.values.insert(key, FieldValue::new_lww(normal_value));
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
                .insert(field_name.clone(), Field::lww_unchecked(&field_name));
        }

        // Set the value
        if let Some(fv) = self.values.get_mut(&field_name) {
            fv.set_value(normal_value);
        } else {
            self.values
                .insert(field_name, FieldValue::new_lww(normal_value));
        }

        self.is_dirty = true;
    }

    /// Set a field value with a specific CRDT type.
    ///
    /// Returns an error if:
    /// - The field name is empty
    /// - The value type is incompatible with the CRDT type
    pub fn set_with_crdt(
        &mut self,
        field: impl Into<String>,
        crdt_type: CType,
        value: impl Into<NormalValue>,
    ) -> Result<()> {
        let field_name = field.into();
        let normal_value = value.into();

        let field_def = Field::new(&field_name, crdt_type)?;
        let field_value = FieldValue::new(crdt_type, normal_value)?;
        self.fields.insert(field_name.clone(), field_def);
        self.values.insert(field_name, field_value);
        self.is_dirty = true;
        Ok(())
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
    ///
    /// Returns an error if any field contains non-finite floats (NaN, Infinity),
    /// matching Go's encoding/json behavior.
    pub fn to_map(&self) -> Result<HashMap<String, serde_json::Value>> {
        let mut map = HashMap::new();

        if let Some(ref id) = self.id {
            map.insert(
                DOC_ID.to_string(),
                serde_json::Value::String(id.to_string()),
            );
        }

        for (key, field_value) in &self.values {
            map.insert(key.clone(), normal_value_to_json(field_value.value())?);
        }

        Ok(map)
    }

    /// Encode the document to CBOR bytes.
    ///
    /// This encodes only the values, not the metadata (id, head, dirty flag).
    /// Uses canonical CBOR ordering (RFC 7049 Section 3.9) for Go compatibility:
    /// - Keys sorted by length first (shorter keys first)
    /// - Then lexicographically (bytewise) within same length
    pub fn to_cbor(&self) -> Result<Vec<u8>> {
        // Collect keys and sort using canonical CBOR ordering
        let mut keys: Vec<&str> = self.values.keys().map(|k| k.as_str()).collect();
        keys.sort_by(canonical_cbor_key_order);

        // Build CBOR map using ciborium::Value for proper map encoding
        let mut map_entries: Vec<(ciborium::Value, ciborium::Value)> =
            Vec::with_capacity(keys.len());
        for k in keys {
            let key = ciborium::Value::Text(k.to_string());
            let value = normal_value_to_cbor(
                self.values
                    .get(k)
                    .ok_or_else(|| Error::FieldNotFound(k.to_string()))?
                    .value(),
            )?;
            map_entries.push((key, value));
        }

        let cbor_map = ciborium::Value::Map(map_entries);

        let mut buf = Vec::new();
        ciborium::into_writer(&cbor_map, &mut buf).map_err(|e| Error::CborEncode(e.to_string()))?;
        Ok(buf)
    }

    /// Decode a document from CBOR bytes.
    ///
    /// This decodes only the values. The document will have no ID, head, or collection set.
    /// The document is marked as clean (not dirty) since it was loaded from storage.
    pub fn from_cbor(bytes: &[u8]) -> Result<Self> {
        let cbor_map: ciborium::Value =
            ciborium::from_reader(bytes).map_err(|e| Error::CborDecode(e.to_string()))?;

        let map = match cbor_map {
            ciborium::Value::Map(m) => m,
            _ => return Err(Error::CborDecode("expected CBOR map".into())),
        };

        let mut doc = Document::new();
        doc.is_dirty = false; // Loaded from storage, not dirty

        for (k, v) in map {
            let key = match k {
                ciborium::Value::Text(s) => s,
                _ => return Err(Error::CborDecode("map key must be text".into())),
            };

            let normal_value = cbor_to_normal_value(v)?;
            // Data from storage was previously validated
            let field = Field::lww_unchecked(&key);
            doc.fields.insert(key.clone(), field);
            doc.values
                .insert(key, FieldValue::new_clean_lww(normal_value));
        }

        Ok(doc)
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

        let map = doc.to_map().unwrap();
        assert_eq!(
            map.get("name"),
            Some(&serde_json::Value::String("Charlie".into()))
        );
        assert_eq!(map.get("count"), Some(&serde_json::json!(42)));
    }

    #[test]
    fn test_to_map_non_finite_float_error() {
        let mut doc = Document::new();
        doc.set("name", "test");
        doc.set("value", f64::NAN);

        let result = doc.to_map();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::NonFiniteFloat(_)));
    }

    #[test]
    fn test_to_map_infinity_error() {
        let mut doc = Document::new();
        doc.set("value", f64::INFINITY);

        let result = doc.to_map();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::NonFiniteFloat(_)));
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
    fn test_doc_id_field_order_independence() {
        // Verify that documents with same content but different field insertion order
        // produce the same DocID (canonical CBOR ordering ensures this)
        let mut doc1 = Document::new();
        doc1.set("a", "first");
        doc1.set("b", "second");
        doc1.set("c", "third");

        let mut doc2 = Document::new();
        doc2.set("c", "third"); // Different insertion order
        doc2.set("a", "first");
        doc2.set("b", "second");

        let mut doc3 = Document::new();
        doc3.set("b", "second"); // Another order
        doc3.set("c", "third");
        doc3.set("a", "first");

        let id1 = doc1.generate_doc_id().unwrap();
        let id2 = doc2.generate_doc_id().unwrap();
        let id3 = doc3.generate_doc_id().unwrap();

        assert_eq!(
            id1, id2,
            "DocID should be same regardless of field insertion order"
        );
        assert_eq!(
            id2, id3,
            "DocID should be same regardless of field insertion order"
        );

        // Also verify CBOR encoding is identical
        assert_eq!(doc1.to_cbor().unwrap(), doc2.to_cbor().unwrap());
        assert_eq!(doc2.to_cbor().unwrap(), doc3.to_cbor().unwrap());
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

        // First byte should be 0xa2 (map with 2 entries)
        assert_eq!(cbor[0], 0xa2, "CBOR should start with map marker");
    }

    #[test]
    fn test_cbor_roundtrip() {
        let mut doc = Document::new();
        doc.set("name", "Alice");
        doc.set("age", 30i64);
        doc.set("active", true);
        doc.set("score", 95.5);

        let cbor = doc.to_cbor().unwrap();
        let decoded = Document::from_cbor(&cbor).unwrap();

        assert_eq!(
            doc.get("name").and_then(|v| v.as_str()),
            decoded.get("name").and_then(|v| v.as_str())
        );
        assert_eq!(
            doc.get("age").and_then(|v| v.as_int()),
            decoded.get("age").and_then(|v| v.as_int())
        );
        assert_eq!(
            doc.get("active").and_then(|v| v.as_bool()),
            decoded.get("active").and_then(|v| v.as_bool())
        );
        assert_eq!(
            doc.get("score").and_then(|v| v.as_float64()),
            decoded.get("score").and_then(|v| v.as_float64())
        );

        // Decoded document should not be dirty (loaded from storage)
        assert!(!decoded.is_dirty());
    }

    #[test]
    fn test_cbor_roundtrip_empty_doc() {
        let doc = Document::new();
        let cbor = doc.to_cbor().unwrap();
        let decoded = Document::from_cbor(&cbor).unwrap();

        assert!(decoded.is_empty());
        assert!(!decoded.is_dirty());
    }

    #[test]
    fn test_cbor_roundtrip_arrays() {
        let mut doc = Document::new();
        doc.set("ints", NormalValue::IntArray(vec![1, 2, 3]));
        doc.set(
            "strings",
            NormalValue::StringArray(vec!["a".into(), "b".into()]),
        );
        doc.set("bools", NormalValue::BoolArray(vec![true, false]));

        let cbor = doc.to_cbor().unwrap();
        let decoded = Document::from_cbor(&cbor).unwrap();

        assert!(decoded.get("ints").unwrap().is_array());
        assert!(decoded.get("strings").unwrap().is_array());
        assert!(decoded.get("bools").unwrap().is_array());
    }

    #[test]
    fn test_from_cbor_invalid_bytes() {
        let result = Document::from_cbor(&[0xff, 0xfe, 0xfd]);
        assert!(result.is_err());
    }

    #[test]
    fn test_from_cbor_not_a_map() {
        // CBOR integer instead of map
        let result = Document::from_cbor(&[0x18, 0x2a]); // Integer 42
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::CborDecode(_)));
    }

    #[test]
    fn test_cbor_roundtrip_go_simple_doc() {
        // Decode Go's CBOR and verify we can read it
        let decoded = Document::from_cbor(&[
            0xa2, 0x63, 0x41, 0x67, 0x65, 0x18, 0x1a, 0x64, 0x4e, 0x61, 0x6d, 0x65, 0x64, 0x4a,
            0x6f, 0x68, 0x6e,
        ])
        .unwrap();

        assert_eq!(decoded.get("Name").and_then(|v| v.as_str()), Some("John"));
        assert_eq!(decoded.get("Age").and_then(|v| v.as_int()), Some(26));
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

    // ==========================================================================
    // Golden tests for Go wire compatibility
    // Generated from Go DefraDB using: go test -v -run TestGenerateRustFixtures ./client/
    // ==========================================================================

    // SDN Namespace UUID for verification (must match Go's SDNNamespaceV0)
    const GO_SDN_NAMESPACE: &str = "c94acbfa-dd53-40d0-97f3-29ce16c333fc";

    // Test case 1: Simple document {"Name": "John", "Age": 26}
    const GO_SIMPLE_DOC_CBOR: &[u8] = &[
        0xa2, 0x63, 0x41, 0x67, 0x65, 0x18, 0x1a, 0x64, 0x4e, 0x61, 0x6d, 0x65, 0x64, 0x4a, 0x6f,
        0x68, 0x6e,
    ];

    // Test case 2: String-only document {"Name": "Alice"}
    const GO_STRING_DOC_CBOR: &[u8] = &[
        0xa1, 0x64, 0x4e, 0x61, 0x6d, 0x65, 0x65, 0x41, 0x6c, 0x69, 0x63, 0x65,
    ];

    // Test case 3: Boolean document {"Active": true}
    const GO_BOOL_DOC_CBOR: &[u8] = &[0xa1, 0x66, 0x41, 0x63, 0x74, 0x69, 0x76, 0x65, 0xf5];

    // Test case 4: Empty document {}
    const GO_EMPTY_DOC_CBOR: &[u8] = &[0xa0];

    #[test]
    fn test_sdn_namespace_matches_go() {
        use crate::SDN_NAMESPACE_V0;
        assert_eq!(
            SDN_NAMESPACE_V0.to_string(),
            GO_SDN_NAMESPACE,
            "SDN namespace UUID must match Go's SDNNamespaceV0"
        );
    }

    #[test]
    fn test_cbor_matches_go_simple_doc() {
        // Go test case: {"Name": "John", "Age": 26}
        let mut doc = Document::new();
        doc.set("Name", "John");
        doc.set("Age", 26i64);

        let cbor = doc.to_cbor().unwrap();
        assert_eq!(
            cbor, GO_SIMPLE_DOC_CBOR,
            "CBOR encoding must match Go's output.\nRust: {:02x?}\nGo:   {:02x?}",
            cbor, GO_SIMPLE_DOC_CBOR
        );
    }

    #[test]
    fn test_cbor_matches_go_string_doc() {
        // Go test case: {"Name": "Alice"}
        let mut doc = Document::new();
        doc.set("Name", "Alice");

        let cbor = doc.to_cbor().unwrap();
        assert_eq!(
            cbor, GO_STRING_DOC_CBOR,
            "CBOR encoding must match Go's output.\nRust: {:02x?}\nGo:   {:02x?}",
            cbor, GO_STRING_DOC_CBOR
        );
    }

    #[test]
    fn test_cbor_matches_go_bool_doc() {
        // Go test case: {"Active": true}
        let mut doc = Document::new();
        doc.set("Active", true);

        let cbor = doc.to_cbor().unwrap();
        assert_eq!(
            cbor, GO_BOOL_DOC_CBOR,
            "CBOR encoding must match Go's output.\nRust: {:02x?}\nGo:   {:02x?}",
            cbor, GO_BOOL_DOC_CBOR
        );
    }

    #[test]
    fn test_cbor_matches_go_empty_doc() {
        // Go test case: {}
        let doc = Document::new();

        let cbor = doc.to_cbor().unwrap();
        assert_eq!(
            cbor, GO_EMPTY_DOC_CBOR,
            "CBOR encoding must match Go's output.\nRust: {:02x?}\nGo:   {:02x?}",
            cbor, GO_EMPTY_DOC_CBOR
        );
    }

    #[test]
    fn test_canonical_cbor_key_ordering() {
        // Verify canonical CBOR ordering: shorter keys first, then lexicographic
        // Keys: "z" (1 char), "aa" (2 chars), "ab" (2 chars)
        let mut doc = Document::new();
        doc.set("ab", 3i64);
        doc.set("z", 1i64);
        doc.set("aa", 2i64);

        let cbor = doc.to_cbor().unwrap();

        // Expected order: z (1 char), aa (2 chars), ab (2 chars)
        // a3 = map with 3 entries
        // 61 7a = "z"
        // 01 = 1
        // 62 61 61 = "aa"
        // 02 = 2
        // 62 61 62 = "ab"
        // 03 = 3
        let expected = &[
            0xa3, 0x61, 0x7a, 0x01, 0x62, 0x61, 0x61, 0x02, 0x62, 0x61, 0x62, 0x03,
        ];
        assert_eq!(
            cbor, expected,
            "Keys should be sorted by length first, then lexicographically.\nGot: {:02x?}\nExpected: {:02x?}",
            cbor, expected
        );
    }

    #[test]
    fn test_doc_id_string_format() {
        // DocID string should start with "bae-" (base32 encoded version 0x01)
        let mut doc = Document::new();
        doc.set("test", "value");
        doc.generate_and_set_doc_id().unwrap();

        let doc_id_str = doc.id().unwrap().to_string();
        assert!(
            doc_id_str.starts_with("bae-"),
            "DocID should start with 'bae-' prefix, got: {}",
            doc_id_str
        );

        // Should contain a valid UUID after the prefix
        let uuid_part = &doc_id_str[4..]; // Skip "bae-"
        assert!(
            uuid::Uuid::parse_str(uuid_part).is_ok(),
            "DocID should contain valid UUID after prefix, got: {}",
            uuid_part
        );
    }
}
