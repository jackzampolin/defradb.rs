
//! Document type for DefraDB

use std::collections::HashMap;

use cid::Cid;
use multihash::MultihashGeneric;
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
        let mh: MultihashGeneric<64> = MultihashGeneric::wrap(SHA2_256_CODE, &hash_bytes)
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
