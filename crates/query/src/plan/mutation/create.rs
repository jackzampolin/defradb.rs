//! CreateNode for creating new documents
//!
//! This node creates documents in storage during query execution, following
//! the Go DefraDB pattern where persistence happens within the plan node.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, FixedOffset, Utc};
use document::Document;
use schema::{CollectionVersion, FieldKind, ScalarArrayKind, ScalarKind};
use serde_json::Value as JsonValue;
use tracing;

use crate::document::DocumentMapping;
use crate::error::{QueryError, Result};
use crate::mutator::{CreateResult, DocMutator};
use crate::planner::{Doc, PlanNode};

/// Input for a create mutation - field values to set on the new document.
#[derive(Debug, Clone)]
pub struct CreateInput {
    /// Field values keyed by field name
    pub fields: std::collections::HashMap<String, JsonValue>,
}

impl CreateInput {
    /// Create a new empty input.
    pub fn new() -> Self {
        Self {
            fields: std::collections::HashMap::new(),
        }
    }

    /// Add a field value.
    pub fn with_field(mut self, name: impl Into<String>, value: JsonValue) -> Self {
        self.fields.insert(name.into(), value);
        self
    }

    /// Convert to a Document for storage (without schema-aware type coercion).
    pub fn to_document(&self) -> Result<Document> {
        let mut doc = Document::new();

        for (field_name, value) in &self.fields {
            // Convert JsonValue to appropriate NormalValue
            let normal_value = json_to_normal_value(value)?;
            doc.set(field_name.clone(), normal_value);
        }

        Ok(doc)
    }

    /// Convert to a Document for storage with schema-aware type coercion.
    ///
    /// This method uses the collection schema to properly coerce values,
    /// such as parsing RFC 3339 strings as DateTime values when the field
    /// type is DateTime (matching Go DefraDB behavior).
    pub fn to_document_with_schema(&self, collection: &CollectionVersion) -> Result<Document> {
        let mut doc = Document::new();

        // Set collection on document for proper docID generation.
        // Go DefraDB includes the collection_id in the docID hash, so we must too.
        doc.set_collection(collection.clone());

        for (field_name, value) in &self.fields {
            // Look up the field in the schema to get its kind
            let field_kind = collection
                .fields
                .iter()
                .find(|f| f.name == *field_name)
                .map(|f| &f.kind);

            // Convert JsonValue to appropriate NormalValue, using schema for type coercion
            let normal_value = json_to_normal_value_with_kind(value, field_kind)?;
            doc.set(field_name.clone(), normal_value);
        }

        Ok(doc)
    }
}

impl Default for CreateInput {
    fn default() -> Self {
        Self::new()
    }
}

/// Convert a JSON value to a document NormalValue.
pub fn json_to_normal_value(value: &JsonValue) -> Result<document::NormalValue> {
    use document::NormalValue;

    match value {
        JsonValue::Null => Ok(NormalValue::Null),
        JsonValue::Bool(b) => Ok(NormalValue::Bool(*b)),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(NormalValue::Int(i))
            } else if let Some(f) = n.as_f64() {
                Ok(NormalValue::Float64(f))
            } else {
                Err(QueryError::execution("Invalid number value"))
            }
        }
        JsonValue::String(s) => Ok(NormalValue::String(s.clone())),
        JsonValue::Array(arr) => {
            // Empty arrays default to empty string array
            if arr.is_empty() {
                return Ok(NormalValue::StringArray(Vec::new()));
            }

            // Determine array type from first non-null element
            let first_non_null = arr.iter().find(|v| !v.is_null());

            match first_non_null {
                Some(JsonValue::Bool(_)) => {
                    let mut bools = Vec::with_capacity(arr.len());
                    for (i, v) in arr.iter().enumerate() {
                        match v {
                            JsonValue::Bool(b) => bools.push(*b),
                            JsonValue::Null => bools.push(false),
                            _ => {
                                return Err(QueryError::execution(format!(
                                    "Array element at index {} is not a boolean (found {:?})",
                                    i, v
                                )))
                            }
                        }
                    }
                    Ok(NormalValue::BoolArray(bools))
                }
                Some(JsonValue::Number(n)) if n.is_i64() => {
                    let mut ints = Vec::with_capacity(arr.len());
                    for (i, v) in arr.iter().enumerate() {
                        match v {
                            JsonValue::Number(n) if n.as_i64().is_some() => {
                                ints.push(n.as_i64().unwrap())
                            }
                            JsonValue::Null => ints.push(0),
                            _ => {
                                return Err(QueryError::execution(format!(
                                    "Array element at index {} is not an integer (found {:?})",
                                    i, v
                                )))
                            }
                        }
                    }
                    Ok(NormalValue::IntArray(ints))
                }
                Some(JsonValue::Number(_)) => {
                    let mut floats = Vec::with_capacity(arr.len());
                    for (i, v) in arr.iter().enumerate() {
                        match v {
                            JsonValue::Number(n) => floats.push(n.as_f64().unwrap_or(0.0)),
                            JsonValue::Null => floats.push(0.0),
                            _ => {
                                return Err(QueryError::execution(format!(
                                    "Array element at index {} is not a number (found {:?})",
                                    i, v
                                )))
                            }
                        }
                    }
                    Ok(NormalValue::Float64Array(floats))
                }
                Some(JsonValue::String(_)) => {
                    let mut strings = Vec::with_capacity(arr.len());
                    for (i, v) in arr.iter().enumerate() {
                        match v {
                            JsonValue::String(s) => strings.push(s.clone()),
                            JsonValue::Null => strings.push(String::new()),
                            _ => {
                                return Err(QueryError::execution(format!(
                                    "Array element at index {} is not a string (found {:?})",
                                    i, v
                                )))
                            }
                        }
                    }
                    Ok(NormalValue::StringArray(strings))
                }
                // Array contains only nulls - default to empty strings
                None => {
                    let strings: Vec<String> = arr.iter().map(|_| String::new()).collect();
                    Ok(NormalValue::StringArray(strings))
                }
                // Nested arrays or objects - store as JSON
                Some(_) => Ok(NormalValue::Json(JsonValue::Array(arr.clone()))),
            }
        }
        JsonValue::Object(_) => {
            // Nested objects could be sub-documents - for now, store as JSON
            Ok(NormalValue::Json(value.clone()))
        }
    }
}

/// Convert a JSON value to a document NormalValue with schema-aware type coercion.
///
/// This function uses the field kind to properly coerce values. For example,
/// when the field kind is DateTime, it parses RFC 3339 strings as DateTime values.
/// This matches Go DefraDB's `validateFieldSchema` behavior.
pub fn json_to_normal_value_with_kind(
    value: &JsonValue,
    field_kind: Option<&FieldKind>,
) -> Result<document::NormalValue> {
    use document::NormalValue;

    // Handle null regardless of expected type
    if value.is_null() {
        return Ok(NormalValue::Null);
    }

    // If we have schema information, use it for type coercion
    if let Some(kind) = field_kind {
        match kind {
            // JSON fields: wrap ALL values as JSON (primitives, objects, arrays)
            // This matches Go DefraDB behavior where JSON fields accept any value type
            FieldKind::Scalar(ScalarKind::Json) => Ok(NormalValue::Json(value.clone())),
            // DateTime fields: parse RFC 3339 strings or special values like UTC_NOW
            // CRITICAL: Must preserve original timezone to match Go's CID calculation
            FieldKind::Scalar(ScalarKind::DateTime) => {
                match value {
                    JsonValue::String(s) => {
                        // Handle special value UTC_NOW - return current time in UTC
                        if s == "UTC_NOW" {
                            // Convert to FixedOffset for consistent type
                            let utc_offset = FixedOffset::east_opt(0).unwrap();
                            return Ok(NormalValue::Time(Utc::now().with_timezone(&utc_offset)));
                        }
                        // Parse RFC 3339 string to DateTime, preserving original timezone
                        // Go's time.Parse(time.RFC3339, s) preserves the original timezone
                        let dt = DateTime::parse_from_rfc3339(s).map_err(|e| {
                            QueryError::execution(format!(
                                "Invalid DateTime format '{}': expected RFC 3339 (e.g., '2024-01-15T10:30:00Z'). Error: {}",
                                s, e
                            ))
                        })?;
                        Ok(NormalValue::Time(dt))
                    }
                    // Already a number (Unix timestamp) - not common but handle it
                    JsonValue::Number(n) => {
                        if let Some(ts) = n.as_i64() {
                            let dt = DateTime::from_timestamp(ts, 0).ok_or_else(|| {
                                QueryError::execution(format!("Invalid Unix timestamp: {}", ts))
                            })?;
                            // Convert to FixedOffset in UTC
                            let utc_offset = FixedOffset::east_opt(0).unwrap();
                            Ok(NormalValue::Time(dt.with_timezone(&utc_offset)))
                        } else {
                            Err(QueryError::execution(format!(
                                "Expected DateTime string or Unix timestamp, got: {:?}",
                                value
                            )))
                        }
                    }
                    _ => Err(QueryError::execution(format!(
                        "Expected DateTime string, got: {:?}",
                        value
                    ))),
                }
            }
            // ScalarArray fields: handle empty arrays and nillable elements
            FieldKind::ScalarArray(array_kind) => match value {
                JsonValue::Array(arr) => json_array_to_normal_value_with_kind(arr, *array_kind),
                _ => Err(QueryError::execution(format!(
                    "Expected array, got: {:?}",
                    value
                ))),
            },
            // Float64 fields: convert integers to float64 for Go compatibility
            // Go DefraDB coerces JSON integers to the schema's float type
            FieldKind::Scalar(ScalarKind::Float64) => match value {
                JsonValue::Number(n) => {
                    // Convert any numeric value to f64
                    let f = n.as_f64().ok_or_else(|| {
                        QueryError::execution(format!("Cannot convert number to f64: {:?}", n))
                    })?;
                    Ok(NormalValue::Float64(f))
                }
                _ => Err(QueryError::execution(format!(
                    "Expected number for Float64 field, got: {:?}",
                    value
                ))),
            },
            // Float32 fields: convert integers to float32 for Go compatibility
            FieldKind::Scalar(ScalarKind::Float32) => match value {
                JsonValue::Number(n) => {
                    // Convert any numeric value to f32
                    let f = n.as_f64().ok_or_else(|| {
                        QueryError::execution(format!("Cannot convert number to f32: {:?}", n))
                    })? as f32;
                    Ok(NormalValue::Float32(f))
                }
                _ => Err(QueryError::execution(format!(
                    "Expected number for Float32 field, got: {:?}",
                    value
                ))),
            },
            // For other scalar types, fall through to default conversion
            _ => json_to_normal_value(value),
        }
    } else {
        // No schema info - use default conversion
        json_to_normal_value(value)
    }
}

/// Convert a JSON array to NormalValue using schema-aware type coercion.
///
/// This function properly handles:
/// - Empty arrays: returns the correct typed empty array based on array_kind
/// - Nillable elements: preserves null values instead of converting to defaults
fn json_array_to_normal_value_with_kind(
    arr: &[JsonValue],
    array_kind: ScalarArrayKind,
) -> Result<document::NormalValue> {
    use document::NormalValue;

    match array_kind {
        // Boolean arrays
        ScalarArrayKind::BoolArray => {
            let mut bools = Vec::with_capacity(arr.len());
            for (i, v) in arr.iter().enumerate() {
                match v {
                    JsonValue::Bool(b) => bools.push(*b),
                    JsonValue::Null => bools.push(false),
                    _ => {
                        return Err(QueryError::execution(format!(
                            "Array element at index {} is not a boolean (found {:?})",
                            i, v
                        )))
                    }
                }
            }
            Ok(NormalValue::BoolArray(bools))
        }
        ScalarArrayKind::NillableBoolArray => {
            let mut bools = Vec::with_capacity(arr.len());
            for (i, v) in arr.iter().enumerate() {
                match v {
                    JsonValue::Bool(b) => bools.push(Some(*b)),
                    JsonValue::Null => bools.push(None),
                    _ => {
                        return Err(QueryError::execution(format!(
                            "Array element at index {} is not a boolean (found {:?})",
                            i, v
                        )))
                    }
                }
            }
            Ok(NormalValue::NillableBoolElementArray(bools))
        }

        // Integer arrays
        ScalarArrayKind::IntArray => {
            let mut ints = Vec::with_capacity(arr.len());
            for (i, v) in arr.iter().enumerate() {
                match v {
                    JsonValue::Number(n) if n.as_i64().is_some() => ints.push(n.as_i64().unwrap()),
                    JsonValue::Null => ints.push(0),
                    _ => {
                        return Err(QueryError::execution(format!(
                            "Array element at index {} is not an integer (found {:?})",
                            i, v
                        )))
                    }
                }
            }
            Ok(NormalValue::IntArray(ints))
        }
        ScalarArrayKind::NillableIntArray => {
            let mut ints = Vec::with_capacity(arr.len());
            for (i, v) in arr.iter().enumerate() {
                match v {
                    JsonValue::Number(n) if n.as_i64().is_some() => {
                        ints.push(Some(n.as_i64().unwrap()))
                    }
                    JsonValue::Null => ints.push(None),
                    _ => {
                        return Err(QueryError::execution(format!(
                            "Array element at index {} is not an integer (found {:?})",
                            i, v
                        )))
                    }
                }
            }
            Ok(NormalValue::NillableIntElementArray(ints))
        }

        // Float64 arrays
        ScalarArrayKind::Float64Array => {
            let mut floats = Vec::with_capacity(arr.len());
            for (i, v) in arr.iter().enumerate() {
                match v {
                    JsonValue::Number(n) => floats.push(n.as_f64().unwrap_or(0.0)),
                    JsonValue::Null => floats.push(0.0),
                    _ => {
                        return Err(QueryError::execution(format!(
                            "Array element at index {} is not a number (found {:?})",
                            i, v
                        )))
                    }
                }
            }
            Ok(NormalValue::Float64Array(floats))
        }
        ScalarArrayKind::NillableFloat64Array => {
            let mut floats = Vec::with_capacity(arr.len());
            for (i, v) in arr.iter().enumerate() {
                match v {
                    JsonValue::Number(n) => floats.push(Some(n.as_f64().unwrap_or(0.0))),
                    JsonValue::Null => floats.push(None),
                    _ => {
                        return Err(QueryError::execution(format!(
                            "Array element at index {} is not a number (found {:?})",
                            i, v
                        )))
                    }
                }
            }
            Ok(NormalValue::NillableFloat64ElementArray(floats))
        }

        // Float32 arrays
        ScalarArrayKind::Float32Array => {
            let mut floats = Vec::with_capacity(arr.len());
            for (i, v) in arr.iter().enumerate() {
                match v {
                    JsonValue::Number(n) => floats.push(n.as_f64().unwrap_or(0.0) as f32),
                    JsonValue::Null => floats.push(0.0),
                    _ => {
                        return Err(QueryError::execution(format!(
                            "Array element at index {} is not a number (found {:?})",
                            i, v
                        )))
                    }
                }
            }
            Ok(NormalValue::Float32Array(floats))
        }
        ScalarArrayKind::NillableFloat32Array => {
            let mut floats = Vec::with_capacity(arr.len());
            for (i, v) in arr.iter().enumerate() {
                match v {
                    JsonValue::Number(n) => floats.push(Some(n.as_f64().unwrap_or(0.0) as f32)),
                    JsonValue::Null => floats.push(None),
                    _ => {
                        return Err(QueryError::execution(format!(
                            "Array element at index {} is not a number (found {:?})",
                            i, v
                        )))
                    }
                }
            }
            Ok(NormalValue::NillableFloat32ElementArray(floats))
        }

        // String arrays
        ScalarArrayKind::StringArray => {
            let mut strings = Vec::with_capacity(arr.len());
            for (i, v) in arr.iter().enumerate() {
                match v {
                    JsonValue::String(s) => strings.push(s.clone()),
                    JsonValue::Null => strings.push(String::new()),
                    _ => {
                        return Err(QueryError::execution(format!(
                            "Array element at index {} is not a string (found {:?})",
                            i, v
                        )))
                    }
                }
            }
            Ok(NormalValue::StringArray(strings))
        }
        ScalarArrayKind::NillableStringArray => {
            let mut strings = Vec::with_capacity(arr.len());
            for (i, v) in arr.iter().enumerate() {
                match v {
                    JsonValue::String(s) => strings.push(Some(s.clone())),
                    JsonValue::Null => strings.push(None),
                    _ => {
                        return Err(QueryError::execution(format!(
                            "Array element at index {} is not a string (found {:?})",
                            i, v
                        )))
                    }
                }
            }
            Ok(NormalValue::NillableStringElementArray(strings))
        }
    }
}

/// CreateNode creates new documents in a collection.
///
/// This node implements the Volcano iterator model, yielding created documents
/// one at a time. On the first call to `next()`, all documents are created in
/// storage via the `DocMutator`. Subsequent calls iterate through the results.
///
/// # Example
///
/// ```ignore
/// let input = CreateInput::new()
///     .with_field("name", json!("Alice"))
///     .with_field("age", json!(30));
///
/// let mut node = CreateNode::new("Users", mutator, mapping)
///     .with_input(input);
///
/// node.init().await?;
/// node.start().await?;
///
/// while node.next().await? {
///     let created_doc = node.value();
///     println!("Created: {:?}", created_doc.doc_id());
/// }
/// ```
pub struct CreateNode {
    /// Collection name to create documents in
    collection_name: String,
    /// Document mutator for storage operations
    mutator: Arc<dyn DocMutator>,
    /// Document mapping for field positions
    document_mapping: DocumentMapping,
    /// Collection schema for schema-aware type coercion (e.g., DateTime parsing)
    collection: Option<Arc<CollectionVersion>>,
    /// Input documents to create
    inputs: Vec<CreateInput>,
    /// Created documents (populated after first next())
    created_docs: Vec<Doc>,
    /// Current position in created_docs
    position: usize,
    /// Current document being yielded
    current_doc: Doc,
    /// Whether documents have been created yet
    did_create: bool,
    /// Whether the node has been initialized
    initialized: bool,
}

impl CreateNode {
    /// Create a new create node for a collection.
    ///
    /// # Arguments
    ///
    /// * `collection_name` - Name of the collection to create documents in
    /// * `mutator` - Document mutator for storage operations
    /// * `document_mapping` - Field mapping for result documents
    pub fn new(
        collection_name: impl Into<String>,
        mutator: Arc<dyn DocMutator>,
        document_mapping: DocumentMapping,
    ) -> Self {
        Self {
            collection_name: collection_name.into(),
            mutator,
            document_mapping,
            collection: None,
            inputs: Vec::new(),
            created_docs: Vec::new(),
            position: 0,
            current_doc: Doc::default(),
            did_create: false,
            initialized: false,
        }
    }

    /// Add an input document to create.
    pub fn with_input(mut self, input: CreateInput) -> Self {
        self.inputs.push(input);
        self
    }

    /// Add multiple input documents.
    pub fn with_inputs(mut self, inputs: Vec<CreateInput>) -> Self {
        self.inputs = inputs;
        self
    }

    /// Set the collection schema for schema-aware type coercion.
    ///
    /// When set, the node will use the schema to properly coerce values during
    /// document creation (e.g., parsing RFC 3339 strings as DateTime values).
    pub fn with_collection(mut self, collection: Arc<CollectionVersion>) -> Self {
        self.collection = Some(collection);
        self
    }

    /// Get the number of documents that were created.
    pub fn created_count(&self) -> usize {
        self.created_docs.len()
    }

    /// Convert a CreateResult to a plan Doc using our document mapping.
    fn create_result_to_doc(&self, result: &CreateResult) -> Result<Doc> {
        let num_fields = self.document_mapping.next_index();
        let mut doc = Doc::new(num_fields);

        // Set document ID at index 0
        doc.set_doc_id(result.doc_id.to_string());

        // Map each field from the created document
        for (field_name, field_value) in result.document.values() {
            if let Some(index) = self.document_mapping.first_index_of_name(field_name) {
                // Convert NormalValue back to JsonValue for the plan Doc
                let json_value = normal_value_to_json(field_value.value());
                doc.set(index, json_value);
            }
        }

        Ok(doc)
    }
}

/// Convert a NormalValue to JsonValue for plan Doc storage.
pub fn normal_value_to_json(value: &document::NormalValue) -> JsonValue {
    use document::NormalValue;

    match value {
        NormalValue::Null => JsonValue::Null,
        NormalValue::Bool(b) => JsonValue::Bool(*b),
        NormalValue::Int(i) => JsonValue::Number((*i).into()),
        NormalValue::Float64(f) => {
            if f.is_nan() || f.is_infinite() {
                // NaN and Infinity cannot be represented in JSON - use null but log
                tracing::warn!(value = %f, "Float64 value cannot be represented in JSON, using null");
                JsonValue::Null
            } else {
                serde_json::Number::from_f64(*f)
                    .map(JsonValue::Number)
                    .unwrap_or(JsonValue::Null)
            }
        }
        NormalValue::Float32(f) => {
            if f.is_nan() || f.is_infinite() {
                tracing::warn!(value = %f, "Float32 value cannot be represented in JSON, using null");
                JsonValue::Null
            } else {
                serde_json::Number::from_f64(*f as f64)
                    .map(JsonValue::Number)
                    .unwrap_or(JsonValue::Null)
            }
        }
        NormalValue::String(s) => JsonValue::String(s.clone()),
        NormalValue::Bytes(b) => {
            // Store bytes as JSON array of numbers
            JsonValue::Array(b.iter().map(|byte| JsonValue::Number((*byte).into())).collect())
        }
        NormalValue::Json(j) => j.clone(),
        // Arrays
        NormalValue::BoolArray(arr) => {
            JsonValue::Array(arr.iter().map(|b| JsonValue::Bool(*b)).collect())
        }
        NormalValue::IntArray(arr) => {
            JsonValue::Array(arr.iter().map(|i| JsonValue::Number((*i).into())).collect())
        }
        NormalValue::Float64Array(arr) => JsonValue::Array(
            arr.iter()
                .map(|f| {
                    if f.is_nan() || f.is_infinite() {
                        tracing::warn!(value = %f, "Float64 array element cannot be represented in JSON, using null");
                        JsonValue::Null
                    } else {
                        serde_json::Number::from_f64(*f)
                            .map(JsonValue::Number)
                            .unwrap_or(JsonValue::Null)
                    }
                })
                .collect(),
        ),
        NormalValue::StringArray(arr) => {
            JsonValue::Array(arr.iter().map(|s| JsonValue::String(s.clone())).collect())
        }
        // Handle remaining array types
        NormalValue::Float32Array(arr) => JsonValue::Array(
            arr.iter()
                .map(|f| {
                    if f.is_nan() || f.is_infinite() {
                        tracing::warn!(value = %f, "Float32 array element cannot be represented in JSON, using null");
                        JsonValue::Null
                    } else {
                        serde_json::Number::from_f64(*f as f64)
                            .map(JsonValue::Number)
                            .unwrap_or(JsonValue::Null)
                    }
                })
                .collect(),
        ),
        NormalValue::BytesArray(arr) => JsonValue::Array(
            arr.iter()
                .map(|bytes| {
                    JsonValue::Array(bytes.iter().map(|b| JsonValue::Number((*b).into())).collect())
                })
                .collect(),
        ),
        // DateTime handling - convert to RFC 3339 string with Z suffix for UTC (matching Go DefraDB)
        // Go's time.RFC3339 format uses "Z" for UTC, not "+00:00"
        NormalValue::Time(t) => {
            JsonValue::String(t.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true))
        }
        NormalValue::NillableTime(Some(t)) => {
            JsonValue::String(t.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true))
        }
        NormalValue::NillableTime(None) => JsonValue::Null,
        NormalValue::TimeArray(arr) => JsonValue::Array(
            arr.iter()
                .map(|t| JsonValue::String(t.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)))
                .collect(),
        ),
        // Arrays with nillable elements - preserve null values
        NormalValue::NillableBoolElementArray(arr) => JsonValue::Array(
            arr.iter()
                .map(|opt| match opt {
                    Some(b) => JsonValue::Bool(*b),
                    None => JsonValue::Null,
                })
                .collect(),
        ),
        NormalValue::NillableIntElementArray(arr) => JsonValue::Array(
            arr.iter()
                .map(|opt| match opt {
                    Some(i) => JsonValue::Number((*i).into()),
                    None => JsonValue::Null,
                })
                .collect(),
        ),
        NormalValue::NillableFloat64ElementArray(arr) => JsonValue::Array(
            arr.iter()
                .map(|opt| match opt {
                    Some(f) => {
                        if f.is_nan() || f.is_infinite() {
                            JsonValue::Null
                        } else {
                            serde_json::Number::from_f64(*f)
                                .map(JsonValue::Number)
                                .unwrap_or(JsonValue::Null)
                        }
                    }
                    None => JsonValue::Null,
                })
                .collect(),
        ),
        NormalValue::NillableFloat32ElementArray(arr) => JsonValue::Array(
            arr.iter()
                .map(|opt| match opt {
                    Some(f) => {
                        if f.is_nan() || f.is_infinite() {
                            JsonValue::Null
                        } else {
                            serde_json::Number::from_f64(*f as f64)
                                .map(JsonValue::Number)
                                .unwrap_or(JsonValue::Null)
                        }
                    }
                    None => JsonValue::Null,
                })
                .collect(),
        ),
        NormalValue::NillableStringElementArray(arr) => JsonValue::Array(
            arr.iter()
                .map(|opt| match opt {
                    Some(s) => JsonValue::String(s.clone()),
                    None => JsonValue::Null,
                })
                .collect(),
        ),
        // For unknown types, log a warning and return null
        other => {
            tracing::warn!("Unexpected NormalValue variant encountered, converting to null: {:?}", other);
            JsonValue::Null
        }
    }
}

#[async_trait]
impl PlanNode for CreateNode {
    async fn init(&mut self) -> Result<()> {
        self.position = 0;
        self.created_docs.clear();
        self.did_create = false;
        self.initialized = true;
        Ok(())
    }

    async fn start(&mut self) -> Result<()> {
        Ok(())
    }

    async fn next(&mut self) -> Result<bool> {
        if !self.initialized {
            return Err(QueryError::execution(
                "CreateNode.next() called before init()",
            ));
        }

        // On first call, create all documents
        if !self.did_create {
            for input in &self.inputs {
                // Convert input to Document (using schema-aware conversion if available)
                let doc = if let Some(ref collection) = self.collection {
                    input.to_document_with_schema(collection)?
                } else {
                    input.to_document()?
                };

                // Create in storage (generates DocID)
                let result = self.mutator.create(&self.collection_name, doc).await?;

                // Convert result to plan Doc
                let plan_doc = self.create_result_to_doc(&result)?;
                self.created_docs.push(plan_doc);
            }
            self.did_create = true;
        }

        // Iterate through created documents
        if self.position >= self.created_docs.len() {
            return Ok(false);
        }

        self.current_doc = self.created_docs[self.position].deep_clone();
        self.position += 1;
        Ok(true)
    }

    fn value(&self) -> &Doc {
        &self.current_doc
    }

    async fn close(&mut self) -> Result<()> {
        self.created_docs.clear();
        self.initialized = false;
        Ok(())
    }

    fn source(&self) -> Option<&dyn PlanNode> {
        None // CreateNode is a leaf node (generates data)
    }

    fn document_map(&self) -> &DocumentMapping {
        &self.document_mapping
    }

    fn kind(&self) -> &'static str {
        "createNode"
    }
}
