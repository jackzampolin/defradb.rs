//! Document conversion utilities

use document::Document;
use serde_json::Value as JsonValue;

use crate::error::Result;
use crate::json_convert::normal_value_to_json;
use crate::planner::Doc;

use super::DocumentMapping;

/// Convert a storage `Document` to a plan `Doc` using the given mapping.
///
/// This function maps fields from the storage document to the positions
/// defined in the `DocumentMapping`, producing a `Doc` that can be used
/// in query plan execution.
///
/// # Arguments
///
/// * `doc` - The storage document to convert
/// * `mapping` - The document mapping defining field positions
///
/// # Returns
///
/// A `Doc` with fields positioned according to the mapping, or an error
/// if value conversion fails.
pub fn document_to_plan_doc(doc: &Document, mapping: &DocumentMapping) -> Result<Doc> {
    let num_fields = mapping.next_index();
    let mut fields: Vec<Option<JsonValue>> = vec![None; num_fields];

    // Set _docID if present in mapping
    if let Some(index) = mapping.first_index_of_name("_docID") {
        if let Some(doc_id) = doc.id() {
            fields[index] = Some(JsonValue::String(doc_id.to_string()));
        }
    }

    // Set other fields
    for field_name in doc.field_names() {
        if let Some(index) = mapping.first_index_of_name(field_name) {
            if let Some(value) = doc.get(field_name) {
                let json = normal_value_to_json(value)?;
                fields[index] = Some(json);
            }
        }
    }

    Ok(Doc::with_fields(fields))
}

/// Convert a slice of storage `Document`s to plan `Doc`s using the given mapping.
///
/// # Arguments
///
/// * `docs` - The storage documents to convert
/// * `mapping` - The document mapping defining field positions
///
/// # Returns
///
/// A vector of `Doc`s, or an error if any value conversion fails.
pub fn documents_to_plan_docs(docs: &[Document], mapping: &DocumentMapping) -> Result<Vec<Doc>> {
    let mut result = Vec::with_capacity(docs.len());
    for doc in docs {
        result.push(document_to_plan_doc(doc, mapping)?);
    }
    Ok(result)
}
