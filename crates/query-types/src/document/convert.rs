//! Document conversion utilities

use document::Document;
use serde_json::Value as JsonValue;

use crate::error::Result;
use crate::json_convert::normal_value_to_json;
use crate::doc::{Doc, DocStatus};

use super::DocumentMapping;

/// The reserved field name for document deletion status.
pub const DELETED_FIELD_NAME: &str = "_deleted";

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
    document_to_plan_doc_with_status(doc, mapping, false)
}

/// Convert a storage `Document` to a plan `Doc` with deletion status.
///
/// This function maps fields from the storage document to the positions
/// defined in the `DocumentMapping`, and sets the deletion status on the
/// resulting `Doc`.
///
/// # Arguments
///
/// * `doc` - The storage document to convert
/// * `mapping` - The document mapping defining field positions
/// * `is_deleted` - Whether the document is marked as deleted
///
/// # Returns
///
/// A `Doc` with fields positioned according to the mapping and deletion
/// status set, or an error if value conversion fails.
pub fn document_to_plan_doc_with_status(
    doc: &Document,
    mapping: &DocumentMapping,
    is_deleted: bool,
) -> Result<Doc> {
    let num_fields = mapping.next_index();
    let mut fields: Vec<Option<JsonValue>> = vec![None; num_fields];

    // Set _docID if present in mapping
    if let Some(index) = mapping.first_index_of_name("_docID") {
        if let Some(doc_id) = doc.id() {
            fields[index] = Some(JsonValue::String(doc_id.to_string()));
        }
    }

    // Set _deleted if present in mapping
    if let Some(index) = mapping.first_index_of_name(DELETED_FIELD_NAME) {
        fields[index] = Some(JsonValue::Bool(is_deleted));
    }

    // Set other fields from the document.
    // Count ALL stored fields (matching Go's KV-pair counting), not just those in the mapping.
    let stored_field_count = doc.field_names().count();
    for field_name in doc.field_names() {
        if let Some(index) = mapping.first_index_of_name(field_name) {
            if let Some(value) = doc.get(field_name) {
                let json = normal_value_to_json(value)?;
                fields[index] = Some(json);
            }
        }
    }

    let mut plan_doc = Doc::with_fields(fields);
    plan_doc.stored_field_count = stored_field_count;
    if is_deleted {
        plan_doc.status = DocStatus::Deleted;
    }
    Ok(plan_doc)
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

/// Convert a slice of storage `Document`s with deletion status to plan `Doc`s.
///
/// # Arguments
///
/// * `docs` - The storage documents with their deletion status (document, is_deleted)
/// * `mapping` - The document mapping defining field positions
///
/// # Returns
///
/// A vector of `Doc`s with deletion status set, or an error if any value conversion fails.
pub fn documents_with_status_to_plan_docs(
    docs: &[(Document, bool)],
    mapping: &DocumentMapping,
) -> Result<Vec<Doc>> {
    let mut result = Vec::with_capacity(docs.len());
    for (doc, is_deleted) in docs {
        result.push(document_to_plan_doc_with_status(doc, mapping, *is_deleted)?);
    }
    Ok(result)
}
