//! Compatibility facade for query document mapping and conversion helpers.

pub use query_plan::document::{
    document_to_plan_doc, document_to_plan_doc_with_status, documents_to_plan_docs,
    documents_with_status_to_plan_docs, render_doc_to_json, DocumentMapping, RenderKey,
    DELETED_FIELD_NAME,
};
