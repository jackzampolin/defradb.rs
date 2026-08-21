//! Document types and mapping for query results

mod convert;
mod mapping;

pub use convert::{
    document_to_plan_doc, document_to_plan_doc_with_status, documents_to_plan_docs,
    documents_with_status_to_plan_docs, DELETED_FIELD_NAME,
};
pub use mapping::{DocumentMapping, RenderKey, DOC_ID_FIELD_INDEX};
