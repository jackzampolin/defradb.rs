//! Document types and mapping for query results

mod convert;
mod mapping;

pub use convert::{document_to_plan_doc, documents_to_plan_docs};
pub use mapping::{DocumentMapping, RenderKey, DOC_ID_FIELD_INDEX};
