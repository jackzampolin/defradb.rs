//! Query planning and plan node execution primitives.

pub mod document;
pub mod fetcher;
pub mod mutator;
pub mod plan;
pub mod planner;
mod json_convert;

pub mod error {
    pub use query_model::error::{QueryError, Result};
}

pub mod mapper {
    pub use query_model::mapper::*;
}

pub use document::{
    document_to_plan_doc, document_to_plan_doc_with_status, documents_to_plan_docs,
    documents_with_status_to_plan_docs, render_doc_to_json, DocumentMapping, RenderKey,
    DELETED_FIELD_NAME,
};
pub use fetcher::{
    CommitsQueryOptions, DocFetcher, FetchByIdsResult, IndexScanResult,
};
pub use mutator::{
    BroadcastStatus, CreateResult, DeleteResult, DocMutator, MutationBatch,
    MutationBatchController, UpdateResult,
};
pub use plan::{
    CreateInput, CreateNode, DeleteNode, DocPermissionChecker, JoinDirection, JoinSide,
    LimitNode, ScanNode, SelectNode, TypeJoinMany, TypeJoinOne, UpdateInput, UpdateNode,
    UpsertAction, UpsertInput, UpsertNode,
};
pub use planner::{Doc, DocStatus, ExecInfo, PlanNode, Planner};
