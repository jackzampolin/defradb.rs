/// Errors for the defra-fs crate.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("database error: {0}")]
    Db(#[from] db::Error),

    #[error("document error: {0}")]
    Document(#[from] document::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("collection not found: {0}")]
    CollectionNotFound(String),

    #[error("document not found: {0}")]
    DocumentNotFound(String),
}
