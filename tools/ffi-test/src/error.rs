use thiserror::Error;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum FfiTestError {
    #[error("Worktree detection failed: {0}")]
    WorktreeDetection(String),

    #[error(
        "Go checkout not found at {path}. It must be a checkout of \
         sourcenetwork/defradb carrying the rustffi client"
    )]
    GoWorktreeNotFound { path: String },

    #[error("FFI build failed: {0}")]
    FfiBuild(String),

    #[error("Header generation failed: {0}")]
    HeaderGeneration(String),

    #[error("Test execution failed: {0}")]
    TestExecution(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Worktree has uncommitted changes: {path}")]
    UncommittedChanges { path: String },

    #[error("Worktree has unpushed commits: {path}")]
    UnpushedCommits { path: String },

    #[error("cbindgen not found. Install with: cargo install cbindgen")]
    CbindgenNotFound,

    #[error(
        "Go checkout at {path} is on {found}, not the pinned client commit {}. \
         Check it out at the pin, or update GO_FFI_CLIENT_COMMIT if the client moved",
        defra_version::GO_FFI_CLIENT_COMMIT
    )]
    GoPinMismatch { path: String, found: String },

    #[error("Not in a defradb.rs worktree")]
    NotInWorktree,
}

pub type Result<T> = std::result::Result<T, FfiTestError>;
