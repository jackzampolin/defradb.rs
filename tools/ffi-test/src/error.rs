use thiserror::Error;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum FfiTestError {
    #[error("Worktree detection failed: {0}")]
    WorktreeDetection(String),

    #[error(
        "Go worktree not found at {path}. Create it with: git worktree add {path} -b {branch}"
    )]
    GoWorktreeNotFound { path: String, branch: String },

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

    #[error("Not in a defradb.rs worktree")]
    NotInWorktree,
}

pub type Result<T> = std::result::Result<T, FfiTestError>;
