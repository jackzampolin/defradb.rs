use std::path::{Path, PathBuf};
use tokio::process::Command;

use crate::config::{GO_REPO_BASE, GO_WORKTREE_PREFIX, RUST_WORKTREE_PREFIX};
use crate::error::{FfiTestError, Result};

/// Information about the current worktree context
#[derive(Debug, Clone)]
pub struct WorktreeContext {
    /// Path to the Rust worktree root
    pub rust_path: PathBuf,
    /// Path to the paired Go worktree root
    pub go_path: PathBuf,
    /// Worktree suffix (empty for main worktree)
    #[allow(dead_code)]
    pub suffix: String,
    /// Current git branch
    pub branch: String,
    /// Current git commit SHA (short)
    pub commit: String,
    /// Whether the worktree has uncommitted changes
    pub dirty: bool,
}

impl WorktreeContext {
    /// Detect the current worktree context from the current directory
    pub async fn detect() -> Result<Self> {
        let cwd = std::env::current_dir()?;
        Self::from_path(&cwd).await
    }

    /// Detect worktree context from a given path
    pub async fn from_path(path: &Path) -> Result<Self> {
        // Find the git root
        let rust_path = find_git_root(path).await?;

        // Extract suffix from path
        let suffix = extract_suffix(&rust_path)?;

        // Derive Go worktree path
        let go_path = derive_go_path(&suffix);

        // Validate Go worktree exists
        if !go_path.exists() {
            let branch = if suffix.is_empty() {
                "jack/ffi-rust-compat".to_string()
            } else {
                format!("ffi/{}", suffix)
            };
            return Err(FfiTestError::GoWorktreeNotFound {
                path: go_path.display().to_string(),
                branch,
            });
        }

        // Get git info
        let branch = get_git_branch(&rust_path).await?;
        let commit = get_git_commit(&rust_path).await?;
        let dirty = is_git_dirty(&rust_path).await?;

        Ok(WorktreeContext {
            rust_path,
            go_path,
            suffix,
            branch,
            commit,
            dirty,
        })
    }
}

/// Find the git root from a given path
async fn find_git_root(path: &Path) -> Result<PathBuf> {
    let output = Command::new("git")
        .args(["rev-parse", "--show-toplevel"])
        .current_dir(path)
        .output()
        .await?;

    if !output.status.success() {
        return Err(FfiTestError::NotInWorktree);
    }

    let root = String::from_utf8_lossy(&output.stdout).trim().to_string();
    Ok(PathBuf::from(root))
}

/// Extract suffix from Rust worktree path
fn extract_suffix(rust_path: &Path) -> Result<String> {
    let dir_name = rust_path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| FfiTestError::WorktreeDetection("Invalid path".to_string()))?;

    // Check if this is a defradb.rs worktree
    if !dir_name.starts_with(RUST_WORKTREE_PREFIX) {
        return Err(FfiTestError::NotInWorktree);
    }

    // Extract suffix: "defradb.rs-index" -> "index", "defradb.rs" -> ""
    let suffix = if dir_name == RUST_WORKTREE_PREFIX {
        String::new()
    } else if let Some(stripped) = dir_name.strip_prefix(&format!("{}-", RUST_WORKTREE_PREFIX)) {
        stripped.to_string()
    } else {
        return Err(FfiTestError::WorktreeDetection(format!(
            "Unexpected directory name format: {}",
            dir_name
        )));
    };

    Ok(suffix)
}

/// Derive Go worktree path from suffix
fn derive_go_path(suffix: &str) -> PathBuf {
    let go_dir = if suffix.is_empty() {
        GO_WORKTREE_PREFIX.to_string()
    } else {
        format!("{}-{}", GO_WORKTREE_PREFIX, suffix)
    };
    PathBuf::from(GO_REPO_BASE).join(go_dir)
}

/// Get the current git branch
async fn get_git_branch(path: &Path) -> Result<String> {
    let output = Command::new("git")
        .args(["rev-parse", "--abbrev-ref", "HEAD"])
        .current_dir(path)
        .output()
        .await?;

    if !output.status.success() {
        return Err(FfiTestError::WorktreeDetection(
            "Failed to get git branch".to_string(),
        ));
    }

    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

/// Get the current git commit (short SHA)
async fn get_git_commit(path: &Path) -> Result<String> {
    let output = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .current_dir(path)
        .output()
        .await?;

    if !output.status.success() {
        return Err(FfiTestError::WorktreeDetection(
            "Failed to get git commit".to_string(),
        ));
    }

    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

/// Check if the worktree has uncommitted changes
async fn is_git_dirty(path: &Path) -> Result<bool> {
    let output = Command::new("git")
        .args(["status", "--porcelain"])
        .current_dir(path)
        .output()
        .await?;

    Ok(!output.stdout.is_empty())
}

/// Check for uncommitted changes in a worktree
pub async fn check_uncommitted_changes(path: &Path) -> Result<bool> {
    is_git_dirty(path).await
}

/// Check for unpushed commits
pub async fn check_unpushed_commits(path: &Path) -> Result<bool> {
    let output = Command::new("git")
        .args(["log", "@{u}..", "--oneline"])
        .current_dir(path)
        .output()
        .await?;

    // If the command fails (e.g., no upstream), treat as having unpushed commits
    if !output.status.success() {
        return Ok(true);
    }

    Ok(!output.stdout.is_empty())
}

/// List all defradb.rs worktrees
pub async fn list_rust_worktrees() -> Result<Vec<(PathBuf, String)>> {
    let base = PathBuf::from(GO_REPO_BASE);
    let mut worktrees = Vec::new();

    let mut entries = tokio::fs::read_dir(&base).await?;
    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();

        if name_str.starts_with(RUST_WORKTREE_PREFIX) && entry.file_type().await?.is_dir() {
            let path = entry.path();

            // Get branch for this worktree
            if let Ok(branch) = get_git_branch(&path).await {
                worktrees.push((path, branch));
            }
        }
    }

    worktrees.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(worktrees)
}

/// Create paired worktrees for Rust and Go
pub async fn create_worktree_pair(suffix: &str) -> Result<(PathBuf, PathBuf)> {
    let branch = format!("ffi/{}", suffix);
    let rust_path =
        PathBuf::from(GO_REPO_BASE).join(format!("{}-{}", RUST_WORKTREE_PREFIX, suffix));
    let go_path = PathBuf::from(GO_REPO_BASE).join(format!("{}-{}", GO_WORKTREE_PREFIX, suffix));

    // Create Rust worktree
    let main_rust = PathBuf::from(GO_REPO_BASE).join(RUST_WORKTREE_PREFIX);
    let output = Command::new("git")
        .args([
            "worktree",
            "add",
            rust_path.to_str().unwrap(),
            "-b",
            &branch,
        ])
        .current_dir(&main_rust)
        .output()
        .await?;

    if !output.status.success() {
        return Err(FfiTestError::WorktreeDetection(format!(
            "Failed to create Rust worktree: {}",
            String::from_utf8_lossy(&output.stderr)
        )));
    }

    // Create Go worktree
    let main_go = PathBuf::from(GO_REPO_BASE).join(GO_WORKTREE_PREFIX);
    let output = Command::new("git")
        .args(["worktree", "add", go_path.to_str().unwrap(), "-b", &branch])
        .current_dir(&main_go)
        .output()
        .await?;

    if !output.status.success() {
        // Clean up Rust worktree on failure
        let _ = Command::new("git")
            .args(["worktree", "remove", rust_path.to_str().unwrap()])
            .current_dir(&main_rust)
            .output()
            .await;

        return Err(FfiTestError::WorktreeDetection(format!(
            "Failed to create Go worktree: {}",
            String::from_utf8_lossy(&output.stderr)
        )));
    }

    Ok((rust_path, go_path))
}

/// Remove paired worktrees
pub async fn remove_worktree_pair(suffix: &str, force: bool, delete_branch: bool) -> Result<()> {
    let rust_path =
        PathBuf::from(GO_REPO_BASE).join(format!("{}-{}", RUST_WORKTREE_PREFIX, suffix));
    let go_path = PathBuf::from(GO_REPO_BASE).join(format!("{}-{}", GO_WORKTREE_PREFIX, suffix));
    let branch = format!("ffi/{}", suffix);

    // Check for uncommitted changes if not forcing
    if !force {
        if check_uncommitted_changes(&rust_path).await? {
            return Err(FfiTestError::UncommittedChanges {
                path: rust_path.display().to_string(),
            });
        }
        if check_uncommitted_changes(&go_path).await? {
            return Err(FfiTestError::UncommittedChanges {
                path: go_path.display().to_string(),
            });
        }
        if check_unpushed_commits(&rust_path).await? {
            return Err(FfiTestError::UnpushedCommits {
                path: rust_path.display().to_string(),
            });
        }
    }

    let main_rust = PathBuf::from(GO_REPO_BASE).join(RUST_WORKTREE_PREFIX);
    let main_go = PathBuf::from(GO_REPO_BASE).join(GO_WORKTREE_PREFIX);

    // Remove Rust worktree
    let mut args = vec!["worktree", "remove"];
    if force {
        args.push("--force");
    }
    args.push(rust_path.to_str().unwrap());

    let output = Command::new("git")
        .args(&args)
        .current_dir(&main_rust)
        .output()
        .await?;

    if !output.status.success() {
        return Err(FfiTestError::WorktreeDetection(format!(
            "Failed to remove Rust worktree: {}",
            String::from_utf8_lossy(&output.stderr)
        )));
    }

    // Remove Go worktree
    let mut go_args = vec!["worktree", "remove"];
    if force {
        go_args.push("--force");
    }
    go_args.push(go_path.to_str().unwrap());
    let output = Command::new("git")
        .args(&go_args)
        .current_dir(&main_go)
        .output()
        .await?;

    if !output.status.success() {
        return Err(FfiTestError::WorktreeDetection(format!(
            "Failed to remove Go worktree: {}",
            String::from_utf8_lossy(&output.stderr)
        )));
    }

    // Delete branches if requested
    if delete_branch {
        let _ = Command::new("git")
            .args(["branch", "-d", &branch])
            .current_dir(&main_rust)
            .output()
            .await;

        let _ = Command::new("git")
            .args(["branch", "-d", &branch])
            .current_dir(&main_go)
            .output()
            .await;
    }

    Ok(())
}
