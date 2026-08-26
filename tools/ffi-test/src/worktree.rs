use std::path::{Path, PathBuf};
use tokio::process::Command;

use crate::config::{GO_REPO_ENV, GO_WORKTREE_PREFIX, RUST_WORKTREE_PREFIX};
use crate::error::{FfiTestError, Result};

/// Information about the current worktree context
#[derive(Debug, Clone)]
pub struct WorktreeContext {
    /// Path to the Rust worktree root
    pub rust_path: PathBuf,
    /// Path to the paired Go worktree root
    pub go_path: PathBuf,
    /// Current git branch
    pub branch: String,
    /// Current git commit SHA (short)
    pub commit: String,
    /// Whether the worktree has uncommitted changes
    pub dirty: bool,
}

impl WorktreeContext {
    /// Detect the current worktree context, preferring an explicitly supplied Go checkout
    pub async fn detect_with(go_path_override: Option<PathBuf>) -> Result<Self> {
        let cwd = std::env::current_dir()?;
        let from_env = std::env::var_os(GO_REPO_ENV).map(PathBuf::from);
        Self::from_path_with(&cwd, go_path_override, from_env).await
    }

    /// Detect worktree context from a given path, pairing it with the Go worktree beside it
    pub async fn from_path(path: &Path) -> Result<Self> {
        Self::from_path_with(path, None, None).await
    }

    async fn from_path_with(
        path: &Path,
        explicit_go_path: Option<PathBuf>,
        go_path_from_env: Option<PathBuf>,
    ) -> Result<Self> {
        // Find the git root
        let rust_path = find_git_root(path).await?;

        // Resolve the Go checkout: explicit path, then environment, then worktree pairing
        let go_path = resolve_go_path(&rust_path, explicit_go_path, go_path_from_env)?;

        // Validate Go worktree exists
        if !go_path.exists() {
            return Err(FfiTestError::GoWorktreeNotFound {
                path: go_path.display().to_string(),
            });
        }

        // Get git info
        let branch = get_git_branch(&rust_path).await?;
        let commit = get_git_commit(&rust_path).await?;
        let dirty = is_git_dirty(&rust_path).await?;

        Ok(WorktreeContext {
            rust_path,
            go_path,
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

/// Resolve the Go checkout: explicit path first, then `DEFRADB_GO_REPO`, then worktree pairing
fn resolve_go_path(
    rust_path: &Path,
    explicit: Option<PathBuf>,
    from_env: Option<PathBuf>,
) -> Result<PathBuf> {
    match explicit.or(from_env) {
        // An override names the Go checkout outright, so neither side has to
        // follow the paired naming convention.
        Some(path) => Ok(path),
        None => derive_go_path(rust_path, &extract_suffix(rust_path)?),
    }
}

/// Derive Go worktree path from suffix
fn derive_go_path(rust_path: &Path, suffix: &str) -> Result<PathBuf> {
    let base = repository_base(rust_path)?;
    let go_dir = if suffix.is_empty() {
        GO_WORKTREE_PREFIX.to_string()
    } else {
        format!("{}-{}", GO_WORKTREE_PREFIX, suffix)
    };
    Ok(base.join(go_dir))
}

fn repository_base(rust_path: &Path) -> Result<&Path> {
    rust_path.parent().ok_or_else(|| {
        FfiTestError::WorktreeDetection(format!(
            "Could not determine repository base from {}",
            rust_path.display()
        ))
    })
}

pub async fn worktree_pair_paths(suffix: &str) -> Result<(PathBuf, PathBuf)> {
    let cwd = std::env::current_dir()?;
    let rust_root = find_git_root(&cwd).await?;
    let base = repository_base(&rust_root)?;
    let suffix = format!("-{}", suffix);

    Ok((
        base.join(format!("{}{}", RUST_WORKTREE_PREFIX, suffix)),
        base.join(format!("{}{}", GO_WORKTREE_PREFIX, suffix)),
    ))
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

/// Fail unless the Go checkout sits on the pinned client commit.
///
/// The oracle's result is only meaningful against a known Go tree: the test
/// corpus and the harness seams both come from there, so an unpinned checkout
/// silently changes what the pass rate means.
pub async fn verify_go_pin(go_path: &Path) -> Result<()> {
    let found = get_git_commit(go_path).await?;
    let pin = defra_version::GO_FFI_CLIENT_COMMIT;

    if found.starts_with(pin) || pin.starts_with(&found) {
        return Ok(());
    }

    Err(FfiTestError::GoPinMismatch {
        path: go_path.display().to_string(),
        found,
    })
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
    let cwd = std::env::current_dir()?;
    let rust_root = find_git_root(&cwd).await?;
    let base = repository_base(&rust_root)?;
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
    let (rust_path, go_path) = worktree_pair_paths(suffix).await?;
    let base = repository_base(&rust_path)?;

    // Create Rust worktree
    let main_rust = base.join(RUST_WORKTREE_PREFIX);
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
    let main_go = base.join(GO_WORKTREE_PREFIX);
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
    let (rust_path, go_path) = worktree_pair_paths(suffix).await?;
    let base = repository_base(&rust_path)?;
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

    let main_rust = base.join(RUST_WORKTREE_PREFIX);
    let main_go = base.join(GO_WORKTREE_PREFIX);

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_explicit_go_path_beats_the_environment_and_the_pairing() {
        let rust_path = Path::new("/home/user/source/defradb.rs-feature");

        assert_eq!(
            resolve_go_path(
                rust_path,
                Some(PathBuf::from("/elsewhere/go-checkout")),
                Some(PathBuf::from("/from/env")),
            )
            .unwrap(),
            Path::new("/elsewhere/go-checkout")
        );
    }

    #[test]
    fn the_environment_beats_the_pairing_when_no_path_was_passed() {
        let rust_path = Path::new("/home/user/source/defradb.rs-feature");

        assert_eq!(
            resolve_go_path(rust_path, None, Some(PathBuf::from("/from/env"))).unwrap(),
            Path::new("/from/env")
        );
    }

    #[test]
    fn the_pairing_still_resolves_when_neither_is_set() {
        let rust_path = Path::new("/home/user/source/defradb.rs-feature");

        assert_eq!(
            resolve_go_path(rust_path, None, None).unwrap(),
            Path::new("/home/user/source/defradb-feature")
        );
    }

    #[test]
    fn derives_go_worktree_next_to_rust_worktree() {
        let rust_path = Path::new("/home/user/source/defradb.rs-feature");

        assert_eq!(
            derive_go_path(rust_path, "feature").unwrap(),
            Path::new("/home/user/source/defradb-feature")
        );
    }

    #[test]
    fn the_pin_mismatch_names_both_the_pin_and_what_was_found() {
        let hint = FfiTestError::GoPinMismatch {
            path: "/r/defradb-rustffi".to_string(),
            found: "deadbeef".to_string(),
        }
        .to_string();

        assert!(
            hint.contains(defra_version::GO_FFI_CLIENT_COMMIT),
            "names the pin: {hint}"
        );
        assert!(hint.contains("deadbeef"), "names what was found: {hint}");
    }

    #[test]
    fn the_missing_go_worktree_hint_names_the_client_branch() {
        let hint = FfiTestError::GoWorktreeNotFound {
            path: "/r/defradb-ffi-port".to_string(),
        }
        .to_string();

        assert!(
            hint.contains("jack/ffi-rust-compat"),
            "names the branch carrying the client: {hint}"
        );
        // the path may have come from --go-path or DEFRADB_GO_REPO, where
        // `git worktree add` is the wrong mechanism entirely
        assert!(
            !hint.contains("worktree add"),
            "must not prescribe a mechanism that only fits pairing: {hint}"
        );
    }

    #[test]
    fn an_override_frees_the_rust_checkout_from_the_naming_convention() {
        let odd = Path::new("/somewhere/my-fork-of-defradb-rs");
        let explicit = Some(PathBuf::from("/go/checkout"));

        // the suffix is only needed to derive a paired path; with an override
        // there is nothing to derive, so the name must not matter
        let path = resolve_go_path(odd, explicit, None).unwrap();

        assert_eq!(path, PathBuf::from("/go/checkout"));
        assert!(
            resolve_go_path(odd, None, None).is_err(),
            "without an override the pairing convention still applies"
        );
    }
}
