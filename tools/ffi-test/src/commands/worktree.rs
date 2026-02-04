use colored::Colorize;

use crate::config::{GO_REPO_BASE, GO_WORKTREE_PREFIX, RUST_WORKTREE_PREFIX};
use crate::error::Result;
use crate::worktree::{
    check_uncommitted_changes, check_unpushed_commits, create_worktree_pair, list_rust_worktrees,
    remove_worktree_pair, WorktreeContext,
};

/// List all paired worktrees
pub async fn list() -> Result<()> {
    println!("{}", "Paired Worktrees".bold());
    println!();

    let worktrees = list_rust_worktrees().await?;

    if worktrees.is_empty() {
        println!("{}", "No defradb.rs worktrees found".dimmed());
        return Ok(());
    }

    for (rust_path, branch) in worktrees {
        let ctx = match WorktreeContext::from_path(&rust_path).await {
            Ok(c) => c,
            Err(e) => {
                println!(
                    "{} {} - {}",
                    "✗".red(),
                    rust_path.display(),
                    e.to_string().red()
                );
                continue;
            }
        };

        let status = if ctx.dirty {
            " (dirty)".red().to_string()
        } else {
            String::new()
        };

        println!(
            "{} {} @ {}{}",
            "✓".green(),
            branch.cyan(),
            ctx.commit.yellow(),
            status
        );
        println!("  Rust: {}", ctx.rust_path.display());
        println!("  Go:   {}", ctx.go_path.display());
        println!();
    }

    Ok(())
}

/// Create a new paired worktree
pub async fn create(suffix: &str) -> Result<()> {
    println!("{} {}", "Creating worktree pair:".bold(), suffix.cyan());

    let rust_path =
        std::path::PathBuf::from(GO_REPO_BASE).join(format!("{}-{}", RUST_WORKTREE_PREFIX, suffix));
    let go_path =
        std::path::PathBuf::from(GO_REPO_BASE).join(format!("{}-{}", GO_WORKTREE_PREFIX, suffix));

    // Check if either already exists
    if rust_path.exists() {
        println!(
            "{} Rust worktree already exists: {}",
            "Error:".red(),
            rust_path.display()
        );
        return Ok(());
    }
    if go_path.exists() {
        println!(
            "{} Go worktree already exists: {}",
            "Error:".red(),
            go_path.display()
        );
        return Ok(());
    }

    let (rust, go) = create_worktree_pair(suffix).await?;

    println!();
    println!("{}", "Created:".green());
    println!("  Rust: {}", rust.display());
    println!("  Go:   {}", go.display());
    println!();
    println!("Branch: {}", format!("ffi/{}", suffix).cyan());

    Ok(())
}

/// Remove a paired worktree
pub async fn remove(suffix: &str, force: bool, delete_branch: bool) -> Result<()> {
    println!("{} {}", "Removing worktree pair:".bold(), suffix.cyan());

    let rust_path =
        std::path::PathBuf::from(GO_REPO_BASE).join(format!("{}-{}", RUST_WORKTREE_PREFIX, suffix));
    let go_path =
        std::path::PathBuf::from(GO_REPO_BASE).join(format!("{}-{}", GO_WORKTREE_PREFIX, suffix));

    // Check for uncommitted changes
    if !force {
        if rust_path.exists() {
            if check_uncommitted_changes(&rust_path).await? {
                println!(
                    "{} Rust worktree has uncommitted changes. Use --force to override.",
                    "Error:".red()
                );
                return Ok(());
            }
            if check_unpushed_commits(&rust_path).await? {
                println!(
                    "{} Rust worktree has unpushed commits. Use --force to override.",
                    "Warning:".yellow()
                );
                return Ok(());
            }
        }

        if go_path.exists() && check_uncommitted_changes(&go_path).await? {
            println!(
                "{} Go worktree has uncommitted changes. Use --force to override.",
                "Error:".red()
            );
            return Ok(());
        }
    }

    remove_worktree_pair(suffix, force, delete_branch).await?;

    println!();
    println!("{}", "Removed:".green());
    println!("  Rust: {}", rust_path.display());
    println!("  Go:   {}", go_path.display());

    if delete_branch {
        println!("  Branch: {}", format!("ffi/{}", suffix).cyan());
    }

    Ok(())
}
