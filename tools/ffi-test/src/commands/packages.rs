use std::path::PathBuf;

use crate::error::Result;
use crate::runner::{discover_subpackages, list_packages};
use crate::worktree::WorktreeContext;

/// List available test packages from the Go integration test directory
pub async fn execute(filter: Option<&str>, go_path: Option<PathBuf>) -> Result<()> {
    let ctx = WorktreeContext::detect_with(go_path).await?;

    let packages = match filter {
        Some(prefix) => discover_subpackages(&ctx.go_path, prefix).await?,
        None => list_packages(&ctx.go_path).await?,
    };

    if packages.is_empty() {
        if let Some(prefix) = filter {
            eprintln!("No test packages found matching '{}'", prefix);
        } else {
            eprintln!("No test packages found");
        }
        return Ok(());
    }

    for package in &packages {
        println!("{}", package);
    }

    eprintln!("\n{} packages", packages.len());

    Ok(())
}
