use std::collections::HashMap;

use colored::Colorize;

use crate::error::Result;
use crate::report::{load_all_for_branch, Report};
use crate::runner::list_packages;
use crate::worktree::{list_rust_worktrees, WorktreeContext};

/// Show status of FFI tests
pub async fn execute(all: bool) -> Result<()> {
    if all {
        show_all_worktrees().await
    } else {
        show_current_worktree().await
    }
}

async fn show_current_worktree() -> Result<()> {
    let ctx = WorktreeContext::detect().await?;

    println!(
        "{} {} @ {}{}",
        "FFI Test Status:".bold(),
        ctx.branch.cyan(),
        ctx.commit.yellow(),
        if ctx.dirty {
            " (dirty)".red().to_string()
        } else {
            String::new()
        }
    );
    println!();

    // Get available packages
    let packages = list_packages(&ctx.go_path).await?;

    // Load reports for this branch
    let reports = load_all_for_branch(&ctx.branch).await?;

    // Group reports by package (keep only latest per package)
    let mut latest_by_package: HashMap<String, Report> = HashMap::new();
    for report in reports {
        latest_by_package
            .entry(report.package.clone())
            .or_insert(report);
    }

    // Print table header
    println!(
        "{:<25} {:<20} {:>6} {:>6} {:>6} {:>6}",
        "Package".bold(),
        "Last Run".bold(),
        "Pass".bold(),
        "Fail".bold(),
        "Skip".bold(),
        "Total".bold()
    );
    println!("{}", "─".repeat(75));

    // Print each package
    for package in &packages {
        if let Some(report) = latest_by_package.get(package) {
            let timestamp = report.timestamp.format("%Y-%m-%d %H:%M");
            let pass = report.summary.passed.to_string().green();
            let fail = if report.summary.failed > 0 {
                report.summary.failed.to_string().red()
            } else {
                report.summary.failed.to_string().normal()
            };
            let skip = report.summary.skipped.to_string().yellow();

            println!(
                "{:<25} {:<20} {:>6} {:>6} {:>6} {:>6}",
                package, timestamp, pass, fail, skip, report.summary.total
            );
        } else {
            println!(
                "{:<25} {:<20} {:>6} {:>6} {:>6} {:>6}",
                package,
                "-".dimmed(),
                "-".dimmed(),
                "-".dimmed(),
                "-".dimmed(),
                "-".dimmed()
            );
        }
    }

    if packages.is_empty() {
        println!("{}", "No test packages found".dimmed());
    }

    Ok(())
}

async fn show_all_worktrees() -> Result<()> {
    println!("{}", "FFI Test Status (All Worktrees)".bold());
    println!();

    let worktrees = list_rust_worktrees().await?;

    if worktrees.is_empty() {
        println!("{}", "No defradb.rs worktrees found".dimmed());
        return Ok(());
    }

    for (rust_path, branch) in worktrees {
        // Try to get context for this worktree
        let ctx = match WorktreeContext::from_path(&rust_path).await {
            Ok(c) => c,
            Err(_) => continue,
        };

        println!("{} @ {}", branch.cyan().bold(), ctx.commit.yellow());
        println!("  Rust: {}", ctx.rust_path.display());
        println!("  Go:   {}", ctx.go_path.display());

        // Load reports for this branch
        let reports = load_all_for_branch(&branch).await?;

        if reports.is_empty() {
            println!("  {}", "No test reports".dimmed());
        } else {
            // Group by package
            let mut latest_by_package: HashMap<String, &Report> = HashMap::new();
            for report in &reports {
                latest_by_package
                    .entry(report.package.clone())
                    .or_insert(report);
            }

            // Calculate totals
            let mut total_pass = 0;
            let mut total_fail = 0;
            let mut total_skip = 0;

            for report in latest_by_package.values() {
                total_pass += report.summary.passed;
                total_fail += report.summary.failed;
                total_skip += report.summary.skipped;
            }

            let total = total_pass + total_fail + total_skip;

            println!(
                "  Tests: {} passed, {} failed, {} skipped ({} total)",
                total_pass.to_string().green(),
                if total_fail > 0 {
                    total_fail.to_string().red()
                } else {
                    total_fail.to_string().normal()
                },
                total_skip.to_string().yellow(),
                total
            );
        }

        println!();
    }

    Ok(())
}
