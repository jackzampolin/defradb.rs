use std::collections::{BTreeMap, HashMap};

use colored::Colorize;

use crate::error::Result;
use crate::report::{load_all_for_branch, load_all_reports, Report};
use crate::worktree::{list_rust_worktrees, WorktreeContext};

/// Show status of FFI tests
pub async fn execute(all: bool, depth: usize) -> Result<()> {
    if all {
        show_all_worktrees().await
    } else {
        show_current_worktree(depth).await
    }
}

/// Truncate a package path to the given depth
/// e.g., "query/simple/with_filter" at depth 1 = "query"
/// e.g., "query/simple/with_filter" at depth 2 = "query/simple"
fn truncate_to_depth(package: &str, depth: usize) -> String {
    let parts: Vec<&str> = package.split('/').collect();
    parts.into_iter().take(depth).collect::<Vec<_>>().join("/")
}

async fn show_current_worktree(depth: usize) -> Result<()> {
    let ctx = WorktreeContext::detect().await?;

    // Special case: if on main, show latest from ALL worktrees
    let is_main = ctx.branch == "main" || ctx.branch == "master";

    println!(
        "{} {} @ {}{}",
        "FFI Test Status:".bold(),
        if is_main {
            format!("{} (all worktrees)", ctx.branch).cyan()
        } else {
            ctx.branch.cyan()
        },
        ctx.commit.yellow(),
        if ctx.dirty {
            " (dirty)".red().to_string()
        } else {
            String::new()
        }
    );
    println!();

    // Load reports - from all branches if on main, otherwise just current branch
    let reports = if is_main {
        load_all_reports().await?
    } else {
        load_all_for_branch(&ctx.branch).await?
    };

    if reports.is_empty() {
        println!("{}", "No test reports found".dimmed());
        return Ok(());
    }

    // Group reports by package (keep only latest per package)
    let mut latest_by_package: HashMap<String, Report> = HashMap::new();
    for report in reports {
        latest_by_package
            .entry(report.package.clone())
            .or_insert(report);
    }

    // Group packages by truncated path at specified depth
    // Use BTreeMap for sorted output
    let mut groups: BTreeMap<String, Vec<&Report>> = BTreeMap::new();
    for (pkg, report) in &latest_by_package {
        let group_key = truncate_to_depth(pkg, depth);
        groups.entry(group_key).or_default().push(report);
    }

    // Print table header
    println!(
        "{:<30} {:<12} {:<12} {:>6} {:>6} {:>6} {:>6} {:>6}",
        "Package".bold(),
        "Branch".bold(),
        "Timestamp".bold(),
        "Pass".bold(),
        "Fail".bold(),
        "Skip".bold(),
        "Total".bold(),
        "Rate".bold()
    );
    println!("{}", "─".repeat(94));

    // Print each group
    for (group_name, group_reports) in &groups {
        // Aggregate stats from all reports in this group
        let mut total_pass = 0;
        let mut total_fail = 0;
        let mut total_skip = 0;
        let mut latest_report: Option<&Report> = None;

        for report in group_reports {
            total_pass += report.summary.passed;
            total_fail += report.summary.failed;
            total_skip += report.summary.skipped;

            // Track the most recent report for branch/timestamp
            if latest_report.is_none() || report.timestamp > latest_report.unwrap().timestamp {
                latest_report = Some(report);
            }
        }

        let total = total_pass + total_fail + total_skip;
        let pass_rate = if total > 0 {
            total_pass * 100 / total
        } else {
            100
        };

        let rate_str = if total == 0 {
            "-".dimmed().to_string()
        } else if pass_rate == 100 {
            format!("{}%", pass_rate).green().to_string()
        } else if pass_rate >= 90 {
            format!("{}%", pass_rate).yellow().to_string()
        } else {
            format!("{}%", pass_rate).red().to_string()
        };

        let report = latest_report.unwrap();
        let timestamp = report.timestamp.format("%m-%d %H:%M").to_string();
        let branch_display = report.branch.trim_start_matches("ffi/");

        // Show total as - if no tests
        let total_str = if total == 0 {
            "-".to_string()
        } else {
            total.to_string()
        };

        println!(
            "{:<30} {:<12} {:<12} {:>6} {:>6} {:>6} {:>6} {:>6}",
            group_name,
            branch_display.dimmed(),
            timestamp.dimmed(),
            if total == 0 {
                "-".to_string()
            } else {
                total_pass.to_string()
            },
            if total == 0 {
                "-".to_string()
            } else {
                total_fail.to_string()
            },
            if total == 0 {
                "-".to_string()
            } else {
                total_skip.to_string()
            },
            total_str,
            rate_str
        );
    }

    // Print totals
    println!("{}", "─".repeat(94));

    let mut grand_pass = 0;
    let mut grand_fail = 0;
    let mut grand_skip = 0;

    for report in latest_by_package.values() {
        grand_pass += report.summary.passed;
        grand_fail += report.summary.failed;
        grand_skip += report.summary.skipped;
    }

    let grand_total = grand_pass + grand_fail + grand_skip;
    let pass_rate = if grand_total > 0 {
        grand_pass * 100 / grand_total
    } else {
        100
    };

    let rate_str = if pass_rate == 100 {
        format!("{}%", pass_rate).green().bold()
    } else if pass_rate >= 90 {
        format!("{}%", pass_rate).yellow().bold()
    } else {
        format!("{}%", pass_rate).red().bold()
    };

    let label = format!("TOTAL ({} packages)", latest_by_package.len());

    println!(
        "{:<30} {:<12} {:<12} {:>6} {:>6} {:>6} {:>6} {:>6}",
        label.bold(),
        "",
        "",
        grand_pass,
        grand_fail,
        grand_skip,
        grand_total,
        rate_str
    );

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
            let pass_pct = if total > 0 {
                total_pass * 100 / total
            } else {
                0
            };
            let fail_pct = if total > 0 {
                total_fail * 100 / total
            } else {
                0
            };

            println!(
                "  Tests: {} passed ({}%), {} failed ({}%), {} skipped ({} total, {} packages)",
                total_pass.to_string().green(),
                pass_pct,
                if total_fail > 0 {
                    total_fail.to_string().red()
                } else {
                    total_fail.to_string().normal()
                },
                fail_pct,
                total_skip.to_string().yellow(),
                total,
                latest_by_package.len()
            );
        }

        println!();
    }

    Ok(())
}
