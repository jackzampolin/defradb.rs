use std::collections::{BTreeMap, HashMap};

use colored::Colorize;

use crate::error::Result;
use crate::report::{load_all_for_branch, load_all_reports, Report};
use crate::worktree::{list_rust_worktrees, WorktreeContext};

/// Show status of FFI tests
pub async fn execute(all: bool, depth: usize, filter: Option<&str>) -> Result<()> {
    if all {
        show_all_worktrees().await
    } else {
        show_current_worktree(depth, filter).await
    }
}

/// Truncate a package path to the given depth
/// e.g., "query/simple/with_filter" at depth 1 = "query"
/// e.g., "query/simple/with_filter" at depth 2 = "query/simple"
fn truncate_to_depth(package: &str, depth: usize) -> String {
    let parts: Vec<&str> = package.split('/').collect();
    parts.into_iter().take(depth).collect::<Vec<_>>().join("/")
}

/// Compute the maximum depth of any package path
fn max_depth(packages: &[&String]) -> usize {
    packages
        .iter()
        .map(|p| p.split('/').count())
        .max()
        .unwrap_or(1)
}

async fn show_current_worktree(depth: usize, filter: Option<&str>) -> Result<()> {
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

    // Apply package filter if provided
    if let Some(prefix) = filter {
        latest_by_package
            .retain(|pkg, _| pkg == prefix || pkg.starts_with(&format!("{}/", prefix)));
    }

    if latest_by_package.is_empty() {
        if let Some(prefix) = filter {
            println!(
                "{}",
                format!("No test reports found for '{}'", prefix).dimmed()
            );
        } else {
            println!("{}", "No test reports found".dimmed());
        }
        return Ok(());
    }

    // When filtering to a specific package, auto-expand to full depth
    let effective_depth = if filter.is_some() {
        let keys: Vec<&String> = latest_by_package.keys().collect();
        max_depth(&keys)
    } else {
        depth
    };

    // Group packages by truncated path at specified depth
    // Use BTreeMap for sorted output
    let mut groups: BTreeMap<String, Vec<&Report>> = BTreeMap::new();
    for (pkg, report) in &latest_by_package {
        let group_key = truncate_to_depth(pkg, effective_depth);
        groups.entry(group_key).or_default().push(report);
    }

    // Compute dynamic column widths from content
    let pkg_col_width = groups
        .keys()
        .map(|k| k.len())
        .max()
        .unwrap_or(7)
        .max(7) // minimum "Package" header width
        + 2; // padding

    let branch_col_width = groups
        .values()
        .filter_map(|reports| {
            reports
                .iter()
                .max_by_key(|r| r.timestamp)
                .map(|r| r.branch.trim_start_matches("ffi/").len())
        })
        .max()
        .unwrap_or(6)
        .max(6) // minimum "Branch" header width
        + 2; // padding

    let line_width = pkg_col_width + branch_col_width + 12 + 6 + 6 + 6 + 6 + 6 + 7; // field widths + gaps

    // Print table header
    println!(
        "{:<pw$} {:<bw$} {:<12} {:>6} {:>6} {:>6} {:>6}   {}",
        "Package".bold(),
        "Branch".bold(),
        "Timestamp".bold(),
        "Pass".bold(),
        "Fail".bold(),
        "Skip".bold(),
        "Total".bold(),
        "Rate".bold(),
        pw = pkg_col_width,
        bw = branch_col_width
    );
    println!("{}", "─".repeat(line_width));

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
        let pass_rate = (total_pass * 100).checked_div(total).unwrap_or(100);

        // Pre-pad rate text before coloring (ANSI codes break {:>N} alignment)
        let rate_str = if total == 0 {
            format!("{:>5}", "-").dimmed().to_string()
        } else if pass_rate == 100 {
            format!("{:>4}%", pass_rate).green().to_string()
        } else if pass_rate >= 90 {
            format!("{:>4}%", pass_rate).yellow().to_string()
        } else {
            format!("{:>4}%", pass_rate).red().to_string()
        };

        let report = latest_report.unwrap();
        let timestamp = report.timestamp.format("%m-%d %H:%M").to_string();
        let branch_display = report.branch.trim_start_matches("ffi/");

        let total_str = if total == 0 {
            "-".to_string()
        } else {
            total.to_string()
        };

        println!(
            "{:<pw$} {:<bw$} {:<12} {:>6} {:>6} {:>6} {:>6}   {}",
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
            rate_str,
            pw = pkg_col_width,
            bw = branch_col_width
        );
    }

    // Print totals
    println!("{}", "─".repeat(line_width));

    let mut grand_pass = 0;
    let mut grand_fail = 0;
    let mut grand_skip = 0;

    for report in latest_by_package.values() {
        grand_pass += report.summary.passed;
        grand_fail += report.summary.failed;
        grand_skip += report.summary.skipped;
    }

    let grand_total = grand_pass + grand_fail + grand_skip;
    let pass_rate = (grand_pass * 100).checked_div(grand_total).unwrap_or(100);

    // Pre-pad rate text before coloring (ANSI codes break {:>N} alignment)
    let rate_str = if pass_rate == 100 {
        format!("{:>4}%", pass_rate).green().bold()
    } else if pass_rate >= 90 {
        format!("{:>4}%", pass_rate).yellow().bold()
    } else {
        format!("{:>4}%", pass_rate).red().bold()
    };

    let label = format!("TOTAL ({} packages)", latest_by_package.len());

    println!(
        "{:<pw$} {:<bw$} {:<12} {:>6} {:>6} {:>6} {:>6}   {}",
        label.bold(),
        "",
        "",
        grand_pass,
        grand_fail,
        grand_skip,
        grand_total,
        rate_str,
        pw = pkg_col_width,
        bw = branch_col_width
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
            let pass_pct = (total_pass * 100).checked_div(total).unwrap_or(0);
            let fail_pct = (total_fail * 100).checked_div(total).unwrap_or(0);

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
