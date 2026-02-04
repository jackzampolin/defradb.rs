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

/// Format a percentage, returning empty string for 0 total
fn format_pct(value: usize, total: usize) -> String {
    if total == 0 {
        String::new()
    } else {
        format!("({:>3}%)", value * 100 / total)
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
        "{:<50} {:<8} {:>12} {:>12} {:>12} {:>6}",
        "Package".bold(),
        "Commit".bold(),
        "Pass".bold(),
        "Fail".bold(),
        "Skip".bold(),
        "Total".bold()
    );
    println!("{}", "─".repeat(102));

    // Track totals
    let mut grand_total = 0;
    let mut grand_pass = 0;
    let mut grand_fail = 0;
    let mut grand_skip = 0;
    let mut packages_run = 0;

    // Print each package
    for package in &packages {
        if let Some(report) = latest_by_package.get(package) {
            let pass_pct = format_pct(report.summary.passed, report.summary.total);
            let fail_pct = format_pct(report.summary.failed, report.summary.total);
            let skip_pct = format_pct(report.summary.skipped, report.summary.total);

            let pass_str = format!("{:>4} {}", report.summary.passed, pass_pct);
            let fail_str = format!("{:>4} {}", report.summary.failed, fail_pct);
            let skip_str = format!("{:>4} {}", report.summary.skipped, skip_pct);

            let pass_colored = pass_str.green();
            let fail_colored = if report.summary.failed > 0 {
                fail_str.red()
            } else {
                fail_str.normal()
            };
            let skip_colored = if report.summary.skipped > 0 {
                skip_str.yellow()
            } else {
                skip_str.normal()
            };

            println!(
                "{:<50} {:<8} {:>12} {:>12} {:>12} {:>6}",
                package,
                report.commit.dimmed(),
                pass_colored,
                fail_colored,
                skip_colored,
                report.summary.total
            );

            // Accumulate totals
            grand_total += report.summary.total;
            grand_pass += report.summary.passed;
            grand_fail += report.summary.failed;
            grand_skip += report.summary.skipped;
            packages_run += 1;
        } else {
            println!(
                "{:<50} {:<8} {:>12} {:>12} {:>12} {:>6}",
                package,
                "-".dimmed(),
                "-".dimmed(),
                "-".dimmed(),
                "-".dimmed(),
                "-".dimmed()
            );
        }
    }

    // Print totals if we have any data
    if packages_run > 0 {
        println!("{}", "─".repeat(102));

        let pass_pct = format_pct(grand_pass, grand_total);
        let fail_pct = format_pct(grand_fail, grand_total);
        let skip_pct = format_pct(grand_skip, grand_total);

        let pass_str = format!("{:>4} {}", grand_pass, pass_pct);
        let fail_str = format!("{:>4} {}", grand_fail, fail_pct);
        let skip_str = format!("{:>4} {}", grand_skip, skip_pct);

        let pass_colored = pass_str.green().bold();
        let fail_colored = if grand_fail > 0 {
            fail_str.red().bold()
        } else {
            fail_str.bold()
        };
        let skip_colored = if grand_skip > 0 {
            skip_str.yellow().bold()
        } else {
            skip_str.bold()
        };

        println!(
            "{:<50} {:<8} {:>12} {:>12} {:>12} {:>6}",
            format!("TOTAL ({} packages)", packages_run).bold(),
            "",
            pass_colored,
            fail_colored,
            skip_colored,
            grand_total.to_string().bold()
        );
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
            let pass_pct = if total > 0 { total_pass * 100 / total } else { 0 };
            let fail_pct = if total > 0 { total_fail * 100 / total } else { 0 };

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
