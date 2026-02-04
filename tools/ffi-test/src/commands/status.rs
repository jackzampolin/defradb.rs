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
        "{:<50} {:<8} {:<12} {:>12} {:>12} {:>12} {:>6}",
        "Package".bold(),
        "Commit".bold(),
        "Timestamp".bold(),
        "Pass".bold(),
        "Fail".bold(),
        "Skip".bold(),
        "Total".bold()
    );
    println!("{}", "─".repeat(114));

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

            let timestamp = report.timestamp.format("%m-%d %H:%M").to_string();

            // Color based on pass rate: green=100%, yellow=90%+, red=<90%
            let pass_rate = if report.summary.total > 0 {
                report.summary.passed * 100 / report.summary.total
            } else {
                100
            };

            if pass_rate == 100 {
                println!(
                    "{:<50} {:<8} {:<12} {:>12} {:>12} {:>12} {:>6}",
                    package.green(),
                    report.commit.dimmed(),
                    timestamp.dimmed(),
                    pass_str.green(),
                    fail_str.green(),
                    skip_str.green(),
                    report.summary.total.to_string().green()
                );
            } else if pass_rate >= 90 {
                println!(
                    "{:<50} {:<8} {:<12} {:>12} {:>12} {:>12} {:>6}",
                    package.yellow(),
                    report.commit.dimmed(),
                    timestamp.dimmed(),
                    pass_str.yellow(),
                    fail_str.yellow(),
                    skip_str.yellow(),
                    report.summary.total.to_string().yellow()
                );
            } else {
                println!(
                    "{:<50} {:<8} {:<12} {:>12} {:>12} {:>12} {:>6}",
                    package.red(),
                    report.commit.dimmed(),
                    timestamp.dimmed(),
                    pass_str.red(),
                    fail_str.red(),
                    skip_str.red(),
                    report.summary.total.to_string().red()
                );
            }

            // Accumulate totals
            grand_total += report.summary.total;
            grand_pass += report.summary.passed;
            grand_fail += report.summary.failed;
            grand_skip += report.summary.skipped;
            packages_run += 1;
        } else {
            println!(
                "{:<50} {:<8} {:<12} {:>12} {:>12} {:>12} {:>6}",
                package,
                "-".dimmed(),
                "-".dimmed(),
                "-".dimmed(),
                "-".dimmed(),
                "-".dimmed(),
                "-".dimmed()
            );
        }
    }

    // Print totals if we have any data
    // For totals, only count packages where no parent package has a report
    // (parent packages include child package tests, so we avoid double counting)
    if packages_run > 0 {
        println!("{}", "─".repeat(114));

        // Get list of packages with reports
        let reported_packages: Vec<&String> = latest_by_package.keys().collect();

        // Calculate totals excluding packages whose parent has a report
        let mut root_total = 0;
        let mut root_pass = 0;
        let mut root_fail = 0;
        let mut root_skip = 0;
        let mut root_count = 0;

        for (pkg, report) in &latest_by_package {
            // Check if any parent of this package has a report
            let has_parent_report = reported_packages.iter().any(|other| {
                *other != pkg && pkg.starts_with(&format!("{}/", other))
            });

            if !has_parent_report {
                root_total += report.summary.total;
                root_pass += report.summary.passed;
                root_fail += report.summary.failed;
                root_skip += report.summary.skipped;
                root_count += 1;
            }
        }

        let pass_pct = format_pct(root_pass, root_total);
        let fail_pct = format_pct(root_fail, root_total);
        let skip_pct = format_pct(root_skip, root_total);

        let pass_str = format!("{:>4} {}", root_pass, pass_pct);
        let fail_str = format!("{:>4} {}", root_fail, fail_pct);
        let skip_str = format!("{:>4} {}", root_skip, skip_pct);

        // Color based on pass rate: green=100%, yellow=90%+, red=<90%
        let pass_rate = if root_total > 0 {
            root_pass * 100 / root_total
        } else {
            100
        };

        let label = format!("TOTAL ({} root packages)", root_count);
        if pass_rate == 100 {
            println!(
                "{:<50} {:<8} {:<12} {:>12} {:>12} {:>12} {:>6}",
                label.green().bold(),
                "",
                "",
                pass_str.green().bold(),
                fail_str.green().bold(),
                skip_str.green().bold(),
                root_total.to_string().green().bold()
            );
        } else if pass_rate >= 90 {
            println!(
                "{:<50} {:<8} {:<12} {:>12} {:>12} {:>12} {:>6}",
                label.yellow().bold(),
                "",
                "",
                pass_str.yellow().bold(),
                fail_str.yellow().bold(),
                skip_str.yellow().bold(),
                root_total.to_string().yellow().bold()
            );
        } else {
            println!(
                "{:<50} {:<8} {:<12} {:>12} {:>12} {:>12} {:>6}",
                label.red().bold(),
                "",
                "",
                pass_str.red().bold(),
                fail_str.red().bold(),
                skip_str.red().bold(),
                root_total.to_string().red().bold()
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
