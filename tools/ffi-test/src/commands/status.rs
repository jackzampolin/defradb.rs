use std::collections::HashMap;

use colored::Colorize;

use crate::error::Result;
use crate::report::{load_all_for_branch, load_all_reports, Report};
use crate::runner::list_packages;
use crate::worktree::{list_rust_worktrees, WorktreeContext};

/// Show status of FFI tests
pub async fn execute(all: bool, subpackages: bool) -> Result<()> {
    if all {
        show_all_worktrees().await
    } else {
        show_current_worktree(subpackages).await
    }
}

async fn show_current_worktree(subpackages: bool) -> Result<()> {
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

    // Get available packages
    let all_packages = list_packages(&ctx.go_path).await?;

    // Filter to root packages only (no parent package in the list) unless --subpackages
    let packages: Vec<String> = if subpackages {
        all_packages
    } else {
        all_packages
            .iter()
            .filter(|pkg| {
                // Keep if no other package is a parent of this one
                !all_packages
                    .iter()
                    .any(|other| other != *pkg && pkg.starts_with(&format!("{}/", other)))
            })
            .cloned()
            .collect()
    };

    // Load reports - from all branches if on main, otherwise just current branch
    let reports = if is_main {
        load_all_reports().await?
    } else {
        load_all_for_branch(&ctx.branch).await?
    };

    // Group reports by package (keep only latest per package)
    let mut latest_by_package: HashMap<String, Report> = HashMap::new();
    for report in reports {
        latest_by_package
            .entry(report.package.clone())
            .or_insert(report);
    }

    // Print table header
    println!(
        "{:<50} {:<12} {:<12} {:>6} {:>6} {:>6} {:>6} {:>6}",
        "Package".bold(),
        "Branch".bold(),
        "Timestamp".bold(),
        "Pass".bold(),
        "Fail".bold(),
        "Skip".bold(),
        "Total".bold(),
        "Rate".bold()
    );
    println!("{}", "─".repeat(110));

    // Track how many packages have reports
    let mut packages_run = 0;

    // Print each package
    for package in &packages {
        if subpackages {
            // Show exact match only when displaying all subpackages
            if let Some(report) = latest_by_package.get(package) {
                print_package_row(package, report);
                packages_run += 1;
            } else {
                print_empty_row(package);
            }
        } else {
            // Roll up: aggregate this package + all subpackages
            let matching_reports: Vec<&Report> = latest_by_package
                .iter()
                .filter(|(rp, _)| *rp == package || rp.starts_with(&format!("{}/", package)))
                .map(|(_, r)| r)
                .collect();

            if matching_reports.is_empty() {
                print_empty_row(package);
            } else {
                // Aggregate stats from all matching reports
                let mut total_pass = 0;
                let mut total_fail = 0;
                let mut total_skip = 0;
                let mut latest_report: Option<&Report> = None;

                for report in &matching_reports {
                    total_pass += report.summary.passed;
                    total_fail += report.summary.failed;
                    total_skip += report.summary.skipped;

                    // Track the most recent report for branch/timestamp
                    if latest_report.is_none()
                        || report.timestamp > latest_report.unwrap().timestamp
                    {
                        latest_report = Some(report);
                    }
                }

                let total = total_pass + total_fail + total_skip;
                let pass_rate = if total > 0 {
                    total_pass * 100 / total
                } else {
                    100
                };

                let rate_str = if pass_rate == 100 {
                    format!("{}%", pass_rate).green()
                } else if pass_rate >= 90 {
                    format!("{}%", pass_rate).yellow()
                } else {
                    format!("{}%", pass_rate).red()
                };

                let report = latest_report.unwrap();
                let timestamp = report.timestamp.format("%m-%d %H:%M").to_string();
                let branch_display = report.branch.trim_start_matches("ffi/");

                println!(
                    "{:<50} {:<12} {:<12} {:>6} {:>6} {:>6} {:>6} {:>6}",
                    package,
                    branch_display.dimmed(),
                    timestamp.dimmed(),
                    total_pass,
                    total_fail,
                    total_skip,
                    total,
                    rate_str
                );

                packages_run += 1;
            }
        }
    }

    // Print totals if we have any data
    // With auto-split, each package has its own report, so count ALL packages
    if packages_run > 0 {
        println!("{}", "─".repeat(110));

        // Calculate totals from ALL packages with reports
        let mut total_tests = 0;
        let mut total_pass = 0;
        let mut total_fail = 0;
        let mut total_skip = 0;

        for report in latest_by_package.values() {
            total_tests += report.summary.total;
            total_pass += report.summary.passed;
            total_fail += report.summary.failed;
            total_skip += report.summary.skipped;
        }

        let pkg_count = latest_by_package.len();

        // Only Rate is colored: green=100%, yellow=90%+, red=<90%
        let pass_rate = if total_tests > 0 {
            total_pass * 100 / total_tests
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

        let label = format!("TOTAL ({} packages)", pkg_count);

        println!(
            "{:<50} {:<12} {:<12} {:>6} {:>6} {:>6} {:>6} {:>6}",
            label.bold(),
            "",
            "",
            total_pass,
            total_fail,
            total_skip,
            total_tests,
            rate_str
        );
    }

    if packages.is_empty() {
        println!("{}", "No test packages found".dimmed());
    }

    Ok(())
}

fn print_package_row(package: &str, report: &Report) {
    let timestamp = report.timestamp.format("%m-%d %H:%M").to_string();
    let branch_display = report.branch.trim_start_matches("ffi/");

    let pass_rate = if report.summary.total > 0 {
        report.summary.passed * 100 / report.summary.total
    } else {
        100
    };

    let rate_str = if pass_rate == 100 {
        format!("{}%", pass_rate).green()
    } else if pass_rate >= 90 {
        format!("{}%", pass_rate).yellow()
    } else {
        format!("{}%", pass_rate).red()
    };

    println!(
        "{:<50} {:<12} {:<12} {:>6} {:>6} {:>6} {:>6} {:>6}",
        package,
        branch_display.dimmed(),
        timestamp.dimmed(),
        report.summary.passed,
        report.summary.failed,
        report.summary.skipped,
        report.summary.total,
        rate_str
    );
}

fn print_empty_row(package: &str) {
    println!(
        "{:<50} {:<12} {:<12} {:>6} {:>6} {:>6} {:>6} {:>6}",
        package,
        "-".dimmed(),
        "-".dimmed(),
        "-".dimmed(),
        "-".dimmed(),
        "-".dimmed(),
        "-".dimmed(),
        "-".dimmed()
    );
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
