use std::collections::HashMap;
use std::path::PathBuf;

use colored::Colorize;

use crate::error::Result;
use crate::report::load_for_diff;
use crate::runner::TestStatus;
use crate::worktree::WorktreeContext;

/// Show diff between two test runs
pub async fn execute(package: &str, go_path: Option<PathBuf>) -> Result<()> {
    let ctx = WorktreeContext::detect_with(go_path).await?;

    println!(
        "{} {} - {}",
        "FFI Test Diff:".bold(),
        ctx.branch.cyan(),
        package.cyan()
    );
    println!();

    // Load the two most recent reports
    let reports = load_for_diff(&ctx.branch, package, 2).await?;

    if reports.len() < 2 {
        println!(
            "{}",
            "Need at least 2 reports to diff. Run more tests first.".yellow()
        );
        return Ok(());
    }

    let newer = &reports[0];
    let older = &reports[1];

    println!(
        "Comparing: {} ({}) vs {} ({})",
        newer.commit.green(),
        newer.timestamp.format("%Y-%m-%d %H:%M"),
        older.commit.red(),
        older.timestamp.format("%Y-%m-%d %H:%M")
    );
    println!();

    // Build maps for comparison
    let older_tests: HashMap<&str, &TestStatus> = older
        .tests
        .iter()
        .map(|t| (t.name.as_str(), &t.status))
        .collect();

    let newer_tests: HashMap<&str, &TestStatus> = newer
        .tests
        .iter()
        .map(|t| (t.name.as_str(), &t.status))
        .collect();

    // Find changes
    let mut fixed = Vec::new();
    let mut broken = Vec::new();
    let mut new_tests = Vec::new();
    let mut removed_tests = Vec::new();

    for test in &newer.tests {
        match older_tests.get(test.name.as_str()) {
            Some(old_status) => {
                if **old_status == TestStatus::Fail && test.status == TestStatus::Pass {
                    fixed.push(&test.name);
                } else if **old_status == TestStatus::Pass && test.status == TestStatus::Fail {
                    broken.push(&test.name);
                }
            }
            None => {
                new_tests.push(&test.name);
            }
        }
    }

    for test in &older.tests {
        if !newer_tests.contains_key(test.name.as_str()) {
            removed_tests.push(&test.name);
        }
    }

    // Print results
    if !fixed.is_empty() {
        println!("{} ({}):", "Fixed".green().bold(), fixed.len());
        for name in &fixed {
            println!("  {} {}", "✓".green(), name);
        }
        println!();
    }

    if !broken.is_empty() {
        println!("{} ({}):", "Broken".red().bold(), broken.len());
        for name in &broken {
            println!("  {} {}", "✗".red(), name);
        }
        println!();
    }

    if !new_tests.is_empty() {
        println!("{} ({}):", "New".cyan().bold(), new_tests.len());
        for name in &new_tests {
            println!("  {} {}", "+".cyan(), name);
        }
        println!();
    }

    if !removed_tests.is_empty() {
        println!("{} ({}):", "Removed".yellow().bold(), removed_tests.len());
        for name in &removed_tests {
            println!("  {} {}", "-".yellow(), name);
        }
        println!();
    }

    if fixed.is_empty() && broken.is_empty() && new_tests.is_empty() && removed_tests.is_empty() {
        println!("{}", "No changes detected".dimmed());
    }

    // Summary comparison
    println!("{}", "─".repeat(50));
    println!(
        "Summary: {} → {}",
        format!(
            "{}/{}/{}",
            older.summary.passed, older.summary.failed, older.summary.skipped
        )
        .red(),
        format!(
            "{}/{}/{}",
            newer.summary.passed, newer.summary.failed, newer.summary.skipped
        )
        .green()
    );

    let pass_delta = newer.summary.passed as i32 - older.summary.passed as i32;
    let fail_delta = newer.summary.failed as i32 - older.summary.failed as i32;

    if pass_delta != 0 || fail_delta != 0 {
        print!("Delta: ");
        if pass_delta > 0 {
            print!("{} ", format!("+{} pass", pass_delta).green());
        } else if pass_delta < 0 {
            print!("{} ", format!("{} pass", pass_delta).red());
        }
        if fail_delta > 0 {
            print!("{}", format!("+{} fail", fail_delta).red());
        } else if fail_delta < 0 {
            print!("{}", format!("{} fail", fail_delta).green());
        }
        println!();
    }

    Ok(())
}
