use crate::error::{FfiTestError, Result};
use crate::report::load_for_diff;
use crate::runner::TestStatus;
use crate::worktree::WorktreeContext;

/// Execute the logs command
pub async fn execute(
    package: &str,
    test_filter: Option<&str>,
    failed_only: bool,
    all_output: bool,
) -> Result<()> {
    let ctx = WorktreeContext::detect().await?;

    // Load the most recent report for this package
    let reports = load_for_diff(&ctx.branch, package, 1).await?;

    if reports.is_empty() {
        return Err(FfiTestError::TestExecution(format!(
            "No reports found for package '{}' on branch '{}'. Run 'ffi-test run {}' first.",
            package, ctx.branch, package
        )));
    }

    let report = &reports[0];

    println!(
        "Logs from: {} @ {} ({})",
        report.package,
        report.commit,
        report.timestamp.format("%Y-%m-%d %H:%M:%S")
    );
    println!();

    let mut shown_count = 0;

    for test in &report.tests {
        // Apply filters
        if failed_only && test.status != TestStatus::Fail {
            continue;
        }

        if let Some(filter) = test_filter {
            if !test.name.contains(filter) {
                continue;
            }
        }

        // Status indicator with color
        let status_str = match test.status {
            TestStatus::Pass => "\x1b[32m✓ PASS\x1b[0m",
            TestStatus::Fail => "\x1b[31m✗ FAIL\x1b[0m",
            TestStatus::Skip => "\x1b[33m○ SKIP\x1b[0m",
        };

        println!("{} {}", status_str, test.name);

        // Show output if requested or if test failed
        let should_show_output = all_output || test.status == TestStatus::Fail;

        if should_show_output && !test.output.is_empty() {
            println!("\x1b[90m{}\x1b[0m", "─".repeat(60));
            for line in &test.output {
                // Trim trailing newlines for cleaner output
                print!("  {}", line.trim_end_matches('\n'));
                if !line.ends_with('\n') {
                    println!();
                } else {
                    println!();
                }
            }
            println!("\x1b[90m{}\x1b[0m", "─".repeat(60));
            println!();
        }

        shown_count += 1;
    }

    if shown_count == 0 {
        if failed_only {
            println!("\x1b[32mNo failed tests!\x1b[0m");
        } else if test_filter.is_some() {
            println!("No tests matching filter '{}'", test_filter.unwrap());
        }
    } else {
        println!(
            "\nShowed {} test{}",
            shown_count,
            if shown_count == 1 { "" } else { "s" }
        );
    }

    Ok(())
}
