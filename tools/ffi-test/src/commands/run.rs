use colored::Colorize;

use crate::builder::build_ffi;
use crate::error::Result;
use crate::report::Report;
use crate::runner::{run_tests, TestStatus};
use crate::worktree::WorktreeContext;

/// Run FFI tests for a package
pub async fn execute(
    package: &str,
    test_filter: Option<&str>,
    verbose: bool,
    skip_build: bool,
) -> Result<()> {
    // Detect worktree context
    let ctx = WorktreeContext::detect().await?;

    println!(
        "{} {} @ {}{}",
        "FFI Test:".bold(),
        ctx.branch.cyan(),
        ctx.commit.yellow(),
        if ctx.dirty {
            " (dirty)".red().to_string()
        } else {
            String::new()
        }
    );
    println!("  Rust: {}", ctx.rust_path.display());
    println!("  Go:   {}", ctx.go_path.display());
    println!();

    // Build FFI library (unless skipped)
    if !skip_build {
        println!("{}", "Building FFI...".bold());
        build_ffi(&ctx, verbose).await?;
        println!();
    }

    // Run tests
    println!("{} {}", "Running tests:".bold(), package.cyan());
    if let Some(filter) = test_filter {
        println!("  Filter: {}", filter);
    }
    println!();

    let result = run_tests(&ctx, package, test_filter, verbose).await?;

    // Print summary
    println!();
    println!("{}", "─".repeat(60));
    println!(
        "{}: {} total, {} passed, {} failed, {} skipped",
        "Summary".bold(),
        result.summary.total,
        result.summary.passed.to_string().green(),
        if result.summary.failed > 0 {
            result.summary.failed.to_string().red()
        } else {
            result.summary.failed.to_string().normal()
        },
        result.summary.skipped.to_string().yellow()
    );
    println!("Duration: {:.2}s", result.duration_secs);

    // Print failed tests
    let failed: Vec<_> = result
        .tests
        .iter()
        .filter(|t| t.status == TestStatus::Fail)
        .collect();
    if !failed.is_empty() {
        println!();
        println!("{}", "Failed tests:".red().bold());
        for test in &failed {
            println!("  {} {}", "✗".red(), test.name);
            if verbose && !test.output.is_empty() {
                for line in &test.output {
                    println!("    {}", line.trim_end());
                }
            }
        }
    }

    // Save report
    let report = Report::new(&ctx, package, result);
    let report_path = report.save().await?;
    println!();
    println!("Report saved: {}", report_path.display());

    // Exit with error if tests failed
    if report.summary.failed > 0 {
        std::process::exit(1);
    }

    Ok(())
}
