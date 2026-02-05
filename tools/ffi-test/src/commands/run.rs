use colored::Colorize;

use crate::builder::build_ffi;
use crate::error::Result;
use crate::report::Report;
use crate::runner::{discover_subpackages, run_tests, RunResult, TestStatus, TestSummary};
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

    // Discover all subpackages under this package
    let packages = discover_subpackages(&ctx.go_path, package).await?;

    if packages.is_empty() {
        println!(
            "{} No test packages found under '{}'",
            "Warning:".yellow(),
            package
        );
        return Ok(());
    }

    // Run tests for each package
    if packages.len() == 1 {
        // Single package - run directly
        run_single_package(&ctx, &packages[0], test_filter, verbose).await
    } else {
        // Multiple packages - run each with progress
        run_multiple_packages(&ctx, &packages, test_filter, verbose).await
    }
}

/// Run tests for a single package
async fn run_single_package(
    ctx: &WorktreeContext,
    package: &str,
    test_filter: Option<&str>,
    verbose: bool,
) -> Result<()> {
    println!("{} {}", "Running tests:".bold(), package.cyan());
    if let Some(filter) = test_filter {
        println!("  Filter: {}", filter);
    }
    println!();

    let result = run_tests(ctx, package, test_filter, verbose).await?;

    // Print summary
    print_summary(&result);

    // Print failed tests
    print_failed_tests(&result, verbose);

    // Save report
    let report = Report::new(ctx, package, result);
    let report_path = report.save().await?;
    println!();
    println!("Report saved: {}", report_path.display());

    // Exit with error if tests failed
    if report.summary.failed > 0 {
        std::process::exit(1);
    }

    Ok(())
}

/// Run tests for multiple packages with progress display
async fn run_multiple_packages(
    ctx: &WorktreeContext,
    packages: &[String],
    test_filter: Option<&str>,
    verbose: bool,
) -> Result<()> {
    println!(
        "{} {} ({} packages found)",
        "Running tests:".bold(),
        packages[0].split('/').next().unwrap_or(&packages[0]).cyan(),
        packages.len()
    );
    if let Some(filter) = test_filter {
        println!("  Filter: {}", filter);
    }
    println!();

    // Print header
    println!(
        "{:<50} {:>8} {:>8} {:>8} {:>8}",
        "Package".bold(),
        "Pass".bold(),
        "Fail".bold(),
        "Skip".bold(),
        "Rate".bold()
    );
    println!("{}", "─".repeat(84));

    // Track grand totals
    let mut grand_total = TestSummary::default();
    let mut all_failed_tests: Vec<(String, Vec<String>)> = Vec::new();
    let mut any_failures = false;

    for package in packages {
        // Run tests for this package
        let result = run_tests(ctx, package, test_filter, verbose).await;

        match result {
            Ok(result) => {
                // Calculate pass rate
                let pass_rate = if result.summary.total > 0 {
                    result.summary.passed * 100 / result.summary.total
                } else {
                    100
                };

                // Format the rate with color
                let rate_str = if pass_rate == 100 {
                    format!("{}%", pass_rate).green()
                } else if pass_rate >= 90 {
                    format!("{}%", pass_rate).yellow()
                } else {
                    format!("{}%", pass_rate).red()
                };

                // Print status line
                let status_char = if result.summary.failed > 0 {
                    "✗".red()
                } else {
                    "✓".green()
                };

                println!(
                    "{} {:<48} {:>8} {:>8} {:>8} {:>8}",
                    status_char,
                    package,
                    result.summary.passed,
                    result.summary.failed,
                    result.summary.skipped,
                    rate_str
                );

                // Accumulate totals
                grand_total.total += result.summary.total;
                grand_total.passed += result.summary.passed;
                grand_total.failed += result.summary.failed;
                grand_total.skipped += result.summary.skipped;

                // Collect failed tests for later display
                let failed_tests: Vec<String> = result
                    .tests
                    .iter()
                    .filter(|t| t.status == TestStatus::Fail)
                    .map(|t| t.name.clone())
                    .collect();

                if !failed_tests.is_empty() {
                    all_failed_tests.push((package.clone(), failed_tests));
                    any_failures = true;
                }

                // Save report for this package
                let report = Report::new(ctx, package, result);
                let _ = report.save().await;
            }
            Err(e) => {
                // Package failed to run
                println!(
                    "{} {:<48} {:>8} {:>8} {:>8} {:>8}",
                    "✗".red(),
                    package,
                    "-".dimmed(),
                    "-".dimmed(),
                    "-".dimmed(),
                    "ERR".red()
                );

                if verbose {
                    eprintln!("  Error: {}", e);
                }

                any_failures = true;
            }
        }
    }

    // Print totals
    println!("{}", "─".repeat(84));
    let grand_pass_rate = if grand_total.total > 0 {
        grand_total.passed * 100 / grand_total.total
    } else {
        100
    };

    let grand_rate_str = if grand_pass_rate == 100 {
        format!("{}%", grand_pass_rate).green().bold()
    } else if grand_pass_rate >= 90 {
        format!("{}%", grand_pass_rate).yellow().bold()
    } else {
        format!("{}%", grand_pass_rate).red().bold()
    };

    println!(
        "  {:<48} {:>8} {:>8} {:>8} {:>8}",
        format!("TOTAL ({} packages)", packages.len()).bold(),
        grand_total.passed,
        grand_total.failed,
        grand_total.skipped,
        grand_rate_str
    );

    // Print all failed tests at the end
    if !all_failed_tests.is_empty() {
        println!();
        println!("{}", "Failed tests:".red().bold());
        for (package, tests) in &all_failed_tests {
            println!("  {}:", package.dimmed());
            for test in tests {
                println!("    {} {}", "✗".red(), test);
            }
        }
    }

    println!();
    println!(
        "Reports saved to: ~/.defra-ffi-reports/runs/ ({} reports)",
        packages.len()
    );

    // Exit with error if any tests failed
    if any_failures {
        std::process::exit(1);
    }

    Ok(())
}

/// Print a test run summary
fn print_summary(result: &RunResult) {
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
}

/// Print failed tests
fn print_failed_tests(result: &RunResult, verbose: bool) {
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
}
