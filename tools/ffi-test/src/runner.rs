use std::collections::HashMap;
use std::path::Path;
use std::time::Instant;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;

use crate::config::FFI_CRATE_PATH;
use crate::error::{FfiTestError, Result};
use crate::worktree::WorktreeContext;

/// A single Go test event from JSON output
#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GoTestEvent {
    #[allow(dead_code)]
    time: Option<String>,
    action: String,
    #[allow(dead_code)]
    package: Option<String>,
    test: Option<String>,
    output: Option<String>,
    elapsed: Option<f64>,
}

/// Status of a test
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum TestStatus {
    Pass,
    Fail,
    Skip,
}

/// Result of a single test
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestResult {
    pub name: String,
    pub status: TestStatus,
    pub elapsed_secs: f64,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub output: Vec<String>,
}

/// Summary of test run
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TestSummary {
    pub total: usize,
    pub passed: usize,
    pub failed: usize,
    pub skipped: usize,
}

/// Result of running tests
#[derive(Debug)]
pub struct RunResult {
    pub summary: TestSummary,
    pub tests: Vec<TestResult>,
    pub duration_secs: f64,
}

/// Run Go tests for a package
pub async fn run_tests(
    ctx: &WorktreeContext,
    package: &str,
    test_filter: Option<&str>,
    verbose: bool,
) -> Result<RunResult> {
    let start = Instant::now();

    // Build environment variables
    let env = build_env(ctx);

    // Build the go test command
    let mut cmd = Command::new("go");
    cmd.arg("test").arg("-json").arg("-count=1");

    if let Some(filter) = test_filter {
        cmd.arg("-run").arg(filter);
    }

    cmd.arg(format!("./tests/integration/{}/...", package))
        .current_dir(&ctx.go_path)
        .envs(env);

    if verbose {
        println!(
            "Running: go test -json -count=1 ./tests/integration/{}/...",
            package
        );
    }

    let mut child = cmd
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()?;

    let stdout = child.stdout.take().expect("Failed to capture stdout");
    let reader = BufReader::new(stdout);
    let mut lines = reader.lines();

    // Accumulate test results
    let mut test_outputs: HashMap<String, Vec<String>> = HashMap::new();
    let mut test_results: HashMap<String, TestResult> = HashMap::new();

    while let Some(line) = lines.next_line().await? {
        if line.is_empty() {
            continue;
        }

        let event: GoTestEvent = match serde_json::from_str(&line) {
            Ok(e) => e,
            Err(_) => continue, // Skip non-JSON lines
        };

        // Only process events with test names (skip package-level events)
        let test_name = match &event.test {
            Some(name) => name.clone(),
            None => continue,
        };

        match event.action.as_str() {
            "run" => {
                test_outputs.insert(test_name.clone(), Vec::new());
            }
            "output" => {
                if let Some(output) = event.output {
                    if verbose {
                        print!("{}", output.trim_end_matches('\n'));
                        if !output.ends_with('\n') {
                            println!();
                        }
                    }
                    if let Some(outputs) = test_outputs.get_mut(&test_name) {
                        outputs.push(output);
                    }
                }
            }
            "pass" => {
                let output = test_outputs.remove(&test_name).unwrap_or_default();
                test_results.insert(
                    test_name.clone(),
                    TestResult {
                        name: test_name,
                        status: TestStatus::Pass,
                        elapsed_secs: event.elapsed.unwrap_or(0.0),
                        output: if output
                            .iter()
                            .any(|o| o.contains("FAIL") || o.contains("Error"))
                        {
                            output
                        } else {
                            Vec::new()
                        },
                    },
                );
            }
            "fail" => {
                let output = test_outputs.remove(&test_name).unwrap_or_default();
                test_results.insert(
                    test_name.clone(),
                    TestResult {
                        name: test_name,
                        status: TestStatus::Fail,
                        elapsed_secs: event.elapsed.unwrap_or(0.0),
                        output,
                    },
                );
            }
            "skip" => {
                let output = test_outputs.remove(&test_name).unwrap_or_default();
                test_results.insert(
                    test_name.clone(),
                    TestResult {
                        name: test_name,
                        status: TestStatus::Skip,
                        elapsed_secs: event.elapsed.unwrap_or(0.0),
                        output,
                    },
                );
            }
            _ => {}
        }
    }

    // Wait for the command to complete
    let status = child.wait().await?;
    let duration_secs = start.elapsed().as_secs_f64();

    // If no tests ran and command failed, report error
    if test_results.is_empty() && !status.success() {
        return Err(FfiTestError::TestExecution(
            "No tests found or test execution failed".to_string(),
        ));
    }

    // Build summary
    let mut summary = TestSummary::default();
    let mut tests: Vec<TestResult> = test_results.into_values().collect();
    tests.sort_by(|a, b| a.name.cmp(&b.name));

    for test in &tests {
        summary.total += 1;
        match test.status {
            TestStatus::Pass => summary.passed += 1,
            TestStatus::Fail => summary.failed += 1,
            TestStatus::Skip => summary.skipped += 1,
        }
    }

    Ok(RunResult {
        summary,
        tests,
        duration_secs,
    })
}

/// Build environment variables for Go test
fn build_env(ctx: &WorktreeContext) -> HashMap<String, String> {
    let mut env = HashMap::new();

    // CGO flags
    let ffi_include = ctx.rust_path.join(FFI_CRATE_PATH);
    let lib_dir = ctx.rust_path.join("target").join("release");

    env.insert(
        "CGO_CFLAGS".to_string(),
        format!("-I{}", ffi_include.display()),
    );
    env.insert(
        "CGO_LDFLAGS".to_string(),
        format!("-L{}", lib_dir.display()),
    );
    env.insert("CGO_ENABLED".to_string(), "1".to_string());

    // Enable Rust FFI client
    env.insert("DEFRA_CLIENT_RUST_FFI".to_string(), "true".to_string());

    env
}

/// List available test packages in the Go integration tests
pub async fn list_packages(go_path: &Path) -> Result<Vec<String>> {
    let tests_dir = go_path.join("tests").join("integration");

    if !tests_dir.exists() {
        return Ok(Vec::new());
    }

    let mut packages = Vec::new();
    collect_packages(&tests_dir, &tests_dir, &mut packages).await?;

    packages.sort();
    Ok(packages)
}

/// Recursively collect test packages
async fn collect_packages(base: &Path, current: &Path, packages: &mut Vec<String>) -> Result<()> {
    let mut entries = tokio::fs::read_dir(current).await?;

    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();

        if entry.file_type().await?.is_dir() {
            // Check if this directory contains Go test files
            let mut has_tests = false;
            let mut dir_entries = tokio::fs::read_dir(&path).await?;

            while let Some(file) = dir_entries.next_entry().await? {
                let name = file.file_name();
                let name_str = name.to_string_lossy();
                if name_str.ends_with("_test.go") {
                    has_tests = true;
                    break;
                }
            }

            if has_tests {
                let relative = path.strip_prefix(base).unwrap();
                packages.push(relative.to_string_lossy().to_string());
            }

            // Recurse into subdirectory
            Box::pin(collect_packages(base, &path, packages)).await?;
        }
    }

    Ok(())
}
