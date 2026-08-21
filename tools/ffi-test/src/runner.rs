use std::collections::HashMap;
use std::path::Path;
use std::time::Instant;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
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
#[non_exhaustive]
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

/// Run Go tests for a package (non-recursive - runs only tests in that specific package)
pub async fn run_tests(
    ctx: &WorktreeContext,
    package: &str,
    test_filter: Option<&str>,
    verbose: bool,
) -> Result<RunResult> {
    let start = Instant::now();

    // Build environment variables
    let env = build_env(ctx, package);

    // Build the go test command
    let mut cmd = Command::new("go");
    cmd.arg("test")
        .arg("-json")
        .arg("-count=1")
        .arg("-tags=rust_ffi");

    if let Some(filter) = test_filter {
        cmd.arg("-run").arg(filter);
    }

    // Run non-recursively (no /...) to run just this package's tests
    cmd.arg(format!("./tests/integration/{}", package))
        .current_dir(&ctx.go_path)
        .envs(env);

    if verbose {
        println!(
            "Running: go test -json -count=1 ./tests/integration/{}",
            package
        );
    }

    let mut child = cmd
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()?;

    let stdout = child.stdout.take().expect("Failed to capture stdout");
    let stderr = child.stderr.take().expect("Failed to capture stderr");
    let stderr_task = tokio::spawn(async move {
        let mut stderr = BufReader::new(stderr);
        let mut output = String::new();
        stderr.read_to_string(&mut output).await?;
        Ok::<_, std::io::Error>(output)
    });
    let reader = BufReader::new(stdout);
    let mut lines = reader.lines();

    // Accumulate test results
    let mut package_output = Vec::new();
    let mut test_outputs: HashMap<String, Vec<String>> = HashMap::new();
    let mut test_results: HashMap<String, TestResult> = HashMap::new();

    while let Some(line) = lines.next_line().await? {
        if line.is_empty() {
            continue;
        }

        let event: GoTestEvent = match serde_json::from_str(&line) {
            Ok(e) => e,
            Err(_) => {
                package_output.push(line);
                continue;
            }
        };

        // Only process events with test names (skip package-level events)
        let test_name = match &event.test {
            Some(name) => name.clone(),
            None => {
                if event.action == "output" {
                    if let Some(output) = event.output {
                        package_output.push(output);
                    }
                }
                continue;
            }
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
    let stderr = stderr_task.await.map_err(|e| {
        FfiTestError::TestExecution(format!("failed to read go test stderr: {e}"))
    })??;
    let duration_secs = start.elapsed().as_secs_f64();

    // If no tests ran and command failed, report error
    if test_results.is_empty() && !status.success() {
        return Err(FfiTestError::TestExecution(test_execution_error(
            &package_output,
            &stderr,
        )));
    }
    if !status.success() {
        record_package_failure(&mut test_results, package, &package_output, &stderr);
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

fn record_package_failure(
    test_results: &mut HashMap<String, TestResult>,
    package: &str,
    package_output: &[String],
    stderr: &str,
) {
    if test_results
        .values()
        .any(|result| result.status == TestStatus::Fail)
    {
        return;
    }

    let name = format!("{package} (go test)");
    test_results.insert(
        name.clone(),
        TestResult {
            name,
            status: TestStatus::Fail,
            elapsed_secs: 0.0,
            output: vec![test_execution_error(package_output, stderr)],
        },
    );
}

fn test_execution_error(package_output: &[String], stderr: &str) -> String {
    let mut output = package_output.concat();
    if !output.is_empty() && !output.ends_with('\n') && !stderr.is_empty() {
        output.push('\n');
    }
    output.push_str(stderr);

    let output = output.trim();
    if output.is_empty() {
        "go test failed without output".to_string()
    } else {
        output.to_string()
    }
}

/// Per-worktree, per-package Go build cache.
///
/// Without this every paired worktree shares one cgo cache and concurrent runs
/// poison each other. Mirrors the Go Makefile's `gocache-$RUST_LIB-$PKG`.
fn gocache_dir(rust_path: &std::path::Path, package: &str) -> std::path::PathBuf {
    let worktree = rust_path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "unknown".to_string());
    let package = package.replace('/', "-");

    std::env::temp_dir().join(format!("gocache-{worktree}-{package}"))
}

/// Build environment variables for Go test
fn build_env(ctx: &WorktreeContext, package: &str) -> HashMap<String, String> {
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

    // Permit deterministic encryption key/nonce mode only for this Go test
    // process. The release FFI library refuses to enable the hidden
    // Go-compatibility crypto switches without this explicit test gate.
    env.insert(
        "DEFRA_ALLOW_DETERMINISTIC_TEST_CRYPTO".to_string(),
        "1".to_string(),
    );

    // Enable vector embedding tests
    env.insert("DEFRA_VECTOR_EMBEDDING".to_string(), "true".to_string());

    // Enable file-based database tests (Go env var name; Rust uses redb for persistence)
    env.insert("DEFRA_BADGER_FILE".to_string(), "true".to_string());

    // Isolate the cgo build cache per worktree and package
    env.insert(
        "GOCACHE".to_string(),
        gocache_dir(&ctx.rust_path, package)
            .to_string_lossy()
            .into_owned(),
    );

    // Pass through Go test framework configuration from the environment.
    // These control the test matrix: which ACP type, mutation type, etc.
    // Example: DEFRA_DOCUMENT_ACP_TYPE=source-hub ffi-test run encryption
    for key in &[
        "DEFRA_DOCUMENT_ACP_TYPE",
        "DEFRA_MUTATION_TYPE",
        "DEFRA_SOURCEHUB_IMAGE",
    ] {
        if let Ok(val) = std::env::var(key) {
            env.insert(key.to_string(), val);
        }
    }

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

/// Discover all subpackages under a given package prefix (including the root if it has tests)
/// Returns packages sorted by name, which naturally groups hierarchically
pub async fn discover_subpackages(go_path: &Path, package: &str) -> Result<Vec<String>> {
    let all_packages = list_packages(go_path).await?;

    // Filter to packages that match or are under the given package
    let subpackages: Vec<String> = all_packages
        .into_iter()
        .filter(|p| {
            // Exact match or child package
            p == package || p.starts_with(&format!("{}/", package))
        })
        .collect();

    Ok(subpackages)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reports_package_level_test_failure() {
        let output = test_execution_error(
            &["./adapter.go:42: undefined: list_actions\n".to_string()],
            "build failed\n",
        );

        assert_eq!(
            output,
            "./adapter.go:42: undefined: list_actions\nbuild failed"
        );
    }

    #[test]
    fn nonzero_exit_cannot_leave_named_results_green() {
        let mut results = HashMap::from([(
            "TestPassed".to_string(),
            TestResult {
                name: "TestPassed".to_string(),
                status: TestStatus::Pass,
                elapsed_secs: 0.1,
                output: Vec::new(),
            },
        )]);

        record_package_failure(
            &mut results,
            "query/simple",
            &["package setup failed\n".to_string()],
            "",
        );

        assert_eq!(results.len(), 2);
        let package_failure = &results["query/simple (go test)"];
        assert_eq!(package_failure.status, TestStatus::Fail);
        assert_eq!(package_failure.output, ["package setup failed"]);
    }

    #[test]
    fn each_worktree_and_package_gets_its_own_go_build_cache() {
        let here = gocache_dir(
            std::path::Path::new("/r/defradb.rs-ffi-port"),
            "query/simple",
        );
        let other_worktree =
            gocache_dir(std::path::Path::new("/r/defradb.rs-index"), "query/simple");
        let other_package = gocache_dir(std::path::Path::new("/r/defradb.rs-ffi-port"), "acp/nac");

        assert_ne!(
            here, other_worktree,
            "concurrent worktrees must not share a cgo cache"
        );
        assert_ne!(here, other_package);
        assert!(here.to_string_lossy().contains("defradb.rs-ffi-port"));
    }

    #[test]
    fn the_go_child_process_is_told_which_build_cache_to_use() {
        let ctx = WorktreeContext {
            rust_path: std::path::PathBuf::from("/r/defradb.rs-ffi-port"),
            go_path: std::path::PathBuf::from("/r/defradb-ffi-port"),
            branch: "edjroz/1395-rustffi-port".to_string(),
            commit: "0000000".to_string(),
            dirty: false,
        };

        let env = build_env(&ctx, "query/simple");

        assert_eq!(
            env.get("GOCACHE").map(String::as_str),
            gocache_dir(&ctx.rust_path, "query/simple").to_str()
        );
    }
}
