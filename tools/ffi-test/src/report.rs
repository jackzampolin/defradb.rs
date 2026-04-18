use std::path::PathBuf;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::config::{reports_dir, REPORT_RETENTION_COUNT};
use crate::error::Result;
use crate::runner::{RunResult, TestResult, TestSummary};
use crate::worktree::WorktreeContext;

/// A saved test report
#[derive(Debug, Serialize, Deserialize)]
pub struct Report {
    pub timestamp: DateTime<Utc>,
    pub branch: String,
    pub commit: String,
    pub dirty: bool,
    pub package: String,
    pub rust_worktree: String,
    pub go_worktree: String,
    pub duration_secs: f64,
    pub summary: TestSummary,
    pub tests: Vec<TestResult>,
}

impl Report {
    /// Create a new report from a test run
    pub fn new(ctx: &WorktreeContext, package: &str, result: RunResult) -> Self {
        Report {
            timestamp: Utc::now(),
            branch: ctx.branch.clone(),
            commit: ctx.commit.clone(),
            dirty: ctx.dirty,
            package: package.to_string(),
            rust_worktree: ctx.rust_path.display().to_string(),
            go_worktree: ctx.go_path.display().to_string(),
            duration_secs: result.duration_secs,
            summary: result.summary,
            tests: result.tests,
        }
    }

    /// Generate filename for this report
    fn filename(&self) -> String {
        let branch_safe = self.branch.replace('/', "_");
        let package_safe = self.package.replace('/', "_");
        let timestamp = self.timestamp.format("%Y%m%d_%H%M%S");
        format!(
            "{}_{}_{}_{}.json",
            branch_safe, package_safe, timestamp, self.commit
        )
    }

    /// Save the report to disk
    pub async fn save(&self) -> Result<PathBuf> {
        let dir = reports_dir();
        tokio::fs::create_dir_all(&dir).await?;

        let path = dir.join(self.filename());
        let json = serde_json::to_string_pretty(self)?;
        tokio::fs::write(&path, json).await?;

        // Enforce retention
        enforce_retention(&self.branch, &self.package).await?;

        Ok(path)
    }
}

/// Enforce retention policy for reports
async fn enforce_retention(branch: &str, package: &str) -> Result<()> {
    let dir = reports_dir();
    if !dir.exists() {
        return Ok(());
    }

    let branch_safe = branch.replace('/', "_");
    let package_safe = package.replace('/', "_");
    let prefix = format!("{}_{}_", branch_safe, package_safe);

    // Collect matching reports
    let mut reports: Vec<(PathBuf, DateTime<Utc>)> = Vec::new();
    let mut entries = tokio::fs::read_dir(&dir).await?;

    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();

        if name_str.starts_with(&prefix) && name_str.ends_with(".json") {
            // Parse timestamp from filename
            if let Some(ts) = parse_timestamp_from_filename(&name_str) {
                reports.push((entry.path(), ts));
            }
        }
    }

    // Sort by timestamp, newest first
    reports.sort_by_key(|r| std::cmp::Reverse(r.1));

    // Delete oldest reports beyond retention count
    for (path, _) in reports.into_iter().skip(REPORT_RETENTION_COUNT) {
        let _ = tokio::fs::remove_file(path).await;
    }

    Ok(())
}

/// Parse timestamp from report filename
fn parse_timestamp_from_filename(filename: &str) -> Option<DateTime<Utc>> {
    // Format: branch_package_YYYYMMDD_HHMMSS_commit.json
    let parts: Vec<&str> = filename.trim_end_matches(".json").split('_').collect();
    if parts.len() < 4 {
        return None;
    }

    // Find the date part (YYYYMMDD format, 8 digits)
    for (i, part) in parts.iter().enumerate() {
        if part.len() == 8
            && part.chars().all(|c| c.is_ascii_digit())
            && i + 1 < parts.len()
            && parts[i + 1].len() == 6
        {
            let date_str = format!("{}_{}", part, parts[i + 1]);
            if let Ok(naive) = chrono::NaiveDateTime::parse_from_str(&date_str, "%Y%m%d_%H%M%S") {
                return Some(DateTime::from_naive_utc_and_offset(naive, Utc));
            }
        }
    }

    None
}

/// Load all reports from all branches (for main worktree unified view)
pub async fn load_all_reports() -> Result<Vec<Report>> {
    let dir = reports_dir();
    if !dir.exists() {
        return Ok(Vec::new());
    }

    let mut reports = Vec::new();
    let mut entries = tokio::fs::read_dir(&dir).await?;

    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();

        if name_str.ends_with(".json") {
            let content = tokio::fs::read_to_string(entry.path()).await?;
            if let Ok(report) = serde_json::from_str::<Report>(&content) {
                reports.push(report);
            }
        }
    }

    // Sort by package then timestamp (newest first)
    reports.sort_by(|a, b| {
        a.package
            .cmp(&b.package)
            .then_with(|| b.timestamp.cmp(&a.timestamp))
    });

    Ok(reports)
}

/// Load all reports for a branch
pub async fn load_all_for_branch(branch: &str) -> Result<Vec<Report>> {
    let dir = reports_dir();
    if !dir.exists() {
        return Ok(Vec::new());
    }

    let branch_safe = branch.replace('/', "_");
    let prefix = format!("{}_", branch_safe);

    let mut reports = Vec::new();
    let mut entries = tokio::fs::read_dir(&dir).await?;

    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();

        if name_str.starts_with(&prefix) && name_str.ends_with(".json") {
            let content = tokio::fs::read_to_string(entry.path()).await?;
            if let Ok(report) = serde_json::from_str::<Report>(&content) {
                reports.push(report);
            }
        }
    }

    // Sort by package then timestamp
    reports.sort_by(|a, b| {
        a.package
            .cmp(&b.package)
            .then_with(|| b.timestamp.cmp(&a.timestamp))
    });

    Ok(reports)
}

/// Load two reports for comparison
pub async fn load_for_diff(branch: &str, package: &str, count: usize) -> Result<Vec<Report>> {
    let dir = reports_dir();
    if !dir.exists() {
        return Ok(Vec::new());
    }

    let branch_safe = branch.replace('/', "_");
    let package_safe = package.replace('/', "_");
    let prefix = format!("{}_{}_", branch_safe, package_safe);

    let mut reports_with_ts: Vec<(Report, DateTime<Utc>)> = Vec::new();
    let mut entries = tokio::fs::read_dir(&dir).await?;

    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();

        if name_str.starts_with(&prefix) && name_str.ends_with(".json") {
            if let Some(ts) = parse_timestamp_from_filename(&name_str) {
                let content = tokio::fs::read_to_string(entry.path()).await?;
                if let Ok(report) = serde_json::from_str::<Report>(&content) {
                    reports_with_ts.push((report, ts));
                }
            }
        }
    }

    // Sort by timestamp, newest first
    reports_with_ts.sort_by_key(|r| std::cmp::Reverse(r.1));

    Ok(reports_with_ts
        .into_iter()
        .take(count)
        .map(|(r, _)| r)
        .collect())
}
