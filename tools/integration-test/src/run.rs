use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

/// Isolated directory for a single test run.
///
/// Creates `target/e2e/{timestamp}-{random}/` and removes it on drop
/// unless `DEFRA_E2E_KEEP=1` is set.
pub struct TestRunDir {
    path: PathBuf,
    keep: bool,
}

impl TestRunDir {
    pub fn new() -> Result<Self> {
        let base = crate::workspace_root().join("target/e2e");
        fs::create_dir_all(&base).context("failed to create target/e2e")?;

        let now = chrono_lite_timestamp();
        let rand_hex = format!("{:08x}", rand::random::<u32>());
        let dir_name = format!("{}-{}", now, rand_hex);
        let path = base.join(dir_name);
        fs::create_dir_all(&path)
            .with_context(|| format!("failed to create run dir {}", path.display()))?;

        let keep = std::env::var("DEFRA_E2E_KEEP").is_ok_and(|v| v == "1");

        Ok(Self { path, keep })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Create a subdirectory for a node within this run.
    pub fn node_dir(&self, name: &str) -> Result<PathBuf> {
        let dir = self.path.join(name);
        fs::create_dir_all(&dir)
            .with_context(|| format!("failed to create node dir {}", dir.display()))?;
        Ok(dir)
    }
}

impl Drop for TestRunDir {
    fn drop(&mut self) {
        if self.keep {
            tracing::info!("keeping test run dir: {}", self.path.display());
            return;
        }
        if let Err(e) = fs::remove_dir_all(&self.path) {
            tracing::warn!(
                "failed to remove test run dir {}: {}",
                self.path.display(),
                e
            );
        }
    }
}

/// Produce a `YYYYMMDD-HHMMSS` timestamp without pulling in chrono.
fn chrono_lite_timestamp() -> String {
    use std::time::SystemTime;

    let duration = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = duration.as_secs();

    // Simple UTC breakdown
    let days = secs / 86400;
    let time_of_day = secs % 86400;
    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;

    // Gregorian date from days since epoch (1970-01-01)
    let (year, month, day) = days_to_date(days);

    format!(
        "{:04}{:02}{:02}-{:02}{:02}{:02}",
        year, month, day, hours, minutes, seconds
    )
}

fn days_to_date(days_since_epoch: u64) -> (u64, u64, u64) {
    // Algorithm from http://howardhinnant.github.io/date_algorithms.html
    let z = days_since_epoch + 719468;
    let era = z / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}
