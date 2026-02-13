use serde::{Deserialize, Serialize};
use sysinfo::System;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub timestamp: String,
    pub elapsed_secs: u64,
    pub defra_rss_mb: f64,
    pub indexer_rss_mb: f64,
    pub defra_cpu_pct: f32,
    pub indexer_cpu_pct: f32,
    pub disk_usage_mb: f64,
    pub block_height: Option<u64>,
    pub blocks_per_min: f64,
    pub error_count: u64,
}

/// Collect RSS (in MB) for a given PID.
pub fn process_rss_mb(sys: &System, pid: u32) -> f64 {
    use sysinfo::Pid;
    sys.process(Pid::from_u32(pid))
        .map(|p| p.memory() as f64 / (1024.0 * 1024.0))
        .unwrap_or(0.0)
}

/// Collect CPU % for a given PID.
pub fn process_cpu_pct(sys: &System, pid: u32) -> f32 {
    use sysinfo::Pid;
    sys.process(Pid::from_u32(pid))
        .map(|p| p.cpu_usage())
        .unwrap_or(0.0)
}

/// Compute disk usage in MB for a directory.
pub fn disk_usage_mb(path: &std::path::Path) -> f64 {
    fn dir_size(path: &std::path::Path) -> u64 {
        let mut total = 0;
        if let Ok(entries) = std::fs::read_dir(path) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    total += dir_size(&path);
                } else if let Ok(meta) = path.metadata() {
                    total += meta.len();
                }
            }
        }
        total
    }
    dir_size(path) as f64 / (1024.0 * 1024.0)
}

/// Count ERROR lines in a log file.
pub fn count_errors(log_path: &std::path::Path) -> u64 {
    use std::io::BufRead;
    let file = match std::fs::File::open(log_path) {
        Ok(f) => f,
        Err(_) => return 0,
    };
    std::io::BufReader::new(file)
        .lines()
        .map_while(Result::ok)
        .filter(|l| l.contains("ERROR"))
        .count() as u64
}
