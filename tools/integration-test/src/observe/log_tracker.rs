use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

/// Events emitted by parsing node log output.
#[derive(Clone, Debug)]
pub enum LogEvent {
    Ready,
    Error(String),
    Custom(String),
}

/// Tails a node's stdout.log and emits structured events.
pub struct LogTracker {
    tx: broadcast::Sender<LogEvent>,
    task: JoinHandle<()>,
}

impl LogTracker {
    /// Start tailing `log_path`, matching the ready pattern and any custom patterns.
    pub fn start(log_path: PathBuf, custom_patterns: Vec<regex::Regex>) -> Self {
        let (tx, _) = broadcast::channel(64);
        let tx_clone = tx.clone();

        let task = tokio::spawn(async move {
            if let Err(e) = tail_loop(log_path, tx_clone, custom_patterns).await {
                tracing::warn!("log tracker stopped: {}", e);
            }
        });

        Self { tx, task }
    }

    /// Wait for the Ready event or timeout.
    pub async fn wait_for_ready(&self, timeout: Duration) -> Result<()> {
        let mut rx = self.tx.subscribe();
        let result = tokio::time::timeout(timeout, async {
            loop {
                match rx.recv().await {
                    Ok(LogEvent::Ready) => return Ok(()),
                    Ok(LogEvent::Error(e)) => {
                        return Err(anyhow::anyhow!("node error: {}", e));
                    }
                    Ok(LogEvent::Custom(_)) => {}
                    Err(broadcast::error::RecvError::Lagged(_)) => {}
                    Err(broadcast::error::RecvError::Closed) => {
                        return Err(anyhow::anyhow!("log tracker closed"));
                    }
                }
            }
        })
        .await;

        match result {
            Ok(inner) => inner,
            Err(_) => Err(anyhow::anyhow!("timed out waiting for node ready")),
        }
    }
}

impl Drop for LogTracker {
    fn drop(&mut self) {
        self.task.abort();
    }
}

const READY_PATTERN: &str = "DefraDB HTTP server listening";

async fn tail_loop(
    log_path: PathBuf,
    tx: broadcast::Sender<LogEvent>,
    custom_patterns: Vec<regex::Regex>,
) -> Result<()> {
    // Wait for the log file to appear
    loop {
        if log_path.exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let file = tokio::fs::File::open(&log_path)
        .await
        .with_context(|| format!("failed to open {}", log_path.display()))?;

    let mut reader = BufReader::new(file);
    let mut line = String::new();

    loop {
        line.clear();
        match reader.read_line(&mut line).await {
            Ok(0) => {
                // EOF — sleep and retry (tail -f behavior)
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Ok(_) => {
                if line.contains(READY_PATTERN) {
                    let _ = tx.send(LogEvent::Ready);
                }
                if line.contains("ERROR") {
                    let _ = tx.send(LogEvent::Error(line.trim().to_string()));
                }
                for pattern in &custom_patterns {
                    if pattern.is_match(&line) {
                        let _ = tx.send(LogEvent::Custom(line.trim().to_string()));
                    }
                }
            }
            Err(e) => {
                return Err(anyhow::anyhow!("error reading log: {}", e));
            }
        }
    }
}
