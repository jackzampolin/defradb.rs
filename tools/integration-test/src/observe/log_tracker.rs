use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use super::patterns::NamedPattern;

/// Events emitted by parsing node log output.
#[derive(Clone, Debug)]
pub enum LogEvent {
    Ready,
    Error(String),
    Pattern { name: String, line: String },
}

/// Stores the first matched line for each named pattern.
type SeenPatterns = Arc<Mutex<HashMap<String, String>>>;

/// Tails a node's stdout.log and emits structured events.
pub struct LogTracker {
    tx: broadcast::Sender<LogEvent>,
    seen: SeenPatterns,
    task: JoinHandle<()>,
}

impl LogTracker {
    /// Create an empty placeholder (no background task).
    pub fn empty() -> Self {
        let (tx, _) = broadcast::channel(1);
        let task = tokio::spawn(async {});
        Self {
            tx,
            seen: Arc::new(Mutex::new(HashMap::new())),
            task,
        }
    }

    /// Start tailing `log_path`, matching the ready pattern and any named patterns.
    pub fn start(log_path: PathBuf, named_patterns: Vec<NamedPattern>) -> Self {
        let (tx, _) = broadcast::channel(64);
        let tx_clone = tx.clone();
        let seen: SeenPatterns = Arc::new(Mutex::new(HashMap::new()));
        let seen_clone = seen.clone();

        let task = tokio::spawn(async move {
            if let Err(e) = tail_loop(log_path, tx_clone, named_patterns, seen_clone).await {
                tracing::warn!("log tracker stopped: {}", e);
            }
        });

        Self { tx, seen, task }
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
                    Ok(LogEvent::Pattern { .. }) => {}
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

    /// Wait for a named pattern to match, returning the matched line.
    ///
    /// If the pattern was already matched before this call, returns immediately.
    pub async fn wait_for_pattern(&self, name: &str, timeout: Duration) -> Result<String> {
        // Check if already seen
        if let Some(line) = self.seen.lock().unwrap().get(name) {
            return Ok(line.clone());
        }

        let mut rx = self.tx.subscribe();
        let name_owned = name.to_string();
        let seen = self.seen.clone();

        let result = tokio::time::timeout(timeout, async move {
            loop {
                // Re-check seen map in case of race between check above and subscribe
                if let Some(line) = seen.lock().unwrap().get(&name_owned) {
                    return Ok(line.clone());
                }

                match rx.recv().await {
                    Ok(LogEvent::Pattern {
                        name: ref n,
                        ref line,
                    }) if *n == name_owned => {
                        return Ok(line.clone());
                    }
                    Ok(_) => {}
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        // After lag, re-check seen map
                        if let Some(line) = seen.lock().unwrap().get(&name_owned) {
                            return Ok(line.clone());
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        return Err(anyhow::anyhow!("log tracker closed"));
                    }
                }
            }
        })
        .await;

        match result {
            Ok(inner) => inner,
            Err(_) => Err(anyhow::anyhow!("timed out waiting for pattern '{}'", name)),
        }
    }
}

impl Drop for LogTracker {
    fn drop(&mut self) {
        self.task.abort();
    }
}

const READY_PATTERN: &str = "Providing HTTP API at";

async fn tail_loop(
    log_path: PathBuf,
    tx: broadcast::Sender<LogEvent>,
    named_patterns: Vec<NamedPattern>,
    seen: SeenPatterns,
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
                for pattern in &named_patterns {
                    if pattern.regex.is_match(&line) {
                        let trimmed = line.trim().to_string();
                        seen.lock()
                            .unwrap()
                            .entry(pattern.name.to_string())
                            .or_insert_with(|| trimmed.clone());
                        let _ = tx.send(LogEvent::Pattern {
                            name: pattern.name.to_string(),
                            line: trimmed,
                        });
                    }
                }
            }
            Err(e) => {
                return Err(anyhow::anyhow!("error reading log: {}", e));
            }
        }
    }
}
