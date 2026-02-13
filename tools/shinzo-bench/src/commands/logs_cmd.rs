use clap::Parser;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;

use crate::config;

#[derive(Parser)]
pub struct LogsArgs {
    /// Which log to tail: defra, indexer, or both (default)
    #[arg(default_value = "both")]
    pub target: String,
}

pub async fn logs(args: LogsArgs) -> anyhow::Result<()> {
    match args.target.as_str() {
        "defra" => tail_file(&config::defra_log()).await,
        "indexer" => tail_file(&config::indexer_log()).await,
        _ => {
            let defra = tail_file_task(config::defra_log(), "defra");
            let indexer = tail_file_task(config::indexer_log(), "indexer");
            tokio::select! {
                r = defra => r,
                r = indexer => r,
                _ = tokio::signal::ctrl_c() => {
                    println!("\nStopped.");
                    Ok(())
                }
            }
        }
    }
}

async fn tail_file(path: &std::path::Path) -> anyhow::Result<()> {
    if !path.exists() {
        anyhow::bail!("Log file not found: {}", path.display());
    }

    let mut child = Command::new("tail")
        .arg("-f")
        .arg("-n")
        .arg("50")
        .arg(path)
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        .spawn()?;

    tokio::select! {
        status = child.wait() => {
            let status = status?;
            if !status.success() {
                anyhow::bail!("tail exited with status: {}", status);
            }
        }
        _ = tokio::signal::ctrl_c() => {
            child.kill().await?;
        }
    }

    Ok(())
}

async fn tail_file_task(path: std::path::PathBuf, label: &'static str) -> anyhow::Result<()> {
    if !path.exists() {
        eprintln!("[{}] Log file not found: {}", label, path.display());
        // Wait forever so the other task can still run
        std::future::pending::<()>().await;
        return Ok(());
    }

    let mut child = Command::new("tail")
        .arg("-f")
        .arg("-n")
        .arg("20")
        .arg(&path)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()?;

    let stdout = child.stdout.take().unwrap();
    let reader = BufReader::new(stdout);
    let mut lines = reader.lines();

    while let Some(line) = lines.next_line().await? {
        println!("[{}] {}", label, line);
    }

    child.wait().await?;
    Ok(())
}
