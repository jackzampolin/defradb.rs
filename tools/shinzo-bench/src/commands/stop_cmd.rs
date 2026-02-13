use colored::Colorize;

use crate::config;
use crate::process;

pub async fn stop() -> anyhow::Result<()> {
    let (defra_pid, indexer_pid) = process::load_pids(&config::pids_file());
    let mut stopped = false;

    if let Some(pid) = indexer_pid {
        if process::is_alive(pid) {
            println!("Stopping indexer (PID {})...", pid);
            process::kill_process(pid);
            stopped = true;
        }
    }

    if let Some(pid) = defra_pid {
        if process::is_alive(pid) {
            println!("Stopping defra (PID {})...", pid);
            process::kill_process(pid);
            stopped = true;
        }
    }

    if stopped {
        // Wait briefly for processes to exit
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        println!("{}", "All processes stopped.".green());
    } else {
        println!("No running processes found.");
    }

    // Clean up PID file
    let _ = tokio::fs::remove_file(config::pids_file()).await;

    Ok(())
}

pub async fn clean() -> anyhow::Result<()> {
    // Stop first
    stop().await?;

    let base_dir = config::base_dir();
    if base_dir.exists() {
        println!("Removing {}...", base_dir.display());
        tokio::fs::remove_dir_all(&base_dir).await?;
        println!("{}", "Cleaned.".green());
    } else {
        println!("Nothing to clean.");
    }

    Ok(())
}
