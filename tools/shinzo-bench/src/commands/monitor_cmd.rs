use colored::Colorize;
use sysinfo::System;

use crate::config;
use crate::metrics;
use crate::process;

pub async fn monitor() -> anyhow::Result<()> {
    let ports = process::load_ports(&config::ports_file());
    let is_ffi = ports.get("RUST_FFI").is_some_and(|v| v == "1");
    let store = ports
        .get("STORE")
        .cloned()
        .unwrap_or_else(|| "unknown".into());
    let api_port: Option<u16> = ports.get("API_PORT").and_then(|p| p.parse().ok());

    println!(
        "{} (store={}, mode={})",
        "=== Shinzo Monitor ===".cyan().bold(),
        store,
        if is_ffi { "FFI" } else { "HTTP" }
    );
    println!(
        "{:>8} {:>10} {:>10} {:>8} {:>8} {:>10} {:>12} {:>8}",
        "Time", "Defra RSS", "Idx RSS", "D CPU%", "I CPU%", "Disk MB", "Blk Height", "Errors"
    );
    println!("{}", "-".repeat(86));

    let start = std::time::Instant::now();
    let mut sys = System::new_all();
    let mut prev_block_height: Option<u64> = None;
    let mut prev_check_time = std::time::Instant::now();

    let client = reqwest::Client::new();

    loop {
        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {}
            _ = tokio::signal::ctrl_c() => {
                println!("\n{}", "Monitor stopped.".yellow());
                break;
            }
        }

        let (defra_pid, indexer_pid) = process::load_pids(&config::pids_file());

        // Refresh process info
        let pids_to_refresh: Vec<sysinfo::Pid> = [defra_pid, indexer_pid]
            .iter()
            .flatten()
            .map(|p| sysinfo::Pid::from_u32(*p))
            .collect();
        sys.refresh_processes(sysinfo::ProcessesToUpdate::Some(&pids_to_refresh), true);

        let defra_rss = defra_pid.map_or(0.0, |p| metrics::process_rss_mb(&sys, p));
        let indexer_rss = indexer_pid.map_or(0.0, |p| metrics::process_rss_mb(&sys, p));
        let defra_cpu = defra_pid.map_or(0.0, |p| metrics::process_cpu_pct(&sys, p));
        let indexer_cpu = indexer_pid.map_or(0.0, |p| metrics::process_cpu_pct(&sys, p));
        let disk_mb = metrics::disk_usage_mb(&config::base_dir());
        let error_count = metrics::count_errors(&config::indexer_log());

        // Query block height if HTTP mode
        let block_height = if let Some(port) = api_port {
            query_block_height(&client, port).await
        } else {
            None
        };

        // Calculate blocks/min
        let blocks_per_min = if let (Some(cur), Some(prev)) = (block_height, prev_block_height) {
            let elapsed = prev_check_time.elapsed().as_secs_f64() / 60.0;
            if elapsed > 0.0 && cur > prev {
                (cur - prev) as f64 / elapsed
            } else {
                0.0
            }
        } else {
            0.0
        };

        if block_height.is_some() {
            prev_block_height = block_height;
            prev_check_time = std::time::Instant::now();
        }

        let elapsed = start.elapsed().as_secs();
        let time_str = format!("{}:{:02}", elapsed / 60, elapsed % 60);

        let height_str = block_height
            .map(|h| format!("{}", h))
            .unwrap_or_else(|| "-".into());

        println!(
            "{:>8} {:>8.1}MB {:>8.1}MB {:>7.1}% {:>7.1}% {:>9.1} {:>12} {:>8}",
            time_str,
            defra_rss,
            indexer_rss,
            defra_cpu,
            indexer_cpu,
            disk_mb,
            height_str,
            error_count
        );

        // Save metrics snapshot
        let snapshot = metrics::Snapshot {
            timestamp: chrono::Utc::now().to_rfc3339(),
            elapsed_secs: elapsed,
            defra_rss_mb: defra_rss,
            indexer_rss_mb: indexer_rss,
            defra_cpu_pct: defra_cpu,
            indexer_cpu_pct: indexer_cpu,
            disk_usage_mb: disk_mb,
            block_height,
            blocks_per_min,
            error_count,
        };

        // Append to metrics file
        if let Ok(json) = serde_json::to_string(&snapshot) {
            let metrics_path = config::metrics_file();
            let mut content = std::fs::read_to_string(&metrics_path).unwrap_or_default();
            content.push_str(&json);
            content.push('\n');
            let _ = std::fs::write(&metrics_path, content);
        }

        // Check if both processes are dead
        let defra_alive = defra_pid.is_some_and(process::is_alive);
        let indexer_alive = indexer_pid.is_some_and(process::is_alive);
        if !defra_alive && !indexer_alive && defra_pid.is_some() {
            println!("{}", "\nAll processes have exited.".yellow());
            break;
        }
    }

    Ok(())
}

async fn query_block_height(client: &reqwest::Client, port: u16) -> Option<u64> {
    let url = format!("http://127.0.0.1:{}/api/v0/graphql", port);
    let query =
        r#"{"query":"{ Ethereum__Mainnet__Block(limit:1, order:{number:DESC}) { number } }"}"#;

    let resp = client
        .post(&url)
        .header("Content-Type", "application/json")
        .body(query)
        .send()
        .await
        .ok()?;

    let json: serde_json::Value = resp.json().await.ok()?;
    json["data"]["Ethereum__Mainnet__Block"]
        .as_array()?
        .first()?["number"]
        .as_u64()
}
