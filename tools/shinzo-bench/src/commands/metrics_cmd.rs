use crate::config;
use crate::metrics;
use crate::process;

pub async fn metrics() -> anyhow::Result<()> {
    let (defra_pid, indexer_pid) = process::load_pids(&config::pids_file());
    let ports = process::load_ports(&config::ports_file());
    let api_port: Option<u16> = ports.get("API_PORT").and_then(|p| p.parse().ok());

    let mut sys = sysinfo::System::new_all();
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

    // Query block height if available
    let block_height = if let Some(port) = api_port {
        let client = reqwest::Client::new();
        let url = format!("http://127.0.0.1:{}/api/v0/graphql", port);
        let query =
            r#"{"query":"{ Ethereum__Mainnet__Block(limit:1, order:{number:DESC}) { number } }"}"#;
        if let Ok(resp) = client
            .post(&url)
            .header("Content-Type", "application/json")
            .body(query)
            .send()
            .await
        {
            if let Ok(json) = resp.json::<serde_json::Value>().await {
                json["data"]["Ethereum__Mainnet__Block"]
                    .as_array()
                    .and_then(|a| a.first())
                    .and_then(|b| b["number"].as_u64())
            } else {
                None
            }
        } else {
            None
        }
    } else {
        None
    };

    let snapshot = metrics::Snapshot {
        timestamp: chrono::Utc::now().to_rfc3339(),
        elapsed_secs: 0,
        defra_rss_mb: defra_rss,
        indexer_rss_mb: indexer_rss,
        defra_cpu_pct: defra_cpu,
        indexer_cpu_pct: indexer_cpu,
        disk_usage_mb: disk_mb,
        block_height,
        blocks_per_min: 0.0,
        error_count,
    };

    println!("{}", serde_json::to_string_pretty(&snapshot)?);

    Ok(())
}
