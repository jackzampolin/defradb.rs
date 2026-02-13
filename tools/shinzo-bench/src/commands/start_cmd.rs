use std::path::PathBuf;

use clap::Parser;
use colored::Colorize;
use tokio::process::Command;

use crate::config;
use crate::process;

#[derive(Parser)]
pub struct StartArgs {
    /// Storage backend: redb, fjall, rocksdb
    #[arg(long, env = "STORE", default_value = "fjall")]
    pub store: String,

    /// Use Rust FFI embedded mode
    #[arg(long, env = "RUST_FFI")]
    pub rust_ffi: bool,

    /// Concurrent blocks to index
    #[arg(long, env = "CONCURRENCY", default_value = "4")]
    pub concurrency: u32,

    /// Receipt workers
    #[arg(long, env = "RECEIPT_WORKERS", default_value = "4")]
    pub receipt_workers: u32,

    /// Ethereum start block height
    #[arg(long, env = "START_HEIGHT_OVERRIDE", default_value = "23700000")]
    pub start_height: u64,

    /// Stop after N blocks (0 = unlimited)
    #[arg(long, default_value = "0")]
    pub max_blocks: u64,

    /// Watchdog RSS limit in MB
    #[arg(long, env = "WATCHDOG_RSS_LIMIT_MB", default_value = "12000")]
    pub watchdog_rss_limit: u64,

    /// Watchdog disk limit in GB
    #[arg(long, env = "WATCHDOG_DISK_LIMIT_GB", default_value = "200")]
    pub watchdog_disk_limit: u64,

    /// Skip cargo build step
    #[arg(long)]
    pub skip_build: bool,
}

pub async fn start(args: StartArgs) -> anyhow::Result<()> {
    let base_dir = config::base_dir();

    // Check for existing processes
    let (defra_pid, indexer_pid) = process::load_pids(&config::pids_file());
    if let Some(pid) = defra_pid {
        if process::is_alive(pid) {
            anyhow::bail!(
                "defra is already running (PID {}). Run `shinzo-bench stop` first.",
                pid
            );
        }
    }
    if let Some(pid) = indexer_pid {
        if process::is_alive(pid) {
            anyhow::bail!(
                "indexer is already running (PID {}). Run `shinzo-bench stop` first.",
                pid
            );
        }
    }

    // Build release if needed
    if !args.skip_build {
        println!("{}", "Building release binary...".cyan());
        let features: Vec<&str> = match args.store.as_str() {
            "fjall" => vec!["fjall"],
            "rocksdb" => vec!["rocksdb"],
            _ => vec![],
        };
        if args.rust_ffi {
            let mut ffi_features = features.clone();
            ffi_features.push("ffi");
            // Build with features for the FFI crate
            let mut cmd = Command::new("cargo");
            cmd.arg("build").arg("--release").arg("-p").arg("ffi");
            if !features.is_empty() {
                cmd.arg("--features").arg(features.join(","));
            }
            let status = cmd.status().await?;
            if !status.success() {
                anyhow::bail!("cargo build --release -p ffi failed");
            }
        } else {
            process::cargo_build_release(&features).await?;
        }
    }

    // Create base directory
    tokio::fs::create_dir_all(&base_dir).await?;
    tokio::fs::create_dir_all(config::defra_data_dir()).await?;

    let mode = if args.rust_ffi {
        "Rust FFI embedded"
    } else {
        "HTTP"
    };

    println!("{}", "=== Shinzo Integration Test ===".green().bold());
    println!("  Mode:         {} ({} backend)", mode, args.store);
    println!(
        "  Concurrency:  {} blocks / {} workers",
        args.concurrency, args.receipt_workers
    );
    println!("  Start height: {}", args.start_height);
    println!("  Base dir:     {}", base_dir.display());
    if args.max_blocks > 0 {
        println!("  Max blocks:   {}", args.max_blocks);
    }
    println!();

    if args.rust_ffi {
        start_ffi_mode(&args, &base_dir).await
    } else {
        start_http_mode(&args, &base_dir).await
    }
}

async fn start_http_mode(args: &StartArgs, _base_dir: &PathBuf) -> anyhow::Result<()> {
    let defra_bin = config::defra_bin();
    if !defra_bin.exists() {
        anyhow::bail!(
            "defra binary not found at {}. Run `cargo build --release` first.",
            defra_bin.display()
        );
    }

    // Pick random ports
    let api_port = process::find_free_port().await?;
    let p2p_port = process::find_free_port().await?;

    // Save ports
    let ports_content = format!(
        "API_PORT={}\nP2P_PORT={}\nRUST_FFI=0\nSTORE={}\n",
        api_port, p2p_port, args.store
    );
    tokio::fs::write(config::ports_file(), &ports_content).await?;

    println!("Starting defra on port {}...", api_port);

    // Start defra
    let defra_log = std::fs::File::create(config::defra_log())?;
    let defra_child = Command::new(&defra_bin)
        .arg("start")
        .arg("--url")
        .arg(format!("127.0.0.1:{}", api_port))
        .arg("--store")
        .arg(&args.store)
        .arg("--rootdir")
        .arg(config::defra_data_dir())
        .arg("--no-p2p")
        .stdout(defra_log.try_clone()?)
        .stderr(defra_log)
        .spawn()?;

    let defra_pid = defra_child.id().unwrap_or(0);
    println!("  defra PID: {}", defra_pid);

    // Wait for defra to be ready
    println!("Waiting for defra to be ready...");
    let defra_url = format!("http://127.0.0.1:{}/api/v0/graphql", api_port);
    let client = reqwest::Client::new();
    for i in 0..30 {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        if client.get(&defra_url).send().await.is_ok() {
            println!("  defra ready after {}s", i + 1);
            break;
        }
        if i == 29 {
            anyhow::bail!("defra did not start within 30s");
        }
    }

    // Save PIDs
    let pids_content = format!("DEFRA_PID={}\nINDEXER_PID=\n", defra_pid);
    tokio::fs::write(config::pids_file(), &pids_content).await?;

    println!(
        "{}",
        "defra started successfully. Start the indexer manually.".green()
    );
    println!("  API: http://127.0.0.1:{}/api/v0/graphql", api_port);

    Ok(())
}

async fn start_ffi_mode(args: &StartArgs, _base_dir: &PathBuf) -> anyhow::Result<()> {
    // Save ports file for FFI mode
    let ports_content = format!("RUST_FFI=1\nSTORE={}\n", args.store);
    tokio::fs::write(config::ports_file(), &ports_content).await?;

    println!(
        "{}",
        "FFI mode: start the indexer with RUST_FFI=1 manually.".yellow()
    );
    println!("  The indexer will embed the Rust DefraDB directly.");
    println!("  Use `shinzo-bench monitor` to track progress.");

    Ok(())
}
