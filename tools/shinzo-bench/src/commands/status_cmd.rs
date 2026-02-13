use colored::Colorize;

use crate::config;
use crate::metrics;
use crate::process;

pub async fn status() -> anyhow::Result<()> {
    let ports = process::load_ports(&config::ports_file());
    let (defra_pid, indexer_pid) = process::load_pids(&config::pids_file());
    let base_dir = config::base_dir();

    println!("{}", "=== Shinzo Bench Status ===".cyan().bold());
    println!();

    // Mode
    let is_ffi = ports.get("RUST_FFI").is_some_and(|v| v == "1");
    let store = ports
        .get("STORE")
        .cloned()
        .unwrap_or_else(|| "unknown".into());
    println!("  Mode:    {}", if is_ffi { "Rust FFI" } else { "HTTP" });
    println!("  Store:   {}", store);

    // Ports
    if let Some(port) = ports.get("API_PORT") {
        println!("  API:     http://127.0.0.1:{}/api/v0/graphql", port);
    }

    println!();

    // Processes
    println!("{}", "Processes:".bold());
    print_process_status("  defra", defra_pid);
    print_process_status("  indexer", indexer_pid);

    println!();

    // Disk
    if base_dir.exists() {
        let disk_mb = metrics::disk_usage_mb(&base_dir);
        println!("{}", "Disk:".bold());
        println!("  Total:   {:.1} MB", disk_mb);
    }

    // Errors
    let error_count = metrics::count_errors(&config::indexer_log());
    if error_count > 0 {
        println!(
            "  Errors:  {} (in indexer.log)",
            format!("{}", error_count).red()
        );
    }

    Ok(())
}

fn print_process_status(label: &str, pid: Option<u32>) {
    match pid {
        Some(pid) if process::is_alive(pid) => {
            let mut sys = sysinfo::System::new();
            sys.refresh_processes(
                sysinfo::ProcessesToUpdate::Some(&[sysinfo::Pid::from_u32(pid)]),
                true,
            );
            let rss = metrics::process_rss_mb(&sys, pid);
            println!(
                "{}: {} (PID {}, RSS {:.1} MB)",
                label,
                "running".green(),
                pid,
                rss
            );
        }
        Some(pid) => {
            println!("{}: {} (PID {})", label, "dead".red(), pid);
        }
        None => {
            println!("{}: {}", label, "not started".yellow());
        }
    }
}
