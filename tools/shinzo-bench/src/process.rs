use std::path::Path;
use tokio::process::Command;

/// Find a free TCP port.
pub async fn find_free_port() -> anyhow::Result<u16> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

/// Read saved PIDs from the pids file.
pub fn load_pids(pids_file: &Path) -> (Option<u32>, Option<u32>) {
    let content = match std::fs::read_to_string(pids_file) {
        Ok(c) => c,
        Err(_) => return (None, None),
    };
    let mut defra_pid = None;
    let mut indexer_pid = None;
    for line in content.lines() {
        if let Some(val) = line.strip_prefix("DEFRA_PID=") {
            defra_pid = val.trim().parse().ok();
        }
        if let Some(val) = line.strip_prefix("INDEXER_PID=") {
            indexer_pid = val.trim().parse().ok();
        }
    }
    (defra_pid, indexer_pid)
}

/// Read saved ports from the ports file.
pub fn load_ports(ports_file: &Path) -> std::collections::HashMap<String, String> {
    let content = match std::fs::read_to_string(ports_file) {
        Ok(c) => c,
        Err(_) => return std::collections::HashMap::new(),
    };
    let mut map = std::collections::HashMap::new();
    for line in content.lines() {
        if let Some((key, val)) = line.split_once('=') {
            map.insert(key.trim().to_string(), val.trim().to_string());
        }
    }
    map
}

/// Check if a process with the given PID is alive.
pub fn is_alive(pid: u32) -> bool {
    use sysinfo::{Pid, System};
    let mut sys = System::new();
    sys.refresh_processes(
        sysinfo::ProcessesToUpdate::Some(&[Pid::from_u32(pid)]),
        true,
    );
    sys.process(Pid::from_u32(pid)).is_some()
}

/// Kill a process by PID (SIGTERM).
pub fn kill_process(pid: u32) -> bool {
    unsafe { libc::kill(pid as i32, libc::SIGTERM) == 0 }
}

/// Build the release binary.
pub async fn cargo_build_release(features: &[&str]) -> anyhow::Result<()> {
    let mut cmd = Command::new("cargo");
    cmd.arg("build").arg("--release");
    if !features.is_empty() {
        cmd.arg("--features").arg(features.join(","));
    }
    let status = cmd.status().await?;
    if !status.success() {
        anyhow::bail!("cargo build --release failed");
    }
    Ok(())
}
