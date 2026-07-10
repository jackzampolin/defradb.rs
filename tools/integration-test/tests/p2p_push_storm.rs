//! #1103: deterministic outbound push-storm reproduction and profiling harness.
//!
//! Run the default 1 hub + 8 peer workload with:
//!
//! ```text
//! cargo test -p integration-test --test p2p_push_storm -- --ignored --nocapture
//! ```
//!
//! `DEFRA_PUSH_STORM_PEERS`, `DEFRA_PUSH_STORM_DOCS`,
//! `DEFRA_PUSH_STORM_UPDATES`, and `DEFRA_PUSH_STORM_STALLED_PEERS` tune the
//! fleet shape. `DEFRA_PUSH_STORM_MIN_CPU_RATIO=10` turns the current-main CPU
//! observation into a red regression assertion; after #1102,
//! `DEFRA_PUSH_STORM_MAX_CPU_RATIO=3` is the bounded-CPU acceptance assertion.
//! `DEFRA_PUSH_STORM_REQUIRE_CONVERGENCE=1` gates every healthy peer on the
//! latest heads once #1101 makes successful selective CAR replies possible.
//!
//! On macOS, capture a symbolized sample from the optimized hub with:
//!
//! ```text
//! cargo build --profile profile -p cli && DEFRA_RUST_BINARY="$PWD/target/profile/defra" DEFRA_PUSH_STORM_HUB_STORE=rocksdb DEFRA_PUSH_STORM_SAMPLE_OUTPUT="$PWD/push-storm.sample.txt" cargo test -p integration-test --test p2p_push_storm -- --ignored --nocapture
//! ```
//!
//! The sample targets the hub child process rather than the test runner. Turn
//! it into an SVG with `inferno-collapse-sample < push-storm.sample.txt |
//! inferno-flamegraph > push-storm.svg`.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use integration_test::{DefraClient, TestCluster};

const SCHEMA: &str = "type StormDoc { lifecycle_state: String  status: String }";
const DEFAULT_PEERS: usize = 8;
const DEFAULT_DOCS: usize = 13;
const DEFAULT_UPDATES: usize = 16;
const DEFAULT_STALLED_PEERS: usize = 2;
const DEFAULT_IDLE_SECS: u64 = 3;
const DEFAULT_OBSERVE_SECS: u64 = 60;
const DEFAULT_SAMPLE_SECS: u64 = 10;
const DEFAULT_CONVERGENCE_SECS: u64 = 15;

#[derive(Debug)]
struct StormConfig {
    peers: usize,
    docs: usize,
    updates: usize,
    stalled_peers: usize,
    idle: Duration,
    observe: Duration,
}

impl StormConfig {
    fn from_env() -> Self {
        let peers = env_usize("DEFRA_PUSH_STORM_PEERS", DEFAULT_PEERS);
        let stalled_peers = env_usize(
            "DEFRA_PUSH_STORM_STALLED_PEERS",
            DEFAULT_STALLED_PEERS.min(peers.saturating_sub(1)),
        );
        assert!(peers > 1, "the storm needs at least two peers");
        assert!(
            stalled_peers > 0 && stalled_peers < peers,
            "the storm needs at least one stalled and one healthy peer"
        );

        Self {
            peers,
            docs: env_usize("DEFRA_PUSH_STORM_DOCS", DEFAULT_DOCS),
            updates: env_usize("DEFRA_PUSH_STORM_UPDATES", DEFAULT_UPDATES),
            stalled_peers,
            idle: Duration::from_secs(env_u64("DEFRA_PUSH_STORM_IDLE_SECS", DEFAULT_IDLE_SECS)),
            observe: Duration::from_secs(env_u64(
                "DEFRA_PUSH_STORM_OBSERVE_SECS",
                DEFAULT_OBSERVE_SECS,
            )),
        }
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .map(|value| value.parse().unwrap_or_else(|_| panic!("invalid {name}")))
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .map(|value| value.parse().unwrap_or_else(|_| panic!("invalid {name}")))
        .unwrap_or(default)
}

fn env_f64(name: &str) -> Option<f64> {
    std::env::var(name)
        .ok()
        .map(|value| value.parse().unwrap_or_else(|_| panic!("invalid {name}")))
}

fn signal(pid: u32, signal: &str) {
    let status = Command::new("kill")
        .arg(signal)
        .arg(pid.to_string())
        .status()
        .expect("spawn kill");
    assert!(status.success(), "kill {signal} {pid} failed");
}

struct StalledPeers(Vec<u32>);

impl Drop for StalledPeers {
    fn drop(&mut self) {
        for pid in &self.0 {
            signal(*pid, "-CONT");
        }
    }
}

async fn sync_status(api_url: &str) -> serde_json::Value {
    reqwest::get(format!("{api_url}/api/v0/p2p/sync/status"))
        .await
        .expect("sync status request")
        .json()
        .await
        .expect("sync status json")
}

fn hub_log(cluster: &TestCluster) -> PathBuf {
    cluster.nodes[0]
        .rootdir
        .parent()
        .expect("hub rootdir has a parent")
        .join("logs/stdout.log")
}

fn log_occurrences(path: &Path, needle: &str) -> u64 {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .matches(needle)
        .count() as u64
}

#[cfg(target_os = "macos")]
fn process_cpu_seconds(pid: u32) -> Option<f64> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage_info_v2>::uninit();
    // SAFETY: `usage` points to writable storage for the exact flavor passed.
    let result = unsafe {
        libc::proc_pid_rusage(
            pid as libc::c_int,
            libc::RUSAGE_INFO_V2,
            usage.as_mut_ptr() as _,
        )
    };
    if result != 0 {
        return None;
    }
    // SAFETY: proc_pid_rusage returned success and initialized the structure.
    let usage = unsafe { usage.assume_init() };
    let mut timebase = mach2::mach_time::mach_timebase_info_data_t { numer: 0, denom: 0 };
    // SAFETY: timebase points to an initialized, writable Mach timebase structure.
    if unsafe { mach2::mach_time::mach_timebase_info(&mut timebase) } != 0 || timebase.denom == 0 {
        return None;
    }
    let ticks = (usage.ri_user_time + usage.ri_system_time) as f64;
    Some(ticks * f64::from(timebase.numer) / f64::from(timebase.denom) / 1_000_000_000.0)
}

#[cfg(target_os = "linux")]
fn process_cpu_seconds(pid: u32) -> Option<f64> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let (_, fields) = stat.rsplit_once(") ")?;
    let fields: Vec<&str> = fields.split_whitespace().collect();
    let ticks = fields.get(11)?.parse::<u64>().ok()? + fields.get(12)?.parse::<u64>().ok()?;
    // SAFETY: sysconf has no pointer preconditions and only reads this constant.
    let ticks_per_second = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    (ticks_per_second > 0).then_some(ticks as f64 / ticks_per_second as f64)
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn process_cpu_seconds(_pid: u32) -> Option<f64> {
    None
}

fn process_rss_kib(pid: u32) -> Option<u64> {
    let output = Command::new("ps")
        .args(["-p", &pid.to_string(), "-o", "rss="])
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().parse().ok())?
}

fn start_rss_monitor(pid: u32) -> (Arc<AtomicBool>, Arc<AtomicU64>, std::thread::JoinHandle<()>) {
    let running = Arc::new(AtomicBool::new(true));
    let peak = Arc::new(AtomicU64::new(process_rss_kib(pid).unwrap_or(0)));
    let thread_running = Arc::clone(&running);
    let thread_peak = Arc::clone(&peak);
    let handle = std::thread::spawn(move || {
        while thread_running.load(Ordering::Relaxed) {
            if let Some(rss) = process_rss_kib(pid) {
                thread_peak.fetch_max(rss, Ordering::Relaxed);
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    });
    (running, peak, handle)
}

fn start_symbolized_sample(pid: u32) -> Option<Child> {
    let output = std::env::var("DEFRA_PUSH_STORM_SAMPLE_OUTPUT").ok()?;
    let duration = env_u64("DEFRA_PUSH_STORM_SAMPLE_SECS", DEFAULT_SAMPLE_SECS);
    let child = Command::new("/usr/bin/sample")
        .args([
            &pid.to_string(),
            &duration.to_string(),
            "1",
            "-mayDie",
            "-fullPaths",
            "-file",
            &output,
        ])
        .stdout(Stdio::null())
        .spawn()
        .expect("start macOS sample profiler");
    std::thread::sleep(Duration::from_millis(250));
    Some(child)
}

async fn wait_for_backlog_idle(api_url: &str) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let status = sync_status(api_url).await;
        let backlog = &status["push_backlog"];
        if backlog["queued_items"].as_u64() == Some(0) && backlog["active_jobs"].as_u64() == Some(0)
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "initial document pushes did not drain: {backlog}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn wait_for_stalled_push(
    api_url: &str,
    stalled_peer_ids: &HashSet<String>,
) -> serde_json::Value {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let status = sync_status(api_url).await;
        let stalled_is_active =
            status["push_backlog"]["per_peer"]
                .as_array()
                .is_some_and(|peers| {
                    peers.iter().any(|peer| {
                        peer["active_jobs"].as_u64().unwrap_or(0) > 0
                            && peer["peer_id"]
                                .as_str()
                                .is_some_and(|id| stalled_peer_ids.contains(id))
                    })
                });
        if stalled_is_active {
            return status;
        }
        assert!(
            Instant::now() < deadline,
            "no selected stalled peer entered an active push slot: {}",
            status["push_backlog"]
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_timeout_log(path: &Path, baseline: u64, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        if log_occurrences(path, "PushLog to replicator timed out") > baseline {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "an active stalled push did not reach its transport timeout within {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn run_update_storm(client: DefraClient, doc_id: String, updates: usize) {
    for update in 0..updates {
        let mutation = format!(
            r#"mutation {{ update_StormDoc(docID: "{doc_id}", input: {{lifecycle_state: "lifecycle-{update}", status: "status-{update}"}}) {{ _docID }} }}"#
        );
        client.query(&mutation).expect("storm document update");
    }
}

async fn measure_healthy_convergence(
    cluster: &TestCluster,
    healthy_peers: &[usize],
    doc_ids: &[String],
    final_update: usize,
) -> usize {
    let expected_lifecycle = format!("lifecycle-{final_update}");
    let expected_status = format!("status-{final_update}");
    let mut incomplete: HashSet<usize> = healthy_peers.iter().copied().collect();
    let deadline = Instant::now()
        + Duration::from_secs(env_u64(
            "DEFRA_PUSH_STORM_CONVERGENCE_SECS",
            DEFAULT_CONVERGENCE_SECS,
        ));
    loop {
        incomplete.retain(|peer| {
            let result = cluster
                .client(*peer)
                .query("query { StormDoc { _docID lifecycle_state status } }")
                .ok();
            let converged: HashSet<&str> = result
                .as_ref()
                .and_then(|value| value["StormDoc"].as_array())
                .into_iter()
                .flatten()
                .filter(|row| {
                    row["lifecycle_state"].as_str() == Some(expected_lifecycle.as_str())
                        && row["status"].as_str() == Some(expected_status.as_str())
                })
                .filter_map(|row| row["_docID"].as_str())
                .collect();
            !doc_ids.iter().all(|id| converged.contains(id.as_str()))
        });
        if incomplete.is_empty() || Instant::now() >= deadline {
            return healthy_peers.len() - incomplete.len();
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

fn optional_counter(status: &serde_json::Value, pointers: &[&str]) -> Option<u64> {
    pointers
        .iter()
        .find_map(|pointer| status.pointer(pointer).and_then(serde_json::Value::as_u64))
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "manual stress/profile harness; takes at least one 30s push timeout"]
async fn outbound_push_storm_matches_fleet_shape() {
    let config = StormConfig::from_env();
    assert!(config.docs > 0, "DEFRA_PUSH_STORM_DOCS must be positive");
    assert!(
        config.updates > 0,
        "DEFRA_PUSH_STORM_UPDATES must be positive"
    );

    let mut cluster_builder = TestCluster::builder()
        .rust_nodes(1 + config.peers)
        .with_p2p();
    if let Ok(store) = std::env::var("DEFRA_PUSH_STORM_HUB_STORE") {
        cluster_builder = cluster_builder.with_node_store(0, store);
    }
    let cluster = cluster_builder.build().await.expect("cluster start");
    for node in 0..=config.peers {
        cluster
            .wait_for_log(node, "p2p_listening", Duration::from_secs(30))
            .await
            .unwrap_or_else(|error| panic!("node{node} P2P listener did not start: {error}"));
    }

    let hub = cluster.client(0);
    hub.schema_add(SCHEMA).expect("hub schema");
    let mut peers_by_dispatch_order = Vec::with_capacity(config.peers);
    for peer in 1..=config.peers {
        let client = cluster.client(peer);
        client.schema_add(SCHEMA).expect("peer schema");
        let info = client.p2p_info().expect("peer p2p info");
        let addr = info
            .as_array()
            .and_then(|values| values.first())
            .and_then(|value| value.as_str())
            .expect("peer has no P2P address")
            .to_string();
        let peer_id = addr
            .rsplit('/')
            .next()
            .expect("peer address has no ID")
            .to_string();
        hub.p2p_connect(&[&addr]).expect("connect hub to peer");
        hub.p2p_replicator_set(&["StormDoc"], &addr)
            .expect("set hub to peer replicator");
        peers_by_dispatch_order.push((peer_id, peer));
    }
    peers_by_dispatch_order.sort_unstable_by(|left, right| left.0.cmp(&right.0));

    let mut doc_ids = Vec::with_capacity(config.docs);
    for doc in 0..config.docs {
        let mutation = format!(
            r#"mutation {{ add_StormDoc(input: {{lifecycle_state: "initial-{doc}", status: "initial-{doc}"}}) {{ _docID }} }}"#
        );
        let result = hub.query(&mutation).expect("create storm document");
        doc_ids.push(
            result["add_StormDoc"][0]["_docID"]
                .as_str()
                .expect("created document has no ID")
                .to_string(),
        );
    }

    let hub_api = cluster.api_url(0).to_string();
    wait_for_backlog_idle(&hub_api).await;
    let hub_pid = cluster.nodes[0].process.id().expect("hub pid");
    let hub_log = hub_log(&cluster);
    let baseline_status = sync_status(&hub_api).await;
    assert_eq!(
        baseline_status["push_backlog"]["worker_count"].as_u64(),
        Some(8),
        "the default-concurrency repro contract changed"
    );
    let baseline_deferrals = baseline_status["push_backlog"]["rejected_items_total"]
        .as_u64()
        .unwrap_or(0)
        + baseline_status["push_backlog"]["rejected_bytes_total"]
            .as_u64()
            .unwrap_or(0);
    let baseline_timeout_logs = log_occurrences(&hub_log, "PushLog to replicator timed out");
    let baseline_deferral_logs = log_occurrences(
        &hub_log,
        "Outbound push backlog full; deferring push to persisted retry",
    );

    let stalled_indices: HashSet<usize> = peers_by_dispatch_order
        .iter()
        .take(config.stalled_peers)
        .map(|(_, peer)| *peer)
        .collect();
    let stalled_peer_ids: HashSet<String> = peers_by_dispatch_order
        .iter()
        .take(config.stalled_peers)
        .map(|(peer_id, _)| peer_id.clone())
        .collect();
    let healthy_indices: Vec<usize> = (1..=config.peers)
        .filter(|peer| !stalled_indices.contains(peer))
        .collect();
    let stalled_pids: Vec<u32> = stalled_indices
        .iter()
        .map(|peer| cluster.nodes[*peer].process.id().expect("stalled peer pid"))
        .collect();
    for pid in &stalled_pids {
        signal(*pid, "-STOP");
    }
    let _stalled = StalledPeers(stalled_pids);

    let idle_started = Instant::now();
    let idle_cpu_started = process_cpu_seconds(hub_pid).expect("read initial hub CPU time");
    tokio::time::sleep(config.idle).await;
    let idle_cpu = process_cpu_seconds(hub_pid).expect("read idle hub CPU time") - idle_cpu_started;
    let idle_wall = idle_started.elapsed().as_secs_f64();
    let idle_cores = idle_cpu / idle_wall;

    let active_started = Instant::now();
    let active_cpu_started = process_cpu_seconds(hub_pid).expect("read active hub CPU time");
    let rss_started = process_rss_kib(hub_pid).unwrap_or(0);
    let (rss_running, peak_rss, rss_thread) = start_rss_monitor(hub_pid);
    let mut sample = start_symbolized_sample(hub_pid);

    std::thread::scope(|scope| {
        for doc_id in doc_ids.iter().cloned() {
            let client = cluster.client(0);
            scope.spawn(move || run_update_storm(client, doc_id, config.updates));
        }
    });

    wait_for_stalled_push(&hub_api, &stalled_peer_ids).await;
    wait_for_timeout_log(&hub_log, baseline_timeout_logs, config.observe).await;
    let active_cpu =
        process_cpu_seconds(hub_pid).expect("read final hub CPU time") - active_cpu_started;
    let active_wall = active_started.elapsed().as_secs_f64();
    let active_cores = active_cpu / active_wall;
    let cpu_ratio = active_cores / idle_cores.max(0.001);

    rss_running.store(false, Ordering::Relaxed);
    rss_thread.join().expect("RSS monitor panicked");
    if let Some(child) = sample.as_mut() {
        let status = child.wait().expect("wait for macOS sample profiler");
        assert!(status.success(), "macOS sample profiler failed: {status}");
    }

    let status = sync_status(&hub_api).await;
    let deferrals = status["push_backlog"]["rejected_items_total"]
        .as_u64()
        .unwrap_or(0)
        + status["push_backlog"]["rejected_bytes_total"]
            .as_u64()
            .unwrap_or(0)
        - baseline_deferrals;
    let timeout_logs =
        log_occurrences(&hub_log, "PushLog to replicator timed out") - baseline_timeout_logs;
    let deferral_logs = log_occurrences(
        &hub_log,
        "Outbound push backlog full; deferring push to persisted retry",
    ) - baseline_deferral_logs;

    assert!(
        timeout_logs > 0,
        "the stalled peers produced no PushLog timeout"
    );
    assert!(deferrals > 0, "the workload produced no backlog deferral");

    let converged_healthy_peers =
        measure_healthy_convergence(&cluster, &healthy_indices, &doc_ids, config.updates - 1).await;

    let result = serde_json::json!({
        "hub_pid": hub_pid,
        "peers": config.peers,
        "docs": config.docs,
        "update_cycles": config.updates,
        "stalled_peers": config.stalled_peers,
        "healthy_peers": healthy_indices.len(),
        "converged_healthy_peers": converged_healthy_peers,
        "idle_cpu_seconds": idle_cpu,
        "idle_wall_seconds": idle_wall,
        "idle_cpu_cores": idle_cores,
        "active_cpu_seconds": active_cpu,
        "active_wall_seconds": active_wall,
        "active_cpu_cores": active_cores,
        "active_to_idle_cpu_ratio": cpu_ratio,
        "rss_start_kib": rss_started,
        "rss_peak_kib": peak_rss.load(Ordering::Relaxed),
        "pushlog_timeouts": timeout_logs,
        "pushlog_deferrals": deferrals,
        "pushlog_deferral_logs": deferral_logs,
        "stale_head_retirements": optional_counter(&status, &[
            "/push_backlog/stale_head_retirements_total",
            "/stale_head_retirements_total",
        ]),
        "encode_cache_hits": optional_counter(&status, &[
            "/push_backlog/encode_cache_hits_total",
            "/encode_cache_hits_total",
        ]),
        "retry_attempts": optional_counter(&status, &[
            "/push_backlog/retry_attempts_total",
            "/retry_attempts_total",
        ]),
    });
    println!(
        "PUSH_STORM_RESULT {}",
        serde_json::to_string_pretty(&result).unwrap()
    );

    if let Some(minimum) = env_f64("DEFRA_PUSH_STORM_MIN_CPU_RATIO") {
        assert!(
            cpu_ratio >= minimum,
            "push-storm CPU ratio {cpu_ratio:.2} was below {minimum:.2}"
        );
    }
    if let Some(maximum) = env_f64("DEFRA_PUSH_STORM_MAX_CPU_RATIO") {
        assert!(
            cpu_ratio <= maximum,
            "push-storm CPU ratio {cpu_ratio:.2} exceeded {maximum:.2}"
        );
    }
    if std::env::var_os("DEFRA_PUSH_STORM_REQUIRE_CONVERGENCE").is_some() {
        assert_eq!(
            converged_healthy_peers,
            healthy_indices.len(),
            "not every healthy peer converged to the latest heads"
        );
    }
}
