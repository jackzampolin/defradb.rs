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
//! CPU gates use the update-storm window and automatically extend the idle
//! baseline to 10 seconds; the later stalled-transport window is reported
//! separately so its timeout wait does not dilute the gate.
//! The manual driver uses blocking client subprocesses across document
//! threads. Run CPU-gated comparisons on an otherwise-idle host so driver
//! contention does not perturb the hub-PID-only measurements.
//! `DEFRA_PUSH_STORM_REQUIRE_CONVERGENCE=1` gates every healthy peer on the
//! latest heads once #1101 makes successful selective CAR replies possible.
//!
//! On macOS, capture a symbolized sample from the optimized hub with:
//!
//! ```text
//! cargo build --profile profile -p cli && DEFRA_RUST_BINARY="$PWD/target/profile/defra" DEFRA_PUSH_STORM_HUB_STORE=rocksdb DEFRA_PUSH_STORM_SAMPLE_OUTPUT="$PWD/push-storm.sample.txt" cargo test -p integration-test --test p2p_push_storm -- --ignored --nocapture
//! ```
//!
//! The sample targets the hub child process rather than the test runner. Keep
//! its compact stacks with `inferno-collapse-sample < push-storm.sample.txt >
//! push-storm.folded`; render them locally with `inferno-flamegraph <
//! push-storm.folded > push-storm.svg`.

use std::collections::HashSet;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::process::Command;
#[cfg(target_os = "macos")]
use std::process::{Child, Stdio};
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
const DEFAULT_GATED_IDLE_SECS: u64 = 10;
const DEFAULT_STORM_SETTLE_MILLIS: u64 = 1_000;
const DEFAULT_OBSERVE_SECS: u64 = 60;
#[cfg(target_os = "macos")]
const DEFAULT_SAMPLE_SECS: u64 = 10;
const DEFAULT_CONVERGENCE_SECS: u64 = 15;
const MIN_TRUSTED_IDLE_CORES: f64 = 0.001;
const PUSH_TIMEOUT_LOG: &str = "PushLog to replicator timed out";
const PUSH_UNAVAILABLE_LOG: &str =
    "PushLog to replicator failed because the connection became unavailable";
const PUSH_DEFERRAL_LOG: &str = "Outbound push backlog full; deferring push to persisted retry";

#[derive(Debug)]
struct StormConfig {
    peers: usize,
    docs: usize,
    updates: usize,
    stalled_peers: usize,
    idle: Duration,
    storm_settle: Duration,
    observe: Duration,
    minimum_cpu_ratio: Option<f64>,
    maximum_cpu_ratio: Option<f64>,
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
        let minimum_cpu_ratio = env_f64("DEFRA_PUSH_STORM_MIN_CPU_RATIO");
        let maximum_cpu_ratio = env_f64("DEFRA_PUSH_STORM_MAX_CPU_RATIO");
        let default_idle_secs = if minimum_cpu_ratio.is_some() || maximum_cpu_ratio.is_some() {
            DEFAULT_GATED_IDLE_SECS
        } else {
            DEFAULT_IDLE_SECS
        };

        Self {
            peers,
            docs: env_usize("DEFRA_PUSH_STORM_DOCS", DEFAULT_DOCS),
            updates: env_usize("DEFRA_PUSH_STORM_UPDATES", DEFAULT_UPDATES),
            stalled_peers,
            idle: Duration::from_secs(env_u64("DEFRA_PUSH_STORM_IDLE_SECS", default_idle_secs)),
            storm_settle: Duration::from_millis(env_u64(
                "DEFRA_PUSH_STORM_STORM_SETTLE_MS",
                DEFAULT_STORM_SETTLE_MILLIS,
            )),
            observe: Duration::from_secs(env_u64(
                "DEFRA_PUSH_STORM_OBSERVE_SECS",
                DEFAULT_OBSERVE_SECS,
            )),
            minimum_cpu_ratio,
            maximum_cpu_ratio,
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

fn send_signal(pid: u32, signal: &str) -> std::io::Result<std::process::ExitStatus> {
    Command::new("kill")
        .arg(signal)
        .arg(pid.to_string())
        .status()
}

fn require_signal(pid: u32, signal: &str) {
    let status = send_signal(pid, signal).expect("spawn kill");
    assert!(status.success(), "kill {signal} {pid} failed");
}

struct StalledPeers(Vec<u32>);

impl Drop for StalledPeers {
    fn drop(&mut self) {
        for pid in &self.0 {
            match send_signal(*pid, "-CONT") {
                Ok(status) if status.success() => {}
                Ok(status) => eprintln!("kill -CONT {pid} failed: {status}"),
                Err(error) => eprintln!("could not spawn kill -CONT {pid}: {error}"),
            }
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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct LogCounts {
    timeouts: u64,
    connection_unavailable: u64,
    deferrals: u64,
}

struct LogCursor {
    file: File,
    offset: u64,
    pending: Vec<u8>,
    counts: LogCounts,
}

impl LogCursor {
    fn open(path: &Path) -> Self {
        Self {
            file: File::open(path)
                .unwrap_or_else(|error| panic!("open hub log {}: {error}", path.display())),
            offset: 0,
            pending: Vec::new(),
            counts: LogCounts::default(),
        }
    }

    fn scan(&mut self) -> LogCounts {
        let mut appended = Vec::new();
        self.file
            .seek(SeekFrom::Start(self.offset))
            .expect("seek hub log");
        self.file.read_to_end(&mut appended).expect("read hub log");
        self.offset += appended.len() as u64;
        self.pending.extend(appended);
        let Some(complete_len) = self.pending.iter().rposition(|byte| *byte == b'\n') else {
            return self.counts;
        };
        let complete_len = complete_len + 1;
        let complete = String::from_utf8_lossy(&self.pending[..complete_len]);
        self.counts.timeouts += complete.matches(PUSH_TIMEOUT_LOG).count() as u64;
        self.counts.connection_unavailable += complete.matches(PUSH_UNAVAILABLE_LOG).count() as u64;
        self.counts.deferrals += complete.matches(PUSH_DEFERRAL_LOG).count() as u64;
        self.pending.drain(..complete_len);
        self.counts
    }
}

#[test]
fn log_cursor_counts_only_complete_appended_lines() {
    use std::io::Write;

    let mut log = tempfile::NamedTempFile::new().expect("create test log");
    let mut cursor = LogCursor::open(log.path());

    write!(log, "PushLog to replicator ").expect("write partial log");
    log.flush().expect("flush partial log");
    assert_eq!(cursor.scan(), LogCounts::default());

    writeln!(log, "timed out").expect("complete timeout log");
    writeln!(log, "{PUSH_DEFERRAL_LOG}").expect("write deferral log");
    log.flush().expect("flush complete logs");
    assert_eq!(
        cursor.scan(),
        LogCounts {
            timeouts: 1,
            connection_unavailable: 0,
            deferrals: 1,
        }
    );
    assert_eq!(
        cursor.scan(),
        LogCounts {
            timeouts: 1,
            connection_unavailable: 0,
            deferrals: 1,
        }
    );
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
    panic!("hub CPU measurement unsupported on this platform")
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

#[cfg(target_os = "macos")]
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

#[cfg(not(target_os = "macos"))]
fn start_symbolized_sample(_pid: u32) -> Option<()> {
    if std::env::var_os("DEFRA_PUSH_STORM_SAMPLE_OUTPUT").is_some() {
        eprintln!("DEFRA_PUSH_STORM_SAMPLE_OUTPUT is ignored outside macOS");
    }
    None
}

#[cfg(target_os = "macos")]
fn finish_symbolized_sample(mut sample: Option<Child>) {
    if let Some(child) = sample.as_mut() {
        let status = child.wait().expect("wait for macOS sample profiler");
        assert!(status.success(), "macOS sample profiler failed: {status}");
    }
}

#[cfg(not(target_os = "macos"))]
fn finish_symbolized_sample(_sample: Option<()>) {}

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

#[derive(Debug)]
struct StalledOutcome {
    timeout_observed: bool,
    connection_unavailable_observed: bool,
    stalled_peer_failure_observed: bool,
}

fn stalled_peer_failed(status: &serde_json::Value, stalled_peer_ids: &HashSet<String>) -> bool {
    status["push_backlog"]["per_peer"]
        .as_array()
        .is_some_and(|peers| {
            peers.iter().any(|peer| {
                peer["consecutive_failures"].as_u64().unwrap_or(0) > 0
                    && peer["peer_id"]
                        .as_str()
                        .is_some_and(|id| stalled_peer_ids.contains(id))
            })
        })
}

async fn wait_for_stalled_outcome(
    api_url: &str,
    stalled_peer_ids: &HashSet<String>,
    log_cursor: &mut LogCursor,
    baseline_logs: LogCounts,
    baseline_failed_jobs: u64,
    timeout: Duration,
) -> StalledOutcome {
    let deadline = Instant::now() + timeout;
    loop {
        let logs = log_cursor.scan();
        let status = sync_status(api_url).await;
        let timeout_observed = logs.timeouts > baseline_logs.timeouts;
        let connection_unavailable_observed =
            logs.connection_unavailable > baseline_logs.connection_unavailable;
        let stalled_peer_failure_observed = stalled_peer_failed(&status, stalled_peer_ids);
        let failed_jobs = status["push_backlog"]["failed_total"].as_u64().unwrap_or(0);
        if timeout_observed
            || (failed_jobs > baseline_failed_jobs
                && (stalled_peer_failure_observed || connection_unavailable_observed))
        {
            return StalledOutcome {
                timeout_observed,
                connection_unavailable_observed,
                stalled_peer_failure_observed,
            };
        }
        assert!(
            Instant::now() < deadline,
            "an active stalled push produced neither a timeout nor a failed-job outcome within \
             {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
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
#[ignore = "manual stress/profile harness; waits for a stalled transport outcome"]
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
    let mut peers_sorted_for_stall_selection = Vec::with_capacity(config.peers);
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
        peers_sorted_for_stall_selection.push((peer_id, peer));
    }
    peers_sorted_for_stall_selection.sort_unstable_by(|left, right| left.0.cmp(&right.0));

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
    let mut log_cursor = LogCursor::open(&hub_log);
    let baseline_logs = log_cursor.scan();
    let baseline_status = sync_status(&hub_api).await;
    assert_eq!(
        baseline_status["push_backlog"]["worker_count"].as_u64(),
        Some(8),
        "the default-concurrency repro contract changed"
    );
    let baseline_item_quota_deferrals = baseline_status["push_backlog"]["rejected_items_total"]
        .as_u64()
        .unwrap_or(0);
    let baseline_byte_quota_deferrals = baseline_status["push_backlog"]["rejected_bytes_total"]
        .as_u64()
        .unwrap_or(0);
    let baseline_failed_jobs = baseline_status["push_backlog"]["failed_total"]
        .as_u64()
        .unwrap_or(0);

    let stalled_indices: HashSet<usize> = peers_sorted_for_stall_selection
        .iter()
        .take(config.stalled_peers)
        .map(|(_, peer)| *peer)
        .collect();
    let stalled_peer_ids: HashSet<String> = peers_sorted_for_stall_selection
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
        require_signal(*pid, "-STOP");
    }
    let _stalled = StalledPeers(stalled_pids);

    let idle_started = Instant::now();
    let idle_cpu_started = process_cpu_seconds(hub_pid).expect("read initial hub CPU time");
    tokio::time::sleep(config.idle).await;
    let idle_cpu = process_cpu_seconds(hub_pid).expect("read idle hub CPU time") - idle_cpu_started;
    let idle_wall = idle_started.elapsed().as_secs_f64();
    let idle_cores = idle_cpu / idle_wall;

    let rss_started = process_rss_kib(hub_pid).unwrap_or(0);
    let (rss_running, peak_rss, rss_thread) = start_rss_monitor(hub_pid);
    let sample = start_symbolized_sample(hub_pid);
    let storm_started = Instant::now();
    let storm_cpu_started = process_cpu_seconds(hub_pid).expect("read storm hub CPU time");

    std::thread::scope(|scope| {
        for doc_id in doc_ids.iter().cloned() {
            let client = cluster.client(0);
            scope.spawn(move || run_update_storm(client, doc_id, config.updates));
        }
    });
    tokio::time::sleep(config.storm_settle).await;
    let storm_cpu =
        process_cpu_seconds(hub_pid).expect("read post-storm hub CPU time") - storm_cpu_started;
    let storm_wall = storm_started.elapsed().as_secs_f64();
    let storm_cores = storm_cpu / storm_wall;
    let storm_cpu_ratio = storm_cores / idle_cores.max(MIN_TRUSTED_IDLE_CORES);

    let stall_started = Instant::now();
    let stall_cpu_started = process_cpu_seconds(hub_pid).expect("read stalled hub CPU time");
    wait_for_stalled_push(&hub_api, &stalled_peer_ids).await;
    let stalled_outcome = wait_for_stalled_outcome(
        &hub_api,
        &stalled_peer_ids,
        &mut log_cursor,
        baseline_logs,
        baseline_failed_jobs,
        config.observe,
    )
    .await;
    let stall_cpu =
        process_cpu_seconds(hub_pid).expect("read post-stall hub CPU time") - stall_cpu_started;
    let stall_wall = stall_started.elapsed().as_secs_f64();
    let stall_cores = stall_cpu / stall_wall;
    let stall_cpu_ratio = stall_cores / idle_cores.max(MIN_TRUSTED_IDLE_CORES);

    rss_running.store(false, Ordering::Relaxed);
    rss_thread.join().expect("RSS monitor panicked");
    finish_symbolized_sample(sample);

    let status = sync_status(&hub_api).await;
    let item_quota_deferrals = status["push_backlog"]["rejected_items_total"]
        .as_u64()
        .unwrap_or(0)
        .saturating_sub(baseline_item_quota_deferrals);
    let byte_quota_deferrals = status["push_backlog"]["rejected_bytes_total"]
        .as_u64()
        .unwrap_or(0)
        .saturating_sub(baseline_byte_quota_deferrals);
    let deferrals = item_quota_deferrals + byte_quota_deferrals;
    let logs = log_cursor.scan();
    let timeout_logs = logs.timeouts.saturating_sub(baseline_logs.timeouts);
    let connection_unavailable_logs = logs
        .connection_unavailable
        .saturating_sub(baseline_logs.connection_unavailable);
    let deferral_logs = logs.deferrals.saturating_sub(baseline_logs.deferrals);
    let failed_jobs = status["push_backlog"]["failed_total"]
        .as_u64()
        .unwrap_or(0)
        .saturating_sub(baseline_failed_jobs);

    assert!(
        timeout_logs > 0
            || (failed_jobs > 0
                && (stalled_outcome.stalled_peer_failure_observed
                    || connection_unavailable_logs > 0)),
        "the stalled peers produced neither a timeout nor a failed-job outcome"
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
        "storm_cpu_seconds": storm_cpu,
        "storm_wall_seconds": storm_wall,
        "storm_cpu_cores": storm_cores,
        "storm_to_idle_cpu_ratio": storm_cpu_ratio,
        "stall_cpu_seconds": stall_cpu,
        "stall_wall_seconds": stall_wall,
        "stall_cpu_cores": stall_cores,
        "stall_to_idle_cpu_ratio": stall_cpu_ratio,
        "rss_start_kib": rss_started,
        "rss_peak_kib": peak_rss.load(Ordering::Relaxed),
        "pushlog_timeouts": timeout_logs,
        "pushlog_connection_unavailable": connection_unavailable_logs,
        "pushlog_failed_jobs": failed_jobs,
        "stalled_timeout_observed": stalled_outcome.timeout_observed,
        "stalled_connection_unavailable_observed": stalled_outcome.connection_unavailable_observed,
        "stalled_peer_failure_observed": stalled_outcome.stalled_peer_failure_observed,
        "pushlog_deferrals": deferrals,
        "deferrals_item_quota": item_quota_deferrals,
        "deferrals_byte_quota": byte_quota_deferrals,
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
        "receive_single_flight_suppressed": status["single_flight_suppressed"].as_u64(),
        "receive_already_merged_fast_path": status["already_merged_fast_path"].as_u64(),
        "receive_pending_dag_capacity_shed": status["pending_dag_capacity_shed"].as_u64(),
    });
    println!(
        "PUSH_STORM_RESULT {}",
        serde_json::to_string_pretty(&result).unwrap()
    );

    if config.minimum_cpu_ratio.is_some() || config.maximum_cpu_ratio.is_some() {
        assert!(
            idle_cores >= MIN_TRUSTED_IDLE_CORES,
            "idle CPU {idle_cores:.6} cores is below the trusted ratio floor; increase \
             DEFRA_PUSH_STORM_IDLE_SECS"
        );
    }
    if let Some(minimum) = config.minimum_cpu_ratio {
        assert!(
            storm_cpu_ratio >= minimum,
            "push-storm CPU ratio {storm_cpu_ratio:.2} was below {minimum:.2}"
        );
    }
    if let Some(maximum) = config.maximum_cpu_ratio {
        assert!(
            storm_cpu_ratio <= maximum,
            "push-storm CPU ratio {storm_cpu_ratio:.2} exceeded {maximum:.2}"
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
