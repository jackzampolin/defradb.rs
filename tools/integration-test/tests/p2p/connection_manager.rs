use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::Duration;

use integration_test::node::{
    start_node, DefraNode, KeyringBackend, NodeConfig, RunningNode, RustNode,
};
use integration_test::ports::allocate_node_ports;
use integration_test::{poll_until, DefraClient, NodeKind};

const NODE_COUNT: usize = 5;
const LOW_WATER: u32 = 2;
const HIGH_WATER: u32 = 3;
const GRACE_PERIOD_MS: u64 = 200;
const READY_TIMEOUT: Duration = Duration::from_secs(30);
const P2P_TIMEOUT: Duration = Duration::from_secs(15);
const POLL_INTERVAL: Duration = Duration::from_millis(100);

static RUST_BUILD_DONE: OnceLock<()> = OnceLock::new();

struct RustNodeWithStartArgs {
    binary_path: PathBuf,
    extra_start_args: Vec<String>,
}

impl RustNodeWithStartArgs {
    fn new(binary_path: PathBuf, extra_start_args: Vec<String>) -> Self {
        Self {
            binary_path,
            extra_start_args,
        }
    }
}

impl DefraNode for RustNodeWithStartArgs {
    fn kind(&self) -> NodeKind {
        NodeKind::Rust
    }

    fn command_parts(&self, config: &NodeConfig) -> (PathBuf, Vec<String>, Vec<(String, String)>) {
        let node = RustNode::from_binary(self.binary_path.clone());
        let (program, mut args, envs) = node.command_parts(config);
        args.extend(self.extra_start_args.clone());
        (program, args, envs)
    }

    fn binary_path(&self) -> &Path {
        &self.binary_path
    }
}

fn build_rust_binary() -> PathBuf {
    RUST_BUILD_DONE.get_or_init(|| {
        RustNode::build().expect("failed to build Rust defra binary");
    });
    RustNode::workspace_binary_path()
}

fn client(node: &RunningNode) -> DefraClient {
    DefraClient::new(
        node.binary_path.clone(),
        node.http_addr.clone(),
        NodeKind::Rust,
    )
}

fn first_p2p_addr(client: &DefraClient) -> String {
    client
        .p2p_info()
        .expect("p2p_info")
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|value| value.as_str())
        .expect("node has no P2P address")
        .to_string()
}

fn peer_id_from_addr(addr: &str) -> String {
    addr.rsplit_once("/p2p/")
        .map_or(addr, |(_, peer_id)| peer_id)
        .to_string()
}

fn active_peer_ids(client: &DefraClient) -> Vec<String> {
    client
        .p2p_active_peers()
        .expect("p2p_active_peers")
        .as_array()
        .expect("active_peers not array")
        .iter()
        .map(|value| peer_id_from_addr(value.as_str().expect("active peer must be a string")))
        .collect()
}

async fn wait_for_active_peer(client: &DefraClient, peer_id: &str) {
    poll_until(
        || active_peer_ids(client).iter().any(|id| id == peer_id),
        P2P_TIMEOUT,
        POLL_INTERVAL,
        "peer did not appear in active_peers",
    )
    .await;
}

async fn start_pruning_cluster() -> (tempfile::TempDir, Vec<RunningNode>) {
    let binary_path = build_rust_binary();
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let mut ports = allocate_node_ports(NODE_COUNT).expect("allocate node ports");
    let mut nodes = Vec::with_capacity(NODE_COUNT);

    // Node 0 is the constrained target; nodes 1..=4 are peer dialers.
    for (index, port) in ports.iter_mut().enumerate() {
        let name = format!("rust-connmgr-{index}");
        let rootdir = temp_dir.path().join(format!("node-{index}/data"));
        let log_dir = temp_dir.path().join(format!("node-{index}/logs"));
        let mut config = NodeConfig::new(
            name.clone(),
            rootdir,
            log_dir,
            format!("127.0.0.1:{}", port.http),
        );
        config.p2p_enabled = true;
        config.p2p_addr = Some(format!("/ip4/127.0.0.1/tcp/{}", port.p2p));
        config.keyring = KeyringBackend::None;

        let extra_start_args = if index == 0 {
            vec![
                "--connection-manager-low-water".to_string(),
                LOW_WATER.to_string(),
                "--connection-manager-high-water".to_string(),
                HIGH_WATER.to_string(),
                "--connection-manager-grace-period-ms".to_string(),
                GRACE_PERIOD_MS.to_string(),
            ]
        } else {
            Vec::new()
        };

        let node = RustNodeWithStartArgs::new(binary_path.clone(), extra_start_args);
        port.release();
        let running = start_node(&node, config, READY_TIMEOUT)
            .await
            .unwrap_or_else(|error| panic!("failed to start {name}: {error}"));
        nodes.push(running);
    }

    for node in &nodes {
        node.log_tracker
            .wait_for_pattern("p2p_listening", P2P_TIMEOUT)
            .await
            .unwrap_or_else(|error| panic!("{} P2P listener did not start: {error}", node.name));
    }

    (temp_dir, nodes)
}

#[tokio::test]
async fn rust_connection_manager_prunes_to_low_water() {
    let (_temp_dir, nodes) = start_pruning_cluster().await;
    let clients = nodes.iter().map(client).collect::<Vec<_>>();
    let target = &clients[0];
    let target_addr = first_p2p_addr(target);
    let peer_addrs = clients[1..].iter().map(first_p2p_addr).collect::<Vec<_>>();
    let peer_ids = peer_addrs
        .iter()
        .map(|addr| peer_id_from_addr(addr))
        .collect::<Vec<_>>();

    for (index, (peer_client, peer_id)) in clients[1..4].iter().zip(peer_ids.iter()).enumerate() {
        peer_client
            .p2p_connect(&[&target_addr])
            .unwrap_or_else(|error| panic!("peer {} failed to connect: {error}", index + 1));
        wait_for_active_peer(target, peer_id).await;
        tokio::time::sleep(Duration::from_millis(GRACE_PERIOD_MS + 75)).await;
    }

    let active_before_prune = active_peer_ids(target);
    assert_eq!(
        active_before_prune.len(),
        HIGH_WATER as usize,
        "expected target to have exactly the high-water number of peers before pruning"
    );
    assert!(
        peer_ids
            .iter()
            .take(3)
            .all(|peer_id| active_before_prune.contains(peer_id)),
        "expected first three peers to be active before pruning: {active_before_prune:?}"
    );

    clients[4]
        .p2p_connect(&[&target_addr])
        .expect("fourth peer failed to connect");
    wait_for_active_peer(target, &peer_ids[3]).await;

    // The endpoint reports unique peers while watermarks count physical connections.
    // Exact oldest-first selection is covered by the connection manager unit tests.
    poll_until(
        || active_peer_ids(target).len() <= LOW_WATER as usize,
        Duration::from_secs(10),
        POLL_INTERVAL,
        "connection manager did not prune to low water",
    )
    .await;

    let active_after_prune = active_peer_ids(target);
    assert!(
        active_after_prune.len() <= LOW_WATER as usize,
        "expected no more than {LOW_WATER} peers after pruning: {active_after_prune:?}"
    );
}
