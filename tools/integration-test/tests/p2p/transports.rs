use std::net::{TcpListener, UdpSocket};
use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::Duration;

use integration_test::node::{
    start_node, GoNode, KeyringBackend, NodeConfig, RunningNode, RustNode,
};
use integration_test::{poll_until, DefraClient, NodeKind};

const NODE_COUNT: usize = 2;
const READY_TIMEOUT: Duration = Duration::from_secs(30);
const P2P_TIMEOUT: Duration = Duration::from_secs(15);
const POLL_INTERVAL: Duration = Duration::from_millis(100);

static RUST_BUILD_DONE: OnceLock<()> = OnceLock::new();

struct TransportNodePorts {
    http: u16,
    tcp: u16,
    quic: u16,
    ws: u16,
    tcp_guards: Option<Vec<TcpListener>>,
    udp_guard: Option<UdpSocket>,
}

impl TransportNodePorts {
    fn allocate() -> std::io::Result<Self> {
        let http_guard = TcpListener::bind("127.0.0.1:0")?;
        let tcp_guard = TcpListener::bind("127.0.0.1:0")?;
        let ws_guard = TcpListener::bind("127.0.0.1:0")?;
        let udp_guard = UdpSocket::bind("127.0.0.1:0")?;

        Ok(Self {
            http: http_guard.local_addr()?.port(),
            tcp: tcp_guard.local_addr()?.port(),
            quic: udp_guard.local_addr()?.port(),
            ws: ws_guard.local_addr()?.port(),
            tcp_guards: Some(vec![http_guard, tcp_guard, ws_guard]),
            udp_guard: Some(udp_guard),
        })
    }

    fn p2p_addr_arg(&self) -> String {
        format!(
            "/ip4/127.0.0.1/tcp/{},/ip4/127.0.0.1/udp/{}/quic-v1,/ip4/127.0.0.1/tcp/{}/ws",
            self.tcp, self.quic, self.ws
        )
    }

    fn quic_p2p_addr_arg(&self) -> String {
        format!("/ip4/127.0.0.1/udp/{}/quic-v1", self.quic)
    }

    fn release(&mut self) {
        self.tcp_guards = None;
        self.udp_guard = None;
    }
}

fn build_rust_binary() -> PathBuf {
    RUST_BUILD_DONE.get_or_init(|| {
        RustNode::build().expect("failed to build Rust defra binary");
    });
    RustNode::workspace_binary_path()
}

fn client(node: &RunningNode) -> DefraClient {
    client_with_kind(node, NodeKind::Rust)
}

fn client_with_kind(node: &RunningNode, kind: NodeKind) -> DefraClient {
    DefraClient::new(node.binary_path.clone(), node.http_addr.clone(), kind)
}

fn full_p2p_addrs(client: &DefraClient) -> Vec<String> {
    client
        .p2p_info()
        .expect("p2p_info")
        .as_array()
        .expect("p2p_info not array")
        .iter()
        .map(|value| {
            value
                .as_str()
                .expect("P2P address must be a string")
                .to_string()
        })
        .collect()
}

fn peer_id_from_addr(addr: &str) -> String {
    addr.rsplit_once("/p2p/")
        .map_or(addr, |(_, peer_id)| peer_id)
        .to_string()
}

fn active_peer_addrs(client: &DefraClient) -> Vec<String> {
    client
        .p2p_active_peers()
        .expect("p2p_active_peers")
        .as_array()
        .expect("active_peers not array")
        .iter()
        .map(|value| {
            value
                .as_str()
                .expect("active peer must be a string")
                .to_string()
        })
        .collect()
}

fn is_websocket_addr(addr: &str) -> bool {
    addr.ends_with("/ws") || addr.contains("/ws/")
}

async fn wait_for_configured_transports(client: &DefraClient) -> Vec<String> {
    poll_until(
        || {
            let addrs = full_p2p_addrs(client);
            addrs
                .iter()
                .any(|addr| addr.contains("/tcp/") && !addr.contains("/ws"))
                && addrs
                    .iter()
                    .any(|addr| addr.contains("/udp/") && addr.contains("/quic-v1"))
                && addrs.iter().any(|addr| is_websocket_addr(addr))
        },
        P2P_TIMEOUT,
        POLL_INTERVAL,
        "node did not expose TCP, QUIC, and WebSocket P2P listen addresses",
    )
    .await;

    full_p2p_addrs(client)
}

async fn wait_for_quic_transport(client: &DefraClient) -> Vec<String> {
    poll_until(
        || {
            full_p2p_addrs(client)
                .iter()
                .any(|addr| addr.contains("/udp/") && addr.contains("/quic-v1"))
        },
        P2P_TIMEOUT,
        POLL_INTERVAL,
        "node did not expose a QUIC P2P listen address",
    )
    .await;

    full_p2p_addrs(client)
}

async fn start_configured_transport_cluster() -> (tempfile::TempDir, Vec<RunningNode>) {
    let binary_path = build_rust_binary();
    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let mut ports = (0..NODE_COUNT)
        .map(|_| TransportNodePorts::allocate().expect("allocate node ports"))
        .collect::<Vec<_>>();
    let mut nodes = Vec::with_capacity(NODE_COUNT);

    for (index, port) in ports.iter_mut().enumerate() {
        let name = format!("rust-transport-{index}");
        let rootdir = temp_dir.path().join(format!("node-{index}/data"));
        let log_dir = temp_dir.path().join(format!("node-{index}/logs"));
        let mut config = NodeConfig::new(
            name.clone(),
            rootdir,
            log_dir,
            format!("127.0.0.1:{}", port.http),
        );
        config.p2p_enabled = true;
        config.p2p_addr = Some(port.p2p_addr_arg());
        config.keyring = KeyringBackend::None;

        let node = RustNode::from_binary(binary_path.clone());
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

async fn start_rust_go_quic_cluster() -> (tempfile::TempDir, RunningNode, RunningNode) {
    let rust_binary_path = build_rust_binary();
    GoNode::check_available().expect("Go defradb binary not available");

    let temp_dir = tempfile::tempdir().expect("create temp dir");
    let mut rust_ports = TransportNodePorts::allocate().expect("allocate Rust node ports");
    let mut go_ports = TransportNodePorts::allocate().expect("allocate Go node ports");

    let mut rust_config = NodeConfig::new(
        "rust-quic-cross",
        temp_dir.path().join("rust/data"),
        temp_dir.path().join("rust/logs"),
        format!("127.0.0.1:{}", rust_ports.http),
    );
    rust_config.p2p_enabled = true;
    rust_config.p2p_addr = Some(rust_ports.quic_p2p_addr_arg());
    rust_config.keyring = KeyringBackend::None;

    let rust_node = RustNode::from_binary(rust_binary_path);
    rust_ports.release();
    let rust_running = start_node(&rust_node, rust_config, READY_TIMEOUT)
        .await
        .expect("failed to start Rust QUIC node");

    let mut go_config = NodeConfig::new(
        "go-quic-cross",
        temp_dir.path().join("go/data"),
        temp_dir.path().join("go/logs"),
        format!("127.0.0.1:{}", go_ports.http),
    );
    go_config.p2p_enabled = true;
    go_config.p2p_addr = Some(go_ports.quic_p2p_addr_arg());
    go_config.keyring = KeyringBackend::None;

    let go_node = GoNode::from_path();
    go_ports.release();
    let go_running = start_node(&go_node, go_config, READY_TIMEOUT)
        .await
        .expect("failed to start Go QUIC node");

    for node in [&rust_running, &go_running] {
        node.log_tracker
            .wait_for_pattern("p2p_listening", P2P_TIMEOUT)
            .await
            .unwrap_or_else(|error| panic!("{} P2P listener did not start: {error}", node.name));
    }

    (temp_dir, rust_running, go_running)
}

#[tokio::test]
async fn rust_rust_connects_over_quic_when_quic_multiaddr_is_dialed() {
    let (_temp_dir, nodes) = start_configured_transport_cluster().await;
    let clients = nodes.iter().map(client).collect::<Vec<_>>();

    let node0_addrs = wait_for_configured_transports(&clients[0]).await;
    let node1_addrs = wait_for_configured_transports(&clients[1]).await;

    assert!(
        node0_addrs
            .iter()
            .any(|addr| addr.contains("/udp/") && addr.contains("/quic-v1")),
        "node0 should expose a QUIC listen address: {node0_addrs:?}"
    );

    let node1_quic_addr = node1_addrs
        .iter()
        .find(|addr| addr.contains("/udp/") && addr.contains("/quic-v1"))
        .expect("node1 should expose a QUIC listen address")
        .to_string();
    let node1_peer_id = peer_id_from_addr(&node1_quic_addr);

    clients[0]
        .p2p_connect(&[&node1_quic_addr])
        .expect("node0 failed to connect to node1 over QUIC");

    poll_until(
        || {
            active_peer_addrs(&clients[0])
                .iter()
                .any(|addr| peer_id_from_addr(addr) == node1_peer_id)
        },
        P2P_TIMEOUT,
        POLL_INTERVAL,
        "node1 did not appear as an active peer after dialing its QUIC address",
    )
    .await;
}

#[tokio::test]
async fn rust_rust_connects_over_websocket_when_websocket_multiaddr_is_dialed() {
    let (_temp_dir, nodes) = start_configured_transport_cluster().await;
    let clients = nodes.iter().map(client).collect::<Vec<_>>();

    let node0_addrs = wait_for_configured_transports(&clients[0]).await;
    let node1_addrs = wait_for_configured_transports(&clients[1]).await;

    assert!(
        node0_addrs.iter().any(|addr| is_websocket_addr(addr)),
        "node0 should expose a WebSocket listen address: {node0_addrs:?}"
    );

    let node1_ws_addr = node1_addrs
        .iter()
        .find(|addr| is_websocket_addr(addr))
        .expect("node1 should expose a WebSocket listen address")
        .to_string();
    let node1_peer_id = peer_id_from_addr(&node1_ws_addr);

    clients[0]
        .p2p_connect(&[&node1_ws_addr])
        .expect("node0 failed to connect to node1 over WebSocket");

    poll_until(
        || {
            active_peer_addrs(&clients[0])
                .iter()
                .any(|addr| peer_id_from_addr(addr) == node1_peer_id)
        },
        P2P_TIMEOUT,
        POLL_INTERVAL,
        "node1 did not appear as an active peer after dialing its WebSocket address",
    )
    .await;
}

#[tokio::test]
async fn rust_go_connects_over_quic_when_quic_multiaddr_is_dialed() {
    let (_temp_dir, rust_node, go_node) = start_rust_go_quic_cluster().await;
    let rust_client = client(&rust_node);
    let go_client = client_with_kind(&go_node, NodeKind::Go);

    wait_for_quic_transport(&rust_client).await;

    let go_quic_addr = wait_for_quic_transport(&go_client)
        .await
        .into_iter()
        .find(|addr| addr.contains("/udp/") && addr.contains("/quic-v1"))
        .expect("Go node should expose a QUIC listen address");
    let go_peer_id = peer_id_from_addr(&go_quic_addr);

    rust_client
        .p2p_connect(&[&go_quic_addr])
        .expect("Rust node failed to connect to Go node over QUIC");

    poll_until(
        || {
            active_peer_addrs(&rust_client)
                .iter()
                .any(|addr| peer_id_from_addr(addr) == go_peer_id)
        },
        P2P_TIMEOUT,
        POLL_INTERVAL,
        "Go node did not appear as an active peer after dialing its QUIC address",
    )
    .await;
}
