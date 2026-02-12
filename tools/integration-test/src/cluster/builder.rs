use std::time::Duration;

use anyhow::{Context, Result};
use reqwest::Client;

use crate::node::{DefraNode, GoNode, NodeConfig, RustNode};
use crate::observe::LogTracker;
use crate::ports::allocate_node_ports;
use crate::process::ManagedProcess;
use crate::run::TestRunDir;

use super::health::health_check_all;
use super::runtime::{RunningNode, TestCluster};

pub struct TestClusterBuilder {
    rust_nodes: usize,
    go_nodes: usize,
    p2p_enabled: bool,
    health_timeout: Duration,
    build_rust: bool,
}

impl Default for TestClusterBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl TestClusterBuilder {
    pub fn new() -> Self {
        Self {
            rust_nodes: 0,
            go_nodes: 0,
            p2p_enabled: false,
            health_timeout: Duration::from_secs(30),
            build_rust: true,
        }
    }

    pub fn rust_nodes(mut self, n: usize) -> Self {
        self.rust_nodes = n;
        self
    }

    pub fn go_nodes(mut self, n: usize) -> Self {
        self.go_nodes = n;
        self
    }

    pub fn with_p2p(mut self) -> Self {
        self.p2p_enabled = true;
        self
    }

    pub fn health_timeout(mut self, d: Duration) -> Self {
        self.health_timeout = d;
        self
    }

    pub fn skip_build(mut self) -> Self {
        self.build_rust = false;
        self
    }

    pub async fn build(self) -> Result<TestCluster> {
        let total = self.rust_nodes + self.go_nodes;
        anyhow::ensure!(total > 0, "must have at least one node");

        // Build Rust binary if needed
        if self.rust_nodes > 0 && self.build_rust {
            RustNode::build().context("failed to build Rust binary")?;
        }

        // Check Go binary if needed
        if self.go_nodes > 0 {
            GoNode::check_available().context("Go defradb binary not available")?;
        }

        // Allocate ports for all nodes
        let all_ports = allocate_node_ports(total)?;

        // Create run directory
        let run_dir = TestRunDir::new()?;

        let mut nodes = Vec::with_capacity(total);

        // Spawn Rust nodes
        for (i, ports) in all_ports.iter().enumerate().take(self.rust_nodes) {
            let name = format!("rust-{}", i);
            let node = RustNode::from_workspace();
            let running = spawn_node(
                &name,
                &node,
                ports.http,
                ports.p2p,
                self.p2p_enabled,
                &run_dir,
                self.health_timeout,
            )
            .await
            .with_context(|| format!("failed to start {}", name))?;
            nodes.push(running);
        }

        // Spawn Go nodes
        for (i, ports) in all_ports.iter().skip(self.rust_nodes).enumerate() {
            let name = format!("go-{}", i);
            let node = GoNode::from_path();
            let running = spawn_node(
                &name,
                &node,
                ports.http,
                ports.p2p,
                self.p2p_enabled,
                &run_dir,
                self.health_timeout,
            )
            .await
            .with_context(|| format!("failed to start {}", name))?;
            nodes.push(running);
        }

        // Confirm all nodes are healthy via HTTP
        let client = Client::new();
        let urls: Vec<String> = nodes.iter().map(|n| n.api_url.clone()).collect();
        health_check_all(&client, &urls, self.health_timeout)
            .await
            .context("health check failed")?;

        Ok(TestCluster::new(nodes, run_dir))
    }
}

async fn spawn_node(
    name: &str,
    node: &dyn DefraNode,
    http_port: u16,
    p2p_port: u16,
    p2p_enabled: bool,
    run_dir: &TestRunDir,
    ready_timeout: Duration,
) -> Result<RunningNode> {
    let node_dir = run_dir.node_dir(name)?;
    let log_dir = node_dir.join("logs");
    let rootdir = node_dir.join("data");
    std::fs::create_dir_all(&rootdir)?;

    let http_addr = format!("127.0.0.1:{}", http_port);
    let api_url = format!("http://{}", http_addr);

    let config = NodeConfig {
        name: name.to_string(),
        rootdir: rootdir.clone(),
        log_dir: log_dir.clone(),
        http_addr: http_addr.clone(),
        p2p_enabled,
        p2p_addr: if p2p_enabled {
            Some(format!("/ip4/127.0.0.1/tcp/{}", p2p_port))
        } else {
            None
        },
        peers: vec![],
        identity: None,
    };

    let cmd = node.command(&config);

    // Start log tracker before spawning so it catches early output
    let stdout_path = log_dir.join("stdout.log");
    let log_tracker = LogTracker::start(stdout_path, vec![]);

    let process = ManagedProcess::spawn(name, cmd, &log_dir)?;

    // Wait for ready signal from logs
    log_tracker
        .wait_for_ready(ready_timeout)
        .await
        .with_context(|| format!("{}: did not become ready", name))?;

    Ok(RunningNode {
        name: name.to_string(),
        api_url,
        http_addr,
        binary_path: node.binary_path().to_path_buf(),
        process,
        log_tracker,
        rootdir,
    })
}
