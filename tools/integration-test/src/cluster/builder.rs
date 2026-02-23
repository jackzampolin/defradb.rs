use std::time::Duration;

use anyhow::{Context, Result};
use reqwest::Client;

use crate::node::{DefraNode, GoNode, NodeConfig, RustNode};
use crate::observe::patterns::{self, NamedPattern};
use crate::observe::LogTracker;
use crate::ports::{allocate_node_ports, allocate_source_hub_ports};
use crate::process::ManagedProcess;
use crate::run::TestRunDir;
use crate::sourcehub::SourceHubNode;

use super::health::health_check_all;
use super::runtime::{NodeKind, RunningNode, TestCluster};

pub struct TestClusterBuilder {
    rust_nodes: usize,
    go_nodes: usize,
    p2p_enabled: bool,
    health_timeout: Duration,
    build_rust: bool,
    acp_document_type: Option<String>,
    node_identity: Option<String>,
    encryption_enabled: bool,
    signing_enabled: bool,
    nac_enabled: bool,
    source_hub_enabled: bool,
    development: bool,
    store: Option<String>,
    query_timeout: Option<u64>,
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
            acp_document_type: None,
            node_identity: None,
            encryption_enabled: false,
            signing_enabled: false,
            nac_enabled: false,
            source_hub_enabled: false,
            development: false,
            store: None,
            query_timeout: None,
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

    pub fn with_acp_local(mut self) -> Self {
        self.acp_document_type = Some("local".to_string());
        self
    }

    pub fn with_identity(mut self, key: impl Into<String>) -> Self {
        self.node_identity = Some(key.into());
        self
    }

    pub fn with_encryption(mut self) -> Self {
        self.encryption_enabled = true;
        self
    }

    pub fn with_signing(mut self) -> Self {
        self.signing_enabled = true;
        self
    }

    pub fn with_nac(mut self) -> Self {
        self.nac_enabled = true;
        self
    }

    pub fn with_source_hub(mut self) -> Self {
        self.source_hub_enabled = true;
        self.acp_document_type = Some("source-hub".to_string());
        self
    }

    pub fn with_development(mut self) -> Self {
        self.development = true;
        self
    }

    pub fn with_store(mut self, store: impl Into<String>) -> Self {
        self.store = Some(store.into());
        self
    }

    pub fn with_query_timeout(mut self, secs: u64) -> Self {
        self.query_timeout = Some(secs);
        self
    }

    pub async fn build(mut self) -> Result<TestCluster> {
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

        // Source Hub or NAC requires an identity at startup.
        if (self.nac_enabled || self.source_hub_enabled) && self.node_identity.is_none() {
            let binary = if self.go_nodes > 0 {
                std::path::PathBuf::from("defradb")
            } else {
                RustNode::from_workspace().binary_path().to_path_buf()
            };
            let id = crate::identity::generate_identity(&binary)
                .context("auto-generating identity for NAC/SourceHub")?;
            self.node_identity = Some(id.private_key_hex);
        }

        // Allocate ports for all nodes
        let all_ports = allocate_node_ports(total)?;

        // Create run directory
        let run_dir = TestRunDir::new()?;

        // Start Source Hub if enabled
        let source_hub = if self.source_hub_enabled {
            let sh_ports = allocate_source_hub_ports().context("allocating source hub ports")?;

            let sh_home = run_dir.node_dir("sourcehub")?;
            let sh_log_dir = sh_home.join("logs");
            std::fs::create_dir_all(&sh_log_dir)?;

            let identity_keys: Vec<String> = self.node_identity.iter().cloned().collect();

            let sh_node = SourceHubNode::start(
                sh_home,
                sh_log_dir,
                &sh_ports,
                &identity_keys,
                Duration::from_secs(60),
            )
            .await
            .context("failed to start source hub node")?;

            Some(sh_node)
        } else {
            None
        };

        let (sh_lcd, sh_comet, sh_chain_id) = match &source_hub {
            Some(sh) => (
                Some(sh.lcd_url.clone()),
                Some(sh.comet_rpc_url.clone()),
                Some(sh.chain_id.clone()),
            ),
            None => (None, None, None),
        };

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
                patterns::rust_patterns(),
                self.acp_document_type.clone(),
                self.node_identity.clone(),
                self.encryption_enabled,
                self.signing_enabled,
                self.nac_enabled,
                sh_lcd.clone(),
                sh_comet.clone(),
                sh_chain_id.clone(),
                self.development,
                self.store.clone(),
                self.query_timeout,
                NodeKind::Rust,
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
                patterns::go_patterns(),
                self.acp_document_type.clone(),
                self.node_identity.clone(),
                self.encryption_enabled,
                self.signing_enabled,
                self.nac_enabled,
                sh_lcd.clone(),
                sh_comet.clone(),
                sh_chain_id.clone(),
                self.development,
                self.store.clone(),
                self.query_timeout,
                NodeKind::Go,
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

        Ok(TestCluster::new(
            nodes,
            run_dir,
            self.node_identity,
            source_hub,
        ))
    }
}

#[allow(clippy::too_many_arguments)]
async fn spawn_node(
    name: &str,
    node: &dyn DefraNode,
    http_port: u16,
    p2p_port: u16,
    p2p_enabled: bool,
    run_dir: &TestRunDir,
    ready_timeout: Duration,
    named_patterns: Vec<NamedPattern>,
    acp_document_type: Option<String>,
    node_identity: Option<String>,
    encryption_enabled: bool,
    signing_enabled: bool,
    nac_enabled: bool,
    source_hub_address: Option<String>,
    source_hub_comet_address: Option<String>,
    source_hub_chain_id: Option<String>,
    development: bool,
    store: Option<String>,
    query_timeout: Option<u64>,
    kind: NodeKind,
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
        identity: node_identity,
        acp_document_type,
        encryption_enabled,
        signing_enabled,
        nac_enabled,
        source_hub_address,
        source_hub_comet_address,
        source_hub_chain_id,
        development,
        store,
        query_timeout,
    };

    let cmd = node.command(&config);

    // Start log tracker before spawning so it catches early output
    let stdout_path = log_dir.join("stdout.log");
    let log_tracker = LogTracker::start(stdout_path, named_patterns);

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
        config,
        kind,
    })
}
