use std::sync::OnceLock;
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

static RUST_BUILD_DONE: OnceLock<()> = OnceLock::new();
static IROH_BUILD_DONE: OnceLock<()> = OnceLock::new();

pub struct TestClusterBuilder {
    rust_nodes: usize,
    go_nodes: usize,
    p2p_enabled: bool,
    health_timeout: Duration,
    build_rust: bool,
    acp_document_type: Option<String>,
    node_identity: Option<String>,
    node_identities: Vec<Option<String>>,
    encryption_enabled: bool,
    signing_enabled: bool,
    nac_enabled: bool,
    source_hub_enabled: bool,
    development: bool,
    store: Option<String>,
    query_timeout: Option<u64>,
    p2p_transport: Option<String>,
    keyring_enabled: bool,
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
            node_identities: Vec::new(),
            encryption_enabled: false,
            signing_enabled: false,
            nac_enabled: false,
            source_hub_enabled: false,
            development: false,
            store: None,
            query_timeout: None,
            p2p_transport: None,
            keyring_enabled: false,
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

    /// Set identity for a specific node (by index). Overrides cluster-wide identity.
    pub fn with_node_identity(mut self, index: usize, key: impl Into<String>) -> Self {
        while self.node_identities.len() <= index {
            self.node_identities.push(None);
        }
        self.node_identities[index] = Some(key.into());
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

    pub fn with_keyring(mut self) -> Self {
        self.keyring_enabled = true;
        self
    }

    pub fn with_iroh_transport(mut self) -> Self {
        self.p2p_transport = Some("iroh".to_string());
        self.p2p_enabled = true;
        self
    }

    pub async fn build(mut self) -> Result<TestCluster> {
        let total = self.rust_nodes + self.go_nodes;
        anyhow::ensure!(total > 0, "must have at least one node");

        // Build Rust binary if needed (once per process, even under parallel tests)
        let is_iroh = self.p2p_transport.as_deref() == Some("iroh");
        if self.rust_nodes > 0 && self.build_rust {
            if is_iroh {
                IROH_BUILD_DONE.get_or_init(|| {
                    RustNode::build_with_features(&["iroh"])
                        .expect("failed to build Rust binary with iroh feature");
                });
            } else {
                RUST_BUILD_DONE.get_or_init(|| {
                    RustNode::build().expect("failed to build Rust binary");
                });
            }
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
        let mut all_ports = allocate_node_ports(total)?;

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
        for (i, ports) in all_ports.iter_mut().enumerate().take(self.rust_nodes) {
            let name = format!("rust-{}", i);
            let node = RustNode::from_workspace();
            let identity = self
                .node_identities
                .get(i)
                .cloned()
                .flatten()
                .or_else(|| self.node_identity.clone());
            // Release port guards right before spawn so the child process can bind
            ports.release();
            let running = spawn_node(
                &name,
                &node,
                ports.http,
                ports.p2p,
                self.p2p_enabled,
                &run_dir,
                self.health_timeout,
                if is_iroh {
                    patterns::iroh_patterns()
                } else {
                    patterns::node_patterns()
                },
                self.acp_document_type.clone(),
                identity,
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
                self.p2p_transport.clone(),
                self.keyring_enabled,
            )
            .await
            .with_context(|| format!("failed to start {}", name))?;
            nodes.push(running);
        }

        // Spawn Go nodes
        for (i, ports) in all_ports.iter_mut().skip(self.rust_nodes).enumerate() {
            let name = format!("go-{}", i);
            let node = GoNode::from_path();
            let go_index = self.rust_nodes + i;
            let identity = self
                .node_identities
                .get(go_index)
                .cloned()
                .flatten()
                .or_else(|| self.node_identity.clone());
            // Release port guards right before spawn so the child process can bind
            ports.release();
            let running = spawn_node(
                &name,
                &node,
                ports.http,
                ports.p2p,
                self.p2p_enabled,
                &run_dir,
                self.health_timeout,
                patterns::node_patterns(),
                self.acp_document_type.clone(),
                identity,
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
                None,
                false,
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

        // Collect effective per-node identities for test assertions
        let effective_identities: Vec<Option<String>> = (0..total)
            .map(|i| {
                self.node_identities
                    .get(i)
                    .cloned()
                    .flatten()
                    .or_else(|| self.node_identity.clone())
            })
            .collect();

        Ok(TestCluster::new(
            nodes,
            run_dir,
            self.node_identity,
            effective_identities,
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
    p2p_transport: Option<String>,
    keyring_enabled: bool,
) -> Result<RunningNode> {
    let node_dir = run_dir.node_dir(name)?;
    let log_dir = node_dir.join("logs");
    let rootdir = node_dir.join("data");
    std::fs::create_dir_all(&rootdir)?;

    let http_addr = format!("127.0.0.1:{}", http_port);
    let api_url = format!("http://{}", http_addr);

    let is_iroh = p2p_transport.as_deref() == Some("iroh");
    let config = NodeConfig {
        name: name.to_string(),
        rootdir: rootdir.clone(),
        log_dir: log_dir.clone(),
        http_addr: http_addr.clone(),
        p2p_enabled,
        p2p_addr: if p2p_enabled && !is_iroh {
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
        p2p_transport,
        keyring_enabled,
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
