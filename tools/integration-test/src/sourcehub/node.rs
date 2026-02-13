use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

use anyhow::{Context, Result};

use crate::observe::patterns::NamedPattern;
use crate::observe::LogTracker;
use crate::ports::SourceHubPorts;
use crate::process::ManagedProcess;

use super::genesis;
use super::identity::source_hub_address;

const DEFAULT_CHAIN_ID: &str = "sourcehub-test";

/// A running Source Hub single-node devnet.
pub struct SourceHubNode {
    #[allow(dead_code)]
    process: ManagedProcess,
    #[allow(dead_code)]
    log_tracker: LogTracker,
    pub lcd_url: String,
    pub comet_rpc_url: String,
    pub chain_id: String,
    #[allow(dead_code)]
    pub home_dir: PathBuf,
}

impl SourceHubNode {
    /// Provision and start a Source Hub devnet node.
    ///
    /// `identity_keys` are hex-encoded secp256k1 private keys whose derived
    /// `source1...` addresses will be funded in genesis.
    pub async fn start(
        home_dir: PathBuf,
        log_dir: PathBuf,
        ports: &SourceHubPorts,
        identity_keys: &[String],
        ready_timeout: Duration,
    ) -> Result<Self> {
        let chain_id = DEFAULT_CHAIN_ID.to_string();

        let funded_addresses: Vec<String> = identity_keys
            .iter()
            .map(|key| source_hub_address(key))
            .collect::<Result<_>>()
            .context("deriving source hub addresses from identity keys")?;

        genesis::provision_genesis(&home_dir, &chain_id, &funded_addresses, ports)
            .context("provisioning source hub genesis")?;

        let mut cmd = Command::new("sourcehubd");
        cmd.arg("start");
        cmd.arg("--home").arg(&home_dir);
        cmd.arg("--rpc.laddr")
            .arg(format!("tcp://0.0.0.0:{}", ports.comet_rpc));
        cmd.arg("--grpc.address")
            .arg(format!("0.0.0.0:{}", ports.grpc));
        cmd.arg("--api.address")
            .arg(format!("tcp://0.0.0.0:{}", ports.lcd));
        cmd.arg("--p2p.laddr")
            .arg(format!("tcp://0.0.0.0:{}", ports.p2p));
        cmd.arg("--minimum-gas-prices").arg("0uopen");
        cmd.arg("--log_no_color");

        let stdout_path = log_dir.join("stdout.log");
        let log_tracker = LogTracker::start(stdout_path, sourcehub_patterns());

        let process = ManagedProcess::spawn("sourcehub", cmd, &log_dir)
            .context("failed to spawn sourcehubd")?;

        let _first_block: String = log_tracker
            .wait_for_pattern("first_block", ready_timeout)
            .await
            .context("sourcehubd did not produce first block")?;

        let lcd_url = format!("http://127.0.0.1:{}", ports.lcd);

        // Health check: wait for LCD to respond
        let client = reqwest::Client::new();
        let health_url = format!("{}/cosmos/base/tendermint/v1beta1/blocks/latest", lcd_url);
        let deadline = tokio::time::Instant::now() + ready_timeout;
        loop {
            match client.get(&health_url).send().await {
                Ok(resp) if resp.status().is_success() => break,
                _ => {}
            }
            if tokio::time::Instant::now() >= deadline {
                anyhow::bail!("sourcehubd LCD health check timed out at {}", health_url);
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }

        Ok(Self {
            process,
            log_tracker,
            lcd_url,
            comet_rpc_url: format!("http://127.0.0.1:{}", ports.comet_rpc),
            chain_id,
            home_dir,
        })
    }
}

fn sourcehub_patterns() -> Vec<NamedPattern> {
    vec![NamedPattern {
        name: "first_block",
        regex: regex::Regex::new(r"committed state.*height=1\b").unwrap(),
    }]
}
