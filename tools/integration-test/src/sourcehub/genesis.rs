use std::path::Path;
use std::process::Command;

use anyhow::{Context, Result};

use crate::ports::SourceHubPorts;

const VALIDATOR_STAKE: &str = "100000000000uopen";
const VALIDATOR_BALANCE: &str = "1000000000000uopen";
const IDENTITY_BALANCE: &str = "100000000uopen";

/// Provision a single-node Source Hub devnet genesis.
///
/// Follows the standard Cosmos SDK pattern:
///   init -> keys add -> add-genesis-account (validator + funded addrs) ->
///   gentx -> collect-gentxs -> patch configs
pub fn provision_genesis(
    home_dir: &Path,
    chain_id: &str,
    funded_addresses: &[String],
    ports: &SourceHubPorts,
) -> Result<()> {
    run_cmd(
        "sourcehubd",
        &[
            "init",
            "test-node",
            "--chain-id",
            chain_id,
            "--home",
            &home_dir.display().to_string(),
        ],
    )
    .context("sourcehubd init failed")?;

    let validator_addr = run_cmd(
        "sourcehubd",
        &[
            "keys",
            "add",
            "validator",
            "--keyring-backend",
            "test",
            "--home",
            &home_dir.display().to_string(),
            "--output",
            "json",
        ],
    )
    .context("sourcehubd keys add failed")?;

    let addr_json: serde_json::Value =
        serde_json::from_str(&validator_addr).context("failed to parse validator key output")?;
    let validator_address = addr_json["address"]
        .as_str()
        .context("missing address in validator key output")?
        .to_string();

    run_cmd(
        "sourcehubd",
        &[
            "genesis",
            "add-genesis-account",
            &validator_address,
            VALIDATOR_BALANCE,
            "--home",
            &home_dir.display().to_string(),
        ],
    )
    .context("add validator genesis account failed")?;

    for addr in funded_addresses {
        run_cmd(
            "sourcehubd",
            &[
                "genesis",
                "add-genesis-account",
                addr,
                IDENTITY_BALANCE,
                "--home",
                &home_dir.display().to_string(),
            ],
        )
        .context(format!("add genesis account {} failed", addr))?;
    }

    run_cmd(
        "sourcehubd",
        &[
            "genesis",
            "gentx",
            "validator",
            VALIDATOR_STAKE,
            "--chain-id",
            chain_id,
            "--keyring-backend",
            "test",
            "--home",
            &home_dir.display().to_string(),
        ],
    )
    .context("sourcehubd gentx failed")?;

    run_cmd(
        "sourcehubd",
        &[
            "genesis",
            "collect-gentxs",
            "--home",
            &home_dir.display().to_string(),
        ],
    )
    .context("sourcehubd collect-gentxs failed")?;

    patch_config_toml(home_dir, ports)?;
    patch_app_toml(home_dir, ports)?;

    Ok(())
}

/// Patch config.toml to bind CometBFT RPC and P2P to allocated ports.
fn patch_config_toml(home_dir: &Path, ports: &SourceHubPorts) -> Result<()> {
    let config_path = home_dir.join("config/config.toml");
    let content = std::fs::read_to_string(&config_path).context("read config.toml")?;

    // Replace default CometBFT RPC port (26657)
    let content = content.replace(
        "laddr = \"tcp://127.0.0.1:26657\"",
        &format!("laddr = \"tcp://0.0.0.0:{}\"", ports.comet_rpc),
    );
    // Replace default P2P port (26656)
    let content = content.replace(
        "laddr = \"tcp://0.0.0.0:26656\"",
        &format!("laddr = \"tcp://0.0.0.0:{}\"", ports.p2p),
    );

    std::fs::write(&config_path, content).context("write config.toml")?;
    Ok(())
}

/// Patch app.toml to bind gRPC and LCD/API to allocated ports.
fn patch_app_toml(home_dir: &Path, ports: &SourceHubPorts) -> Result<()> {
    let app_path = home_dir.join("config/app.toml");
    let content = std::fs::read_to_string(&app_path).context("read app.toml")?;

    // Replace default gRPC port (9090)
    let content = content.replace(
        "address = \"0.0.0.0:9090\"",
        &format!("address = \"0.0.0.0:{}\"", ports.grpc),
    );
    // Replace default LCD/API port (1317)
    let content = content.replace(
        "address = \"tcp://0.0.0.0:1317\"",
        &format!("address = \"tcp://0.0.0.0:{}\"", ports.lcd),
    );
    // Ensure API is enabled
    let content = content.replacen("enable = false", "enable = true", 1);

    std::fs::write(&app_path, content).context("write app.toml")?;
    Ok(())
}

fn run_cmd(program: &str, args: &[&str]) -> Result<String> {
    let output = Command::new(program)
        .args(args)
        .output()
        .with_context(|| format!("failed to run {} {}", program, args.join(" ")))?;

    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        Err(anyhow::anyhow!(
            "{} {} failed (exit {}): stderr={}, stdout={}",
            program,
            args.join(" "),
            output.status,
            stderr.trim(),
            stdout.trim()
        ))
    }
}
