//! Identity command implementation

use clap::{Args, Subcommand};

use crate::config::Config;
use crate::error::{Error, Result};

/// Manage identities
#[derive(Args, Debug)]
pub struct IdentityArgs {
    #[command(subcommand)]
    pub command: IdentityCommand,
}

/// Identity subcommands
#[derive(Subcommand, Debug)]
pub enum IdentityCommand {
    /// Generate a new identity
    New(IdentityNewArgs),
}

/// Arguments for identity new command
#[derive(Args, Debug)]
pub struct IdentityNewArgs {
    /// Key type to generate (secp256k1 or ed25519)
    #[arg(long = "type", default_value = "secp256k1")]
    pub key_type: String,

    /// Store the key in the keyring under this name (outputs only DID)
    #[arg(long)]
    pub name: Option<String>,

    /// Output format: text or json
    #[arg(long, default_value = "text")]
    pub output: String,
}

impl IdentityArgs {
    pub fn execute(&self, config: Config) -> Result<()> {
        match &self.command {
            IdentityCommand::New(args) => args.execute(config),
        }
    }
}

impl IdentityNewArgs {
    pub fn execute(&self, config: Config) -> Result<()> {
        use identity::{Identity, RawIdentity};

        let identity = match self.key_type.to_lowercase().as_str() {
            "secp256k1" => {
                let private_key = crypto::generate_secp256k1().map_err(|e| {
                    Error::Keyring(format!("failed to generate secp256k1 key: {}", e))
                })?;
                RawIdentity::from_secp256k1(private_key)?
            }
            "ed25519" => {
                let private_key = crypto::generate_ed25519().map_err(|e| {
                    Error::Keyring(format!("failed to generate ed25519 key: {}", e))
                })?;
                RawIdentity::from_ed25519(private_key)?
            }
            _ => {
                return Err(Error::InvalidIdentity(format!(
                    "unknown key type: '{}'. Valid types: secp256k1, ed25519",
                    self.key_type
                )));
            }
        };

        let did = identity.did()?;
        let private_key_hex = hex::encode(identity.private_key_bytes());

        if let Some(ref name) = self.name {
            let keyring = super::open_keyring(&config)?;
            keyring
                .set(name, &identity.private_key_bytes())
                .map_err(|e| Error::Keyring(e.to_string()))?;

            match self.output.to_lowercase().as_str() {
                "json" => {
                    let output = serde_json::json!({
                        "did": did.to_string(),
                        "name": name,
                    });
                    println!("{}", serde_json::to_string_pretty(&output)?);
                }
                _ => {
                    println!("DID: {}", did);
                }
            }
        } else {
            match self.output.to_lowercase().as_str() {
                "json" => {
                    let output = serde_json::json!({
                        "private_key": private_key_hex,
                        "did": did.to_string(),
                    });
                    println!("{}", serde_json::to_string_pretty(&output)?);
                }
                _ => {
                    println!("Private key: {}", private_key_hex);
                    println!("DID: {}", did);
                }
            }
        }

        Ok(())
    }
}
