//! Keyring command implementation

use std::io::{self, BufRead, Write};

use clap::{Args, Subcommand};

use crate::config::Config;
use crate::error::{Error, Result};

/// Arguments for the keyring command
#[derive(Args, Debug)]
pub struct KeyringArgs {
    #[command(subcommand)]
    pub command: KeyringCommand,
}

/// Keyring subcommands
#[derive(Subcommand, Debug)]
#[non_exhaustive]
pub enum KeyringCommand {
    /// Create new private keys
    New(NewArgs),

    /// Get a private key
    Get(GetArgs),

    /// Add a private key
    Add(AddArgs),

    /// List all keys in the keyring
    List(ListArgs),
}

impl KeyringArgs {
    pub fn execute(self, config: Config) -> Result<()> {
        match self.command {
            KeyringCommand::New(args) => args.execute(config),
            KeyringCommand::Get(args) => args.execute(config),
            KeyringCommand::Add(args) => args.execute(config),
            KeyringCommand::List(args) => args.execute(config),
        }
    }
}

/// Create a new cryptographic key
#[derive(Args, Debug)]
pub struct NewArgs {
    /// Name of the key to create (omit for Go-compatible mode: creates peer-key + encryption-key)
    pub name: Option<String>,

    /// Key type to create (ed25519, secp256k1, secp256r1, aes256) — only used with a named key
    #[arg(short = 't', long = "key-type", default_value = "ed25519")]
    pub key_type: String,

    /// Skip creating the encryption key (Go-compatible mode only)
    #[arg(long = "no-encryption")]
    pub no_encryption: bool,

    /// Skip creating the peer key (Go-compatible mode only)
    #[arg(long = "no-peer-key")]
    pub no_peer_key: bool,

    /// Overwrite existing keys without error
    #[arg(long)]
    pub force: bool,
}

impl NewArgs {
    pub fn execute(self, config: Config) -> Result<()> {
        use crypto::Key;

        let keyring = super::open_keyring(&config)?;

        if let Some(name) = self.name {
            // Rust extension mode: generate a single named key
            if !self.force && key_exists(keyring.as_ref(), &name)? {
                return Err(Error::Keyring(format!(
                    "key {} already exists, use --force to overwrite",
                    name
                )));
            }

            let key = generate_key_bytes(&self.key_type)?;
            keyring
                .set(&name, &key)
                .map_err(|e| Error::Keyring(e.to_string()))?;
        } else {
            // Go-compatible mode: generate peer-key and/or encryption-key
            if !self.no_peer_key {
                if !self.force && key_exists(keyring.as_ref(), "peer-key")? {
                    return Err(Error::Keyring(
                        "key peer-key already exists, use --force to overwrite".to_string(),
                    ));
                }
                let private_key = crypto::generate_ed25519().map_err(|e| {
                    Error::Keyring(format!("failed to generate Ed25519 key: {}", e))
                })?;
                keyring
                    .set("peer-key", private_key.raw())
                    .map_err(|e| Error::Keyring(e.to_string()))?;
            }

            if !self.no_encryption {
                if !self.force && key_exists(keyring.as_ref(), "encryption-key")? {
                    return Err(Error::Keyring(
                        "key encryption-key already exists, use --force to overwrite".to_string(),
                    ));
                }
                let key = crypto::generate_aes256().map_err(|e| {
                    Error::Keyring(format!("failed to generate AES-256 key: {}", e))
                })?;
                keyring
                    .set("encryption-key", &key)
                    .map_err(|e| Error::Keyring(e.to_string()))?;
            }
        }

        Ok(())
    }
}

fn key_exists(keyring: &dyn keyring::Keyring, name: &str) -> Result<bool> {
    match keyring.get(name) {
        Ok(_) => Ok(true),
        Err(keyring::Error::NotFound(_)) => Ok(false),
        Err(e) => Err(Error::Keyring(e.to_string())),
    }
}

fn generate_key_bytes(key_type: &str) -> Result<Vec<u8>> {
    use crypto::Key;

    match key_type.to_lowercase().as_str() {
        "ed25519" => {
            let private_key = crypto::generate_ed25519()
                .map_err(|e| Error::Keyring(format!("failed to generate Ed25519 key: {}", e)))?;
            Ok(private_key.raw().to_vec())
        }
        "secp256k1" => {
            let private_key = crypto::generate_secp256k1()
                .map_err(|e| Error::Keyring(format!("failed to generate secp256k1 key: {}", e)))?;
            Ok(private_key.raw().to_vec())
        }
        "secp256r1" | "p256" | "p-256" => {
            let private_key = crypto::generate_secp256r1()
                .map_err(|e| Error::Keyring(format!("failed to generate secp256r1 key: {}", e)))?;
            Ok(private_key.raw().to_vec())
        }
        "aes256" | "aes" => {
            let key = crypto::generate_aes256()
                .map_err(|e| Error::Keyring(format!("failed to generate AES-256 key: {}", e)))?;
            Ok(key)
        }
        _ => Err(Error::Keyring(format!(
            "unknown key type: '{}'. Valid types: ed25519, secp256k1, secp256r1, aes256",
            key_type
        ))),
    }
}

/// Get a key from the keyring
#[derive(Args, Debug)]
pub struct GetArgs {
    /// Name of the key to get
    pub name: String,

    /// Output raw bytes instead of hex (Rust extension)
    #[arg(long)]
    pub raw: bool,
}

impl GetArgs {
    pub fn execute(self, config: Config) -> Result<()> {
        require_development(&config)?;

        let keyring = super::open_keyring(&config)?;

        let key = keyring
            .get(&self.name)
            .map_err(|e| Error::Keyring(e.to_string()))?;

        if self.raw {
            io::stdout().write_all(&key)?;
        } else {
            println!("{}", hex_encode(&key));
        }

        Ok(())
    }
}

/// Add a key into the keyring
#[derive(Args, Debug)]
pub struct AddArgs {
    /// Name for the added key
    pub name: String,

    /// Hex-encoded private key (Go-compatible positional argument)
    pub key_hex: Option<String>,

    /// Read hex-encoded key from stdin (Rust extension)
    #[arg(long)]
    pub stdin: bool,
}

impl AddArgs {
    pub fn execute(self, config: Config) -> Result<()> {
        require_development(&config)?;

        let keyring = super::open_keyring(&config)?;

        let key = if let Some(ref hex_str) = self.key_hex {
            hex_decode(hex_str).map_err(|e| Error::Keyring(format!("invalid hex: {}", e)))?
        } else if self.stdin {
            let mut line = String::new();
            io::stdin().lock().read_line(&mut line)?;
            hex_decode(line.trim()).map_err(|e| Error::Keyring(format!("invalid hex: {}", e)))?
        } else {
            return Err(Error::MissingInput(
                "provide hex key as argument or use --stdin".to_string(),
            ));
        };

        keyring
            .set(&self.name, &key)
            .map_err(|e| Error::Keyring(e.to_string()))?;

        Ok(())
    }
}

/// List all keys in the keyring
#[derive(Args, Debug)]
pub struct ListArgs {}

impl ListArgs {
    pub fn execute(self, config: Config) -> Result<()> {
        let keyring = super::open_keyring(&config)?;

        let keys = keyring.list().map_err(|e| Error::Keyring(e.to_string()))?;

        if keys.is_empty() {
            println!("No keys found in the keyring.");
        } else {
            println!("Keys in the keyring:");
            for key in keys {
                println!("- {}", key);
            }
        }

        Ok(())
    }
}

fn require_development(config: &Config) -> Result<()> {
    if config.development {
        Ok(())
    } else {
        Err(Error::OperationRequiresDeveloperMode)
    }
}

fn hex_encode(data: &[u8]) -> String {
    data.iter().map(|b| format!("{:02x}", b)).collect()
}

fn hex_decode(s: &str) -> std::result::Result<Vec<u8>, String> {
    if !s.len().is_multiple_of(2) {
        return Err(format!("hex string has odd length: {}", s.len()));
    }
    hex::decode(s).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_requires_development_mode() {
        let args = AddArgs {
            name: "test-key".to_string(),
            key_hex: Some("deadbeef".to_string()),
            stdin: false,
        };

        let err = args.execute(Config::default()).unwrap_err();
        assert!(matches!(err, Error::OperationRequiresDeveloperMode));
    }

    #[test]
    fn get_requires_development_mode() {
        let args = GetArgs {
            name: "test-key".to_string(),
            raw: false,
        };

        let err = args.execute(Config::default()).unwrap_err();
        assert!(matches!(err, Error::OperationRequiresDeveloperMode));
    }
}
