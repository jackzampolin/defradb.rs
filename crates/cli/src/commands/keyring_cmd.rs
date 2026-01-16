// Copyright 2025 Democratized Data Foundation
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

//! Keyring command implementation

use std::io::{self, BufRead, Read, Write};
use std::path::PathBuf;

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
pub enum KeyringCommand {
    /// Generate a new key and store it in the keyring
    Generate(GenerateArgs),

    /// Export a key from the keyring
    Export(ExportArgs),

    /// Import a key into the keyring
    Import(ImportArgs),

    /// Delete a key from the keyring
    Delete(DeleteArgs),

    /// List all keys in the keyring
    List(ListArgs),
}

impl KeyringArgs {
    pub fn execute(self, config: Config) -> Result<()> {
        match self.command {
            KeyringCommand::Generate(args) => args.execute(config),
            KeyringCommand::Export(args) => args.execute(config),
            KeyringCommand::Import(args) => args.execute(config),
            KeyringCommand::Delete(args) => args.execute(config),
            KeyringCommand::List(args) => args.execute(config),
        }
    }
}

/// Generate a new cryptographic key
#[derive(Args, Debug)]
pub struct GenerateArgs {
    /// Name of the key to generate
    pub name: String,

    /// Key type to generate (ed25519, secp256k1, aes256)
    #[arg(short = 't', long, default_value = "ed25519")]
    pub key_type: String,
}

impl GenerateArgs {
    pub fn execute(self, config: Config) -> Result<()> {
        use crypto::Key;

        let keyring = open_keyring(&config)?;

        let (key, description) = match self.key_type.to_lowercase().as_str() {
            "ed25519" => {
                let private_key = crypto::generate_ed25519().map_err(|e| {
                    Error::Keyring(format!("failed to generate Ed25519 key: {}", e))
                })?;
                (private_key.raw().to_vec(), "64-byte Ed25519")
            }
            "secp256k1" => {
                let private_key = crypto::generate_secp256k1().map_err(|e| {
                    Error::Keyring(format!("failed to generate secp256k1 key: {}", e))
                })?;
                (private_key.raw().to_vec(), "32-byte secp256k1")
            }
            "aes256" | "aes" => {
                let key = crypto::generate_aes256().map_err(|e| {
                    Error::Keyring(format!("failed to generate AES-256 key: {}", e))
                })?;
                (key, "32-byte AES-256")
            }
            _ => {
                return Err(Error::Keyring(format!(
                    "unknown key type: '{}'. Valid types: ed25519, secp256k1, aes256",
                    self.key_type
                )));
            }
        };

        keyring
            .set(&self.name, &key)
            .map_err(|e| Error::Keyring(e.to_string()))?;

        println!("Generated {} key: {}", description, self.name);
        Ok(())
    }
}

/// Export a key from the keyring
#[derive(Args, Debug)]
pub struct ExportArgs {
    /// Name of the key to export
    pub name: String,

    /// Output as hex instead of raw bytes
    #[arg(long)]
    pub hex: bool,
}

impl ExportArgs {
    pub fn execute(self, config: Config) -> Result<()> {
        let keyring = open_keyring(&config)?;

        let key = keyring
            .get(&self.name)
            .map_err(|e| Error::Keyring(e.to_string()))?;

        if self.hex {
            println!("{}", hex_encode(&key));
        } else {
            // Write raw bytes to stdout
            io::stdout().write_all(&key)?;
        }

        Ok(())
    }
}

/// Import a key into the keyring
#[derive(Args, Debug)]
pub struct ImportArgs {
    /// Name for the imported key
    pub name: String,

    /// Input is hex encoded
    #[arg(long)]
    pub hex: bool,
}

impl ImportArgs {
    pub fn execute(self, config: Config) -> Result<()> {
        let keyring = open_keyring(&config)?;

        // Read from stdin
        let stdin = io::stdin();
        let key = if self.hex {
            let mut line = String::new();
            stdin.lock().read_line(&mut line)?;
            hex_decode(line.trim()).map_err(|e| Error::Keyring(format!("invalid hex: {}", e)))?
        } else {
            let mut buf = Vec::new();
            stdin.lock().read_to_end(&mut buf)?;
            // Remove trailing newline if present (from terminal input)
            if buf.last() == Some(&b'\n') {
                buf.pop();
            }
            buf
        };

        keyring
            .set(&self.name, &key)
            .map_err(|e| Error::Keyring(e.to_string()))?;

        println!("Imported key: {}", self.name);
        Ok(())
    }
}

/// Delete a key from the keyring
#[derive(Args, Debug)]
pub struct DeleteArgs {
    /// Name of the key to delete
    pub name: String,
}

impl DeleteArgs {
    pub fn execute(self, config: Config) -> Result<()> {
        let keyring = open_keyring(&config)?;

        keyring
            .delete(&self.name)
            .map_err(|e| Error::Keyring(e.to_string()))?;

        println!("Deleted key: {}", self.name);
        Ok(())
    }
}

/// List all keys in the keyring
#[derive(Args, Debug)]
pub struct ListArgs {}

impl ListArgs {
    pub fn execute(self, config: Config) -> Result<()> {
        let keyring = open_keyring(&config)?;

        let keys = keyring.list().map_err(|e| Error::Keyring(e.to_string()))?;

        if keys.is_empty() {
            println!("No keys found");
        } else {
            for key in keys {
                println!("{}", key);
            }
        }

        Ok(())
    }
}

/// Open the appropriate keyring based on config
fn open_keyring(config: &Config) -> Result<Box<dyn keyring::Keyring>> {
    use crate::config::KeyringBackend;

    if config.keyring.disabled {
        return Err(Error::Keyring("keyring is disabled".to_string()));
    }

    match config.keyring.backend {
        KeyringBackend::File => {
            let path = resolve_keyring_path(config)?;
            let secret =
                keyring::load_secret_from_env().map_err(|e| Error::Keyring(e.to_string()))?;
            let kr = keyring::FileKeyring::open(&path, secret)
                .map_err(|e| Error::Keyring(e.to_string()))?;
            Ok(Box::new(kr))
        }
        KeyringBackend::System => {
            let kr = keyring::SystemKeyring::open(&config.keyring.namespace);
            Ok(Box::new(kr))
        }
    }
}

/// Resolve the keyring path, using rootdir if path is relative
fn resolve_keyring_path(config: &Config) -> Result<PathBuf> {
    let path = PathBuf::from(&config.keyring.path);
    if path.is_absolute() {
        Ok(path)
    } else {
        Ok(config.rootdir.join(path))
    }
}

fn hex_encode(data: &[u8]) -> String {
    data.iter().map(|b| format!("{:02x}", b)).collect()
}

fn hex_decode(s: &str) -> std::result::Result<Vec<u8>, String> {
    if s.len() % 2 != 0 {
        return Err(format!("hex string has odd length: {}", s.len()));
    }
    hex::decode(s).map_err(|e| e.to_string())
}
