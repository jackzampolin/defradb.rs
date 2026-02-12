//! Keyring command implementation

use std::io::{self, BufRead, IsTerminal, Read, Write};

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

        let keyring = super::open_keyring(&config)?;

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
        let keyring = super::open_keyring(&config)?;

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
        let keyring = super::open_keyring(&config)?;

        // Read from stdin
        let stdin = io::stdin();
        let is_terminal = stdin.is_terminal();
        let key = if self.hex {
            let mut line = String::new();
            stdin.lock().read_line(&mut line)?;
            hex_decode(line.trim()).map_err(|e| Error::Keyring(format!("invalid hex: {}", e)))?
        } else {
            let mut buf = Vec::new();
            stdin.lock().read_to_end(&mut buf)?;
            // Only strip trailing newline for terminal input to avoid corrupting binary keys
            // that legitimately end with 0x0A when piped from files
            if is_terminal && buf.last() == Some(&b'\n') {
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
        let keyring = super::open_keyring(&config)?;

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
        let keyring = super::open_keyring(&config)?;

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

fn hex_encode(data: &[u8]) -> String {
    data.iter().map(|b| format!("{:02x}", b)).collect()
}

fn hex_decode(s: &str) -> std::result::Result<Vec<u8>, String> {
    if s.len() % 2 != 0 {
        return Err(format!("hex string has odd length: {}", s.len()));
    }
    hex::decode(s).map_err(|e| e.to_string())
}
