//! Identity command implementation

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
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
    /// Export a named identity as JWK
    Export(IdentityExportArgs),
    /// Import a JWK identity into the keyring
    Import(IdentityImportArgs),
    /// Delete a named identity from the keyring
    Delete(IdentityDeleteArgs),
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

    /// Also output the private key as JWK (only meaningful with --name)
    #[arg(long = "output-key")]
    pub output_key: bool,

    /// Output format: json (default, Go-compatible) or text (Rust extension)
    #[arg(long, default_value = "json")]
    pub output: String,
}

/// Arguments for identity export command
#[derive(Args, Debug)]
pub struct IdentityExportArgs {
    /// Name of the key in the keyring
    #[arg(long)]
    pub name: String,

    /// Output format: text or json
    #[arg(long, default_value = "text")]
    pub output: String,
}

/// Arguments for identity import command
#[derive(Args, Debug)]
pub struct IdentityImportArgs {
    /// Name to store the key under in the keyring
    #[arg(long)]
    pub name: String,

    /// Read JWK from stdin
    #[arg(long)]
    pub stdin: bool,

    /// Read JWK from a file
    #[arg(long)]
    pub file: Option<String>,

    /// Output format: text or json
    #[arg(long, default_value = "text")]
    pub output: String,
}

/// Arguments for identity delete command
#[derive(Args, Debug)]
pub struct IdentityDeleteArgs {
    /// Name of the key to delete from the keyring
    #[arg(long)]
    pub name: String,

    /// Output format: text or json
    #[arg(long, default_value = "text")]
    pub output: String,
}

impl IdentityArgs {
    pub fn execute(&self, config: Config) -> Result<()> {
        match &self.command {
            IdentityCommand::New(args) => args.execute(config),
            IdentityCommand::Export(args) => args.execute(config),
            IdentityCommand::Import(args) => args.execute(config),
            IdentityCommand::Delete(args) => args.execute(config),
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
        let raw_bytes = identity.private_key_bytes();

        if let Some(ref name) = self.name {
            let keyring = super::open_keyring(&config)?;
            keyring
                .set(name, &raw_bytes)
                .map_err(|e| Error::Keyring(e.to_string()))?;

            if self.output_key {
                let key_type = detect_key_type(&raw_bytes)?;
                let jwk = build_jwk(key_type, &raw_bytes)?;
                print_jwk(&jwk, &self.output);
            } else {
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
            }
        } else {
            let private_key_hex = hex::encode(&raw_bytes);
            let public_key_hex = hex::encode(identity.public_key_bytes());
            let key_type_str = identity.identity_key_type().to_string();
            match self.output.to_lowercase().as_str() {
                "text" => {
                    println!("Private key: {}", private_key_hex);
                    println!("DID: {}", did);
                }
                _ => {
                    let output = serde_json::json!({
                        "PrivateKey": private_key_hex,
                        "PublicKey": public_key_hex,
                        "DID": did.to_string(),
                        "KeyType": key_type_str,
                    });
                    println!("{}", serde_json::to_string_pretty(&output)?);
                }
            }
        }

        Ok(())
    }
}

impl IdentityExportArgs {
    pub fn execute(&self, config: Config) -> Result<()> {
        let keyring = open_keyring(&config)?;
        let raw_bytes = keyring
            .get(&self.name)
            .map_err(|e| Error::Keyring(e.to_string()))?;
        let key_type = detect_key_type(&raw_bytes)?;
        let jwk = build_jwk(key_type, &raw_bytes)?;
        print_jwk(&jwk, &self.output);
        Ok(())
    }
}

impl IdentityImportArgs {
    pub fn execute(&self, config: Config) -> Result<()> {
        use identity::{Identity, RawIdentity};

        let jwk_text = if self.stdin {
            use std::io::Read;
            let mut buf = String::new();
            std::io::stdin()
                .read_to_string(&mut buf)
                .map_err(|e| Error::Keyring(format!("failed to read stdin: {}", e)))?;
            buf
        } else if let Some(ref path) = self.file {
            std::fs::read_to_string(path).map_err(|e| Error::ReadFile {
                path: path.into(),
                source: e,
            })?
        } else {
            return Err(Error::MissingInput(
                "one of --stdin or --file is required".to_string(),
            ));
        };

        let raw_bytes = parse_jwk(&jwk_text)?;

        // Validate the key by constructing a RawIdentity
        let key_type = detect_key_type(&raw_bytes)?;
        let identity = RawIdentity::from_identity_key_type(key_type, &raw_bytes)?;
        let did = identity.did()?;

        let keyring = open_keyring(&config)?;
        keyring
            .set(&self.name, &raw_bytes)
            .map_err(|e| Error::Keyring(e.to_string()))?;

        match self.output.to_lowercase().as_str() {
            "json" => {
                let output = serde_json::json!({
                    "did": did.to_string(),
                    "name": &self.name,
                });
                println!("{}", serde_json::to_string_pretty(&output)?);
            }
            _ => {
                println!("DID: {}", did);
            }
        }

        Ok(())
    }
}

impl IdentityDeleteArgs {
    pub fn execute(&self, config: Config) -> Result<()> {
        let keyring = open_keyring(&config)?;
        keyring
            .delete(&self.name)
            .map_err(|e| Error::Keyring(e.to_string()))?;

        match self.output.to_lowercase().as_str() {
            "json" => {
                let output = serde_json::json!({
                    "deleted": true,
                    "name": &self.name,
                });
                println!("{}", serde_json::to_string_pretty(&output)?);
            }
            _ => {
                println!("Deleted: {}", self.name);
            }
        }

        Ok(())
    }
}

// --- JWK helpers ---

/// Auto-detect key type from raw byte length (matches start/mod.rs pattern).
fn detect_key_type(bytes: &[u8]) -> Result<identity::IdentityKeyType> {
    match bytes.len() {
        64 => Ok(identity::IdentityKeyType::Ed25519),
        32 => Ok(identity::IdentityKeyType::Secp256k1),
        n => Err(Error::InvalidIdentity(format!(
            "invalid key length {} bytes: expected 64 (ed25519) or 32 (secp256k1)",
            n
        ))),
    }
}

/// Build a JWK JSON value from raw key bytes.
fn build_jwk(key_type: identity::IdentityKeyType, raw_bytes: &[u8]) -> Result<serde_json::Value> {
    use identity::{Identity, RawIdentity};

    let identity = RawIdentity::from_identity_key_type(key_type, raw_bytes)?;
    let did = identity.did()?;

    match key_type {
        identity::IdentityKeyType::Secp256k1 => {
            let (x, y) = crypto::secp256k1_private_key_to_xy(raw_bytes).map_err(|e| {
                Error::Keyring(format!("failed to extract secp256k1 coordinates: {}", e))
            })?;
            Ok(serde_json::json!({
                "kty": "EC",
                "crv": "secp256k1",
                "d": URL_SAFE_NO_PAD.encode(raw_bytes),
                "x": URL_SAFE_NO_PAD.encode(&x),
                "y": URL_SAFE_NO_PAD.encode(&y),
                "did": did.to_string(),
            }))
        }
        identity::IdentityKeyType::Ed25519 => {
            let seed = &raw_bytes[..32];
            let pubkey = &raw_bytes[32..64];
            Ok(serde_json::json!({
                "kty": "OKP",
                "crv": "Ed25519",
                "d": URL_SAFE_NO_PAD.encode(seed),
                "x": URL_SAFE_NO_PAD.encode(pubkey),
                "did": did.to_string(),
            }))
        }
    }
}

/// Print a JWK value to stdout. Text mode = compact, json mode = pretty.
fn print_jwk(jwk: &serde_json::Value, output_mode: &str) {
    match output_mode.to_lowercase().as_str() {
        "json" => {
            println!("{}", serde_json::to_string_pretty(jwk).unwrap());
        }
        _ => {
            println!("{}", serde_json::to_string(jwk).unwrap());
        }
    }
}

/// Parse a JWK JSON string and return raw private key bytes.
fn parse_jwk(text: &str) -> Result<Vec<u8>> {
    let jwk: serde_json::Value = serde_json::from_str(text.trim())
        .map_err(|e| Error::InvalidIdentity(format!("invalid JWK JSON: {}", e)))?;

    let kty = jwk
        .get("kty")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::InvalidIdentity("JWK missing 'kty' field".to_string()))?;

    let crv = jwk
        .get("crv")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::InvalidIdentity("JWK missing 'crv' field".to_string()))?;

    let d_b64 = jwk
        .get("d")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::InvalidIdentity("JWK missing 'd' field".to_string()))?;

    let d_bytes = URL_SAFE_NO_PAD
        .decode(d_b64)
        .map_err(|e| Error::InvalidIdentity(format!("invalid base64url in 'd': {}", e)))?;

    match (kty, crv) {
        ("EC", "secp256k1") => {
            if d_bytes.len() != 32 {
                return Err(Error::InvalidIdentity(format!(
                    "secp256k1 'd' must be 32 bytes, got {}",
                    d_bytes.len()
                )));
            }
            Ok(d_bytes)
        }
        ("OKP", "Ed25519") => {
            if d_bytes.len() != 32 {
                return Err(Error::InvalidIdentity(format!(
                    "Ed25519 'd' (seed) must be 32 bytes, got {}",
                    d_bytes.len()
                )));
            }
            // Reconstruct 64-byte key from 32-byte seed
            crypto::ed25519_key_from_seed(&d_bytes).map_err(|e| {
                Error::InvalidIdentity(format!("failed to reconstruct Ed25519 key: {}", e))
            })
        }
        _ => Err(Error::InvalidIdentity(format!(
            "unsupported JWK curve: kty={}, crv={}",
            kty, crv
        ))),
    }
}

fn open_keyring(config: &Config) -> Result<Box<dyn keyring::Keyring>> {
    super::open_keyring(config)
}
