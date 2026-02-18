use std::path::Path;
use std::process::Command;

use anyhow::{Context, Result};

/// A generated test identity with hex-encoded private key and DID string.
pub struct TestIdentity {
    pub private_key_hex: String,
    pub did: String,
    pub public_key_hex: Option<String>,
    pub key_type: Option<String>,
}

/// Generate a new identity using the given DefraDB binary.
///
/// Uses the default key type (secp256k1) for consistency between Go and Rust CLIs.
/// Parses both Rust (text) and Go (JSON) output formats:
/// - Rust text: `Private key: <hex>\nDID: <did>`
/// - Rust JSON: `{"private_key":"<hex>","did":"<did>"}`
/// - Go JSON:   `{"PrivateKey":"<hex>","DID":"<did>"}`
pub fn generate_identity(binary_path: &Path) -> Result<TestIdentity> {
    let output = Command::new(binary_path)
        .args(["identity", "new"])
        .output()
        .context("failed to run identity new")?;

    anyhow::ensure!(
        output.status.success(),
        "identity new failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    parse_identity_output(&stdout)
}

/// Generate a new secp256r1 (P-256) identity using the given DefraDB binary.
pub fn generate_secp256r1_identity(binary_path: &Path) -> Result<TestIdentity> {
    let output = Command::new(binary_path)
        .args(["identity", "new", "--type", "secp256r1"])
        .output()
        .context("failed to run identity new --type secp256r1")?;

    anyhow::ensure!(
        output.status.success(),
        "identity new --type secp256r1 failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    parse_identity_output(&stdout)
}

/// Generate a new ed25519 identity using the given DefraDB binary.
pub fn generate_ed25519_identity(binary_path: &Path) -> Result<TestIdentity> {
    let output = Command::new(binary_path)
        .args(["identity", "new", "--type", "ed25519"])
        .output()
        .context("failed to run identity new --type ed25519")?;

    anyhow::ensure!(
        output.status.success(),
        "identity new --type ed25519 failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    parse_identity_output(&stdout)
}

fn parse_identity_output(output: &str) -> Result<TestIdentity> {
    let trimmed = output.trim();

    // Try JSON first (works for both Go and Rust --output json)
    if trimmed.starts_with('{') {
        let val: serde_json::Value =
            serde_json::from_str(trimmed).context("failed to parse identity JSON")?;

        // Rust JSON (new): {"PrivateKey": ..., "PublicKey": ..., "DID": ..., "KeyType": ...}
        // Rust JSON (old): {"private_key": ..., "did": ...}
        // Go JSON:         {"PrivateKey": ..., "PublicKey": ..., "DID": ..., "KeyType": ...}
        let private_key = val
            .get("private_key")
            .or_else(|| val.get("PrivateKey"))
            .and_then(|v| v.as_str())
            .context("missing private_key in identity JSON")?;

        let did = val
            .get("did")
            .or_else(|| val.get("DID"))
            .and_then(|v| v.as_str())
            .context("missing did in identity JSON")?;

        let public_key = val
            .get("PublicKey")
            .or_else(|| val.get("public_key"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let key_type = val
            .get("KeyType")
            .or_else(|| val.get("key_type"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        return Ok(TestIdentity {
            private_key_hex: private_key.to_string(),
            did: did.to_string(),
            public_key_hex: public_key,
            key_type,
        });
    }

    // Rust text format: "Private key: <hex>\nDID: <did>"
    let mut private_key = None;
    let mut did = None;

    for line in trimmed.lines() {
        if let Some(key) = line.strip_prefix("Private key: ") {
            private_key = Some(key.trim().to_string());
        } else if let Some(d) = line.strip_prefix("DID: ") {
            did = Some(d.trim().to_string());
        }
    }

    Ok(TestIdentity {
        private_key_hex: private_key.context("missing 'Private key:' in identity output")?,
        did: did.context("missing 'DID:' in identity output")?,
        public_key_hex: None,
        key_type: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_rust_text_format() {
        let output = "Private key: abcdef0123456789\nDID: did:key:z6Mk...\n";
        let id = parse_identity_output(output).unwrap();
        assert_eq!(id.private_key_hex, "abcdef0123456789");
        assert_eq!(id.did, "did:key:z6Mk...");
        assert!(id.public_key_hex.is_none());
        assert!(id.key_type.is_none());
    }

    #[test]
    fn parse_rust_json_format_legacy() {
        let output = r#"{"private_key":"abcdef","did":"did:key:z6Mk"}"#;
        let id = parse_identity_output(output).unwrap();
        assert_eq!(id.private_key_hex, "abcdef");
        assert_eq!(id.did, "did:key:z6Mk");
    }

    #[test]
    fn parse_go_json_format() {
        let output = r#"{"PrivateKey":"abcdef","PublicKey":"012345","DID":"did:key:z6Mk","KeyType":"secp256k1"}"#;
        let id = parse_identity_output(output).unwrap();
        assert_eq!(id.private_key_hex, "abcdef");
        assert_eq!(id.did, "did:key:z6Mk");
        assert_eq!(id.public_key_hex.as_deref(), Some("012345"));
        assert_eq!(id.key_type.as_deref(), Some("secp256k1"));
    }
}
