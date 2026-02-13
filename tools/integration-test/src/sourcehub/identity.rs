use anyhow::Result;
use cosmrs::crypto::secp256k1::SigningKey;

/// Derive a `source1...` bech32 address from a secp256k1 private key hex string.
///
/// Uses the same derivation as the sourcehub crate's TxSigner:
/// secp256k1 pubkey -> SHA256 -> RIMEMD160 -> bech32("source", ...).
pub fn source_hub_address(private_key_hex: &str) -> Result<String> {
    let key_bytes = hex_decode(private_key_hex)?;
    let signing_key = SigningKey::from_slice(&key_bytes)
        .map_err(|e| anyhow::anyhow!("invalid secp256k1 private key: {}", e))?;
    let public_key = signing_key.public_key();
    let account_id = public_key
        .account_id("source")
        .map_err(|e| anyhow::anyhow!("failed to derive source address: {}", e))?;
    Ok(account_id.to_string())
}

fn hex_decode(s: &str) -> Result<Vec<u8>> {
    (0..s.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&s[i..i + 2], 16)
                .map_err(|e| anyhow::anyhow!("invalid hex at offset {}: {}", i, e))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derives_source_address() {
        // Any valid 32-byte hex key should produce a source1... address
        let key_hex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let addr = source_hub_address(key_hex).unwrap();
        assert!(
            addr.starts_with("source1"),
            "expected source1... prefix, got: {}",
            addr
        );
    }
}
