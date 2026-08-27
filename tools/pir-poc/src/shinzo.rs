//! Canonical selectors shared by the Shinzo host adapter and PIR clients.
//!
//! The host sends only the resulting public bucket to its local PIR sidecar.
//! Domain-separated, length-prefixed inputs make the cross-language contract
//! unambiguous and leave room for additional chains, collections and fields.

use anyhow::{bail, Result};

pub const LIVE_PROTOCOL_VERSION: u32 = 1;
pub const DEFAULT_BUCKET_COUNT: usize = 1 << 16;
pub const ETHEREUM_MAINNET: &str = "ethereum-mainnet";
pub const LOG_COLLECTION: &str = "Ethereum__Mainnet__Log";
pub const LOG_ADDRESS_FIELD: &str = "address";
pub const LOG_TOPIC0_FIELD: &str = "topics[0]";

const SELECTOR_DOMAIN: &[u8] = b"shinzo-pir-live-selector-v1";

pub fn selector_bucket(
    chain: &str,
    collection: &str,
    field: &str,
    normalized_value: &str,
    bucket_count: usize,
) -> Result<usize> {
    if bucket_count < 2 || !bucket_count.is_power_of_two() {
        bail!("Shinzo PIR bucket count must be a power of two greater than one");
    }
    let mut hasher = blake3::Hasher::new();
    hasher.update(SELECTOR_DOMAIN);
    for part in [chain, collection, field, normalized_value] {
        let length = u32::try_from(part.len())?;
        hasher.update(&length.to_le_bytes());
        hasher.update(part.as_bytes());
    }
    let digest = hasher.finalize();
    let prefix = u64::from_le_bytes(
        digest.as_bytes()[..8]
            .try_into()
            .expect("BLAKE3 prefixes are fixed width"),
    );
    Ok(prefix as usize & (bucket_count - 1))
}

pub fn ethereum_log_bucket(field: &str, normalized_value: &str) -> Result<usize> {
    selector_bucket(
        ETHEREUM_MAINNET,
        LOG_COLLECTION,
        field,
        normalized_value,
        DEFAULT_BUCKET_COUNT,
    )
}

pub fn ethereum_log_selector_bucket(
    field: &str,
    value: &str,
    bucket_count: usize,
) -> Result<usize> {
    let expected_bytes = match field {
        LOG_ADDRESS_FIELD => 20,
        LOG_TOPIC0_FIELD => 32,
        _ => bail!("unsupported Shinzo Ethereum log selector field"),
    };
    let normalized = normalize_hex(value, expected_bytes)?;
    selector_bucket(
        ETHEREUM_MAINNET,
        LOG_COLLECTION,
        field,
        &normalized,
        bucket_count,
    )
}

fn normalize_hex(value: &str, expected_bytes: usize) -> Result<String> {
    let value = value.trim();
    let value = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
        .unwrap_or(value);
    let decoded = hex::decode(value)?;
    if decoded.len() != expected_bytes {
        bail!(
            "Shinzo selector must contain {expected_bytes} bytes, got {}",
            decoded.len()
        );
    }
    Ok(format!("0x{}", hex::encode(decoded)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selector_is_domain_separated_and_deterministic() {
        let address = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48";
        let first = ethereum_log_bucket(LOG_ADDRESS_FIELD, address).unwrap();
        assert_eq!(first, 48_215);
        assert_eq!(
            first,
            ethereum_log_bucket(LOG_ADDRESS_FIELD, address).unwrap()
        );
        assert_ne!(
            first,
            ethereum_log_bucket(LOG_TOPIC0_FIELD, address).unwrap()
        );
        assert!(selector_bucket("chain", "collection", "field", "value", 3).is_err());
        assert_eq!(
            first,
            ethereum_log_selector_bucket(
                LOG_ADDRESS_FIELD,
                "0xA0b86991c6218b36c1d19D4a2E9Eb0cE3606eB48",
                DEFAULT_BUCKET_COUNT,
            )
            .unwrap()
        );
    }
}
