use std::sync::Arc;

use identity::{FullIdentity, IdentityKeyType, RawIdentity};
use zeroize::Zeroizing;

use crate::config::Config;
use crate::error::{Error, Result};

const NODE_IDENTITY_KEY: &str = "node-identity-key";

pub(super) fn resolve(
    config: &Config,
    explicit: Option<Arc<RawIdentity>>,
) -> Result<Option<Arc<RawIdentity>>> {
    if explicit.is_some() {
        return Ok(explicit);
    }
    if config.keyring.disabled {
        return if config.development {
            generate(&config.datastore.default_key_type).map(|identity| Some(Arc::new(identity)))
        } else {
            Ok(None)
        };
    }

    let keyring = crate::commands::open_keyring(config)?;
    load_or_create(keyring.as_ref(), &config.datastore.default_key_type)
        .map(|identity| Some(Arc::new(identity)))
}

fn generate(key_type: &str) -> Result<RawIdentity> {
    let key_type: IdentityKeyType = key_type.parse().map_err(|_| {
        Error::InvalidConfig("default-key-type must be ed25519, secp256k1, or secp256r1".into())
    })?;
    let key = crypto::generate_key(key_type.into())?;
    Ok(RawIdentity::from_identity_key_type(key_type, key.raw())?)
}

fn persist(keyring: &dyn keyring::Keyring, identity: &RawIdentity) -> Result<()> {
    let mut encoded = Zeroizing::new(format!("{}:", identity.identity_key_type()).into_bytes());
    encoded.extend_from_slice(identity.priv_key().raw());
    keyring
        .set(NODE_IDENTITY_KEY, &encoded)
        .map_err(|error| Error::Keyring(error.to_string()))
}

fn load_or_create(keyring: &dyn keyring::Keyring, default_type: &str) -> Result<RawIdentity> {
    let encoded = match keyring.get(NODE_IDENTITY_KEY) {
        Ok(encoded) => encoded,
        Err(keyring::Error::NotFound(_)) => {
            let identity = generate(default_type)?;
            persist(keyring, &identity)?;
            return Ok(identity);
        }
        Err(error) => return Err(Error::Keyring(error.to_string())),
    };

    // Legacy Go identities are raw secp256k1 keys; their random bytes may contain ':'.
    let legacy = encoded.len() == 32;
    let (key_type, key_bytes) = if legacy {
        (IdentityKeyType::Secp256k1, encoded.as_slice())
    } else {
        let separator = encoded
            .iter()
            .position(|byte| *byte == b':')
            .ok_or_else(|| {
                Error::InvalidIdentity("node-identity-key is missing its key type".into())
            })?;
        let key_type = std::str::from_utf8(&encoded[..separator])
            .ok()
            .and_then(|name| name.parse::<IdentityKeyType>().ok())
            .ok_or_else(|| Error::InvalidIdentity("invalid node-identity-key type".into()))?;
        (key_type, &encoded[separator + 1..])
    };
    let identity = RawIdentity::from_identity_key_type(key_type, key_bytes)
        .map_err(|_| Error::InvalidIdentity("invalid node-identity-key bytes".into()))?;
    if legacy {
        persist(keyring, &identity)?;
    }
    Ok(identity)
}

#[cfg(test)]
#[path = "../../../tests/start/node_identity.rs"]
mod tests;
