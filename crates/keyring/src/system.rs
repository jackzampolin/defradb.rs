//! System keyring using OS-provided key management
//!
//! Uses the operating system's native keyring service:
//! - macOS: Keychain
//! - Linux: Secret Service (GNOME Keyring, KWallet)
//! - Windows: Credential Manager

use zeroize::Zeroizing;

use crate::error::{Error, Result};
use crate::keyring::Keyring;

use keyring_crate::Entry;

/// System keyring that uses the OS-provided key management service.
///
/// Keys are stored using base64 encoding since OS keyrings typically
/// expect string values. This matches Go DefraDB's SystemKeyring behavior.
pub struct SystemKeyring {
    service: String,
}

impl SystemKeyring {
    /// Opens the system keyring with the given service name.
    ///
    /// The service name is used to namespace keys in the OS keyring.
    pub fn open(service: impl Into<String>) -> Self {
        Self {
            service: service.into(),
        }
    }

    fn entry(&self, name: &str) -> std::result::Result<Entry, keyring_crate::Error> {
        Entry::new(&self.service, name)
    }
}

impl Keyring for SystemKeyring {
    fn set(&self, name: &str, key: &[u8]) -> Result<()> {
        use base64::Engine;
        let encoded = base64::engine::general_purpose::STANDARD.encode(key);

        let entry = self
            .entry(name)
            .map_err(|e| Error::SystemKeyring(format!("failed to create entry: {}", e)))?;

        entry
            .set_password(&encoded)
            .map_err(|e| Error::SystemKeyring(format!("failed to store key: {}", e)))
    }

    fn get(&self, name: &str) -> Result<Zeroizing<Vec<u8>>> {
        use base64::Engine;

        let entry = self
            .entry(name)
            .map_err(|e| Error::SystemKeyring(format!("failed to create entry: {}", e)))?;

        let encoded = entry.get_password().map_err(|e| match e {
            keyring_crate::Error::NoEntry => Error::NotFound(name.to_string()),
            _ => Error::SystemKeyring(format!("failed to retrieve key: {}", e)),
        })?;

        let decoded = base64::engine::general_purpose::STANDARD
            .decode(&encoded)
            .map_err(|e| Error::Decryption(format!("invalid base64: {}", e)))?;
        Ok(Zeroizing::new(decoded))
    }

    fn delete(&self, name: &str) -> Result<()> {
        let entry = self
            .entry(name)
            .map_err(|e| Error::SystemKeyring(format!("failed to create entry: {}", e)))?;

        entry.delete_credential().map_err(|e| match e {
            keyring_crate::Error::NoEntry => Error::NotFound(name.to_string()),
            _ => Error::SystemKeyring(format!("failed to delete key: {}", e)),
        })
    }

    fn list(&self) -> Result<Vec<String>> {
        Err(Error::SystemKeyringListNotSupported)
    }
}
