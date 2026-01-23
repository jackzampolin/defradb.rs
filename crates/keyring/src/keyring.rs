
//! Core Keyring trait

use crate::error::Result;

/// Keyring provides a simple set/get interface for a keyring service.
///
/// Keys are stored as raw bytes and can be used for cryptographic operations.
/// Different backends (file-based, OS keyring) implement this trait.
pub trait Keyring: Send + Sync {
    /// Stores the given key in the keystore under the given name.
    ///
    /// If a key with the given name already exists it will be overridden.
    fn set(&self, name: &str, key: &[u8]) -> Result<()>;

    /// Returns the key with the given name from the keystore.
    ///
    /// Returns `Error::NotFound` if no key with that name exists.
    fn get(&self, name: &str) -> Result<Vec<u8>>;

    /// Removes the key with the given name from the keystore.
    ///
    /// Returns `Error::NotFound` if no key with that name exists.
    fn delete(&self, name: &str) -> Result<()>;

    /// Returns a list of all key names in the keyring.
    ///
    /// Returns `Error::SystemKeyringListNotSupported` if the backend does not
    /// support listing keys (e.g., OS system keyring).
    fn list(&self) -> Result<Vec<String>>;
}
