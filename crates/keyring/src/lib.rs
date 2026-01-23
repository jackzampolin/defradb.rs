
//! Keyring and key management for DefraDB
//!
//! Provides secure storage for cryptographic keys with support for multiple backends:
//! - File-based storage with JWE encryption (PBES2-HS512-A256KW)
//! - System keyring (OS-provided key management)

mod error;
mod file;
mod key_name;
mod keyring;
mod signer;
mod system;

pub use error::{Error, Result};
pub use file::FileKeyring;
pub use key_name::KeyName;
pub use keyring::Keyring;
#[allow(deprecated)]
pub use signer::KeyringSigner;
pub use signer::{KeyHandle, KeyType};
pub use system::SystemKeyring;

/// Environment variable name for the keyring secret
pub const KEYRING_SECRET_ENV: &str = "DEFRA_KEYRING_SECRET";

/// Standard key name for peer identity (Ed25519 64-byte full keypair)
pub const PEER_KEY: &str = "peer-key";

/// Standard key name for data encryption (AES-256)
pub const ENCRYPTION_KEY: &str = "encryption-key";

/// Standard key name for searchable encryption
pub const SEARCHABLE_ENCRYPTION_KEY: &str = "searchable-encryption-key";

/// Loads the keyring secret from the DEFRA_KEYRING_SECRET environment variable.
///
/// Returns `Error::SecretNotSet` if the environment variable is not set.
pub fn load_secret_from_env() -> Result<Vec<u8>> {
    std::env::var(KEYRING_SECRET_ENV)
        .map(|s| s.into_bytes())
        .map_err(|_| Error::SecretNotSet)
}

/// Opens a file keyring using the secret from DEFRA_KEYRING_SECRET.
///
/// Convenience function that combines `load_secret_from_env` with `FileKeyring::open`.
pub fn open_file_keyring(dir: impl AsRef<std::path::Path>) -> Result<FileKeyring> {
    let secret = load_secret_from_env()?;
    FileKeyring::open(dir, secret)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_load_secret_from_env() {
        let test_secret = "my-test-secret";

        env::set_var(KEYRING_SECRET_ENV, test_secret);
        let result = load_secret_from_env();
        env::remove_var(KEYRING_SECRET_ENV);

        assert_eq!(result.unwrap(), test_secret.as_bytes());
    }

    #[test]
    fn test_load_secret_from_env_not_set() {
        env::remove_var(KEYRING_SECRET_ENV);
        let result = load_secret_from_env();
        assert!(matches!(result, Err(Error::SecretNotSet)));
    }

    #[test]
    fn test_open_file_keyring_with_env() {
        let temp_dir = tempfile::tempdir().unwrap();
        let test_secret = "test-keyring-secret";

        env::set_var(KEYRING_SECRET_ENV, test_secret);
        let keyring = open_file_keyring(temp_dir.path());
        env::remove_var(KEYRING_SECRET_ENV);

        assert!(keyring.is_ok());

        let keyring = keyring.unwrap();
        keyring.set("test", b"data").unwrap();
        assert_eq!(keyring.get("test").unwrap(), b"data");
    }
}
