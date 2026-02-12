//! systemd-creds keyring backend for headless Linux servers
//!
//! Encrypts credentials using TPM2 or host key via the `systemd-creds` binary.
//! Each key is stored as a separate `.cred` file in a directory with 0700 permissions.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use crate::error::{Error, Result};
use crate::key_name::KeyName;
use crate::keyring::Keyring;

const CRED_EXTENSION: &str = "cred";

/// Keyring backend that uses `systemd-creds` for encryption.
///
/// Stores each key as a `.cred` file encrypted with TPM2 or a host key.
/// Requires the `systemd-creds` binary to be available on the system.
pub struct SystemdCredsKeyring {
    dir: PathBuf,
}

impl SystemdCredsKeyring {
    /// Opens or creates a systemd-creds keyring in the given directory.
    ///
    /// Returns an error if `systemd-creds` is not available on the system.
    /// The directory is created with 0700 permissions if it does not exist.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        if !systemd_creds_available() {
            return Err(Error::SystemdCreds(
                "systemd-creds not found; install systemd 250+ or use a different backend"
                    .to_string(),
            ));
        }

        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o700))?;

        Ok(Self { dir })
    }

    fn key_path(&self, name: &str) -> Result<PathBuf> {
        KeyName::validate(name)?;
        Ok(self.dir.join(format!("{}.{}", name, CRED_EXTENSION)))
    }

    fn encrypt(&self, data: &[u8]) -> Result<Vec<u8>> {
        let mut child = Command::new("systemd-creds")
            .args(["encrypt", "--with-key=auto", "--name=", "-", "-"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| Error::SystemdCreds(format!("failed to run systemd-creds: {}", e)))?;

        child
            .stdin
            .as_mut()
            .expect("stdin was piped")
            .write_all(data)
            .map_err(|e| Error::Encryption(format!("failed to write to systemd-creds: {}", e)))?;

        let output = child
            .wait_with_output()
            .map_err(|e| Error::Encryption(format!("systemd-creds encrypt failed: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::Encryption(format!(
                "systemd-creds encrypt failed: {}",
                stderr.trim()
            )));
        }

        Ok(output.stdout)
    }

    fn decrypt(&self, data: &[u8]) -> Result<Vec<u8>> {
        let mut child = Command::new("systemd-creds")
            .args(["decrypt", "--name=", "-", "-"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| Error::SystemdCreds(format!("failed to run systemd-creds: {}", e)))?;

        child
            .stdin
            .as_mut()
            .expect("stdin was piped")
            .write_all(data)
            .map_err(|e| Error::Decryption(format!("failed to write to systemd-creds: {}", e)))?;

        let output = child
            .wait_with_output()
            .map_err(|e| Error::Decryption(format!("systemd-creds decrypt failed: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::Decryption(format!(
                "systemd-creds decrypt failed: {}",
                stderr.trim()
            )));
        }

        Ok(output.stdout)
    }
}

impl Keyring for SystemdCredsKeyring {
    fn set(&self, name: &str, key: &[u8]) -> Result<()> {
        let path = self.key_path(name)?;
        let encrypted = self.encrypt(key)?;

        use std::os::unix::fs::OpenOptionsExt;
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&path)?;
        file.write_all(&encrypted)?;

        Ok(())
    }

    fn get(&self, name: &str) -> Result<Vec<u8>> {
        let path = self.key_path(name)?;
        let encrypted = fs::read(&path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                Error::NotFound(name.to_string())
            } else {
                Error::Io(e)
            }
        })?;
        self.decrypt(&encrypted)
    }

    fn delete(&self, name: &str) -> Result<()> {
        let path = self.key_path(name)?;
        fs::remove_file(&path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                Error::NotFound(name.to_string())
            } else {
                Error::Io(e)
            }
        })
    }

    fn list(&self) -> Result<Vec<String>> {
        let entries = fs::read_dir(&self.dir)?;
        let mut keys = Vec::new();
        for entry in entries {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                let file_name = entry.file_name();
                let file_name = file_name.to_string_lossy();
                if let Some(name) = file_name.strip_suffix(&format!(".{}", CRED_EXTENSION)) {
                    if KeyName::validate(name).is_ok() {
                        keys.push(name.to_string());
                    }
                }
            }
        }
        Ok(keys)
    }
}

/// Returns true if the `systemd-creds` binary is available on this system.
pub fn systemd_creds_available() -> bool {
    Command::new("systemd-creds")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}
