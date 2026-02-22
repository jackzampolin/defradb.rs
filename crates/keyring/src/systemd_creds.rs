//! systemd-creds keyring backend
//!
//! Encrypts credentials via the `systemd-creds` binary with automatic key selection.
//! Each key is stored as a separate `.cred` file in a directory with 0700 permissions.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use zeroize::Zeroizing;

use crate::error::{Error, Result};
use crate::key_name::KeyName;
use crate::keyring::Keyring;

/// Keyring backend using `systemd-creds` for encryption.
pub struct SystemdCredsKeyring {
    dir: PathBuf,
}

impl SystemdCredsKeyring {
    /// Opens or creates a systemd-creds keyring in the given directory.
    ///
    /// Returns an error if `systemd-creds` is not available on the system.
    /// The directory is created if it does not exist. Permissions are set to 0700.
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
        Ok(self.dir.join(format!("{}.cred", name)))
    }

    /// Run a systemd-creds subcommand, piping `input` to stdin and returning stdout.
    fn run(&self, args: &[&str], operation: &str, input: &[u8]) -> Result<Vec<u8>> {
        let make_err = |msg: String| -> Error {
            if operation == "encrypt" {
                Error::Encryption(msg)
            } else {
                Error::Decryption(msg)
            }
        };

        let mut child = Command::new("systemd-creds")
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| Error::SystemdCreds(format!("failed to run systemd-creds: {}", e)))?;

        let write_result = {
            let mut stdin = child
                .stdin
                .take()
                .ok_or_else(|| Error::SystemdCreds("failed to open stdin pipe".to_string()))?;
            stdin.write_all(input)
        };

        let output = child
            .wait_with_output()
            .map_err(|e| make_err(format!("systemd-creds {} failed: {}", operation, e)))?;

        // Check write result first — a write failure is the root cause
        // if the process also failed.
        if let Err(e) = write_result {
            return Err(make_err(format!(
                "failed to write to systemd-creds stdin: {}",
                e
            )));
        }

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(make_err(format!(
                "systemd-creds {} failed: {}",
                operation,
                stderr.trim()
            )));
        }

        Ok(output.stdout)
    }

    fn encrypt(&self, data: &[u8]) -> Result<Vec<u8>> {
        let output = self.run(
            &["encrypt", "--with-key=auto", "--name=", "-", "-"],
            "encrypt",
            data,
        )?;

        // Encrypted ciphertext is never empty.
        if output.is_empty() {
            return Err(Error::Encryption(
                "systemd-creds encrypt produced empty output".to_string(),
            ));
        }

        Ok(output)
    }

    fn decrypt(&self, data: &[u8]) -> Result<Vec<u8>> {
        self.run(&["decrypt", "--name=", "-", "-"], "decrypt", data)
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
        file.sync_all()?;

        Ok(())
    }

    fn get(&self, name: &str) -> Result<Zeroizing<Vec<u8>>> {
        let path = self.key_path(name)?;
        let encrypted = fs::read(&path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                Error::NotFound(name.to_string())
            } else {
                Error::Io(e)
            }
        })?;
        let decrypted = self.decrypt(&encrypted)?;
        Ok(Zeroizing::new(decrypted))
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
                let Some(file_name) = file_name.to_str() else {
                    tracing::warn!(
                        "skipping credential file with non-UTF-8 filename in {}",
                        self.dir.display()
                    );
                    continue;
                };
                if let Some(name) = file_name.strip_suffix(".cred") {
                    if KeyName::validate(name).is_ok() {
                        keys.push(name.to_string());
                    } else {
                        tracing::warn!(
                            file = file_name,
                            "skipping .cred file with invalid key name"
                        );
                    }
                }
            }
        }
        Ok(keys)
    }
}

/// Returns true if the `systemd-creds` binary is available on this system.
pub fn systemd_creds_available() -> bool {
    match Command::new("systemd-creds")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
    {
        Ok(status) => status.success(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
        Err(e) => {
            tracing::warn!("systemd-creds availability check failed: {}", e);
            false
        }
    }
}
