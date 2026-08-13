//! File-based keyring with JWE encryption
//!
//! Uses raw JWE compact serialization for Go DefraDB compatibility.
//! The Go implementation uses github.com/lestrrat-go/jwx/v2/jwe with
//! PBES2_HS512_A256KW algorithm.

use std::fs;
use std::path::{Path, PathBuf};

use zeroize::Zeroizing;

use crate::error::{Error, Result};
use crate::jwe;
use crate::key_name::KeyName;
use crate::keyring::Keyring;

/// File-based keyring that stores keys in encrypted files using JWE.
///
/// Each key is stored as a separate file in the directory, encrypted using
/// PBES2-HS512-A256KW (Password-Based Encryption Scheme 2 with HMAC-SHA-512
/// and AES-256-KW for key wrapping) with A256GCM content encryption.
///
/// The file format is compatible with Go DefraDB's file keyring.
pub struct FileKeyring {
    dir: PathBuf,
    /// Password is wrapped in Zeroizing to ensure it is cleared from memory on drop
    password: Zeroizing<Vec<u8>>,
}

impl FileKeyring {
    /// Opens or creates a file keyring in the given directory.
    ///
    /// The directory will be created if it does not exist with restrictive
    /// permissions (0700 on Unix) to protect key material.
    pub fn open(dir: impl AsRef<Path>, password: impl Into<Vec<u8>>) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        // Set restrictive permissions on Unix (owner only)
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&dir, fs::Permissions::from_mode(0o700))?;
        }

        #[cfg(windows)]
        {
            restrict_owner_access(&dir, WindowsObjectKind::Directory)?;

            for entry in fs::read_dir(&dir)? {
                let entry = entry?;
                if entry.file_type()?.is_file() {
                    restrict_owner_access(&entry.path(), WindowsObjectKind::File)?;
                }
            }
        }

        Ok(Self {
            dir,
            password: Zeroizing::new(password.into()),
        })
    }

    fn key_path(&self, name: &str) -> Result<PathBuf> {
        KeyName::validate(name)?;
        Ok(self.dir.join(name))
    }

    fn encrypt(&self, data: &[u8]) -> Result<Vec<u8>> {
        let token = jwe::encrypt(&self.password, data)?;
        Ok(token.into_bytes())
    }

    fn decrypt(&self, cipher: &[u8]) -> Result<Vec<u8>> {
        let token = std::str::from_utf8(cipher)
            .map_err(|e| Error::Decryption(format!("invalid token format: {}", e)))?;
        jwe::decrypt(&self.password, token)
    }
}

impl Keyring for FileKeyring {
    fn set(&self, name: &str, key: &[u8]) -> Result<()> {
        let path = self.key_path(name)?;
        let cipher = self.encrypt(key)?;

        // Set restrictive permissions on key files (owner only)
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .mode(0o600)
                .open(&path)?;
            std::io::Write::write_all(&mut file, &cipher)?;
        }

        #[cfg(not(unix))]
        {
            fs::write(&path, cipher)?;

            #[cfg(windows)]
            restrict_owner_access(&path, WindowsObjectKind::File)?;
        }

        Ok(())
    }

    fn get(&self, name: &str) -> Result<Zeroizing<Vec<u8>>> {
        let path = self.key_path(name)?;
        let cipher = fs::read(&path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                Error::NotFound(name.to_string())
            } else {
                Error::Io(e)
            }
        })?;
        let decrypted = self.decrypt(&cipher)?;
        Ok(Zeroizing::new(decrypted))
    }

    fn delete(&self, name: &str) -> Result<()> {
        let path = self.key_path(name)?;
        // Overwrite file contents with zeros before unlinking.
        // Defense-in-depth: prevents trivial recovery from filesystem journals.
        // (SSD wear-leveling may retain old blocks regardless.)
        if let Ok(metadata) = fs::metadata(&path) {
            let zeros = vec![0u8; metadata.len() as usize];
            let _ = fs::write(&path, &zeros);
        }
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
                if let Some(name) = entry.file_name().to_str() {
                    // Only include valid key names (filter out any hidden files etc.)
                    if KeyName::validate(name).is_ok() {
                        keys.push(name.to_string());
                    }
                }
            }
        }
        Ok(keys)
    }
}

#[cfg(windows)]
#[derive(Clone, Copy)]
enum WindowsObjectKind {
    Directory,
    File,
}

#[cfg(windows)]
fn restrict_owner_access(path: &Path, kind: WindowsObjectKind) -> Result<()> {
    use std::process::Command;

    let mut grant = String::from("*S-1-3-4:");
    grant.push_str(match kind {
        WindowsObjectKind::Directory => "(OI)(CI)F",
        WindowsObjectKind::File => "F",
    });

    let status = Command::new("icacls")
        .arg(path)
        .arg("/inheritance:r")
        .arg("/grant:r")
        .arg(&grant)
        .status()?;

    if status.success() {
        Ok(())
    } else {
        Err(Error::Io(std::io::Error::other(format!(
            "failed to restrict ACLs with icacls for {}",
            path.display()
        ))))
    }
}
