//! CLI subcommands

use std::path::PathBuf;

pub mod client;
mod identity;
mod keyring_cmd;
mod sdl;
mod server_dump;
pub mod start;
mod version;

pub use client::ClientArgs;
pub use identity::IdentityArgs;
pub use keyring_cmd::KeyringArgs;
pub use sdl::SdlArgs;
pub use server_dump::ServerDumpArgs;
pub use start::{Node, StartArgs};
pub use version::VersionArgs;

use crate::config::Config;
use crate::error::{Error, Result};

/// Open the appropriate keyring based on config.
///
/// On Linux, the System backend falls back to systemd-creds
/// when no D-Bus session is available.
pub(crate) fn open_keyring(config: &Config) -> Result<Box<dyn keyring::Keyring>> {
    use crate::config::KeyringBackend;

    if config.keyring.disabled {
        return Err(Error::Keyring("keyring is disabled".to_string()));
    }

    match config.keyring.backend {
        KeyringBackend::File => {
            let path = resolve_keyring_path(config)?;
            let secret =
                keyring::load_secret_from_env().map_err(|e| Error::Keyring(e.to_string()))?;
            let kr = keyring::FileKeyring::open(&path, secret)
                .map_err(|e| Error::Keyring(e.to_string()))?;
            Ok(Box::new(kr))
        }
        KeyringBackend::System => {
            #[cfg(target_os = "linux")]
            {
                if std::env::var("DBUS_SESSION_BUS_ADDRESS").is_ok_and(|v| !v.is_empty()) {
                    Ok(Box::new(keyring::SystemKeyring::open(
                        &config.keyring.namespace,
                    )))
                } else if keyring::systemd_creds_available() {
                    eprintln!(
                        "WARNING: no D-Bus session available; using systemd-creds keyring \
                         instead of secret-service. Keys are stored as .cred files in {:?}.",
                        resolve_keyring_path(config).unwrap_or_default()
                    );
                    tracing::warn!(
                        "no D-Bus session available; falling back to systemd-creds keyring"
                    );
                    open_systemd_creds(config)
                } else {
                    Err(Error::Keyring(
                        "no system keyring available: no D-Bus session for secret-service \
                         and systemd-creds not found; use --keyring-backend file"
                            .to_string(),
                    ))
                }
            }
            #[cfg(not(target_os = "linux"))]
            {
                Ok(Box::new(keyring::SystemKeyring::open(
                    &config.keyring.namespace,
                )))
            }
        }
        KeyringBackend::SystemdCreds => {
            #[cfg(target_os = "linux")]
            {
                open_systemd_creds(config)
            }
            #[cfg(not(target_os = "linux"))]
            {
                Err(Error::Keyring(
                    "systemd-creds backend is only available on Linux".to_string(),
                ))
            }
        }
    }
}

#[cfg(target_os = "linux")]
fn open_systemd_creds(config: &Config) -> Result<Box<dyn keyring::Keyring>> {
    let path = resolve_keyring_path(config)?;
    let kr =
        keyring::SystemdCredsKeyring::open(&path).map_err(|e| Error::Keyring(e.to_string()))?;
    Ok(Box::new(kr))
}

fn resolve_keyring_path(config: &Config) -> Result<PathBuf> {
    let path = PathBuf::from(&config.keyring.path);
    if path.is_absolute() {
        Ok(path)
    } else {
        Ok(config.rootdir.join(path))
    }
}
