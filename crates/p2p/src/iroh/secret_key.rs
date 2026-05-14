/// Load an iroh secret key from disk, or generate and persist a new one.
///
/// - If `path` is `Some` and the file exists, reads the 32-byte key from it.
/// - If `path` is `Some` and the file does not exist, generates a new key,
///   creates parent directories as needed, writes the key, and restricts
///   file permissions to owner-only (0o600 on Unix).
/// - If `path` is `None`, generates an ephemeral key (not persisted).
pub async fn load_or_generate_secret_key(
    path: Option<&std::path::Path>,
) -> anyhow::Result<iroh::SecretKey> {
    use anyhow::{anyhow, Context};

    match path {
        Some(path) if path.exists() => {
            let bytes = tokio::fs::read(path)
                .await
                .with_context(|| format!("failed to read iroh secret key '{}'", path.display()))?;
            let array: [u8; 32] = bytes
                .try_into()
                .map_err(|_| anyhow!("iroh secret key file must contain exactly 32 bytes"))?;
            Ok(iroh::SecretKey::from_bytes(&array))
        }
        Some(path) => {
            let key = iroh::SecretKey::generate();
            if let Some(parent) = path.parent() {
                tokio::fs::create_dir_all(parent).await.with_context(|| {
                    format!("failed to create iroh key directory '{}'", parent.display())
                })?;
            }
            tokio::fs::write(path, key.to_bytes())
                .await
                .with_context(|| format!("failed to write iroh secret key '{}'", path.display()))?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
                    .await
                    .with_context(|| {
                        format!("failed to set permissions on '{}'", path.display())
                    })?;
            }
            Ok(key)
        }
        None => Ok(iroh::SecretKey::generate()),
    }
}
