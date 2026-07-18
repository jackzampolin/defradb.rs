use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};

static SNAPSHOT_SEQUENCE: AtomicUsize = AtomicUsize::new(0);

/// Build and snapshot a CLI feature variant without racing other test processes.
pub fn build_cli_variant(workspace: &Path, features: &[&str], output_name: &str) -> PathBuf {
    let target_dir = workspace.join("target/debug");
    fs::create_dir_all(&target_dir).expect("failed to create target directory");

    let build_lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(target_dir.join(".defra-build.lock"))
        .expect("failed to open defra build lock");
    build_lock
        .lock()
        .expect("failed to acquire defra build lock");

    let mut command = Command::new("cargo");
    command.args(["build", "-p", "cli"]);
    if !features.is_empty() {
        command.args(["--features", &features.join(",")]);
    }

    let status = command
        .current_dir(workspace)
        .status()
        .expect("failed to build defra test binary");
    assert!(status.success(), "cargo build -p cli failed");

    let source = target_dir.join(format!("defra{}", std::env::consts::EXE_SUFFIX));
    let destination = target_dir.join(format!("{output_name}{}", std::env::consts::EXE_SUFFIX));
    let temporary = temporary_snapshot_path(&target_dir, output_name);

    fs::copy(&source, &temporary).expect("failed to snapshot defra test binary");
    publish_snapshot(temporary, destination)
}

fn temporary_snapshot_path(target_dir: &Path, output_name: &str) -> PathBuf {
    let sequence = SNAPSHOT_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    target_dir.join(format!(
        "{output_name}-{}-{sequence}{}",
        std::process::id(),
        std::env::consts::EXE_SUFFIX
    ))
}

fn publish_snapshot(temporary: PathBuf, destination: PathBuf) -> PathBuf {
    match fs::rename(&temporary, &destination) {
        Ok(()) => destination,
        Err(error) => {
            assert!(
                temporary.is_file(),
                "failed to publish defra test binary and temporary snapshot disappeared: {error}"
            );
            eprintln!(
                "failed to publish defra test binary to {}; using {}: {error}",
                destination.display(),
                temporary.display()
            );
            temporary
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_paths_are_unique_within_a_process() {
        let directory = tempfile::tempdir().expect("temporary directory");

        let first = temporary_snapshot_path(directory.path(), "defra-iroh");
        let second = temporary_snapshot_path(directory.path(), "defra-iroh");

        assert_ne!(first, second);
    }

    #[test]
    fn publishes_snapshot_to_stable_destination() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let temporary = directory.path().join("defra-iroh-123");
        let destination = directory.path().join("defra-iroh");
        fs::write(&temporary, b"snapshot").expect("write snapshot");

        let published = publish_snapshot(temporary.clone(), destination.clone());

        assert_eq!(published, destination);
        assert_eq!(
            fs::read(published).expect("read published binary"),
            b"snapshot"
        );
        assert!(!temporary.exists());
    }

    #[test]
    fn retains_temporary_snapshot_when_destination_cannot_be_replaced() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let temporary = directory.path().join("defra-iroh-123");
        let destination = directory.path().join("defra-iroh");
        fs::write(&temporary, b"snapshot").expect("write snapshot");
        fs::create_dir(&destination).expect("create non-replaceable destination");

        let published = publish_snapshot(temporary.clone(), destination.clone());

        assert_eq!(published, temporary);
        assert_eq!(
            fs::read(published).expect("read retained binary"),
            b"snapshot"
        );
        assert!(destination.is_dir());
    }
}
