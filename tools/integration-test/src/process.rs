use std::fs::{self, File};
use std::path::Path;
use std::process::{Child, Command, Stdio};

use anyhow::{Context, Result};

/// A child process that is killed on drop.
///
/// Drop sends SIGTERM, waits up to 500ms, then SIGKILL.
pub struct ManagedProcess {
    name: String,
    child: Option<Child>,
}

impl ManagedProcess {
    /// Spawn a command with stdout/stderr redirected to log files.
    pub fn spawn(name: &str, mut cmd: Command, log_dir: &Path) -> Result<Self> {
        fs::create_dir_all(log_dir)
            .with_context(|| format!("failed to create log dir {}", log_dir.display()))?;

        let stdout_path = log_dir.join("stdout.log");
        let stderr_path = log_dir.join("stderr.log");

        let stdout_file = File::create(&stdout_path)
            .with_context(|| format!("failed to create {}", stdout_path.display()))?;
        let stderr_file = File::create(&stderr_path)
            .with_context(|| format!("failed to create {}", stderr_path.display()))?;

        cmd.stdout(Stdio::from(stdout_file));
        cmd.stderr(Stdio::from(stderr_file));

        let child = cmd
            .spawn()
            .with_context(|| format!("failed to spawn {}", name))?;

        tracing::info!("{}: spawned pid={}", name, child.id());

        Ok(Self {
            name: name.to_string(),
            child: Some(child),
        })
    }

    /// Create an empty placeholder (no child process).
    pub fn empty() -> Self {
        Self {
            name: String::new(),
            child: None,
        }
    }

    pub fn id(&self) -> Option<u32> {
        self.child.as_ref().map(|c| c.id())
    }
}

impl Drop for ManagedProcess {
    fn drop(&mut self) {
        let Some(mut child) = self.child.take() else {
            return;
        };

        let pid = child.id();
        tracing::info!("{}: sending SIGTERM to pid={}", self.name, pid);

        // Send SIGTERM
        unsafe {
            libc::kill(pid as libc::pid_t, libc::SIGTERM);
        }

        // Poll for exit over 500ms (25 iterations x 20ms)
        for _ in 0..25 {
            match child.try_wait() {
                Ok(Some(status)) => {
                    tracing::info!("{}: exited with {}", self.name, status);
                    return;
                }
                Ok(None) => {}
                Err(e) => {
                    tracing::warn!("{}: error polling: {}", self.name, e);
                    break;
                }
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }

        tracing::warn!("{}: sending SIGKILL to pid={}", self.name, pid);
        let _ = child.kill();
        let _ = child.wait();
    }
}
