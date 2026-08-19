//! Optional cooperation with `scripts/bench-perf.sh`.
//!
//! The normal benchmark path does not touch the filesystem or wait on an
//! external process.  When `PIR_POC_PERF_GATE_DIR` and the dense-batch phase
//! selectors are present, the selected server workers publish their Linux
//! thread IDs and stop at a barrier.  The external runner attaches disabled
//! `perf stat` collectors, acknowledges that they are ready, and only then are
//! the workers released.  Each worker enables/disables its own collector
//! immediately around the evaluator call.

use std::{
    env,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    sync::{Arc, Barrier},
    thread,
    time::{Duration, Instant},
};

use anyhow::{bail, Context, Result};
use serde::Serialize;

const GATE_DIRECTORY_ENV: &str = "PIR_POC_PERF_GATE_DIR";
const DEFAULT_TIMEOUT_SECONDS: u64 = 120;

#[derive(Serialize)]
struct DenseBatchPhase<'a> {
    schema: &'static str,
    benchmark: &'static str,
    profile: &'a str,
    batch_size: usize,
    server_count: usize,
    kernel: &'a str,
    sample_index: usize,
    counter_scope: &'static str,
    aggregate_scope: &'static str,
}

/// One externally collected dense-batch server-evaluation phase.
pub(super) struct ServerPerfPhase {
    directory: PathBuf,
    server_count: usize,
    ready: Barrier,
    start: Barrier,
    timeout: Duration,
}

impl ServerPerfPhase {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn dense_batch(
        profile: &str,
        batch_size: usize,
        server_count: usize,
        kernel: &'static str,
        sample_index: usize,
    ) -> Result<Option<Arc<Self>>> {
        let Some(directory) = env::var_os(GATE_DIRECTORY_ENV).map(PathBuf::from) else {
            return Ok(None);
        };
        if !selector_matches("PIR_POC_PERF_PROFILE", profile)
            || !selector_matches_usize("PIR_POC_PERF_BATCH_SIZE", batch_size)?
            || !selector_matches_usize("PIR_POC_PERF_SERVER_COUNT", server_count)?
            || !selector_matches("PIR_POC_PERF_KERNEL", kernel)
            || !selector_matches_usize("PIR_POC_PERF_SAMPLE_INDEX", sample_index)?
        {
            return Ok(None);
        }

        fs::create_dir_all(&directory)
            .with_context(|| format!("create perf gate directory {}", directory.display()))?;
        let timeout = Duration::from_secs(
            env::var("PIR_POC_PERF_GATE_TIMEOUT_SECONDS")
                .ok()
                .map(|value| value.parse::<u64>())
                .transpose()
                .context("PIR_POC_PERF_GATE_TIMEOUT_SECONDS must be an integer")?
                .unwrap_or(DEFAULT_TIMEOUT_SECONDS),
        );
        let phase = DenseBatchPhase {
            schema: "defradb-pir-server-phase-v1",
            benchmark: "bench-dense-batch",
            profile,
            batch_size,
            server_count,
            kernel,
            sample_index,
            counter_scope:
                "one server worker thread, enabled immediately around BatchEvaluator::evaluate",
            aggregate_scope:
                "from release of all ready replicas through completion of all replicas",
        };
        write_atomic(
            &directory.join("phase.json"),
            &serde_json::to_vec_pretty(&phase).context("serialize perf phase")?,
        )?;

        Ok(Some(Arc::new(Self {
            directory,
            server_count,
            ready: Barrier::new(server_count + 1),
            start: Barrier::new(server_count + 1),
            timeout,
        })))
    }

    /// Publish this worker's TID, wait for every collector, then enable only
    /// this worker's counters.  Setup failures still cross both barriers so a
    /// bad collector cannot deadlock the scoped worker set.
    pub(super) fn begin_server(&self, server_index: usize) -> Result<ServerCounterGuard> {
        let setup = self.prepare_server_control(server_index);
        self.ready.wait();
        self.start.wait();
        let mut control = setup?;
        control.command("enable")?;
        Ok(ServerCounterGuard {
            control: Some(control),
        })
    }

    /// Called by the coordinator after all worker handles have been spawned.
    /// Package/uncore events, when configured, are enabled before workers are
    /// released.  The start barrier is crossed even on error.
    pub(super) fn start_envelope(&self) -> Result<Option<PerfControl>> {
        self.ready.wait();
        let setup = self.wait_for_collectors().and_then(|()| {
            if self.directory.join("aggregate.enabled").exists() {
                let mut control = PerfControl::open(
                    &self.directory.join("aggregate.ctl"),
                    &self.directory.join("aggregate.ack"),
                )?;
                control.command("enable")?;
                Ok(Some(control))
            } else {
                Ok(None)
            }
        });
        self.start.wait();
        setup
    }

    pub(super) fn finish_envelope(&self, mut control: Option<PerfControl>) -> Result<()> {
        if let Some(control) = &mut control {
            control.command("disable")?;
        }
        write_atomic(&self.directory.join("phase.done"), b"done\n")
    }

    fn prepare_server_control(&self, server_index: usize) -> Result<PerfControl> {
        if server_index >= self.server_count {
            bail!(
                "perf server index {server_index} exceeds configured count {}",
                self.server_count
            );
        }
        let tid = linux_thread_id()?;
        write_atomic(
            &self.directory.join(format!("server-{server_index}.tid")),
            format!("{tid}\n").as_bytes(),
        )?;
        self.wait_for_path(&self.directory.join("collectors.ready"))?;
        PerfControl::open(
            &self.directory.join(format!("server-{server_index}.ctl")),
            &self.directory.join(format!("server-{server_index}.ack")),
        )
    }

    fn wait_for_collectors(&self) -> Result<()> {
        self.wait_for_path(&self.directory.join("collectors.ready"))
    }

    fn wait_for_path(&self, path: &Path) -> Result<()> {
        let started = Instant::now();
        while !path.exists() {
            if started.elapsed() >= self.timeout {
                bail!("timed out waiting for perf gate {}", path.display());
            }
            thread::sleep(Duration::from_millis(1));
        }
        Ok(())
    }
}

pub(super) struct ServerCounterGuard {
    control: Option<PerfControl>,
}

impl ServerCounterGuard {
    /// Disable before the worker returns so the collector cannot include
    /// reconstruction, joining, or later benchmark phases.
    pub(super) fn finish(mut self) -> Result<()> {
        if let Some(control) = &mut self.control {
            control.command("disable")?;
        }
        self.control = None;
        Ok(())
    }
}

pub(super) struct PerfControl {
    command: File,
    acknowledgement: BufReader<File>,
}

impl PerfControl {
    fn open(command_path: &Path, acknowledgement_path: &Path) -> Result<Self> {
        let command = OpenOptions::new()
            .write(true)
            .open(command_path)
            .with_context(|| format!("open perf control FIFO {}", command_path.display()))?;
        let acknowledgement = OpenOptions::new()
            .read(true)
            .open(acknowledgement_path)
            .with_context(|| {
                format!(
                    "open perf acknowledgement FIFO {}",
                    acknowledgement_path.display()
                )
            })?;
        Ok(Self {
            command,
            acknowledgement: BufReader::new(acknowledgement),
        })
    }

    fn command(&mut self, command: &str) -> Result<()> {
        writeln!(self.command, "{command}").context("write perf control command")?;
        self.command.flush().context("flush perf control command")?;
        let mut acknowledgement = String::new();
        self.acknowledgement
            .read_line(&mut acknowledgement)
            .context("read perf control acknowledgement")?;
        // perf 7.0's FIFO acknowledgement includes a trailing NUL on this
        // runner. `BufRead::read_line` retains it for the next command, so
        // accept only ASCII whitespace/NUL framing around the literal token.
        let acknowledgement = acknowledgement
            .trim_matches(|character: char| character == '\0' || character.is_ascii_whitespace());
        if acknowledgement != "ack" {
            bail!(
                "perf did not acknowledge {command:?}; received {:?}",
                acknowledgement
            );
        }
        Ok(())
    }
}

fn selector_matches(name: &str, actual: &str) -> bool {
    env::var(name).is_ok_and(|expected| expected == actual)
}

fn selector_matches_usize(name: &str, actual: usize) -> Result<bool> {
    let Some(expected) = env::var_os(name) else {
        return Ok(false);
    };
    Ok(expected
        .to_string_lossy()
        .parse::<usize>()
        .with_context(|| format!("{name} must be an integer"))?
        == actual)
}

fn linux_thread_id() -> Result<u64> {
    let target = fs::read_link("/proc/thread-self")
        .context("read /proc/thread-self (perf gate requires Linux procfs)")?;
    target
        .file_name()
        .context("/proc/thread-self target has no thread ID")?
        .to_string_lossy()
        .parse::<u64>()
        .context("parse Linux thread ID")
}

fn write_atomic(path: &Path, contents: &[u8]) -> Result<()> {
    let temporary = path.with_extension("tmp");
    fs::write(&temporary, contents)
        .with_context(|| format!("write temporary perf gate file {}", temporary.display()))?;
    fs::rename(&temporary, path)
        .with_context(|| format!("publish perf gate file {}", path.display()))
}
