//! Provenance for a platform document: who measured, on what, and how quiet
//! the host was.
//!
//! Anything that cannot be read is written as null, never as a plausible
//! default. A run whose host could not be identified says so.

#![allow(dead_code)]

use std::path::Path;
use std::process::Command;

use serde::Serialize;
use serde_json::Value;
use sysinfo::System;

/// The machine and toolchain a platform document was measured on.
#[derive(Debug, Serialize)]
pub struct Host {
    pub cpu: Option<String>,
    pub cores: Option<usize>,
    pub ram_gib: Option<f64>,
    pub os: String,
    pub arch: String,
    /// Which filesystem the stores were opened on, when the runner said.
    pub store: Option<String>,
}

pub fn host() -> Host {
    let mut sys = System::new();
    sys.refresh_memory();
    let cpus = sysinfo::System::new_all();
    Host {
        cpu: cpus.cpus().first().map(|c| c.brand().trim().to_string()),
        cores: std::thread::available_parallelism().ok().map(|n| n.get()),
        ram_gib: match sys.total_memory() {
            0 => None,
            bytes => Some((bytes as f64 / 1_073_741_824.0 * 10.0).round() / 10.0),
        },
        os: std::env::consts::OS.to_string(),
        arch: std::env::consts::ARCH.to_string(),
        store: std::env::var("DEFRA_BENCH_STORE")
            .ok()
            .filter(|s| !s.is_empty()),
    }
}

pub fn toolchain() -> String {
    match Command::new("rustc").arg("--version").output() {
        Ok(o) if o.status.success() => String::from_utf8_lossy(&o.stdout).trim().to_string(),
        Ok(o) => format!("unknown: rustc --version exited {}", o.status),
        Err(e) => format!("unknown: rustc --version failed: {e}"),
    }
}

/// Best guess at the triple when the caller did not name one. Good enough for
/// a local run and never presented as authoritative: CI passes `--target`.
pub fn derived_target() -> String {
    format!(
        "{}-{}-guessed",
        std::env::consts::ARCH,
        std::env::consts::OS
    )
}

/// Whether the host was quiet while the benches ran.
///
/// Fails closed. The guard runs before the benchmarks, in its own CI step, and
/// leaves its verdict in a file: measuring load *after* a benchmark measures
/// the benchmark. When the file is missing or unreadable the host is not
/// certified quiet, so timing families are marked contaminated rather than
/// presumed clean.
#[derive(Debug, Serialize)]
pub struct LoadGuard {
    pub passed: bool,
    pub note: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<Value>,
}

pub fn load_guard(path: Option<&Path>) -> LoadGuard {
    let Some(path) = path else {
        return LoadGuard {
            passed: false,
            note: "No load guard was run, so the host is not certified quiet. Timing families \
                   are marked contaminated; deterministic families stay comparable."
                .into(),
            detail: None,
        };
    };
    let text = match std::fs::read_to_string(path) {
        Ok(t) => t,
        Err(e) => {
            return LoadGuard {
                passed: false,
                note: format!(
                    "could not read the load guard verdict at {}: {e}. The host is not certified \
                     quiet, so timing families are marked contaminated.",
                    path.display()
                ),
                detail: None,
            };
        }
    };
    let verdict: Value = match serde_json::from_str(&text) {
        Ok(v) => v,
        Err(e) => {
            return LoadGuard {
                passed: false,
                note: format!(
                    "the load guard verdict at {} is not JSON: {e}",
                    path.display()
                ),
                detail: None,
            };
        }
    };
    let passed = verdict
        .get("passed")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let note = verdict
        .get("note")
        .and_then(Value::as_str)
        .unwrap_or(if passed {
            "the host was quiet during collection"
        } else {
            "the host was not quiet, so timing families are marked contaminated"
        })
        .to_string();
    LoadGuard {
        passed,
        note,
        detail: Some(verdict),
    }
}

/// UTC, second resolution, RFC 3339. The dashboard sorts runs on this.
pub fn now_iso8601() -> String {
    chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string()
}
