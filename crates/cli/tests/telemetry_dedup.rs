//! End-to-end dedup test for the OTLP exporter (issue #977, follow-up #1004).
//!
//! Docker-free Rust-native port of `tools/otel-smoke/dedup.sh`: spawn the
//! real `defra` binary (built with `--features otel`) pointed at an
//! unreachable collector, drive a GraphQL query to produce an exported span,
//! and assert that:
//!   - the operator hint is emitted exactly once (proves the SDK's
//!     `internal-logs` feature is on AND the dedup fires — this is the
//!     regression guard), and
//!   - the raw, repeated SDK export errors are suppressed (Go parity).
//!
//! Only meaningful when telemetry is compiled in, so the whole test is gated
//! on the `otel` feature. Run with: `cargo test -p cli --features otel`.

#![cfg(feature = "otel")]

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const HINT: &str =
    "OpenTelemetry export failed, ensure your OTLP collector is running and reachable";

/// Drain a child's piped stderr into a shared string on a background thread,
/// so the child never blocks on a full pipe buffer.
fn drain_stderr(stderr: std::process::ChildStderr) -> Arc<Mutex<String>> {
    let buf = Arc::new(Mutex::new(String::new()));
    let buf_thread = Arc::clone(&buf);
    std::thread::spawn(move || {
        let mut rdr = stderr;
        let mut chunk = [0u8; 4096];
        loop {
            match rdr.read(&mut chunk) {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if let Ok(mut s) = buf_thread.lock() {
                        s.push_str(&String::from_utf8_lossy(&chunk[..n]));
                    }
                }
            }
        }
    });
    buf
}

fn wait_for_port(port: u16, child: &mut std::process::Child) -> bool {
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return true;
        }
        if matches!(child.try_wait(), Ok(Some(_))) {
            return false; // exited early
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    false
}

/// Fire a GraphQL query over a raw TCP socket. The `query.execute_request`
/// span it produces is info-level, so it survives the default env filter and
/// reaches the OTLP exporter (a plain HTTP GET would only make a debug-level
/// span, which the filter drops before export).
fn send_graphql(port: u16) {
    let body = r#"{"query":"query { __typename }"}"#;
    let req = format!(
        "POST /api/v0/graphql HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    if let Ok(mut s) = TcpStream::connect(("127.0.0.1", port)) {
        let _ = s.write_all(req.as_bytes());
        let mut resp = String::new();
        let _ = s.read_to_string(&mut resp);
    }
}

#[test]
fn exporter_unreachable_logs_hint_once_and_suppresses_raw() {
    let http_port = portpicker::pick_unused_port().expect("free http port");
    // A port nobody listens on → the HTTP exporter fails to reach it.
    // Guard against portpicker handing back the same port twice (the two
    // calls bind-and-release independently, so equality is possible).
    let dead_otlp_port = std::iter::repeat_with(portpicker::pick_unused_port)
        .flatten()
        .find(|&p| p != http_port)
        .expect("free otlp port distinct from http port");
    let rootdir = tempfile::tempdir().expect("tempdir");

    let mut child = Command::new(env!("CARGO_BIN_EXE_defra"))
        .arg("start")
        .arg("--rootdir")
        .arg(rootdir.path())
        .arg("--no-keyring")
        .arg("--no-p2p")
        .arg("--store")
        .arg("memory")
        .arg("--url")
        .arg(format!("127.0.0.1:{http_port}"))
        .env(
            "OTEL_EXPORTER_OTLP_ENDPOINT",
            format!("http://127.0.0.1:{dead_otlp_port}"),
        )
        .env("OTEL_BSP_SCHEDULE_DELAY", "300") // ms — provoke export quickly
        // Pin the log level so the INFO-level `query.execute_request` span
        // (which drives the export we're testing) survives the env filter.
        // An inherited RUST_LOG/DEFRA_LOG_LEVEL=error would drop it and make
        // the hint count 0 — a false failure.
        .env("DEFRA_LOG_LEVEL", "info")
        .env_remove("RUST_LOG")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn defra");

    let stderr = drain_stderr(child.stderr.take().expect("piped stderr"));

    let ready = wait_for_port(http_port, &mut child);
    if !ready {
        let _ = child.kill();
        let _ = child.wait();
        panic!(
            "defra did not start serving on :{http_port}\n--- stderr ---\n{}",
            stderr.lock().unwrap()
        );
    }

    // Drive a few queries so the batch processor has spans to export, then
    // give it time to attempt (and fail) an export and emit the hint.
    for _ in 0..5 {
        send_graphql(http_port);
        std::thread::sleep(Duration::from_millis(200));
    }
    std::thread::sleep(Duration::from_secs(2));

    let _ = child.kill();
    let _ = child.wait();
    // Let the drain thread flush the tail of the pipe.
    std::thread::sleep(Duration::from_millis(200));

    let captured = stderr.lock().unwrap().clone();

    let hint_count = captured.matches(HINT).count();
    assert_eq!(
        hint_count, 1,
        "expected the operator hint exactly once (0 = internal-logs off / dedup dead; >1 = global once-latch broken).\n--- stderr ---\n{captured}"
    );

    // The raw SDK export-error lines must be suppressed in favor of the hint.
    // (The single hint line itself carries the detail, so exclude it before
    // counting raw lines.)
    let raw_error_lines = captured
        .lines()
        .filter(|l| !l.contains(HINT))
        .filter(|l| l.contains("ERROR"))
        .filter(|l| l.contains("opentelemetry"))
        .filter(|l| {
            l.contains("connection refused")
                || l.contains("HTTP export failed")
                || l.contains("network error")
        })
        .count();
    assert_eq!(
        raw_error_lines, 0,
        "raw exporter-error lines should be suppressed; found {raw_error_lines}.\n--- stderr ---\n{captured}"
    );
}
