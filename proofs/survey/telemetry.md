# Survey: `crates/telemetry/`

## Purpose
OpenTelemetry exporter setup for DefraDB. Mirrors Go's `internal/telemetry/otel.go`.
Five small files:
- `config.rs` — `TelemetryConfig` (service name/version, `install_global` flag).
- `init.rs` — builds OTLP/HTTP span (+ optional metric) exporters, resource attrs,
  installs process-wide globals. `otlp`-feature only.
- `handle.rs` — `TelemetryHandle`: explicit `shutdown` + `Drop` safety-net flush
  (catch_unwind to avoid panic-during-unwind abort).
- `dedup.rs` — `OtelDedupFilter`, a stateless `tracing_subscriber` Filter that
  suppresses "collector unreachable" spam and emits a hint once per process
  (global `OnceLock`).
- `lib.rs` — module wiring / re-exports.

## State machines
None of substance. The only "state" is `HINT_ONCE: OnceLock<()>` (a once-only
latch, Go's `sync.Once` equivalent) and `TelemetryHandle.inner: Option<..>`
(present → taken-on-shutdown). Both are trivial single-transition flags, not
protocols. No concurrency coordination, replication, consistency, or access
gating lives here.

## Candidates

| name | kind | property | already-modeled | priority |
|------|------|----------|-----------------|----------|
| (none) | — | — | — | — |

The dedup filter's correctness (suppress unreachable, pass genuine errors, ignore
non-OTEL targets, never double-suppress reachable-collector status codes) is a
pure-string predicate already pinned by 6 unit tests in `dedup.rs`. The shutdown
flush/Drop behavior is IO lifecycle plumbing exercised at runtime, not a provable
invariant. No algebraic laws, determinism, content-addressing, or ordering.

## Verdict
**Plumbing.** Not model-worthy. Observability glue around the OTel SDK; no
distributed/concurrent state machine for TLA+ and no algebraic law for Lean.
Existing unit tests cover the one non-trivial predicate.
