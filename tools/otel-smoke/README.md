# OTel exporter smoke tests

End-to-end verification for the OTLP exporter wired in issue #977.

Two scripts:

| Script | Purpose | Requires Docker |
| --- | --- | --- |
| `run.sh` | Positive path — starts otel-collector-contrib + Jaeger, runs `defra start --features otel` against them, asserts spans arrive with the expected resource attributes. | yes |
| `dedup.sh` | Negative path — points `defra` at an unreachable port, asserts the "connection refused" message appears at most once (the Rust port of Go's `otel.SetErrorHandler + sync.Once` dedup). | no |

## Quick start

```bash
# Positive — spans arrive at the collector
./tools/otel-smoke/run.sh

# Leave the stack up so you can browse spans in Jaeger
./tools/otel-smoke/run.sh --keep
# → Jaeger UI: http://localhost:16686 (service: DefraDB)
# → Tear down: (cd tools/otel-smoke && docker compose down -v)

# Negative — dedup catches connection-refused spam
./tools/otel-smoke/dedup.sh
```

## What `run.sh` checks

The collector's file exporter writes each received batch as one JSON line under `output/spans.jsonl`. `run.sh` asserts:

- The file is non-empty (at least one batch landed).
- `service.name = "DefraDB"` is present (matches Go DefraDB).
- The Go-parity resource attributes (`service.version`, `os.type`, `host.arch`, `process.pid`, `process.executable.name`) are present.
- The known span names (`request` from `tower_http::TraceLayer`, `query.execute_request` from `QueryRunner::execute`) are reported when the relevant endpoints are exercised.

If `--keep` is passed, the stack stays up so you can:

- Browse spans in the Jaeger UI at <http://localhost:16686> (search service "DefraDB").
- Inspect `output/spans.jsonl` directly.
- Inspect `output/defra.stderr` for OTel SDK diagnostics.

## What `dedup.sh` checks

Boots `defra start --features otel` with `OTEL_EXPORTER_OTLP_ENDPOINT` pointed at a port nobody is listening on. Lets the batch processor retry for a few seconds. Then SIGTERMs and `grep`s the stderr — `connection refused` must appear **at most once**.

## Ports used

| Port | Owner | Purpose |
| --- | --- | --- |
| 4317 | otelcol | OTLP/gRPC ingest (not used by defra, but exposed for parity) |
| 4318 | otelcol | OTLP/HTTP ingest — defra exports here |
| 9181 | defra (run.sh) | defra HTTP API |
| 9182 | defra (dedup.sh) | defra HTTP API (different port to avoid collision) |
| 14250 | jaeger | gRPC receiver from otelcol |
| 16686 | jaeger | UI |
| 14318 | (must be unbound) | dedup target — confirmed empty before the test runs |

The scripts fail fast if a required port is already taken.
