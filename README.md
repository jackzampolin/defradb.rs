# DefraDB.rs

Rust implementation of [DefraDB](https://github.com/sourcenetwork/defradb) — a peer-to-peer document database with content-addressed CRDTs, access control, and GraphQL.

Targets embedded, edge, browser (WASM), and server deployments with **full Go DefraDB network interoperability**.

## Status

Compatible with Go DefraDB v1.0.0-rc1. Full feature parity across CLI, HTTP API, GraphQL query engine, and P2P replication. Go and Rust nodes can connect and replicate data.

## Features

- **GraphQL query engine** — queries, mutations, subscriptions, aggregates, explain - full coverage of the defradb test suite
- **P2P replication** — `libp2p` (primary, go compatable) and [`iroh`](https://github.com/n0-computer/iroh) (optional) transports
- **Access control** — local Zanzibar engine, on-chain via [SourceHub](https://github.com/sourcenetwork/sourcehub) (Cosmos/EVM) and [`hub.rs`](https://github.com/sourcenetwork/hub.rs) (Commonware/EVM)
- **Full-text search** — (rust only) BM25 ranking with language-aware tokenization
- **Schema migration** — non-destructive evolution via WASM transforms (Lens)
- **Searchable encryption** — encrypted indexes with ACP integration
- **Multiple storage backends** — rocksdb (default), redb, fjall, in-memory
- **Postgres compatibility** — connect with `psql` or any Postgres client/ORM (experimental!)
- **WASM client** — full database client compiled to WebAssembly for browsers
- **FFI bindings** — C-compatible static library for embedding in Go and other languages
- **Docker** — multi-arch images at `ghcr.io/sourcenetwork/defradb-rs`

## Building

```bash
cargo build                            # Build all crates
cargo build --release -p cli           # Release binary
cargo test                             # Unit tests
cargo clippy --all -- -D warnings      # Lint
cargo fmt --all                        # Format
```

## Configuration

The CLI exposes GraphQL query guardrails on `defradb start`:

| Flag | Default | Description |
| --- | ---: | --- |
| `--query-max-depth` | `20` | Max GraphQL selection nesting depth (`0` = unlimited). |
| `--query-max-width` | `100` | Max fields at any GraphQL selection level (`0` = unlimited). |
| `--query-max-filter-depth` | `50` | Max recursive filter nesting depth (`0` = unlimited). |

## Telemetry (OpenTelemetry)

Opt in at compile time with `--features otel`:

```bash
cargo build --release -p cli --features otel
```

Mirrors Go DefraDB's `//go:build telemetry` tag — when not compiled in, zero OTel dependencies and zero runtime cost. When compiled in, traces and metrics export to `http://localhost:4318` (OTLP/HTTP) **by default**, same as Go.

| Flag / env var | Effect |
| --- | --- |
| `--no-telemetry` / `DEFRA_NO_TELEMETRY=true` | Disable exporters at runtime. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Override collector URL (standard OTel env var). |
| `OTEL_EXPORTER_OTLP_HEADERS` | Add headers, e.g. for auth. |
| `OTEL_SERVICE_NAME` / `OTEL_RESOURCE_ATTRIBUTES` | Override service name / extra attributes. |
| `OTEL_TRACES_SAMPLER` | Configure sampling (default: parent-based always-on, matching Go). |

When no collector is reachable, repeat `"connection refused"` messages from the OTel SDK are suppressed after the first — port of Go's `otel.SetErrorHandler + sync.Once` dedup (issue #977).

### Embedded usage

Library consumers (via `defra-node`) can either let `NodeBuilder` initialize OTel themselves or bring a pre-built handle:

```rust
use defra_node::{EmbeddedNode, TelemetryConfig};

// Option A — let the builder handle init
let node = EmbeddedNode::builder()
    .with_telemetry(TelemetryConfig::new("my-service", env!("CARGO_PKG_VERSION")))
    .build()
    .await?;

// Option B — bring your own (e.g. defra-agent runs its own OTel stack)
let (handle, _tracer) = telemetry::init(TelemetryConfig::default())?;
// ... compose your own subscriber with `telemetry::otel_layer(_tracer)` ...
let node = EmbeddedNode::builder()
    .with_external_telemetry(handle)
    .build()
    .await?;

// shutdown() flushes buffered spans (which Go DefraDB forgets to do).
node.shutdown().await;
```

Either way requires `defra-node` to be built with `--features otel`.

## Testing

### Integration Tests

Rust-native tests that exercise the full node via CLI + HTTP API. Primary validation method.

```bash
cargo test -p integration-test                              # All areas
cargo test -p integration-test --test basic                  # Specific area
cargo test -p integration-test --test acp -- negative::      # Specific module
```

Areas: `basic`, `query`, `acp`, `nac`, `p2p`, `fts`, `encryption`, `identity`, `backup`, `sourcehub`, `hubrs`

### Go Compatibility Tests

FFI-based tests that build the Rust implementation as a C library and run Go's integration test suite against it. Validates behavioral compatibility between implementations.

```bash
cargo install --path tools/ffi-test
ffi-test run query/simple              # Run specific package
ffi-test status                        # Show pass rates
```

See `tools/ffi-test/README.md` for full usage.

## Documentation

- [DefraDB (Go)](https://github.com/sourcenetwork/defradb) — concepts, architecture, specifications
- [DefraDB Docs](https://docs.source.network/defradb) — user documentation
- `CLAUDE.md` — development workflow and conventions

## License

Apache-2.0 OR MIT
