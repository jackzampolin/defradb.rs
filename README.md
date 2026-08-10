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

Install [`just`](https://github.com/casey/just), then let it install everything else:

```bash
cargo install just    # one time, if you already have a Rust toolchain
just setup            # Rust, protoc, Go, a JDK, Lean/lake, the TLC jar, wasm tooling
```

Without Rust yet, install a prebuilt `just` first, since `just setup` is what
installs the toolchain:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://just.systems/install.sh \
  | bash -s -- --to ~/.local/bin      # or grab a binary from the just releases page
```

`just setup` needs no root and no package manager: every tool is fetched from its
official release into a git-ignored `.tooling/` inside the repo and put on `PATH`.
It is written for Linux and macOS on x86_64 and arm64, and is currently verified
on Linux x86_64. Downloads are pinned by version and checked against a SHA-256
before use. The host needs `bash`, `curl`, `tar`, `unzip` and `git`.

`just doctor` reports what resolved and what is missing.

```bash
just build             # Build all crates
just build-release     # Release binary
just test              # Unit tests
just lint              # Lint (every clippy invocation CI runs)
just fmt               # Format
just gate              # fmt + lint + docs + tests, before asking for a review
just ci                # Reproduce the CI pipeline locally
```

Run `just` on its own to list every target, grouped.

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

Mirrors Go DefraDB's `//go:build telemetry` tag — when not compiled in, zero OTel dependencies and zero runtime cost. When compiled in, **traces** export to `http://localhost:4318` (OTLP/HTTP) by default, same endpoint as Go.

Metrics export is planned alongside the first application metric.

| Flag / env var | Effect |
| --- | --- |
| `--no-telemetry` / `DEFRA_NO_TELEMETRY=true` | Disable exporters at runtime. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Override collector URL (standard OTel env var). |
| `OTEL_EXPORTER_OTLP_HEADERS` | Add headers, e.g. for auth. |
| `OTEL_SERVICE_NAME` / `OTEL_RESOURCE_ATTRIBUTES` | Override service name / extra attributes. |
| `OTEL_TRACES_SAMPLER` | Configure sampling (default: parent-based always-on, matching Go). |

When no collector is reachable, the OTel SDK's repeated export errors are suppressed and a single actionable hint — `OpenTelemetry export failed, ensure your OTLP collector is running and reachable` — is emitted once per process. This ports Go's `otel.SetErrorHandler + sync.Once` behavior (issue #977); genuine non-connectivity OTel errors still log normally.

### Embedded usage

Library consumers (via `defra-node`) own the OTel lifecycle and hand the resulting handle to the node. The node flushes it via `Drop` or via an explicit `shutdown()` — explicit is preferred because `Drop` blocks for up to ~5 s on the SDK's trace batch-thread join.

Add `telemetry` to your `Cargo.toml`:

```toml
[dependencies]
defra-node = { version = "0.5", features = ["otel"] }
telemetry  = { version = "0.5", features = ["otlp"] }
```

```rust
use defra_node::EmbeddedNode;
use telemetry::TelemetryConfig;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

let (handle, tracer) = telemetry::init(
    TelemetryConfig::new("my-service", env!("CARGO_PKG_VERSION"))
)?;

// Compose the OTel bridge onto your tracing subscriber so spans flow
// to the collector. `try_init()` returns Err if a global subscriber is
// already installed — preferred over `.init()` (which panics).
tracing_subscriber::registry()
    .with(tracing_subscriber::fmt::layer())
    .with(telemetry::otel_layer(tracer))
    .try_init()?;

let node = EmbeddedNode::builder()
    .with_telemetry(handle)
    .build()
    .await?;

// ... use the node ...

// Explicit shutdown flushes the buffered batch. Drop is a safety net,
// but blocks the calling thread on the SDK batch-thread join (~5 s);
// call shutdown() explicitly from an async-aware path when possible.
// Note that the node should not be used after shutdown — subsequent spans
// go to a no-op tracer.
node.shutdown().await;
```

If the host process already runs its own OTel stack and you don't want `telemetry::init` to clobber your globals, use `.without_global()`:

```rust
let (handle, _tracer) = telemetry::init(
    TelemetryConfig::new("my-service", "1.0.0").without_global()
)?;
// You'd compose `_tracer` into a layer of your own choosing instead of
// installing it as the process-wide global tracer.
```

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

## P2P Replication

### Filtered replication

A replicator can carry an optional per-collection predicate so a source node only **pushes** documents whose field matches:

```bash
defradb client p2p replicator add -c MyCollection \
  --filter-field agent_did --filter-value did:key:alice <peer-multiaddr>
```

The filter field must be a scalar, `@immutable` LWW field on the collection.

**Filtered replication is a push-path selectivity optimization, not an access-control boundary.** A peer that also subscribes to the collection (`p2p collection add`) joins the collection's gossip topic and receives every document, bypassing the filter. Use ACP and encryption for confidentiality. The `--filter-value` is matched as a JSON string, so only string-valued fields can be filtered today.

## Documentation

- [DefraDB (Go)](https://github.com/sourcenetwork/defradb) — concepts, architecture, specifications
- [DefraDB Docs](https://docs.source.network/defradb) — user documentation
- `CLAUDE.md` — development workflow and conventions

## License

Apache-2.0 OR MIT
