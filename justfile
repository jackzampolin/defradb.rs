# DefraDB.rs task runner.
#
#   just              list every target, grouped
#   just setup        one-command onboarding, no sudo, no package manager
#   just gate         what you must make green before asking for a review
#   just ci           reproduce the CI pipeline locally
#
# Every tool `setup` installs lands in .tooling/ inside the repo and is put on
# PATH by the export below. Nothing is written outside this directory and
# nothing needs root, so the same commands work on Arch, Debian, Fedora, Alpine
# and macOS without a package-manager matrix to keep in sync.

set shell := ["bash", "-euo", "pipefail", "-c"]
set dotenv-load := false

root        := justfile_directory()
tooling     := root / ".tooling"
tooling_bin := tooling / "bin"
go_root     := tooling / "go"
jdk_root    := tooling / "jdk"

# Pinned to what CI uses. ci.yml pins Go 1.25 (:449); Cargo.toml sets
# rust-version 1.91; proofs/lean/lean-toolchain pins Lean v4.18.0; #1310 pins
# TLC 1.8.0 by checksum.
protoc_version := "35.1"
go_version     := "1.25.12"
jdk_version    := "21"
tla_version    := "1.8.0"
tla_sha256     := "e22f8ffb4bacdea0a871f444dd94fe5fb0d8013b3388ae39e82e26f852c735d5"

# .tooling/bin wins over the system copies, so a repo-local protoc/java/go is
# used even when the distro ships an older one. elan installs lake into its own
# home rather than .tooling, so that bin directory has to be on PATH too or
# `just lean` cannot find lake after `just setup`.
elan_bin := env('ELAN_HOME', home_directory() / ".elan") / "bin"
export PATH := tooling_bin + ":" + go_root + "/bin" + ":" + elan_bin + ":" + env('PATH')

# Integration areas, each a [[test]] binary in tools/integration-test.
integration_suites := "basic query acp nac p2p encryption identity backup sourcehub hubrs fts p2p_iroh cursor"

_default:
    @just --list --unsorted

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

# Install every dependency needed to develop, test and verify the database.
[group('setup')]
setup: setup-rust setup-cargo-tools setup-protoc setup-go setup-jdk setup-lean setup-tla
    @echo
    @just doctor

# Rust toolchain, the components CI needs, and the wasm target.
[doc("Rust toolchain, CI's components, and the wasm32 target.")]
[group('setup')]
setup-rust:
    #!/usr/bin/env bash
    set -euo pipefail
    if ! command -v rustup >/dev/null 2>&1; then
        echo "installing rustup (user-space, no sudo)"
        curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --no-modify-path
        # shellcheck disable=SC1090
        source "${CARGO_HOME:-$HOME/.cargo}/env"
    fi
    rustup toolchain install stable --profile minimal --no-self-update
    # ci.yml runs fmt (:64) and 10 clippy invocations; :122 needs the wasm target.
    rustup component add --toolchain stable rustfmt clippy
    rustup target add --toolchain stable wasm32-unknown-unknown
    echo "rust: $(rustc --version)"

# cbindgen generates the FFI C header; wasm-pack and wasm-bindgen-cli build and
# test the browser client. wasm-bindgen-cli must match the wasm-bindgen version
# in Cargo.lock exactly or the test runner refuses the module, so it is derived
# rather than pinned by hand.
[doc("cbindgen, wasm-pack, and a lockfile-matched wasm-bindgen-cli.")]
[group('setup')]
setup-cargo-tools:
    #!/usr/bin/env bash
    set -euo pipefail
    command -v cbindgen  >/dev/null 2>&1 || cargo install cbindgen --locked
    command -v wasm-pack >/dev/null 2>&1 || cargo install wasm-pack --locked
    wb_version="$(cargo metadata --format-version 1 --filter-platform wasm32-unknown-unknown 2>/dev/null \
        | jq -r '.packages[] | select(.name=="wasm-bindgen") | .version' | head -1 || true)"
    if [ -n "${wb_version:-}" ]; then
        have="$(wasm-bindgen --version 2>/dev/null | awk '{print $2}' || true)"
        if [ "${have:-none}" != "$wb_version" ]; then
            cargo install wasm-bindgen-cli --version "$wb_version" --locked
        fi
    else
        echo "note: wasm-bindgen not in the dependency graph; skipping wasm-bindgen-cli" >&2
    fi

# protoc, required by crates/orbis's build.rs (proto/orbis.proto via tonic-prost-build).
[doc("protoc, required by crates/orbis's build.rs.")]
[group('setup')]
setup-protoc:
    #!/usr/bin/env bash
    set -euo pipefail
    target="{{ tooling_bin }}/protoc"
    if [ -x "$target" ]; then echo "protoc: already installed"; exit 0; fi
    case "$(uname -s)" in
        Linux)  os=linux ;;
        Darwin) os=osx ;;
        *) echo "unsupported OS: $(uname -s)" >&2; exit 1 ;;
    esac
    case "$(uname -m)" in
        x86_64|amd64)  arch=x86_64 ;;
        aarch64|arm64) arch=aarch_64 ;;
        *) echo "unsupported arch: $(uname -m)" >&2; exit 1 ;;
    esac
    url="https://github.com/protocolbuffers/protobuf/releases/download/v{{ protoc_version }}/protoc-{{ protoc_version }}-${os}-${arch}.zip"
    tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
    echo "fetching protoc {{ protoc_version }} for ${os}-${arch}"
    curl -fsSL -o "$tmp/protoc.zip" "$url"
    unzip -q "$tmp/protoc.zip" -d "$tmp/out"
    mkdir -p "{{ tooling_bin }}" "{{ tooling }}/protoc"
    cp -R "$tmp/out/include" "{{ tooling }}/protoc/"
    install -m 0755 "$tmp/out/bin/protoc" "$target"
    echo "protoc: $("$target" --version)"

# Go, for the FFI compatibility harness and the Go-parity integration suites.
[doc("Go, for the FFI harness and the Go-parity suites.")]
[group('setup')]
setup-go:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ -x "{{ go_root }}/bin/go" ]; then echo "go: already installed"; exit 0; fi
    case "$(uname -s)" in
        Linux)  os=linux ;;
        Darwin) os=darwin ;;
        *) echo "unsupported OS: $(uname -s)" >&2; exit 1 ;;
    esac
    case "$(uname -m)" in
        x86_64|amd64)  arch=amd64 ;;
        aarch64|arm64) arch=arm64 ;;
        *) echo "unsupported arch: $(uname -m)" >&2; exit 1 ;;
    esac
    url="https://go.dev/dl/go{{ go_version }}.${os}-${arch}.tar.gz"
    tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
    echo "fetching go {{ go_version }} for ${os}-${arch}"
    curl -fsSL -o "$tmp/go.tar.gz" "$url"
    mkdir -p "{{ tooling }}"
    rm -rf "{{ go_root }}"
    tar -C "{{ tooling }}" -xzf "$tmp/go.tar.gz"
    echo "go: $({{ go_root }}/bin/go version)"

# A JDK, required by TLC. proofs/README.md asks for Java 11+; Temurin 21 is LTS.
[doc("A Temurin JDK, required by TLC.")]
[group('setup')]
setup-jdk:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ -x "{{ jdk_root }}/bin/java" ]; then echo "jdk: already installed"; exit 0; fi
    case "$(uname -s)" in
        Linux)  os=linux ;;
        Darwin) os=mac ;;
        *) echo "unsupported OS: $(uname -s)" >&2; exit 1 ;;
    esac
    case "$(uname -m)" in
        x86_64|amd64)  arch=x64 ;;
        aarch64|arm64) arch=aarch64 ;;
        *) echo "unsupported arch: $(uname -m)" >&2; exit 1 ;;
    esac
    url="https://api.adoptium.net/v3/binary/latest/{{ jdk_version }}/ga/${os}/${arch}/jdk/hotspot/normal/eclipse"
    tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
    echo "fetching Temurin JDK {{ jdk_version }} for ${os}-${arch}"
    curl -fsSL -o "$tmp/jdk.tar.gz" "$url"
    mkdir -p "$tmp/x" "{{ tooling_bin }}"
    tar -C "$tmp/x" -xzf "$tmp/jdk.tar.gz"
    rm -rf "{{ jdk_root }}"
    # Temurin tarballs unpack to a single versioned directory; on macOS the JDK
    # lives under Contents/Home inside it.
    extracted="$(find "$tmp/x" -maxdepth 1 -mindepth 1 -type d | head -1)"
    if [ -d "$extracted/Contents/Home" ]; then extracted="$extracted/Contents/Home"; fi
    mv "$extracted" "{{ jdk_root }}"
    # Symlinked onto PATH ahead of the system copy so proofs/tla/tools/tlc picks
    # this one up rather than a stub or an older runtime.
    ln -sf "{{ jdk_root }}/bin/java" "{{ tooling_bin }}/java"
    echo "jdk: $({{ jdk_root }}/bin/java -version 2>&1 | head -1)"

# elan, which provides lake and installs the Lean version that
# proofs/lean/lean-toolchain pins.
[doc("elan, lake, and the Lean version proofs/lean pins.")]
[group('setup')]
setup-lean:
    #!/usr/bin/env bash
    set -euo pipefail
    if ! command -v lake >/dev/null 2>&1; then
        echo "installing elan (provides lake)"
        curl -fsSL https://raw.githubusercontent.com/leanprover/elan/master/elan-init.sh \
            | sh -s -- -y --no-modify-path
    fi
    export PATH="${ELAN_HOME:-$HOME/.elan}/bin:$PATH"
    # Running lake inside proofs/lean makes elan resolve and fetch the pinned
    # toolchain, so the version is never specified twice.
    ( cd "{{ root }}/proofs/lean" && lake --version )
    echo "lean: pinned to $(cat "{{ root }}/proofs/lean/lean-toolchain")"

# Download the TLC jar. Delegates to proofs/tla/tools/tlc, which is the one
# place that knows the pinned version and checksum.
[doc("Download and checksum-verify the TLC jar.")]
[group('setup')]
setup-tla:
    @proofs/tla/tools/tlc --fetch-only

# Report what is installed and what is missing. Never fails the build; it is a
# diagnostic, so it tells you the truth rather than a pass/fail.
[doc("Report which tools are installed and which are missing.")]
[group('setup')]
doctor:
    #!/usr/bin/env bash
    set -uo pipefail
    printf '%-16s %s\n' "tool" "resolved"
    printf '%-16s %s\n' "----" "--------"
    for t in rustc cargo rustfmt clippy-driver protoc go java lake cbindgen wasm-pack wasm-bindgen jq; do
        printf '%-16s %s\n' "$t" "$(command -v "$t" 2>/dev/null || echo 'MISSING')"
    done
    jar="{{ root }}/proofs/tla/tools/tla2tools.jar"
    printf '%-16s %s\n' "tla2tools.jar" "$([ -f "$jar" ] && echo "$jar" || echo 'MISSING (just setup-tla)')"
    printf '%-16s %s\n' "wasm32 target" "$(rustup target list --installed 2>/dev/null | grep -c wasm32-unknown-unknown | sed 's/^0$/MISSING/;s/^1$/installed/')"
    echo
    echo "optional, not installed by setup:"
    for t in docker node npm python3; do
        printf '%-16s %s\n' "$t" "$(command -v "$t" 2>/dev/null || echo 'missing')"
    done

# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

# Debug build of the whole workspace.
[group('build')]
build:
    cargo build --workspace

# Optimized build. Fat LTO, single codegen unit: slow, and what ships.
[group('build')]
build-release:
    cargo build --release -p cli

# Thin-LTO build for iteration. ci.yml:305 measured 1m40s vs 6m44s; never ship
# an artifact from this profile.
[doc("Thin-LTO build for fast iteration (never ship this profile).")]
[group('build')]
build-fast:
    cargo build --profile release-fast -p cli

# Browser client, via wasm-pack, into pkg/wasm (mirrors ci.yml:95).
[group('build')]
build-wasm:
    wasm-pack build crates/wasm --release --target web --out-dir ../../pkg/wasm

# Regenerate the C header for the FFI surface.
[group('build')]
build-ffi-header:
    cbindgen --config crates/ffi/cbindgen.toml --crate ffi --output crates/ffi/defradb.h

# Apple .xcframework plus the Swift import smoke test.
[group('build')]
build-apple:
    tools/apple/build-ffi.sh

# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------

# Workspace unit tests. Mirrors ci.yml:189, which excludes the three suites that
# need a built binary or an external runtime.
[doc("Workspace unit tests (mirrors ci.yml:189).")]
[group('test')]
test:
    cargo test --workspace --exclude integration-test --exclude ffi-test --exclude conformance

# Unit tests for one crate: `just test-crate crdt`.
[group('test')]
test-crate crate *args:
    cargo test -p {{ crate }} {{ args }}

# Feature-gated suites the default workspace run skips. defra-node's p2p tests
# are in-crate and off by default, so nothing else reaches them.
[doc("Feature-gated suites the default workspace run never reaches.")]
[group('test')]
test-features:
    cargo test -p telemetry --features otlp
    cargo test -p cli --features otel --test telemetry_dedup
    cargo test -p defra-node --features p2p

# Every integration area, serially. Each spawns real nodes.
[group('test')]
integration:
    cargo test -p integration-test

# One integration area: `just integration-suite acp`, `just integration-suite p2p`.
[group('test')]
integration-suite suite *args:
    cargo test -p integration-test --test {{ suite }} {{ args }}

# The pure-Rust half of the P2P topologies, serialized. `--skip ::go_` drops the
# dual-runtime variants; the harness port allocator has a bind race in parallel
# (ci.yml:713).
[doc("Pure-Rust P2P topologies, serialized (skips the go_ variants).")]
[group('test')]
integration-p2p:
    cargo test -p integration-test --test p2p -- --test-threads=1 --skip ::go_

# The go_* half, which needs a Go DefraDB binary on PATH.
[group('test')]
integration-go:
    cargo test -p integration-test --test p2p -- --test-threads=1 ::go_

# FFI compatibility against Go. Needs `just setup-go` and the generated header.
[group('test')]
test-ffi:
    cargo test -p ffi-test

# Run every listed integration area one at a time, reporting each. Keeps going
# after a failure and exits non-zero if any failed, so one red area does not
# hide the state of the rest.
[doc("Run every integration area one at a time, reporting each.")]
[group('test')]
integration-all:
    #!/usr/bin/env bash
    set -uo pipefail
    failed=()
    for suite in {{ integration_suites }}; do
        echo "== $suite =="
        cargo test -p integration-test --test "$suite" || failed+=("$suite")
    done
    if [ ${#failed[@]} -gt 0 ]; then
        echo "FAILED: ${failed[*]}" >&2
        exit 1
    fi
    echo "all ${#failed[@]} listed areas passed"

# ---------------------------------------------------------------------------
# Formal methods
# ---------------------------------------------------------------------------

# TLA+, Lean, and both conformance axes. This is proofs/verify-all.sh.
[group('proofs')]
proofs:
    proofs/verify-all.sh

# TLC over every model, checked against the expected red/green oracle.
[group('proofs')]
tla:
    cd proofs/tla && ./run-all.sh

# One model: `just tla-model MC_Ssi_Red_WriteSkew.cfg MC_Ssi_Red_WriteSkew.tla`.
[group('proofs')]
tla-model cfg module:
    cd proofs/tla && ./tools/tlc -config {{ cfg }} {{ module }}

# Lean proofs. Must build clean with zero `sorry`.
[group('proofs')]
lean:
    cd proofs/lean && lake build

# Model-to-code binding. The Lean axis needs no binary; the TLA axis drives the
# real release binary and is serial because each test spins up real nodes.
[doc("Model-to-code binding, Lean axis (fast, no binary needed).")]
[group('proofs')]
conformance:
    cargo test -p conformance --lib --test lean_conformance

[doc("Model-to-code binding, TLA axis against the release binary.")]
[group('proofs')]
conformance-behavioral: build-release
    DEFRA_CONFORMANCE_BINARY="{{ root }}/target/release/defra" \
        cargo test -p conformance --test tla_conformance -- --test-threads=1

# ---------------------------------------------------------------------------
# Quality gates
# ---------------------------------------------------------------------------

# Rewrite formatting in place.
[group('check')]
fmt:
    cargo fmt --all

# Fail on any formatting diff (ci.yml:64).
[group('check')]
fmt-check:
    cargo fmt --all -- --check

# Every clippy invocation CI runs, in CI's order (ci.yml:66-86, :139).
[group('check')]
lint:
    cargo clippy --all -- -D warnings
    cargo clippy -p cli --no-default-features --features release-full --all-targets -- -D warnings
    cargo clippy -p cli --no-default-features --features rocksdb,lark --all-targets -- -D warnings
    cargo clippy -p cli --no-default-features --features lark --all-targets -- -D warnings
    cargo clippy -p telemetry --features otlp --all-targets -- -D warnings
    cargo clippy -p cli --features otel --all-targets -- -D warnings
    cargo clippy -p defra-node --features otel --all-targets -- -D warnings
    cargo clippy -p defra-node --features p2p --all-targets -- -D warnings
    cargo clippy -p db --features p2p --all-targets -- -D warnings

# Lint the test and bench targets too. CI does NOT do this for the workspace, so
# this catches lints that would otherwise sit in the tree unnoticed.
[doc("Clippy including test and bench targets, which CI does not lint.")]
[group('check')]
lint-all-targets:
    cargo clippy --all --all-targets -- -D warnings

# Browser-client lint on the real wasm target (ci.yml:139).
[group('check')]
lint-wasm:
    cargo clippy -p defra-wasm --target wasm32-unknown-unknown --all-targets -- -D warnings

# Docs must build without warnings; a broken intra-doc link fails CI.
[doc("Build docs with warnings denied (broken links fail CI).")]
[group('check')]
doc:
    RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps

# What to make green before asking for a review.
[group('check')]
gate: fmt-check lint doc test
    @echo "gate: green"

# Reproduce the CI pipeline locally, in CI's order.
[group('check')]
ci: fmt-check lint lint-wasm test test-features build-wasm integration proofs
    @echo "ci: green"

# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

# Start a node. `just start`, or `just start --store redb --port 9181`.
[group('run')]
start *args:
    cargo run -p cli -- start {{ args }}

# Start against a chosen backend: lark (default), redb, fjall, rocksdb, memory.
[group('run')]
start-backend backend *args:
    cargo run -p cli -- start --store {{ backend }} {{ args }}

# Any CLI subcommand: `just cli client query '{ User { name } }'`.
[group('run')]
cli *args:
    cargo run -p cli -- {{ args }}

# Standalone WASM Lens transform runner, JSON on stdin to stdout.
[group('run')]
lens-host *args:
    cargo run -p lens-host -- {{ args }}

# OTLP exporter smoke test against a Compose-run collector. Needs docker.
[group('run')]
otel-smoke:
    tools/otel-smoke/run.sh

# Drizzle ORM harness for the Postgres wire protocol. Needs node and npm.
[group('run')]
pg-compat:
    cd tools/pg-compat-harness && npm install && npm test

# Local HuggingFace embedding server for the embedding benchmarks. Needs python3
# with torch, transformers, fastapi and uvicorn.
[doc("Local HuggingFace embedding server for the embedding benchmarks.")]
[group('run')]
embedding-server *args:
    python3 tools/hf_embedding_server.py {{ args }}

# ---------------------------------------------------------------------------
# Housekeeping
# ---------------------------------------------------------------------------

# Criterion benchmarks.
[group('misc')]
bench *args:
    cargo bench --workspace {{ args }}

# Remove build output. Leaves .tooling/ alone.
[group('misc')]
clean:
    cargo clean

# Remove everything setup installed. Rerun `just setup` afterwards.
[group('misc')]
clean-tooling:
    rm -rf "{{ tooling }}" "{{ root }}/proofs/tla/tools/tla2tools.jar"

# Dependency and licence audit. Installs the subcommands on first use.
[group('misc')]
audit:
    #!/usr/bin/env bash
    set -euo pipefail
    command -v cargo-deny >/dev/null 2>&1 || cargo install cargo-deny --locked
    cargo deny check

# Report unused dependencies (#1329).
[group('misc')]
machete:
    #!/usr/bin/env bash
    set -euo pipefail
    command -v cargo-machete >/dev/null 2>&1 || cargo install cargo-machete --locked
    cargo machete

# The Go compatibility baseline, read from its single source of truth.
[group('misc')]
go-baseline:
    @grep -E 'GO_COMPAT_(BRANCH|COMMIT|TAG)' crates/defra-version/src/lib.rs
