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
jdk_release    := "jdk-21.0.12+8"
jq_version     := "1.8.1"
tla_version    := "1.8.0"
tla_sha256     := "e22f8ffb4bacdea0a871f444dd94fe5fb0d8013b3388ae39e82e26f852c735d5"

# protoc publishes no per-asset checksum file, so its hashes are embedded.
# Go and Temurin publish theirs, and are verified against upstream at install
# time rather than duplicated here.
protoc_sha256_linux_x86_64  := "6930ebf62bd4ea607b98fff052596c6ee564b9835b4ce172c75a3f53ae9d91b7"
protoc_sha256_linux_aarch64 := "01bf9d08808c7f96678b63f4bd8efa559bb4f83d5a7a270d5edaf507f9d5d9cf"
protoc_sha256_osx_x86_64    := "537d73604a344ded6fc94e98e07e529d4fe3e4a0b09e59905353950fafc2a1f7"
protoc_sha256_osx_aarch64   := "193289af0470c6a1aada357d4fba0bbf8d78bfaac8b5e42ca30af2ef75583de2"

# .tooling/bin wins over the system copies, so a repo-local protoc/java/go is
# used even when the distro ships an older one. elan installs lake into its own
# home rather than .tooling, so that bin directory has to be on PATH too or
# `just lean` cannot find lake after `just setup`.
# rustup is installed with --no-modify-path, so cargo's bin directory is only
# on PATH if the user already had it. Without this, every recipe after
# setup-rust fails to find cargo on a fresh machine.
cargo_bin := env('CARGO_HOME', home_directory() / ".cargo") / "bin"
elan_bin  := env('ELAN_HOME', home_directory() / ".elan") / "bin"
export PATH := tooling_bin + ":" + go_root + "/bin" + ":" + cargo_bin + ":" + elan_bin + ":" + env('PATH')

# Integration areas, each a [[test]] binary in tools/integration-test.
integration_suites := "basic query acp nac p2p encryption identity backup sourcehub hubrs fts p2p_iroh cursor"

_default:
    @just --list --unsorted

# sha256 of a file, portable across Linux (sha256sum) and macOS (shasum).
[private]
_sha256 file:
    #!/usr/bin/env bash
    set -euo pipefail
    if command -v sha256sum >/dev/null 2>&1; then sha256sum "{{ file }}" | awk '{print $1}'
    elif command -v shasum >/dev/null 2>&1; then shasum -a 256 "{{ file }}" | awk '{print $1}'
    else echo "error: no sha256 tool found" >&2; exit 1; fi

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

# Install every dependency needed to develop, test and verify the database.
[group('setup')]
setup: setup-rust setup-jq setup-cargo-tools setup-protoc setup-go setup-jdk setup-lean setup-tla
    @echo
    @just doctor

# Rust toolchain, the components CI needs, and the wasm target.
#
# Trust boundary: this and setup-lean pipe an upstream install script into sh
# (rustup.rs, elan-init.sh), which is how both ecosystems distribute. Every
# other tool here is fetched as an archive and checksum-verified.
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

# jq is required by setup-cargo-tools (lockfile-matched wasm-bindgen) and by
# setup-jdk (Temurin checksum lookup). Installed rather than assumed, so a host
# without it does not silently skip those steps.
[doc("jq, used by the wasm-bindgen and JDK checksum lookups.")]
[group('setup')]
setup-jq:
    #!/usr/bin/env bash
    set -euo pipefail
    if command -v jq >/dev/null 2>&1; then echo "jq: $(jq --version)"; exit 0; fi
    case "$(uname -s)" in
        Linux)  os=linux ;;
        Darwin) os=macos ;;
        *) echo "unsupported OS: $(uname -s)" >&2; exit 1 ;;
    esac
    case "$(uname -m)" in
        x86_64|amd64)  arch=amd64 ;;
        aarch64|arm64) arch=arm64 ;;
        *) echo "unsupported arch: $(uname -m)" >&2; exit 1 ;;
    esac
    mkdir -p "{{ tooling_bin }}"
    curl -fsSL -o "{{ tooling_bin }}/jq" \
        "https://github.com/jqlang/jq/releases/download/jq-{{ jq_version }}/jq-${os}-${arch}"
    chmod 0755 "{{ tooling_bin }}/jq"
    echo "jq: $({{ tooling_bin }}/jq --version)"

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
    command -v jq >/dev/null 2>&1 || { echo "error: jq is required; run 'just setup-jq'" >&2; exit 1; }
    meta="$(cargo metadata --format-version 1 --filter-platform wasm32-unknown-unknown)"
    wb_version="$(echo "$meta" | jq -r '.packages[] | select(.name=="wasm-bindgen") | .version' | head -1)"
    if [ -z "$wb_version" ] || [ "$wb_version" = "null" ]; then
        echo "error: wasm-bindgen not found in the wasm32 dependency graph" >&2
        echo "       wasm tests cannot run without a matching wasm-bindgen-cli" >&2
        exit 1
    fi
    have="$(wasm-bindgen --version 2>/dev/null | awk '{print $2}' || true)"
    if [ "${have:-none}" != "$wb_version" ]; then
        cargo install wasm-bindgen-cli --version "$wb_version" --locked
    fi
    echo "wasm-bindgen-cli: $wb_version (matched to Cargo.lock)"

# protoc, required by crates/orbis's build.rs (proto/orbis.proto via tonic-prost-build).
[doc("protoc, required by crates/orbis's build.rs.")]
[group('setup')]
setup-protoc:
    #!/usr/bin/env bash
    set -euo pipefail
    target="{{ tooling_bin }}/protoc"
    # Re-fetch when the installed copy does not match the pin, so bumping
    # protoc_version takes effect without a clean-tooling.
    if [ -x "$target" ] && "$target" --version 2>/dev/null | grep -q " {{ protoc_version }}$"; then
        echo "protoc: {{ protoc_version }} already installed"; exit 0
    fi
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
    case "${os}-${arch}" in
        linux-x86_64)  want="{{ protoc_sha256_linux_x86_64 }}" ;;
        linux-aarch_64) want="{{ protoc_sha256_linux_aarch64 }}" ;;
        osx-x86_64)    want="{{ protoc_sha256_osx_x86_64 }}" ;;
        osx-aarch_64)  want="{{ protoc_sha256_osx_aarch64 }}" ;;
    esac
    url="https://github.com/protocolbuffers/protobuf/releases/download/v{{ protoc_version }}/protoc-{{ protoc_version }}-${os}-${arch}.zip"
    tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
    echo "fetching protoc {{ protoc_version }} for ${os}-${arch}"
    curl -fsSL -o "$tmp/protoc.zip" "$url"
    got="$(just _sha256 "$tmp/protoc.zip")"
    [ "$got" = "$want" ] || { echo "error: protoc checksum mismatch" >&2; echo "  expected $want" >&2; echo "  got      $got" >&2; exit 1; }
    unzip -q "$tmp/protoc.zip" -d "$tmp/out"
    # Upstream layout: prefix/bin/protoc next to prefix/include, so protoc
    # resolves the well-known google/protobuf/*.proto imports via ../include.
    rm -rf "{{ tooling }}/protoc"
    mkdir -p "{{ tooling }}/protoc" "{{ tooling_bin }}"
    cp -R "$tmp/out/bin" "$tmp/out/include" "{{ tooling }}/protoc/"
    chmod 0755 "{{ tooling }}/protoc/bin/protoc"
    ln -sf "{{ tooling }}/protoc/bin/protoc" "$target"
    echo "protoc: $("$target" --version)"

# Go, for the FFI compatibility harness and the Go-parity integration suites.
[doc("Go, for the FFI harness and the Go-parity suites.")]
[group('setup')]
setup-go:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ -x "{{ go_root }}/bin/go" ] && "{{ go_root }}/bin/go" version 2>/dev/null | grep -q "go{{ go_version }} "; then
        echo "go: {{ go_version }} already installed"; exit 0
    fi
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
    # go.dev's download index carries a sha256 per file; verify rather than
    # trust. (The .sha256 sibling URL redirects to an HTML page, not a digest.)
    command -v jq >/dev/null 2>&1 || { echo "error: jq is required; run 'just setup-jq'" >&2; exit 1; }
    want="$(curl -fsS "https://go.dev/dl/?mode=json&include=all" \
        | jq -r --arg f "go{{ go_version }}.${os}-${arch}.tar.gz" \
            '.[].files[] | select(.filename==$f) | .sha256' | head -1)"
    [ -n "$want" ] && [ "$want" != "null" ] || { echo "error: no published checksum for go{{ go_version }} ${os}-${arch}" >&2; exit 1; }
    got="$(just _sha256 "$tmp/go.tar.gz")"
    [ "$got" = "$want" ] || { echo "error: go checksum mismatch" >&2; echo "  expected $want" >&2; echo "  got      $got" >&2; exit 1; }
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
    # Version-stamped so bumping jdk_release re-fetches instead of short-circuiting.
    stamp="{{ jdk_root }}/.release"
    if [ -x "{{ jdk_root }}/bin/java" ] && [ "$(cat "$stamp" 2>/dev/null)" = "{{ jdk_release }}" ]; then
        echo "jdk: {{ jdk_release }} already installed"; exit 0
    fi
    command -v jq >/dev/null 2>&1 || { echo "error: jq is required; run 'just setup-jq'" >&2; exit 1; }
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
    # A pinned release, not Adoptium's floating "latest" stream, and the
    # checksum Adoptium publishes for exactly that asset.
    rel_enc="$(printf '%s' "{{ jdk_release }}" | sed 's/+/%2B/')"
    url="https://api.adoptium.net/v3/binary/version/${rel_enc}/${os}/${arch}/jdk/hotspot/normal/eclipse"
    want="$(curl -fsSL "https://api.adoptium.net/v3/assets/release_name/eclipse/${rel_enc}?os=${os}&architecture=${arch}&image_type=jdk" \
        | jq -r '.binaries[0].package.checksum')"
    [ -n "$want" ] && [ "$want" != "null" ] || { echo "error: no published checksum for {{ jdk_release }} ${os}-${arch}" >&2; exit 1; }
    tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
    echo "fetching Temurin {{ jdk_release }} for ${os}-${arch}"
    curl -fsSL -o "$tmp/jdk.tar.gz" "$url"
    got="$(just _sha256 "$tmp/jdk.tar.gz")"
    [ "$got" = "$want" ] || { echo "error: JDK checksum mismatch" >&2; echo "  expected $want" >&2; echo "  got      $got" >&2; exit 1; }
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
    printf '%s' "{{ jdk_release }}" > "{{ jdk_root }}/.release"
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
    @TLA_VERSION={{ tla_version }} TLA_SHA256={{ tla_sha256 }} proofs/tla/tools/tlc --fetch-only

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

# Thin-LTO build for iteration. CI measured 1m40s vs 6m44s cold; never ship
# an artifact from this profile.
[doc("Thin-LTO build for fast iteration (never ship this profile).")]
[group('build')]
build-fast:
    cargo build --profile release-fast -p cli

# Browser client, via wasm-pack, into pkg/wasm. The opt-level override
# matches CI's `Build WASM` step; without it the local artifact differs.
[group('build')]
build-wasm:
    CARGO_PROFILE_RELEASE_OPT_LEVEL=z wasm-pack build crates/wasm --release --target web --out-dir ../../pkg/wasm

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

# Workspace unit tests. Mirrors CI's Build & Test step, which excludes the three suites that
# need a built binary or an external runtime.
[doc("Workspace unit tests (mirrors CI's Build & Test step).")]
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
# (see the P2P Integration job's comment in ci.yml).
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
    total=$(echo {{ integration_suites }} | wc -w | tr -d ' ')
    if [ ${#failed[@]} -gt 0 ]; then
        echo "FAILED ${#failed[@]}/${total}: ${failed[*]}" >&2
        exit 1
    fi
    echo "all ${total} listed areas passed"

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
conformance-behavioral: build-fast
    DEFRA_CONFORMANCE_BINARY="{{ root }}/target/release-fast/defra" \
        cargo test -p conformance --test tla_conformance -- --test-threads=1

# ---------------------------------------------------------------------------
# Quality gates
# ---------------------------------------------------------------------------

# Rewrite formatting in place.
[group('check')]
fmt:
    cargo fmt --all

# Fail on any formatting diff, as CI's Lint job does.
[group('check')]
fmt-check:
    cargo fmt --all -- --check

# Every clippy invocation CI's Lint job runs, in the same order.
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
    cargo clippy -p defra-node --no-default-features --features lark,redb,native --all-targets -- -D warnings
    just check-node-graph

# Feature-graph contracts for defra-node (#1398–#1400). Not a size check.
[group('check')]
check-node-graph:
    bash .github/scripts/assert-defra-node-graph.sh

# Lean combo check (grows as later PRs make the combo legal).
[group('check')]
check-node-lean *features:
    cargo check -p defra-node --no-default-features --features {{ features }}

# Lint the test and bench targets too. CI does NOT do this for the workspace, so
# this catches lints that would otherwise sit in the tree unnoticed.
[doc("Clippy including test and bench targets, which CI does not lint.")]
[group('check')]
lint-all-targets:
    cargo clippy --all --all-targets -- -D warnings

# Browser-client lint on the real wasm target, as CI's WASM job does.
[group('check')]
lint-wasm:
    cargo clippy -p defra-wasm --target wasm32-unknown-unknown --all-targets -- -D warnings
    cargo clippy -p defra-wasm --target wasm32-unknown-unknown --no-default-features --all-targets -- -D warnings

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
ci: fmt-check lint lint-wasm test test-features build-wasm integration proofs conformance-behavioral
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
