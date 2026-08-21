#!/usr/bin/env bash
# Feature-graph contracts for defra-node (#1398–#1400). Not a size check.
#
# Inspect `cargo tree -p <crate> -e normal --locked [features] -i <pkg>` stdout.
# Present iff a line matches ^<pkg> v (e.g. ^sourcehub v). Absent iff exit 101
# or exit 0 with empty stdout (cargo prints `warning: nothing to print.` on
# stderr for unused optional deps). Never treat the -i exit code as the
# predicate. Never pass --workspace: workspace feature unification would pull
# CLI's libp2p into every tree.
set -euo pipefail

# Always `-p defra-node`, `-p cli`, or `-p db`. Workspace trees are forbidden.
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

assert_present() {
  local crate=$1
  local pkg=$2
  shift 2
  local stdout rc
  set +e
  stdout="$(cargo tree -p "$crate" -e normal --locked "$@" -i "$pkg")"
  rc=$?
  set -e
  if printf '%s\n' "$stdout" | grep -q "^${pkg} v"; then
    echo "ok: ${crate}${*:+ $*} contains ^${pkg} v"
    return 0
  fi
  echo "error: expected ^${pkg} v in ${crate}${*:+ $*} tree (cargo tree -i exit ${rc})" >&2
  if [ -n "$stdout" ]; then
    printf '%s\n' "$stdout" >&2
  fi
  exit 1
}

# Present iff stdout matches ^<pkg> v. Absent iff no such line: exit 101 or
# exit 0 + empty stdout both count as absent. Never treat the -i exit code
# as present.
assert_absent() {
  local crate=$1
  local pkg=$2
  shift 2
  local stdout rc
  set +e
  stdout="$(cargo tree -p "$crate" -e normal --locked "$@" -i "$pkg")"
  rc=$?
  set -e
  if printf '%s\n' "$stdout" | grep -q "^${pkg} v"; then
    echo "error: unexpected ^${pkg} v in ${crate}${*:+ $*} tree (cargo tree -i exit ${rc})" >&2
    printf '%s\n' "$stdout" >&2
    exit 1
  fi
  echo "ok: ${crate}${*:+ $*} has no ^${pkg} v"
}

unique_crate_names() {
  cargo tree -p defra-node -e normal --locked --prefix none "$@" \
    | awk '{print $1}' | grep -v '^(*)' | sort -u | wc -l
}

# No libp2p / libp2p-* crates. Grep --prefix none; do not enumerate names.
assert_no_libp2p() {
  local crate=$1
  shift
  local stdout
  stdout="$(cargo tree -p "$crate" -e normal --locked --prefix none "$@")"
  if printf '%s\n' "$stdout" | grep -q '^libp2p'; then
    echo "error: unexpected ^libp2p in ${crate}${*:+ $*} --prefix none tree" >&2
    printf '%s\n' "$stdout" | grep '^libp2p' >&2
    exit 1
  fi
  echo "ok: ${crate}${*:+ $*} --prefix none has no ^libp2p"
}

assert_present defra-node sourcehub
assert_present defra-node wasmtime
assert_present cli libp2p

# Default defra-node (no p2p feature) must not resolve libp2p / libp2p-*.
assert_no_libp2p defra-node

# Isolated db native graph must not resolve the optional libp2p dep.
# cargo tree -i libp2p often exits 0 with empty stdout — still absent.
assert_absent db libp2p --no-default-features --features native

# Lean local-ACP + native host. No SourceHub, no Wasmtime, no libp2p.
assert_absent defra-node sourcehub --no-default-features --features lark,redb,native
assert_absent defra-node acp-light-client --no-default-features --features lark,redb,native
assert_absent defra-node commonware-cryptography --no-default-features --features lark,redb,native
assert_absent defra-node aws-lc-rs --no-default-features --features lark,redb,native
assert_absent defra-node cosmrs --no-default-features --features lark,redb,native
assert_absent defra-node wasmtime --no-default-features --features lark,redb,native
assert_absent defra-node cranelift-codegen --no-default-features --features lark,redb,native

# Lean Iroh P2P: Iroh present, no libp2p / libp2p-*, p2p crate not enabling
# libp2p-transport. `p2p` implies native, so listing native is redundant but
# matches the advertised combo.
LEAN_IROH=(--no-default-features --features lark,redb,native,p2p)
assert_no_libp2p defra-node "${LEAN_IROH[@]}"
assert_present defra-node iroh "${LEAN_IROH[@]}"

# p2p crate feature line (the ^p2p v package, not p2p-*). Default cargo tree
# omits features; --format '{p} {f}' prints them so this can fail.
assert_p2p_crate_iroh_only() {
  local stdout rc line
  set +e
  stdout="$(cargo tree -p defra-node -e normal --locked "${LEAN_IROH[@]}" -i p2p --format '{p} {f}')"
  rc=$?
  set -e
  line="$(printf '%s\n' "$stdout" | grep '^p2p v' || true)"
  if [ -z "$line" ]; then
    echo "error: expected ^p2p v in lean Iroh tree (cargo tree -i exit ${rc})" >&2
    if [ -n "$stdout" ]; then
      printf '%s\n' "$stdout" >&2
    fi
    exit 1
  fi
  if ! printf '%s\n' "$line" | grep -q 'iroh-transport'; then
    echo "error: p2p crate line missing iroh-transport" >&2
    printf '%s\n' "$line" >&2
    exit 1
  fi
  if printf '%s\n' "$line" | grep -q 'libp2p-transport'; then
    echo "error: p2p crate line enables libp2p-transport" >&2
    printf '%s\n' "$line" >&2
    exit 1
  fi
  echo "ok: lean Iroh p2p crate line has iroh-transport and no libp2p-transport"
}
assert_p2p_crate_iroh_only

# Unique crate names. Log only — not a gate and not binary size. Main may move.
echo "defra-node default unique crate names: $(unique_crate_names)"
echo "defra-node --no-default-features --features lark,redb,native unique crate names: $(unique_crate_names --no-default-features --features lark,redb,native)"
echo "defra-node --no-default-features --features lark,redb,native,p2p unique crate names: $(unique_crate_names --no-default-features --features lark,redb,native,p2p)"
