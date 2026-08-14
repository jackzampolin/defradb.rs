#!/usr/bin/env bash
# Prints the sibling studio host's ssh name, or nothing when this machine has
# no sibling. The two studio hosts hold each other's binary cache, so both the
# producer's push and the consumer's pull need to agree on who the peer is.
#
# The machine's own hostname is the primary source: it is what actually
# determines which host-local cache the binaries land in. RUNNER_NAME is a
# fallback for a slot whose runner was registered under a name that does not
# match its host — it is a GitHub-side label and can drift from the machine.
set -euo pipefail

SELF=$(hostname -s 2>/dev/null || true)
case "$SELF" in
  studio-1 | studio-2) ;;
  *) SELF="${RUNNER_NAME:-}" ;;
esac

case "$SELF" in
  studio-1*) echo studio-2 ;;
  studio-2*) echo studio-1 ;;
esac
