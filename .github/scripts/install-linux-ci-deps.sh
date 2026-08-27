#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
"${script_dir}/apt-update-without-nodesource.sh"

sudo apt-get install -y --no-install-recommends \
  clang \
  cmake \
  libclang-dev \
  libdbus-1-dev \
  libssl-dev \
  pkg-config \
  protobuf-compiler
