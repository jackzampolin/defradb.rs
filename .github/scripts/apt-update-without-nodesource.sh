#!/usr/bin/env bash
set -euo pipefail

# Self-hosted runners may have NodeSource configured for operator-managed Node
# upgrades. That third-party repository is not needed by DefraDB builds, and a
# stale or unavailable NodeSource endpoint must not prevent Ubuntu packages
# from being refreshed. Build a request-local source set instead of mutating
# the runner's global APT configuration.
filtered_root="$(mktemp -d)"
trap 'rm -rf "${filtered_root}"' EXIT
mkdir -p "${filtered_root}/sources.list.d"

if [[ -f /etc/apt/sources.list ]]; then
  grep -Fv "deb.nodesource.com" /etc/apt/sources.list \
    >"${filtered_root}/sources.list" || true
else
  : >"${filtered_root}/sources.list"
fi

shopt -s nullglob
for source_file in /etc/apt/sources.list.d/*; do
  case "${source_file}" in
    *.list | *.sources) ;;
    *) continue ;;
  esac
  if grep -Fq "deb.nodesource.com" "${source_file}"; then
    echo "Skipping unavailable NodeSource APT source: ${source_file}"
    continue
  fi
  cp "${source_file}" "${filtered_root}/sources.list.d/"
done

sudo apt-get "$@" \
  -o "Dir::Etc::sourcelist=${filtered_root}/sources.list" \
  -o "Dir::Etc::sourceparts=${filtered_root}/sources.list.d" \
  update
