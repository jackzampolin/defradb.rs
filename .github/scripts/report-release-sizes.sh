#!/usr/bin/env bash
set -euo pipefail

ARTIFACT_DIR=${1:?artifact directory is required}
PREVIOUS_DIR=${2:?previous artifact directory is required}
CURRENT_TAG=${3:?current tag is required}
PREVIOUS_TAG=${4:-}
SUMMARY=${GITHUB_STEP_SUMMARY:-/dev/stdout}

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

format_size() {
  awk -v bytes="$1" 'BEGIN {
    if (bytes >= 1048576) printf "%.2f MiB", bytes / 1048576
    else printf "%.2f KiB", bytes / 1024
  }'
}

format_delta() {
  awk -v current="$1" -v previous="$2" 'BEGIN {
    delta = current - previous
    magnitude = delta < 0 ? -delta : delta
    if (magnitude >= 1048576) printf "%+.2f MiB", delta / 1048576
    else printf "%+.2f KiB", delta / 1024
    if (previous > 0) printf " (%+.1f%%)", delta * 100 / previous
    else printf " (n/a)"
  }'
}

member_size() {
  tar -xOzf "$1" "$2" | wc -c | tr -d ' '
}

report_row() {
  local label=$1 current=$2 previous=${3:-}
  if [ -n "$previous" ]; then
    printf '| `%s` | %s | %s | %s |\n' \
      "$label" "$(format_size "$current")" "$(format_size "$previous")" \
      "$(format_delta "$current" "$previous")" >> "$SUMMARY"
  else
    printf '| `%s` | %s | - | - |\n' "$label" "$(format_size "$current")" >> "$SUMMARY"
  fi
}

previous_archive() {
  local name=$1
  if [ -n "$PREVIOUS_TAG" ]; then
    local previous_name=${name/"$CURRENT_TAG"/"$PREVIOUS_TAG"}
    if [ -f "$PREVIOUS_DIR/$previous_name" ]; then
      printf '%s\n' "$PREVIOUS_DIR/$previous_name"
    fi
  fi
}

warn_previous() {
  echo "::warning::Unable to read previous artifact ${1##*/}; skipping comparison" >&2
}

{
  echo '## Release artifact sizes'
  echo
  if [ -n "$PREVIOUS_TAG" ]; then
    echo "Compared with \`$PREVIOUS_TAG\`."
  else
    echo 'No previous release tag was found.'
  fi
  echo
  echo '| Artifact | Current | Previous | Change |'
  echo '|---|---:|---:|---:|'
} >> "$SUMMARY"

cli_count=0
ffi_count=0
wasm_count=0

for archive in "$ARTIFACT_DIR"/*.tar.gz; do
  name=${archive##*/}
  label=${name/"_$CURRENT_TAG"/}
  label=${label%.tar.gz}
  previous=$(previous_archive "$name")

  case "$name" in
    defra-wasm_*)
      current_wasm="$tmp/current.wasm"
      tar -xOzf "$archive" defra_wasm_bg.wasm > "$current_wasm"
      current_raw=$(wc -c < "$current_wasm" | tr -d ' ')
      current_gzip=$(gzip -9 -n -c "$current_wasm" | wc -c | tr -d ' ')
      current_brotli=$(brotli --quality=11 --stdout "$current_wasm" | wc -c | tr -d ' ')

      previous_raw=
      previous_gzip=
      previous_brotli=
      if [ -n "$previous" ]; then
        previous_wasm="$tmp/previous.wasm"
        if tar -xOzf "$previous" defra_wasm_bg.wasm > "$previous_wasm" 2>/dev/null; then
          previous_raw=$(wc -c < "$previous_wasm" | tr -d ' ')
          previous_gzip=$(gzip -9 -n -c "$previous_wasm" | wc -c | tr -d ' ')
          previous_brotli=$(brotli --quality=11 --stdout "$previous_wasm" | wc -c | tr -d ' ')
        else
          warn_previous "$previous"
        fi
      fi

      report_row "$label raw" "$current_raw" "$previous_raw"
      report_row "$label gzip-9" "$current_gzip" "$previous_gzip"
      report_row "$label brotli-11" "$current_brotli" "$previous_brotli"
      wasm_count=$((wasm_count + 1))
      ;;
    defra-ffi-*ios_xcframework*)
      ;;
    defra-ffi_*)
      member=$(tar -tzf "$archive" | awk '/^libdefra_ffi\.(so|dylib)$/ { member = $0 } END { print member }')
      [ -n "$member" ] || { echo "No FFI library found in $name" >&2; exit 1; }
      current=$(member_size "$archive" "$member")
      previous_size=
      if [ -n "$previous" ]; then
        previous_member=$(tar -tzf "$previous" 2>/dev/null | awk '/^libdefra_ffi\.(so|dylib)$/ { member = $0 } END { print member }' || true)
        if [ -n "$previous_member" ] && previous_size=$(member_size "$previous" "$previous_member" 2>/dev/null); then
          :
        else
          previous_size=
          warn_previous "$previous"
        fi
      fi
      report_row "$label" "$current" "$previous_size"
      ffi_count=$((ffi_count + 1))
      ;;
    defra_*.tar.gz)
      current=$(member_size "$archive" defra)
      previous_size=
      if [ -n "$previous" ]; then
        if ! previous_size=$(member_size "$previous" defra 2>/dev/null); then
          previous_size=
          warn_previous "$previous"
        fi
      fi
      report_row "$label" "$current" "$previous_size"
      cli_count=$((cli_count + 1))
      ;;
  esac
done

if [ "$cli_count" -eq 0 ] || [ "$ffi_count" -eq 0 ] || [ "$wasm_count" -eq 0 ]; then
  echo "Incomplete release artifact set: cli=$cli_count ffi=$ffi_count wasm=$wasm_count" >&2
  exit 1
fi
