#!/usr/bin/env bash
set -euo pipefail

ARTIFACT_DIR=${1:?artifact directory is required}

shopt -s nullglob
manifests=("$ARTIFACT_DIR"/*.capabilities)
if [ ${#manifests[@]} -eq 0 ]; then
  echo "No capability manifests found in $ARTIFACT_DIR" >&2
  exit 1
fi

has_feature() {
  case ",${1##*--features }," in
    *",$2,"*) return 0 ;;
    *) return 1 ;;
  esac
}

rows() {
  local manifest artifact platform features
  for manifest in "${manifests[@]}"; do
    IFS='|' read -r artifact platform features < "$manifest"

    local transports="libp2p"
    has_feature "$features" iroh && transports="libp2p + iroh"

    local lens="no"
    has_feature "$features" wasmtime-runtime && lens="yes"

    local acp="no"
    has_feature "$features" sourcehub && acp="yes"

    printf '%s\t%s\t%s\t%s\t%s\n' "$artifact" "$platform" "$transports" "$lens" "$acp"
  done
}

echo '## Capability matrix'
echo
echo '| Artifact | Platforms | Transports | Lens migrations | SourceHub ACP |'
echo '|---|---|---|---|---|'

rows | awk -F'\t' '
  !seen[$1]++ { order[++count] = $1; transports[$1] = $3; lens[$1] = $4; acp[$1] = $5 }
  { platforms[$1] = platforms[$1] == "" ? $2 : platforms[$1] ", " $2 }
  END {
    for (i = 1; i <= count; i++) {
      artifact = order[i]
      printf "| `%s_*` | %s | %s | %s | %s |\n",
        artifact, platforms[artifact], transports[artifact], lens[artifact], acp[artifact]
    }
  }
'
