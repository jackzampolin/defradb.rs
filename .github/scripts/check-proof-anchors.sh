#!/usr/bin/env bash
# Every `*.rs` filename named in proofs/ or SURVEY.md must resolve to a tracked
# file. Anchors are prose, not code, so nothing else catches them when a rename
# moves the thing being modelled: proofs/src/registry.rs treats `anchor` as free
# text, and a fully-qualified-path grep misses the bare filenames that most
# anchors actually use.
set -euo pipefail
cd "$(dirname "$0")/../.."

BASELINE=proofs/anchor-baseline.txt
tracked=$(mktemp); git ls-files > "$tracked"
trap 'rm -f "$tracked"' EXIT

dead=0
while read -r name; do
    [ -z "$name" ] && continue
    grep -q "/$name\$" "$tracked" && continue
    grep -qxF "$name" "$BASELINE" 2>/dev/null && continue
    echo "DEAD ANCHOR: $name is named under proofs/, SURVEY.md or a rustdoc but no tracked file matches"
    git grep -n "$name" -- proofs SURVEY.md crates tools | sed 's/^/    /'
    dead=$((dead + 1))
done < <( { git grep -ohE '[a-z_0-9]+\.rs' -- proofs SURVEY.md
            # rustdoc in the crates names files too; a rename rots those the
            # same way, and only backticked names are unambiguous there.
            git grep -ohE '`[a-z_0-9]+\.rs`' -- crates tools | tr -d '`'
          } | sort -u )

if [ "$dead" -gt 0 ]; then
    echo
    echo "$dead dead anchor(s). Retarget them, or add to $BASELINE with a reason."
    exit 1
fi
echo "proof anchors: all resolve"
