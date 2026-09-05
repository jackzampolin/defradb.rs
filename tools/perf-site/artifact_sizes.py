#!/usr/bin/env python3
"""Emit what each shipped artifact weighs, as one family record.

Size is not a benchmark. It is read off an artifact that has already been
built, and a bench process cannot build a release CLI or a wasm bundle without
becoming a build script. So this runs where the artifacts are produced and
writes the same family record a bench would, which is what puts it on the
dashboard beside everything else.

Every artifact is named on the command line as ``label=path``. An artifact that
is missing is reported as missing and contributes no row: a shipped binary that
failed to build must not be drawn as a zero-byte one.

The wasm bundle is additionally reported compressed, because that is what a
browser downloads and the raw number is not the one a user waits for. gzip is
computed in-process; brotli is used only when the interpreter has it, and its
absence is stated rather than passed over.
"""

import argparse
import gzip
import json
import pathlib
import sys

try:
    import brotli  # type: ignore
except ImportError:
    brotli = None


def rows_for(label, path):
    data = path.read_bytes()
    rows = [{"name": f"{label}, raw", "value": len(data)}]
    # Only the browser bundle travels compressed. Reporting a gzip size for a
    # binary nobody serves over HTTP would be a number without a question.
    if path.suffix == ".wasm":
        rows.append(
            {"name": f"{label}, gzip", "value": len(gzip.compress(data, compresslevel=9))}
        )
        if brotli is not None:
            rows.append(
                {
                    "name": f"{label}, brotli",
                    "value": len(brotli.compress(data, quality=11)),
                }
            )
    return rows


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "artifacts",
        nargs="+",
        metavar="LABEL=PATH",
        help="an artifact to weigh, e.g. defra=target/release/defra",
    )
    ap.add_argument("--out", required=True, help="append the family record here")
    args = ap.parse_args()

    rows, missing = [], []
    for spec in args.artifacts:
        if "=" not in spec:
            print(f"artifact-sizes: {spec!r} is not LABEL=PATH", file=sys.stderr)
            return 2
        label, _, raw = spec.partition("=")
        path = pathlib.Path(raw)
        if not path.is_file():
            missing.append(f"{label} ({raw})")
            continue
        rows.extend(rows_for(label, path))

    note = (
        "What each shipped artifact weighs. The browser bundle is also reported "
        "compressed, because that is what a browser downloads."
    )
    if brotli is None:
        note += " Brotli was not available on this runner, so only gzip is reported."
    if missing:
        note += (
            " Not built on this runner, so not weighed: " + ", ".join(sorted(missing)) + "."
        )
    if not rows:
        print(
            "artifact-sizes: none of the named artifacts exist, so there is nothing "
            "to weigh. Writing the family as absent rather than as zero.",
            file=sys.stderr,
        )

    family = {
        "title": "Artifact size",
        "note": note,
        # A byte count does not move because a runner was busy.
        "trust": "clean" if rows else "absent",
        "deterministic": True,
        "groups": (
            [
                {
                    "name": "Shipped artifacts",
                    "unit": "B",
                    "lower_is_better": True,
                    "rows": rows,
                }
            ]
            if rows
            else []
        ),
    }
    with open(args.out, "a") as f:
        f.write(json.dumps({"family": "artifact_size", "data": family}) + "\n")
    print(f"artifact-sizes: {len(rows)} row(s) recorded, {len(missing)} artifact(s) missing")
    return 0


if __name__ == "__main__":
    sys.exit(main())
