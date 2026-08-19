#!/usr/bin/env python3
"""Verify an exported Defra PIR corpus before an external artifact reads it."""

import argparse
import hashlib
import json
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--corpus", required=True)
    parser.add_argument("--manifest", required=True)
    args = parser.parse_args()

    corpus_path = Path(args.corpus)
    manifest = json.loads(Path(args.manifest).read_text(encoding="utf-8"))
    expected_bytes = manifest["page_count"] * manifest["page_bytes"]
    expected_sha256 = manifest.get("corpus_sha256")
    if not expected_sha256:
        raise SystemExit(
            "manifest has no corpus_sha256; re-export it with the current pir-poc"
        )

    digest = hashlib.sha256()
    observed_bytes = 0
    with corpus_path.open("rb") as corpus:
        while chunk := corpus.read(1024 * 1024):
            observed_bytes += len(chunk)
            digest.update(chunk)

    if observed_bytes != expected_bytes:
        raise SystemExit(
            f"corpus size mismatch: got {observed_bytes}, expected {expected_bytes}"
        )
    observed_sha256 = digest.hexdigest()
    if observed_sha256 != expected_sha256.lower():
        raise SystemExit(
            f"corpus SHA-256 mismatch: got {observed_sha256}, expected {expected_sha256}"
        )
    print(f"verified corpus: {observed_bytes} bytes, sha256={observed_sha256}")


if __name__ == "__main__":
    main()
