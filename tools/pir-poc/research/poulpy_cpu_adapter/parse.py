#!/usr/bin/env python3
"""Parse one pinned Poulpy AVX2 Defra benchmark process."""

import argparse
import json
import re
from pathlib import Path


def duration_ms(value: str) -> float:
    match = re.fullmatch(r"([0-9.]+)(ns|µs|us|ms|s)", value.strip())
    if not match:
        raise ValueError(f"unknown Rust duration {value!r}")
    number = float(match.group(1))
    return number * {"ns": 1e-6, "µs": 1e-3, "us": 1e-3, "ms": 1.0, "s": 1_000.0}[match.group(2)]


def field(text: str, label: str) -> str:
    match = re.search(rf"^{re.escape(label)}\s*:\s*(.+)$", text, re.MULTILINE)
    if not match:
        raise ValueError(f"missing {label}")
    return match.group(1).strip()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--log", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--cpu", required=True)
    parser.add_argument("--batch", required=True, type=int)
    args = parser.parse_args()
    text = Path(args.log).read_text()
    if field(text, "schema") != "defradb-poulpy-cpu-avx2-v1":
        raise SystemExit("not a Defra Poulpy AVX2 log")
    if not re.search(rf"RESULT\s*:\s*{args.batch}/{args.batch} decoded OK", text):
        raise SystemExit("Poulpy result failed correctness")
    wall_match = re.search(r"ONLINE avg wall .*:\s*([^\s]+)$", text, re.MULTILINE)
    work_match = re.search(r"ONLINE avg work \(sum of phases\):\s*([^\s]+)$", text, re.MULTILINE)
    query_match = re.search(r"QUERY \(build \d+\)\s*:\s*([^\s]+)$", text, re.MULTILINE)
    if not wall_match or not work_match or not query_match:
        raise SystemExit("online timing fields are missing")
    query_bytes = int(re.search(r"QUERY size\s*:\s*(\d+) B", text).group(1))
    response_bytes = int(re.search(r"RESPONSE size\s*:\s*(\d+) B", text).group(1))
    capacity = int(re.search(r"database\s*:\s*(\d+) payloads", text).group(1))
    peak = field(text, "PEAK MEMORY (VmHWM)")
    report = {
        "schema": "defradb-poulpy-cpu-avx2-v1",
        "upstream": {"url": "https://github.com/poulpy-fhe/poulpy-pir", "commit": args.commit},
        "hardware": {"cpu": args.cpu, "backend": "poulpy_cpu_avx::FFT64Avx (AVX2/FMA)"},
        "security": "single-server computational PIR under InsPIRe2 lattice assumptions",
        "entries": capacity,
        "useful_row_bytes": 120,
        "physical_row_bytes": 128,
        "batch": args.batch,
        "setup_ms": duration_ms(field(text, "SETUP")),
        "database_fill_ms": duration_ms(field(text, "database fill")),
        "query_mask_ms": duration_ms(field(text, "SETUP (query mask)")),
        "offline_ms": duration_ms(field(text, "OFFLINE total")),
        "client_query_ms_per_query": duration_ms(query_match.group(1)) / args.batch,
        "server_wall_ms_per_query": duration_ms(wall_match.group(1)) / args.batch,
        "server_summed_phase_work_ms_per_query": duration_ms(work_match.group(1)) / args.batch,
        "upload_bytes_per_query": query_bytes,
        "download_bytes_per_query": response_bytes,
        "peak_rss": peak,
        "correctness": True,
    }
    Path(args.output).write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
