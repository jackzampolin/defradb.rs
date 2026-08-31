#!/usr/bin/env python3
"""Parse the checked inspire-gpu benchmark into Defra's evidence schema."""

import argparse
import json
import re
from pathlib import Path


SIZE_RE = re.compile(r"^=== (\d+) GB \((\d+) entries x (\d+) B\) ===$")
BATCH_RE = re.compile(
    r"^\s+B=\s*(\d+):\s+([0-9.]+) ms/batch\s+"
    r"([0-9.]+) ms/query\s+([0-9.]+) q/s$"
)


def number(line: str, pattern: str, label: str) -> float:
    match = re.search(pattern, line)
    if not match:
        raise ValueError(f"could not parse {label}: {line!r}")
    return float(match.group(1))


def parse_log(text: str) -> dict:
    scales = []
    current = None
    client = None
    for raw in text.splitlines():
        line = raw.rstrip()
        size = SIZE_RE.match(line)
        if size:
            current = {
                "label_gib": int(size.group(1)),
                "entries": int(size.group(2)),
                "useful_row_bytes": int(size.group(3)),
                "batches": [],
            }
            scales.append(current)
            continue
        if current is None:
            continue
        if line.startswith("  geom :"):
            for key in ("db_rows", "db_cols", "n_packed"):
                current[key] = int(number(line, rf"{key}=(\d+)", key))
            current["response_ciphertexts"] = int(number(line, r"c=(\d+)", "c"))
        elif line.startswith("  comm :"):
            current["published_query_kib"] = number(line, r"comm : ([0-9.]+)", "query KiB")
            current["published_response_kib"] = number(line, r"\+ ([0-9.]+)", "response KiB")
        elif line.startswith("  hint :"):
            current["preprocessed_gb"] = number(line, r"hint : ([0-9.]+)", "preprocessed GB")
            current["encoded_database_gb"] = number(line, r"\+ ([0-9.]+)", "database GB")
            current["resident_gb"] = number(line, r"= ([0-9.]+) GB resident", "resident GB")
        elif line.startswith("  materialize:"):
            current["host_materialize_ms"] = number(line, r"materialize: ([0-9.]+)", "materialize")
        elif line.startswith("  preprocess:"):
            current["gpu_preprocess_ms"] = number(line, r"preprocess: ([0-9.]+)", "preprocess")
        elif line.startswith("  server-context:"):
            current["server_context_ms"] = number(line, r"server-context: ([0-9.]+)", "context")
        elif line.startswith("  cold-online:"):
            current["cold_online"] = {
                "client_query_ms": number(line, r"client-query=([0-9.]+)", "cold client query"),
                "server_ms": number(line, r"server=([0-9.]+)", "cold server"),
                "client_extract_ms": number(line, r"client-extract=([0-9.]+)", "cold extract"),
                "correctness": "correctness=true" in line,
            }
        elif line.startswith("  check:"):
            current["correctness"] = {
                "passed": int(number(line, r"check: (\d+)", "passed")),
                "failed": int(number(line, r"ok, (\d+)", "failed")),
            }
        elif line.startswith("  >>> latency:"):
            current["warm_single_server_ms"] = number(line, r"latency: ([0-9.]+)", "warm latency")
        else:
            batch = BATCH_RE.match(line)
            if batch:
                current["batches"].append({
                    "batch": int(batch.group(1)),
                    "batch_server_ms": float(batch.group(2)),
                    "server_ms_per_query": float(batch.group(3)),
                    "queries_per_second": float(batch.group(4)),
                })
            elif line.startswith("    client query build"):
                client = client or {}
                client["query_build_ms"] = number(line, r":\s+([0-9.]+)", "query build")
            elif line.startswith("    client query pack"):
                client = client or {}
                client["query_packed_bytes"] = int(number(line, r"\((\d+) B\)", "query bytes"))
                client["query_pack_ms"] = number(line, r":\s+([0-9.]+)", "query pack")
            elif line.startswith("    server query unpack"):
                client = client or {}
                client["server_query_unpack_ms"] = number(line, r":\s+([0-9.]+)", "query unpack")
            elif line.startswith("    server response compress"):
                client = client or {}
                client["server_response_compress_ms"] = number(line, r":\s+([0-9.]+)", "response compress")
            elif line.startswith("    client extract_compressed"):
                client = client or {}
                client["client_extract_compressed_ms"] = number(line, r":\s+([0-9.]+)", "compressed extract")
                client["client_extract_plain_ms"] = number(line, r"plain extract: ([0-9.]+)", "plain extract")

    if not scales:
        raise ValueError("no inspire-gpu scale found")
    for scale in scales:
        required = ("entries", "db_rows", "warm_single_server_ms", "cold_online")
        missing = [key for key in required if key not in scale]
        if missing:
            raise ValueError(f"scale {scale.get('label_gib')} missing {missing}")
        if not scale["cold_online"]["correctness"] or scale.get("correctness", {}).get("failed") != 0:
            raise ValueError(f"scale {scale['label_gib']} failed correctness")
    if not client:
        raise ValueError("wire-path client measurements missing")
    client["compressed_response_bytes"] = 12_288 * scales[0]["response_ciphertexts"]
    return {"scales": scales, "client": client}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--log", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--gpu", required=True)
    parser.add_argument("--compute-capability", required=True)
    parser.add_argument("--driver", required=True)
    parser.add_argument("--cpu", required=True)
    parser.add_argument("--cuda", required=True)
    parser.add_argument("--total-memory-mib", required=True, type=int)
    parser.add_argument("--selected-tiers", required=True)
    args = parser.parse_args()

    parsed = parse_log(Path(args.log).read_text())
    selected_tiers = [int(value) for value in args.selected_tiers.split()]
    completed_tiers = {scale["label_gib"] for scale in parsed["scales"]}
    if completed_tiers != set(selected_tiers):
        raise SystemExit(
            f"completed tiers {sorted(completed_tiers)} do not match selected "
            f"tiers {selected_tiers}"
        )
    resident_gb = {1: 1.61, 4: 6.44, 16: 25.77}
    recommended_device_mib = {1: 0, 4: 12_000, 16: 30_000}
    capacity = []
    for tier in (1, 4, 16):
        if tier in completed_tiers:
            capacity.append({
                "label_gib": tier,
                "status": "completed",
                "required_resident_gb": resident_gb[tier],
            })
        else:
            capacity.append({
                "label_gib": tier,
                "status": "capacity_blocked",
                "required_resident_gb": resident_gb[tier],
                "available_device_memory_mib": args.total_memory_mib,
                "runner_minimum_device_memory_mib": recommended_device_mib[tier],
                "reason": "runner reserves CUDA, display, WSL, and batch-scratch headroom",
            })

    report = {
        "schema": "defradb-inspire-gpu-suite-v1",
        "profile": args.profile,
        "upstream": {
            "url": "https://github.com/keewoolee/inspire-gpu",
            "commit": args.commit,
            "qualification": "checked benchmark-only instrumentation; cryptographic implementation and parameters unchanged",
        },
        "hardware": {
            "gpu": args.gpu,
            "compute_capability": args.compute_capability,
            "driver": args.driver,
            "cpu": args.cpu,
            "cuda": args.cuda,
        },
        "security": {
            "server_count": 1,
            "privacy": "single-server computational PIR under the InsPIRe lattice assumptions",
            "client_database_hint": False,
            "server_preprocessing": True,
        },
        "scope": {
            "corpus": "same deterministic 120-byte logical records as the Dense/GPU-DPF adapter, encoded into InsPIRe 15-bit slots",
            "cold_client": "first fresh query generation with no database-dependent hint",
            "cold_online_server": "first answer after preprocessing and context construction; table is already GPU-resident",
            "cold_snapshot": "host materialization, GPU preprocessing, and server-context construction reported separately",
            "warm": "upstream warmup and median methodology",
            "excluded": "network transfer time, request scheduling, TLS/OHTTP/Tor, keyword-to-ordinal lookup, queue dwell, and electricity price",
        },
        "capacity": capacity,
        **parsed,
    }
    Path(args.output).write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
