#!/usr/bin/env python3
"""Aggregate isolated Dense, GPU-DPF, and InsPIRe GPU process repetitions."""

import argparse
import json
import statistics
from pathlib import Path


def summary(values: list[float]) -> dict:
    if not values:
        raise ValueError("cannot summarize an empty measurement")
    ordered = sorted(values)
    return {
        "p50": statistics.median(ordered),
        "min": ordered[0],
        "max": ordered[-1],
        "runs": len(ordered),
    }


def load_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    root = Path(args.root)
    dense_files = sorted(root.glob("dense-dpf-rep-*.jsonl"))
    inspire_files = sorted(root.glob("inspire-rep-*.json"))
    if len(dense_files) < 3 or len(dense_files) != len(inspire_files):
        raise SystemExit("need the same number of Dense/DPF and InsPIRe runs (at least three)")

    dense_runs = [load_jsonl(path) for path in dense_files]
    inspire_runs = [json.loads(path.read_text()) for path in inspire_files]
    for run in dense_runs:
        batches = [row["batch"] for row in run]
        if batches != [1, 2, 4, 8, 16, 32]:
            raise SystemExit(f"unexpected Dense/DPF batch matrix: {batches}")
        if any(row["entries"] != 8_388_608 for row in run):
            raise SystemExit("Dense/DPF repetition is not the 2^23-entry tier")
    for run in inspire_runs:
        if len(run["scales"]) != 1 or run["scales"][0]["entries"] != 8_388_608:
            raise SystemExit("InsPIRe repetition is not the 2^23-entry tier")

    rows = []
    for batch in (1, 2, 4, 8, 16, 32):
        dense_rows = [next(row for row in run if row["batch"] == batch) for run in dense_runs]
        inspire_scales = [run["scales"][0] for run in inspire_runs]
        inspire_batches = [
            next(value for value in scale["batches"] if value["batch"] == batch)
            for scale in inspire_scales
        ]
        protocols = []
        for key, label, servers in (
            ("dense_xor", "Dense XOR", 2),
            ("gpu_dpf", "GPU-DPF", 2),
        ):
            values = [row[key] for row in dense_rows]
            protocols.append({
                "protocol": label,
                "servers": servers,
                "aggregate_server_ms_per_query": summary([
                    value["aggregate_server_ms_per_query"] for value in values
                ]),
                "parallel_server_ms_per_query": summary([
                    value["parallel_wall_p50_ms"] / batch for value in values
                ]),
                "first_online_aggregate_server_ms_per_query": summary([
                    value["first_online"]["aggregate_server_ms"] / batch for value in values
                ]),
                "client_query_ms_per_query": summary([
                    value["client_batch_ms"] / batch for value in values
                ]),
                "upload_bytes_per_query": values[0]["aggregate_upload_bytes"] // batch,
                "download_bytes_per_query": values[0]["aggregate_response_bytes"] // batch,
            })

        inspire_clients = [run["client"] for run in inspire_runs]
        protocols.append({
            "protocol": "InsPIRe GPU",
            "servers": 1,
            "aggregate_server_ms_per_query": summary([
                value["server_ms_per_query"] for value in inspire_batches
            ]),
            "service_server_ms_per_query": summary([
                value["server_ms_per_query"]
                + client["server_query_unpack_ms"]
                + client["server_response_compress_ms"]
                for value, client in zip(inspire_batches, inspire_clients)
            ]),
            "first_online_aggregate_server_ms_per_query": summary([
                scale["cold_online"]["server_ms"] for scale in inspire_scales
            ]),
            "client_query_ms_per_query": summary([
                client["query_build_ms"] + client["query_pack_ms"]
                for client in inspire_clients
            ]),
            "client_recover_ms_per_query": summary([
                client["client_extract_compressed_ms"] for client in inspire_clients
            ]),
            "upload_bytes_per_query": inspire_clients[0]["query_packed_bytes"],
            "download_bytes_per_query": inspire_clients[0]["compressed_response_bytes"],
        })
        rows.append({"entries": 8_388_608, "useful_row_bytes": 120,
                     "batch": batch, "protocols": protocols})

    scale_values = [run["scales"][0] for run in inspire_runs]
    report = {
        "schema": "defradb-repeated-gpu-pir-comparison-v1",
        "repetitions": len(dense_runs),
        "process_order": ["Dense/DPF then InsPIRe" if index % 2 == 0
                          else "InsPIRe then Dense/DPF"
                          for index in range(len(dense_runs))],
        "dense_dpf_internal_order": [
            dense_runs[index][0]["protocol_order"] for index in range(len(dense_runs))
        ],
        "hardware": inspire_runs[0]["hardware"],
        "workload": {
            "entries": 8_388_608,
            "useful_row_bytes": 120,
            "logical_bytes": 8_388_608 * 120,
            "dense_physical_bytes_per_replica": 8_388_608 * 128,
        },
        "inspire_cold_snapshot_ms": {
            "host_materialize": summary([value["host_materialize_ms"] for value in scale_values]),
            "gpu_preprocess": summary([value["gpu_preprocess_ms"] for value in scale_values]),
            "server_context": summary([value["server_context_ms"] for value in scale_values]),
        },
        "rows": rows,
        "qualification": [
            "Each repetition is a fresh process; suite order and Dense/DPF internal order alternate.",
            "Aggregate server time is the primary metric. Dense and GPU-DPF sum both replicas.",
            "InsPIRe service time adds measured CPU query unpack and response compression to its GPU answer.",
            "Storage reads and network/queue/transport overhead are excluded.",
        ],
    }
    Path(args.output).write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
