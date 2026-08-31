#!/usr/bin/env python3
"""Join matching Dense, GPU-DPF, and InsPIRe GPU snapshot measurements."""

import argparse
import json
from pathlib import Path

GPU_DPF_COMMIT = "ce23a06af884ee54300b5bc5fd5350e445f10b0b"
INSPIRE_GPU_COMMIT = "c14d1d84a425cdaa9f86ed09465b09c9c9802f13"


def wire_ms(byte_count: int, mbps: int) -> float:
    return byte_count * 8 * 1000 / (mbps * 1_000_000)


def ratio(numerator: float, denominator: float) -> float:
    return numerator / denominator


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dense-dpf", required=True)
    parser.add_argument("--inspire", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    dense_suite = json.loads(Path(args.dense_dpf).read_text())
    inspire_suite = json.loads(Path(args.inspire).read_text())
    if dense_suite["upstream"]["commit"] != GPU_DPF_COMMIT:
        raise SystemExit("Dense/GPU-DPF suite is not the pinned upstream revision")
    if inspire_suite["upstream"]["commit"] != INSPIRE_GPU_COMMIT:
        raise SystemExit("InsPIRe suite is not the pinned upstream revision")
    if dense_suite["profile"] != inspire_suite["profile"]:
        raise SystemExit("profile mismatch between the two suites")
    dense_rows = {
        (row["entries"], row["batch"]): row
        for row in dense_suite["snapshot"]
        if row["mode"] == "snapshot" and row["useful_row_bytes"] == 120
    }
    inspire_rows = {
        (scale["entries"], batch["batch"]): (scale, batch)
        for scale in inspire_suite["scales"]
        for batch in scale["batches"]
    }
    keys = sorted(set(dense_rows).intersection(inspire_rows))
    if not keys:
        raise SystemExit("the suites contain no matching entry-count/batch cases")

    dense_gpu = {row["gpu"] for row in dense_rows.values()}
    if dense_gpu != {inspire_suite["hardware"]["gpu"]}:
        raise SystemExit(
            f"hardware mismatch: Dense/DPF={sorted(dense_gpu)!r}, "
            f"InsPIRe={inspire_suite['hardware']['gpu']!r}"
        )
    dense_hardware = dense_suite.get("hardware")
    if dense_hardware:
        for field in ("gpu", "compute_capability", "cpu"):
            if dense_hardware.get(field) != inspire_suite["hardware"].get(field):
                raise SystemExit(f"hardware mismatch for {field}")

    output_rows = []
    client = inspire_suite["client"]
    inspire_service_overhead = (
        client["server_query_unpack_ms"] + client["server_response_compress_ms"]
    )
    for key in keys:
        dense_row = dense_rows[key]
        scale, inspire_batch = inspire_rows[key]
        if scale["useful_row_bytes"] != 120:
            raise SystemExit("InsPIRe row width is not the common 120-byte geometry")
        batch = dense_row["batch"]
        protocols = []
        for name, label, servers, privacy in (
            (
                "dense_xor",
                "Dense XOR",
                2,
                "information-theoretic n-out-of-n replicated PIR",
            ),
            (
                "gpu_dpf",
                "GPU-DPF",
                2,
                "computational two-server non-colluding PIR",
            ),
        ):
            value = dense_row[name]
            upload = value["aggregate_upload_bytes"] // batch
            download = value["aggregate_response_bytes"] // batch
            protocols.append(
                {
                    "protocol": label,
                    "servers": servers,
                    "privacy": privacy,
                    "aggregate_server_ms_per_query": value[
                        "aggregate_server_ms_per_query"
                    ],
                    "parallel_server_ms_per_query": value["parallel_wall_p50_ms"]
                    / batch,
                    "client_query_ms_per_query": value["client_batch_ms"] / batch,
                    "client_recover_ms_per_query": None,
                    "upload_bytes_per_query": upload,
                    "download_bytes_per_query": download,
                    "network_serialization_ms": {
                        str(mbps): wire_ms(upload + download, mbps)
                        for mbps in (10, 50, 100)
                    },
                }
            )

        inspire_upload = client["query_packed_bytes"]
        inspire_download = client["compressed_response_bytes"]
        protocols.append(
            {
                "protocol": "InsPIRe GPU",
                "servers": 1,
                "privacy": "single-server computational PIR with server-side preprocessing",
                "aggregate_server_ms_per_query": inspire_batch[
                    "server_ms_per_query"
                ],
                "parallel_server_ms_per_query": inspire_batch[
                    "server_ms_per_query"
                ],
                "service_server_ms_per_query": inspire_batch[
                    "server_ms_per_query"
                ]
                + inspire_service_overhead,
                "client_query_ms_per_query": client["query_build_ms"]
                + client["query_pack_ms"],
                "client_recover_ms_per_query": client[
                    "client_extract_compressed_ms"
                ],
                "upload_bytes_per_query": inspire_upload,
                "download_bytes_per_query": inspire_download,
                "network_serialization_ms": {
                    str(mbps): wire_ms(inspire_upload + inspire_download, mbps)
                    for mbps in (10, 50, 100)
                },
            }
        )
        dense_server = protocols[0]["aggregate_server_ms_per_query"]
        dpf_server = protocols[1]["aggregate_server_ms_per_query"]
        inspire_server = protocols[2]["aggregate_server_ms_per_query"]
        cold_dense_dpf = {}
        for protocol_key, label in (
            ("dense_xor", "Dense XOR"),
            ("gpu_dpf", "GPU-DPF"),
        ):
            protocol = dense_row[protocol_key]
            if "first_online" in protocol:
                cold_dense_dpf[label] = {
                    "client_query_ms_per_query": protocol["client_batch_ms"] / batch,
                    "server_context_ms": protocol["server_context_ms"],
                    "first_online": protocol["first_online"],
                }
        cold_value = (
            cold_dense_dpf
            if cold_dense_dpf
            else "not instrumented in this older Dense/DPF result"
        )
        output_rows.append(
            {
                "entries": key[0],
                "batch": key[1],
                "useful_row_bytes": 120,
                "physical_row_bytes": dense_row["physical_row_bytes"],
                "protocols": protocols,
                "aggregate_server_ratios": {
                    "gpu_dpf_over_dense": ratio(dpf_server, dense_server),
                    "inspire_over_dense": ratio(inspire_server, dense_server),
                    "gpu_dpf_over_inspire": ratio(dpf_server, inspire_server),
                },
                "inspire_cold_snapshot": {
                    "host_materialize_ms": scale["host_materialize_ms"],
                    "gpu_preprocess_ms": scale["gpu_preprocess_ms"],
                    "server_context_ms": scale["server_context_ms"],
                },
                "inspire_cold_online": scale["cold_online"],
                "dense_dpf_gpu_table_materialize_ms_per_replica": dense_row.get(
                    "gpu_table_materialize_ms_per_replica"
                ),
                "dense_dpf_cold_online": cold_value,
            }
        )

    report = {
        "schema": "defradb-full-gpu-snapshot-comparison-v1",
        "hardware": inspire_suite["hardware"],
        "primary_metric": "aggregate server time per correct private query",
        "capacity": {
            "dense_dpf": dense_suite.get("capacity", "not recorded by this older suite"),
            "inspire_gpu": inspire_suite.get("capacity", "not recorded by this older suite"),
        },
        "qualifications": [
            "Dense and GPU-DPF aggregate the work of two replicas; their parallel wall is reported separately.",
            "InsPIRe is one-server computational PIR and is not security-equivalent to the replicated protocols.",
            "Network serialization projections contain bytes/bandwidth only: no RTT, congestion, TLS, OHTTP, Tor, or queueing.",
            "InsPIRe service time adds measured CPU query unpack and response compression; the primitive column is the GPU answer only.",
            "First-online values are process-order-sensitive; repeat alternating isolated processes before treating their p50 as a deployment gate.",
        ],
        "rows": output_rows,
    }
    Path(args.output).write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
