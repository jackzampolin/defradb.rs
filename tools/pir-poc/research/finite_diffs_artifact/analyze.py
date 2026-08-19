#!/usr/bin/env python3
"""Deterministic common-corpus accounting for the finite-differences artifact.

This intentionally does not execute the Go/C artifact.  It mirrors the small
parameter-selection and cloud-cardinality routines in the pinned source so the
host guard can be evaluated before any allocation or compilation starts.
"""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path


SCHEMA = "defra-finite-differences-analysis-v1"
REVISION = "4574a4f8c52eeda165e110cbb64f834397d7c049"


def pick_params(records: int, theta: float) -> tuple[int, int]:
    if records <= 0:
        raise ValueError("record count must be positive")
    if theta <= 0 or theta > 0.5:
        raise ValueError("theta must be in (0, 0.5]")

    variables = 10
    while True:
        degree = int(variables * theta)
        if degree % 2 == 0:
            degree += 1
        if math.comb(variables, degree) >= records:
            break
        variables += 5

    while True:
        candidate_variables = variables - 1
        candidate_degree = int(candidate_variables * theta)
        if candidate_degree % 2 == 0:
            candidate_degree += 1
        if math.comb(candidate_variables, candidate_degree) >= records:
            variables = candidate_variables
            degree = candidate_degree
        else:
            return variables, degree


def metric(value: int | float, unit: str, note: str) -> dict[str, object]:
    return {"value": value, "unit": unit, "status": "deterministic", "note": note}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--theta", type=float, default=0.5)
    args = parser.parse_args()

    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    if manifest.get("schema") != "defra-pir-raw-page-corpus-v1":
        raise ValueError("unexpected corpus schema")

    records = int(manifest["page_count"])
    record_bytes = int(manifest["page_bytes"])
    raw_bytes = records * record_bytes
    variables, degree = pick_params(records, args.theta)
    capacity = math.comb(variables, degree)
    radius = degree // 2
    cloud_records = sum(math.comb(variables, weight) for weight in range(radius + 1))
    encoded_bytes = (1 << variables) * record_bytes
    answer_bytes = cloud_records * record_bytes
    query_bytes_per_server = 8  # The pinned prototype represents a query as a Go int on amd64.
    logical_query_bytes_per_server = (variables + 7) // 8

    dense_expected_selected_payload_bytes = raw_bytes
    dense_full_addressable_scan_bytes = 2 * raw_bytes
    finite_diffs_aggregate_read_bytes = 2 * answer_bytes
    report = {
        "schema": SCHEMA,
        "artifact": {
            "repository": "https://github.com/ahenzinger/finite-diffs-pir",
            "revision": REVISION,
            "implementation_scope": "the repository implements the two-server F_2 construction only",
        },
        "workload": {
            "corpus_schema": manifest["schema"],
            "corpus_blake3": manifest["corpus_blake3"],
            "records": records,
            "record_bytes": record_bytes,
            "raw_bytes": raw_bytes,
            "query_index": int(manifest["query_index"]),
            "mapping": "one exact populated 96-byte Defra page is one artifact record",
        },
        "parameters": {
            "theta": args.theta,
            "variables_m": variables,
            "degree_D": degree,
            "record_capacity": capacity,
            "occupancy": records / capacity,
            "cloud_radius": radius,
            "cloud_records_per_server": cloud_records,
        },
        "security": {
            "privacy": "perfect query privacy against either one semi-honest server",
            "collusion_threshold_t": 1,
            "server_count_s": 2,
            "collusion_failure": "the two queries reveal the encoded target when the servers collude",
            "many_server_warning": "Theorem 5.3 is a different q-ary construction and is not implemented by this artifact.",
        },
        "storage": {
            "paper_server_storage": metric(
                encoded_bytes,
                "bytes",
                "Definition 2.4 counts the encoded database DB' once; this is also storage per replica",
            ),
            "aggregate_deployed_storage": metric(
                2 * encoded_bytes,
                "bytes",
                "two independently operated replicas each need the full encoded database",
            ),
            "per_replica_blowup_over_raw": encoded_bytes / raw_bytes,
        },
        "online": {
            "upload_artifact_representation": metric(
                2 * query_bytes_per_server,
                "bytes",
                "two 64-bit query integers; no framing or transport measured",
            ),
            "upload_logical_minimum": metric(
                2 * logical_query_bytes_per_server,
                "bytes",
                "two packed m-bit points; the official prototype does not serialize this form",
            ),
            "download": metric(
                2 * answer_bytes,
                "bytes",
                "sum of both server answer vectors",
            ),
            "logical_record_reads": metric(
                2 * cloud_records,
                "records",
                "paper Definition 2.5 sums the probes made by both servers",
            ),
            "logical_read_bytes": metric(
                finite_diffs_aggregate_read_bytes,
                "bytes",
                "sum of both servers; every 96-byte record is copied by the generic C kernel",
            ),
            "dense_xor_expected_selected_payload_bytes": metric(
                dense_expected_selected_payload_bytes,
                "bytes",
                "aggregate expectation: each of two uniform shares selects half the rows; excludes selectors and address traversal",
            ),
            "dense_xor_full_addressable_scan_bytes": metric(
                dense_full_addressable_scan_bytes,
                "bytes",
                "secondary scope: two servers each traverse the full row address space, regardless of which payloads are XORed",
            ),
            "payload_reduction_vs_dense_expected_selected": dense_expected_selected_payload_bytes
            / finite_diffs_aggregate_read_bytes,
            "addressed_bytes_reduction_vs_two_dense_full_scans": dense_full_addressable_scan_bytes
            / finite_diffs_aggregate_read_bytes,
            "comparison_warning": "Dense streams rows while finite differences makes random probes; logical byte ratios are not elapsed-time ratios.",
        },
        "not_measured": [
            "preprocessing time and peak RSS",
            "client Query and Recover time",
            "summed server Answer time",
            "transport, TLS, filesystem, and energy",
        ],
    }

    encoded = json.dumps(report, indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded, encoding="utf-8")
    print(encoded, end="")


if __name__ == "__main__":
    main()
