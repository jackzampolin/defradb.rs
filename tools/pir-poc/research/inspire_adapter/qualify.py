#!/usr/bin/env python3
"""Wrap an official InsPIRe measurement in the POC accounting schema."""

import argparse
import json
from pathlib import Path


def metric(value, evidence, qualification):
    return {
        "value": value,
        "evidence": evidence,
        "qualification": qualification,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--measurement", required=True)
    parser.add_argument("--mapping", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    measurement = json.loads(Path(args.measurement).read_text())
    mapping = json.loads(Path(args.mapping).read_text())
    manifest = json.loads(Path(args.manifest).read_text())
    specs = measurement["specs"]
    offline = measurement["offline"]
    online = measurement["online"]
    encoded_bytes = round(specs["resizedDatabaseSizeMb"] * 1024 * 1024)

    report = {
        "schema": "pir-aggregate-work-v1",
        "protocol": "InsPIRe",
        "artifact": {
            "repository": None,
            "permanent_record": "https://doi.org/10.5281/zenodo.17361471",
            "archive": "artifact-final.zip",
            "archive_md5": "bfa9edb2d8403f0dc20830fb40608b78",
            "upstream_modifications": "checked out-of-tree corpus patch only; cryptographic kernels and parameters are unchanged",
            "adapter_qualification": mapping["qualification"],
        },
        "comparison_scope": {
            "workload": f'{mapping["page_count"]} populated immutable {mapping["page_bytes"]}-byte Defra tag pages',
            "result": "one exact 96-byte useful page",
            "physical_result": f'{mapping["private_result_block_capacity_bytes"]} plaintext coefficients before encrypted response serialization',
            "public_partition": "global snapshot",
            "leakage": {"class": "exact_query_privacy"},
        },
        "security": {
            "privacy": "single-server computational query privacy under the artifact's LWE/RLWE assumptions",
            "server_count": 1,
            "collusion_tolerance": 0,
            "required_answers": 1,
            "assumptions": "server-side preprocessing, CRS/seeds as in the official research artifact, AVX-512 execution",
            "availability": "the one server is mandatory",
            "integrity": "the adapter checks the selected page byte-for-byte; no malicious-server PIR proof",
        },
        "global_build": {
            "unit": "immutable snapshot",
            "database_encoding_ms": metric(
                offline["encodeTimeMs"],
                "measured",
                "artifact database interpolation/encoding after corpus materialization",
            ),
            "server_preprocessing_ms": metric(
                offline["serverTimeMs"],
                "measured",
                "artifact database-dependent offline phase",
            ),
            "client_download_bytes": metric(
                offline["downloadBytes"],
                "deterministic",
                "official measurement field; InsPIRe uses server-side preprocessing rather than a DB hint",
            ),
            "peak_server_ram_bytes": metric(
                None,
                "not_measured",
                "collect process-isolated RSS on an AVX-512 runner",
            ),
        },
        "online": {
            "unit": "one exact 96-byte page locally selected from one private result block",
            "per_server": [{
                "server_index": 0,
                "server_time_ms": metric(
                    online["serverTimeMs"],
                    "measured",
                    "artifact mean after its warmup behavior; raw samples retained below",
                ),
                "logical_selected_bytes": metric(
                    encoded_bytes,
                    "estimated",
                    "physical plaintext layout size, not hardware memory traffic",
                ),
                "physical_or_scanned_bytes": metric(
                    None,
                    "not_measured",
                    "requires phase-scoped hardware counters",
                ),
                "scans": metric(
                    None,
                    "not_measured",
                    "InsPIRe has multiple algebraic phases; do not collapse them into a Dense scan count",
                ),
            }],
            "server_time_samples_ms": online["allServerTimesMs"],
            "first_pass_time_us": metric(online["firstPassTimeUs"], "measured", "official first-pass field"),
            "second_pass_time_us": metric(online["secondPassTimeUs"], "measured", "official second-pass field"),
            "first_pack_time_us": metric(online["firstPackTimeUs"], "measured", "official packing field"),
            "rgsw_time_us": metric(online["rgswTimeUs"], "measured", "official polynomial-selection field"),
            "useful_result_bytes": metric(mapping["page_bytes"], "deterministic", "complete Defra page"),
            "physical_plaintext_result_capacity_bytes": metric(
                mapping["private_result_block_capacity_bytes"],
                "deterministic",
                "one byte per plaintext coefficient in the adapter mapping",
            ),
            "network_rounds": metric(1, "deterministic", "one query and answer after server preprocessing"),
        },
        "client": {
            "query_cpu_ms": metric(online["clientQueryGenTimeMs"], "measured", "official client query-generation field"),
            "recover_cpu_ms": metric(online["clientDecodeTimeMs"], "measured", "official client decode field"),
            "upload_bytes": metric(online["uploadBytes"], "deterministic", "serialized query and key material"),
            "upload_key_bytes": metric(online["uploadKeys"], "deterministic", "official key subfield"),
            "upload_query_bytes": metric(online["uploadQuery"], "deterministic", "official query subfield"),
            "download_bytes": metric(online["downloadBytes"], "deterministic", "serialized encrypted response"),
            "persistent_state_bytes": metric(None, "not_measured", "artifact does not report an isolated retained-client allocation"),
        },
        "persisted_storage": {
            "server_bytes_per_server": metric(None, "not_measured", "artifact does not serialize or report full preprocessed server state"),
            "physical_plaintext_layout_bytes": metric(encoded_bytes, "estimated", "parameterized resized database before protocol state"),
        },
        "amortization": {
            "global_build": "all queries served by one immutable snapshot",
            "per_client_setup": "client query/key generation is charged online in the artifact",
            "note": "encoding, server preprocessing, client query, server answer, and client recovery remain separate",
        },
        "corpus": {
            "schema": manifest.get("schema"),
            "blake3": manifest.get("corpus_blake3"),
            "page_count": mapping["page_count"],
            "page_bytes": mapping["page_bytes"],
            "logical_bytes": mapping["useful_corpus_bytes"],
            "physical_bits_per_page_for_parameters": mapping["physical_bits_per_page_for_parameters"],
            "pages_per_private_result_block": mapping["pages_per_private_result_block"],
            "block_capacity_bytes": mapping["private_result_block_capacity_bytes"],
            "correctness": mapping["correctness"],
        },
        "runner_diagnostics": {
            "required_cpu_feature": "AVX-512F and the other native features selected by the artifact",
            "hardware_counters": "not collected",
            "warning": "single-server computational PIR is a separate security lane from replicated information-theoretic PIR",
        },
        "upstream_measurement": measurement,
    }

    Path(args.output).write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
