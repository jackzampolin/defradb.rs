#!/usr/bin/env python3
"""Turn perf-stat sidecars into explicit per-server and aggregate JSON."""

from __future__ import annotations

import csv
import json
import pathlib
import sys
from collections import defaultdict
from typing import Any


def normalize_event(event: str) -> str:
    event = event.strip()
    if event.endswith(":u"):
        event = event[:-2]
    return event


def parse_number(text: str) -> float | None:
    text = text.strip()
    if not text or text.startswith("<"):
        return None
    try:
        return float(text)
    except ValueError:
        return None


def parse_perf(path: pathlib.Path) -> dict[str, list[dict[str, Any]]]:
    parsed: dict[str, list[dict[str, Any]]] = defaultdict(list)
    if not path.exists():
        return parsed
    with path.open(newline="", encoding="utf-8", errors="replace") as stream:
        for row in csv.reader(stream, delimiter=";"):
            if not row or row[0].lstrip().startswith("#") or len(row) < 3:
                continue
            event = normalize_event(row[2])
            if not event:
                continue
            value = parse_number(row[0])
            running = parse_number(row[4]) if len(row) > 4 else None
            entry: dict[str, Any] = {
                "status": "measured" if value is not None else "unavailable",
                "value": value,
                "unit": row[1].strip() or "count",
                "percent_running": running,
                "raw_counter_value": row[0].strip(),
            }
            if value is None:
                entry["reason"] = row[0].strip() or "perf emitted no counter value"
            elif running is not None and running < 90.0:
                entry["quality_warning"] = (
                    "counter ran less than 90% of the phase; perf scaled the value"
                )
            parsed[event].append(entry)
    return parsed


def combine_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"status": "unavailable", "reason": "event absent from perf output"}
    unavailable = [row for row in rows if row["status"] != "measured"]
    if unavailable:
        return {
            "status": "unavailable",
            "reason": "; ".join(str(row.get("reason", "not measured")) for row in unavailable),
        }
    result: dict[str, Any] = {
        "status": "measured",
        "value": sum(float(row["value"]) for row in rows),
        "unit": rows[0]["unit"],
        "counter_rows": len(rows),
        "percent_running_min": min(
            (float(row["percent_running"]) for row in rows if row["percent_running"] is not None),
            default=None,
        ),
    }
    warnings = [row["quality_warning"] for row in rows if "quality_warning" in row]
    if warnings:
        result["quality_warning"] = warnings[0]
    return result


def read_lines(path: pathlib.Path) -> list[str]:
    if not path.exists():
        return []
    return [line.strip() for line in path.read_text().splitlines() if line.strip()]


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: parse-server-perf.py RESULT_DIR", file=sys.stderr)
        return 2
    root = pathlib.Path(sys.argv[1])
    phase = json.loads((root / "gate" / "phase.json").read_text())
    requested = read_lines(root / "core-events.txt")
    per_server = []
    per_event_server_values: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for server_index in range(int(phase["server_count"])):
        tid = int((root / "gate" / f"server-{server_index}.tid").read_text().strip())
        parsed = parse_perf(root / f"server-{server_index}.perf.csv")
        events = {}
        for event in requested:
            reading = combine_rows(parsed.get(normalize_event(event), []))
            events[normalize_event(event)] = reading
            per_event_server_values[normalize_event(event)].append(reading)
        per_server.append(
            {
                "server_index": server_index,
                "linux_tid": tid,
                "scope": "only this replica worker's BatchEvaluator::evaluate call",
                "events": events,
            }
        )

    aggregate_core = {}
    for event in map(normalize_event, requested):
        rows = per_event_server_values[event]
        if len(rows) != int(phase["server_count"]) or any(
            row["status"] != "measured" for row in rows
        ):
            aggregate_core[event] = {
                "status": "unavailable",
                "reason": "at least one replica lacks a measured value",
            }
            continue
        aggregate_core[event] = {
            "status": "measured",
            "value": sum(float(row["value"]) for row in rows),
            "unit": rows[0]["unit"],
            "aggregation": "sum of phase-scoped per-server thread counters",
            "percent_running_min": min(
                (
                    float(row["percent_running_min"])
                    for row in rows
                    if row.get("percent_running_min") is not None
                ),
                default=None,
            ),
        }

    cycles = aggregate_core.get("cycles", {})
    instructions = aggregate_core.get("instructions", {})
    ipc = None
    if (
        cycles.get("status") == "measured"
        and instructions.get("status") == "measured"
        and float(cycles["value"]) > 0
    ):
        ipc = float(instructions["value"]) / float(cycles["value"])

    aggregate_parsed = parse_perf(root / "aggregate.perf.csv")
    aggregate_events = []
    metadata_path = root / "aggregate-events.tsv"
    if metadata_path.exists():
        for line in metadata_path.read_text().splitlines():
            if not line.strip():
                continue
            fields = line.split("\t")
            kind = fields[0]
            event = fields[1]
            multiplier = fields[2] if len(fields) > 2 else ""
            reading = combine_rows(aggregate_parsed.get(normalize_event(event), []))
            aggregate_events.append({"kind": kind, "event": event, "reading": reading})
            if kind == "dram_traffic" and reading["status"] == "measured":
                reading["bytes_per_count"] = float(multiplier)
                reading["derived_physical_bytes"] = float(reading["value"]) * float(multiplier)
                reading["derivation"] = (
                    "measured uncore count multiplied by operator-supplied platform mapping"
                )

    unavailable = read_lines(root / "unavailable.txt")
    for event, reading in aggregate_core.items():
        if reading["status"] != "measured":
            unavailable.append(f"aggregate {event}: {reading['reason']}")

    essential_measured = all(
        aggregate_core.get(event, {}).get("status") == "measured"
        for event in ("cycles", "instructions")
    )
    result = {
        "schema": "defradb-pir-server-phase-hardware-v1",
        "phase": phase,
        "evidence": "hardware_measured" if essential_measured else "unavailable",
        "per_server_core_counters": per_server,
        "aggregate_core_counters": {
            "scope": "sum across replica evaluator threads; not wall time and not a process-wide count",
            "events": aggregate_core,
            "derived_instructions_per_cycle": ipc,
        },
        "aggregate_package_or_uncore_counters": {
            "scope": "coordinated server-evaluation envelope across all replicas; never attributed per server",
            "events": aggregate_events,
            "isolation_requirement": "package/uncore values are trustworthy only on an otherwise idle isolated host; taskset alone does not exclude unrelated package traffic",
        },
        "explicitly_unavailable": sorted(set(unavailable)),
        "interpretation": {
            "cache_misses_are_not_dram_bytes": True,
            "energy_is_not_divided_by_server_count": True,
            "client_and_build_work_counted": False,
            "counter_multiplexing": "percent_running is retained; values below 90% are flagged",
        },
    }
    json.dump(result, sys.stdout, indent=2, sort_keys=True, allow_nan=False)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
