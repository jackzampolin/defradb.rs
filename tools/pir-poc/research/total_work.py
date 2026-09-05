#!/usr/bin/env python3
"""Fresh-process aggregate-work screening. Python 3.10+, standard library only.

Run in WSL/Linux against a release pir-poc binary. Artifacts are append-only:
the output directory must not exist. No package installation or network calls.
"""
import argparse
import hashlib
import itertools
import json
import math
import os
from pathlib import Path
import platform
import random
import statistics
import subprocess
import sys
import time


def write_json(path, value):
    with Path(path).open("x", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2, allow_nan=False)
        stream.write("\n")


def matrix(profile):
    rows = 256 if profile == "smoke" else 262144
    queries = 4 if profile == "smoke" else 100
    base = dict(rows=rows, row_bytes=96, queries=queries, field_bits=32,
                max_resident_bytes=512 << 20, fanout=4, payload_slots=4)
    cases = [dict(base, candidate="dense")]
    for g in [2, 4, 6, 8, 10]:
        cases.append(dict(base, candidate="subset", group_bits=g))
    for q in [2, 4, 8, 16, 32]:
        cases.append(dict(base, candidate="single-pass", partitions=q))
    cases.append(dict(base, candidate="finite-differences"))
    for g in [1, 2, 4, 8]:
        for candidate in ["field-public", "field-bitmap"]:
            cases.append(dict(base, candidate=candidate, group_bits=g))
    cases.append(dict(base, candidate="field-postings"))
    cases.append(dict(base, candidate="field-inline"))
    for k in ([8] if profile == "smoke" else [1, 8, 32, 128, 512]):
        for kernel in ["independent", "shared", "blocked", "transposed", "four-russians"]:
            cases.append(dict(base, candidate="batch", batch_size=k, kernel=kernel,
                              group_bits=4, queries=max(4, queries // k)))
    # Actual generation mutation and rebuild, not just a projected discount.
    for candidate in ["dense", "subset", "single-pass", "field-bitmap"]:
        cases.append(dict(base, candidate=candidate, group_bits=4,
                          rebuild_every=2 if profile == "smoke" else 10,
                          update_batch=1 if profile == "smoke" else 100))
    return cases


def field_preflight(n, w, g, row_bytes=96, slots=4):
    groups = w // g
    bitmap = (n + 7) // 8
    index = groups * (1 << g) * bitmap
    # Two random XOR shares per logical group. Payload requests also have two roles.
    download = 2 * groups * bitmap + 2 * slots * row_bytes
    upload = 2 * groups * (((1 << g) + 7) // 8) + 2 * slots * bitmap
    return dict(rows=n, field_bits=w, group_bits=g, logical_groups=groups,
                index_bytes_per_replica=index, aggregate_index_bytes=2*index,
                index_bytes_per_group_replica=index//groups,
                protocol_upload_bytes=upload, protocol_download_bytes=download,
                portable_online_bytes_pass=download <= 1 << 20 and upload <= 1 << 20,
                evidence="exact dense-bitmap dimensions; no CPU extrapolation",
                role_placement=[dict(workers=k, max_groups_per_worker=math.ceil(groups/k),
                    max_index_bytes_per_worker=math.ceil(groups/k)*(index//groups),
                    aggregate_index_bytes=2*index) for k in [1,2,4,8,16,32,64,128]])


def is_prime(n):
    return n >= 2 and all(n % d for d in range(2, math.isqrt(n)+1))


def many_server_preflight(n, field_bits=1):
    """Exact dimensions of the prime-field multivariate/Hermite construction.

    Bounded enumerator, NOT a proof of global optimality. Retains per S the
    smallest encoded field-symbol count under stated m,d,q bounds. Binary
    packing, query extraction, client Hermite solver and CPU are unmeasured.
    """
    results = []
    for servers in [4,8,16,32,64,128]:
        best = None
        for q in [p for p in range(servers+1, 2*servers+16) if is_prime(p)]:
            for m in range(1, 17):
                for d in range(1, q):
                    if math.comb(m+d, m) < n:
                        continue
                    t = d//servers+1
                    derivatives = math.comb(m+t-1, m)
                    symbols = derivatives*q**m
                    item = dict(servers=servers, q=q, m=m, d=d, t=t,
                        polynomial_capacity=math.comb(m+d, m), derivatives=derivatives,
                        encoded_symbols_per_server=symbols,
                        aggregate_encoded_bits=servers*symbols*q.bit_length()*field_bits,
                        aggregate_answer_bits=servers*derivatives*q.bit_length()*field_bits,
                        aggregate_query_bits=servers*m*q.bit_length(),
                        logical_input_bits=n*field_bits)
                    if best is None or item["aggregate_encoded_bits"] < best["aggregate_encoded_bits"]:
                        best = item
        if best:
            best["storage_amplification"] = best["aggregate_encoded_bits"] / best["logical_input_bits"]
            best["budget_frontier_pass"] = {str(b):best["storage_amplification"] <= b for b in [2,8,32,128,512]}
            best["evidence"] = "bounded prime-field dimension screening only; no implemented encoder or online protocol"
            results.append(best)
    return results


def preflight():
    return dict(schema="pir-total-work-preflight-v1",
        fields=[field_preflight(n,w,g) for n,w,g in itertools.product(
            [262144,1048576,10_000_000,100_000_000,1_000_000_000], [16,32,64], [1,2,4,8])],
        many_server=many_server_preflight(262144),
        many_server_source="https://eprint.iacr.org/2024/765",
        many_server_scope="Prime fields only, m <= 16, d < q, primes S < q < 2S+16; bit entries conservatively encoded as separate field symbols. No asymptotic time estimates.",
        additional_runner="run_all_benchmarks.py",
        additional_implementations=["B2 replicated MPC intersection and Batcher compaction", "B5 pinned separated-role Zelda", "B6 executable m=1 Hermite construction", "B7 nonrecursive Path ORAM", "B8 membership mutations, base/delta compaction and canonical witnesses", "fresh-query GPU adapters and native arrival scheduling"],
        remaining_gates=[dict(family="Physical deployment",status="hardware_required",reason="Independent physical hosts, named ARM client, calibrated DRAM/energy counters and NIC shaping are not implied by local process execution.")])


def bootstrap_ratio(a, b, count=2000):
    """Paired run-level bootstrap; do not bootstrap individual queries."""
    if len(a) != len(b) or len(a) < 5 or min(b) <= 0:
        return None
    rng = random.Random(470)
    ratios = []
    for _ in range(count):
        ids = [rng.randrange(len(a)) for _ in a]
        ratios.append(statistics.mean(a[i] for i in ids) / statistics.mean(b[i] for i in ids))
    ratios.sort()
    return [ratios[int(.025*count)], ratios[int(.975*count)]]


def summarize(records):
    groups = {}
    for record in records:
        if record["status"] == "measured":
            groups.setdefault(record["case"], []).append(record)
    table = []
    for case, runs in groups.items():
        reports = [r["report"] for r in runs]
        cpu = [r["server_cpu_ms_per_completed_query"] for r in reports]
        if any(v is None for v in cpu):
            continue
        first = reports[0]
        table.append(dict(case=case, candidate=first["config"]["candidate"],
            workload=first["workload"], runs=len(runs), median_server_cpu_ms=statistics.median(cpu),
            aggregate_storage_bytes=first["aggregate_server_storage_bytes"],
            client_measured_caps_pass=all(r["client_measured_caps_pass"] for r in reports),
            eligibility=first["eligibility"], run_cpu_ms=cpu,
            amortization=[dict(queries_per_client=q, clients=clients,
                estimated_server_cpu_ms_per_query=(statistics.mean(r["global_server_build"]["cpu_ms"] for r in reports)/(q*clients)
                    + statistics.mean(sum(s["server"]["cpu_ms"] for s in r["samples"])/r["completed_logical_queries"] for r in reports)),
                evidence="projection from measured build/online phases; not a multi-client lifecycle measurement")
                for q,clients in itertools.product([1,10,100,1000],[1,100,10000])]
                if first["config"]["candidate"] != "single-pass" and not first["rebuild_count"] else []))
    # Compare only matching semantics and lifecycle; preserve repeated runs pairing.
    for entry in table:
        first = groups[entry["case"]][0]["report"]
        cfg = first["config"]
        baseline_name = "field-inline" if entry["workload"].startswith("equality-") else "dense"
        baseline = next((other for other in table if other["candidate"] == baseline_name
            and first["security"]["private"]
            and other["workload"] == entry["workload"]
            and groups[other["case"]][0]["report"]["completed_logical_queries"] == first["completed_logical_queries"]
            and all(groups[other["case"]][0]["report"]["config"][key] == cfg[key]
                for key in ["rows","row_bytes","rebuild_every","update_batch"])), None)
        if baseline and [r["repetition"] for r in groups[entry["case"]]] == [r["repetition"] for r in groups[baseline["case"]]]:
            ci = bootstrap_ratio(entry["run_cpu_ms"],baseline["run_cpu_ms"])
            ratio = statistics.mean(entry["run_cpu_ms"])/statistics.mean(baseline["run_cpu_ms"])
            entry.update(baseline_candidate=baseline_name, cpu_ratio_to_baseline=ratio, paired_run_bootstrap_95=ci,
                screening_signal=bool(ci and ci[1] < 1 and ratio <= .8 and entry["client_measured_caps_pass"]),
                production_promotion=False)
    return table


def run(args):
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    cases = json.loads(args.matrix.read_text()) if args.matrix else matrix(args.profile)
    if not isinstance(cases,list) or not cases:
        raise ValueError("matrix must be a nonempty JSON array")
    binary = args.binary.resolve()
    manifest = dict(schema="pir-total-work-manifest-v1", created_unix=time.time(),
        platform=platform.platform(), machine=platform.machine(), python=sys.version,
        binary=str(binary), binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest() if binary.exists() else None,
        repetitions=args.repetitions, profile=args.profile, cases=cases,
        rayon_threads=args.threads, timeout_seconds=args.timeout,
        methodology="Fresh processes; case order alternates per run; seeds paired by repetition. Build included. No CPU/GPU metric mixing.")
    manifest["cpu_info"] = Path("/proc/cpuinfo").read_text() if Path("/proc/cpuinfo").exists() else platform.processor()
    manifest["memory_info"] = Path("/proc/meminfo").read_text() if Path("/proc/meminfo").exists() else None
    for name, command in [("git_head",["git","rev-parse","HEAD"]), ("git_status",["git","status","--porcelain"])]:
        manifest[name] = subprocess.check_output(command,text=True).strip()
    source_root = Path(__file__).resolve().parents[1]
    manifest["source_hashes"] = {str(p.relative_to(source_root)):hashlib.sha256(p.read_bytes()).hexdigest()
        for p in list((source_root/"src").rglob("*.rs"))+list((source_root/"examples").glob("*.rs"))+[Path(__file__).resolve(),source_root/"Cargo.toml"]}
    write_json(out/"manifest.json",manifest)
    write_json(out/"preflight.json",preflight())
    if args.dry_run:
        return
    if not binary.is_file():
        raise FileNotFoundError(f"Build release binary first: {binary}")
    records = []
    for repetition in range(args.repetitions):
        order = list(enumerate(cases))
        if repetition % 2:
            order.reverse()
        for index, case in order:
            name = f"case-{index:03d}-run-{repetition:02d}"
            cfg = dict(case,seed=9000+repetition)
            path = out/f"{name}.config.json"
            write_json(path,cfg)
            print(f"{name}: {cfg['candidate']}",flush=True)
            try:
                result = subprocess.run([str(binary),"research","total-work",str(path)],
                    capture_output=True,text=True,timeout=args.timeout,check=False,
                    env=dict(os.environ,RAYON_NUM_THREADS=str(args.threads)))
                (out/f"{name}.stderr.txt").write_text(result.stderr)
                if result.returncode:
                    record = dict(case=index, repetition=repetition,status="failed_or_gated",error=result.stderr,exit_code=result.returncode)
                else:
                    report = json.loads(result.stdout)
                    record = dict(case=index,repetition=repetition,status="measured",report=report)
            except subprocess.TimeoutExpired:
                record = dict(case=index,repetition=repetition,status="timeout",error="Incomplete run; no work/throughput result admitted")
            write_json(out/f"{name}.json",record)
            records.append(record)
    table = summarize(records)
    write_json(out/"comparison.json",table)
    lines = ["# Aggregate CPU screening", "", "CPU includes measured server build and maintenance amortized over completed queries. All entries are microbenchmarks; production promotion is disabled while required metrics remain unmeasured.","",
        "| Case | Candidate | Workload | Runs | CPU ms/query | Aggregate storage MiB | Measured client caps |",
        "|---|---|---|---:|---:|---:|---|"]
    for e in table:
        lines.append(f"| {e['case']} | {e['candidate']} | {e['workload']} | {e['runs']} | {e['median_server_cpu_ms']:.6f} | {e['aggregate_storage_bytes']/2**20:.2f} | {e['client_measured_caps_pass']} |")
    failures = [r for r in records if r["status"] != "measured"]
    lines += ["",f"Failed/gated/timed-out runs: {len(failures)}. Inspect individual JSON and preflight.json. Missing results are not zero cost."]
    (out/"comparison.md").write_text("\n".join(lines)+"\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary",type=Path,default=Path("target/release/examples/total-work"))
    parser.add_argument("--output",type=Path,required=True)
    parser.add_argument("--profile",choices=["smoke","screen"],default="smoke")
    parser.add_argument("--matrix",type=Path,help="Custom JSON array of bounded case configs")
    parser.add_argument("--repetitions",type=int,default=5)
    parser.add_argument("--threads",type=int,default=2)
    parser.add_argument("--timeout",type=float,default=600)
    parser.add_argument("--dry-run",action="store_true")
    args = parser.parse_args()
    if args.repetitions < 1 or args.threads < 1 or args.timeout <= 0:
        parser.error("repetitions, threads and timeout must be positive")
    run(args)
