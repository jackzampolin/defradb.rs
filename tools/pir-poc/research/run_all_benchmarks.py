#!/usr/bin/env python3
"""Execute every benchmark family in isolated processes; retain all attempts."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import random
import signal
import statistics
import subprocess
import sys
import time

from benchmarks.cases import Case
from benchmarks.hardware import probe, perf_prefix
from benchmarks.matrix import matrix

ROOT=Path(__file__).resolve().parent


def write(path,value):
    with path.open("x") as stream:json.dump(value,stream,indent=2)


def invoke(argv,log,timeout,**kwargs):
    # Timeout kills only this invocation's session, including spawned roles.
    with subprocess.Popen(argv,stdout=log,stderr=subprocess.STDOUT,start_new_session=True,**kwargs) as proc:
        try:return proc.wait(timeout)
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid,signal.SIGTERM)
            try:proc.wait(10)
            except subprocess.TimeoutExpired:os.killpg(proc.pid,signal.SIGKILL);proc.wait()
            raise


def execute(args,case,folder):
    engine=case["engine"];config=dict(case["config"])
    if engine=="protocol":Case(**config).validate()
    if engine.startswith("native"):
        if config["rows"]*config["row_bytes"]*4>args.resident_bytes:raise ValueError("native resident preflight exceeded")
        if not args.native or not args.native.is_file():raise ValueError("native release executable unavailable; pass --native")
    if engine=="zelda" and not args.zelda_source:raise ValueError("pinned Zelda checkout required: --zelda-source")
    if engine=="gpu" and not args.gpu_source:raise ValueError("pinned GPU-DPF checkout required: --gpu-source")
    if args.dry_run:return dict(status="preflight-pass",case=case)
    config_path=folder.with_suffix(".json");write(config_path,config)
    env=dict(os.environ,PYTHONPATH=str(ROOT))
    if engine=="protocol":argv=[sys.executable,"-m","benchmarks.run_case",str(config_path),str(folder)]
    elif engine=="native":
        folder.mkdir();argv=[str(args.native),str(config_path)]
    elif engine=="native-clients":
        folder.mkdir();reports=[]
        clients=config.pop("clients")
        for client in range(clients):
            path=folder/f"client-{client}.json";write(path,config)
            with (folder/f"client-{client}.log").open("w") as log:
                status=invoke([str(args.native),str(path)],log,args.timeout)
            if status:raise RuntimeError(f"native client {client} failed; see retained log")
            reports.append(json.loads((folder/f"client-{client}.log").read_text()))
        write(folder/"result.json",dict(schema="pir-native-client-lifecycle-v1",clients=reports,
            accounting="Independent full setups including publication, charged once per client; pessimistic restart scenario. No amortization across clients."))
        return dict(status="passed",case=case,result=str(folder/"result.json"))
    else:
        argv=[sys.executable,"-m","benchmarks.external",engine,str(config_path),str(folder),str(args.zelda_source if engine=="zelda" else args.gpu_source)]
    if args.perf:argv=perf_prefix(folder.with_suffix(".perf.csv"))+argv
    with folder.with_suffix(".log").open("w") as log:
        status=invoke(argv,log,args.timeout,cwd=ROOT,env=env)
    if status:
        log=folder.with_suffix(".log").read_text()
        if engine=="native" and any(message in log for message in ("exceeds resident budget","exceed resident budget","exceeds budget","exceeds download budget","exceeds upload budget","too few rows","finite-differences budget")):
            raise ValueError(log.strip())
        raise RuntimeError(f"exit {status}; see {folder.with_suffix('.log')}")
    if engine=="native":write(folder/"result.json",json.loads(folder.with_suffix(".log").read_text()))
    return dict(status="passed",case=case,result=str(folder/"result.json"))


def summarize(attempts):
    summary=[]
    for name in sorted({a["case"]["name"] for a in attempts}):
        runs=[a for a in attempts if a["case"]["name"]==name]
        values=[]
        for run in runs:
            if run["status"]!="passed":continue
            result=json.loads(Path(run["result"]).read_text())
            value=result.get("server_cpu_ms_per_query",result.get("server_cpu_ms_per_completed_query"))
            if value is None:value=result.get("amortized",{}).get("server_cpu_ms_per_query")
            if value is not None:values.append(value)
        row=dict(name=name,engine=runs[0]["case"]["engine"],attempts=len(runs),passed=sum(r["status"]=="passed" for r in runs),complete_run_set=all(r["status"]=="passed" for r in runs),server_cpu_ms_per_query_median=statistics.median(values) if values else None)
        if len(values)>=5:
            rng=random.Random(7331);boot=sorted(statistics.median(rng.choices(values,k=len(values))) for _ in range(2000))
            row["median_95pct_bootstrap_interval"]=[boot[50],boot[1949]]
        summary.append(row)
    return summary


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--output",type=Path,required=True)
    p.add_argument("--profile",choices=("smoke","screen","scale"),default="smoke")
    p.add_argument("--matrix",type=Path,help="Explicit JSON list of family/engine/name/config cases")
    p.add_argument("--family",action="append",choices=[f"B{i}" for i in range(9)])
    p.add_argument("--engine",action="append",choices=("native","native-clients","protocol","zelda","gpu"))
    p.add_argument("--name",help="case name substring")
    p.add_argument("--repeats",type=int,default=5)
    p.add_argument("--native",type=Path)
    p.add_argument("--zelda-source",type=Path)
    p.add_argument("--gpu-source",type=Path)
    p.add_argument("--resident-bytes",type=int,default=512<<20)
    p.add_argument("--timeout",type=int,default=600)
    p.add_argument("--perf",action="store_true",help="Whole invocation counters (client + server); requires perf access")
    p.add_argument("--dry-run",action="store_true")
    args=p.parse_args()
    if args.repeats<1:p.error("repeats must be positive")
    args.output=args.output.resolve();args.output.mkdir(parents=True,exist_ok=False)
    for field in ("native","zelda_source","gpu_source"):
        value=getattr(args,field)
        if value:setattr(args,field,value.resolve())
    cases=[c for c in (json.loads(args.matrix.read_text()) if args.matrix else matrix(args.profile)) if (not args.family or c["family"] in args.family) and (not args.engine or c["engine"] in args.engine) and (not args.name or args.name in c["name"])]
    if not cases:p.error("no cases selected")
    for case in cases:
        if case["engine"] in ("native","protocol"):case["config"]["max_resident_bytes"]=args.resident_bytes
    sources={str(path.relative_to(ROOT)):hashlib.sha256(path.read_bytes()).hexdigest() for path in ROOT.rglob("*.py") if "__pycache__" not in path.parts}
    write(args.output/"manifest.json",dict(arguments={k:str(v) if isinstance(v,Path) else v for k,v in vars(args).items()},hardware=probe(),source_sha256=sources,
        native_sha256=hashlib.sha256(args.native.read_bytes()).hexdigest() if args.native and args.native.is_file() else None,
        statistics="Fresh process runs, alternating case order. Bootstrap intervals describe independent run medians; no percentile sums or cross-language speedups."))
    write(args.output/"matrix.json",cases)
    attempts=[]
    for repeat in range(1 if args.dry_run else args.repeats):
        for ordinal,case in enumerate(cases if repeat%2==0 else reversed(cases)):
            folder=args.output/f"r{repeat:02d}-{case['name']}";start=time.monotonic()
            try:attempt=execute(args,case,folder)
            except ValueError as error:attempt=dict(status="gated",reason=str(error),case=case)
            except Exception as error:attempt=dict(status="failed",reason=str(error),case=case,
                partial_work_ledger=str(folder/"work-ledger.json") if (folder/"work-ledger.json").exists() else None)
            attempt.update(repeat=repeat,elapsed_seconds=time.monotonic()-start)
            attempts.append(attempt)
            with (args.output/"attempts.jsonl").open("a") as stream:stream.write(json.dumps(attempt)+"\n")
            print(f"{repeat+1}/{args.repeats} {case['name']}: {attempt['status']}",flush=True)
    write(args.output/"summary.json",summarize(attempts))
    if any(a["status"]=="failed" for a in attempts):sys.exit(1)


if __name__=="__main__":main()
