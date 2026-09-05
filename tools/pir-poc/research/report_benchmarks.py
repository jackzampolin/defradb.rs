"""Tables and explicitly modeled break-even curves from immutable run ledgers."""
import argparse
import json
from pathlib import Path
import statistics


def median(values):
    values=[v for v in values if v is not None]
    return statistics.median(values) if values else None


def report(source,output,plots=False):
    output.mkdir(parents=True,exist_ok=False)
    attempts=[json.loads(line) for line in (source/"attempts.jsonl").read_text().splitlines()]
    rows=[];curves=[]
    for name in sorted({a["case"]["name"] for a in attempts}):
        group=[a for a in attempts if a["case"]["name"]==name]
        case=group[0]["case"]
        results=[json.loads(Path(a["result"]).read_text()) for a in group if a["status"]=="passed"]
        cpu=median([r.get("server_cpu_ms_per_query",r.get("server_cpu_ms_per_completed_query")) for r in results])
        gpu=median([r["gpu_active_ms"]/r["completed_logical_queries"] for r in results if r.get("gpu_active_ms") is not None])
        storage=[]
        for r in results:
            value=r.get("aggregate_server_storage_bytes",r.get("aggregate_table_bytes"))
            if value is None and "store_stats" in r:
                value=sum(s.get("stored_bytes",s.get("source_bytes",0)+s.get("index_bytes",0)) for s in r["store_stats"])
            storage.append(value)
        private=results[0].get("private",results[0].get("security",{}).get("private")) if results else None
        caps=[r.get("client_caps_pass",r.get("client_measured_caps_pass")) for r in results]
        row=dict(name=name,family=case["family"],engine=case["engine"],config=case["config"],passed=len(results),attempts=len(group),private=private,
            server_cpu_ms_per_query=cpu,gpu_active_ms_per_query=gpu,aggregate_storage_bytes=median(storage),
            client_caps="pass" if caps and all(v is True for v in caps) else "fail" if any(v is False for v in caps) else "unmeasured",
            failures=[a.get("reason") for a in group if a["status"]!="passed"],evidence="measured, local hardware; no distributed deployment or production promotion")
        rows.append(row)
        if case["engine"]=="native" and results and case["config"].get("candidate") in ("dense","subset","single-pass") and not case["config"].get("cold_cache_bytes",0):
            setup=median([r["global_server_build"]["cpu_ms"] for r in results])
            online=median([sum(s["server"]["cpu_ms"] for s in r["samples"])/r["completed_logical_queries"] for r in results])
            maintenance=median([r["server_maintenance"]["cpu_ms"]/r["rebuild_count"] for r in results if r["rebuild_count"]])
            curves.append(dict(name=name,rows=case["config"]["rows"],row_bytes=case["config"]["row_bytes"],setup_cpu_ms=setup,online_cpu_ms=online,
                rebuild_cpu_ms=maintenance,persistent_client_bytes=median([r["persistent_client_bytes"] for r in results]),aggregate_storage_bytes=row["aggregate_storage_bytes"]))
    (output/"comparison.json").write_text(json.dumps(rows,indent=2))
    (output/"projection-inputs.json").write_text(json.dumps(dict(curves=curves,
        model="single-client stationary projection: initial build / queries + measured mean online + generation replacement cost * update/query ratio. Query horizons and update rates are modeled, never measured throughput. Rebuild includes client-dependent setup only to the extent charged in the source native ledger.",
        query_horizons=[1,10,100,1000,10000],client_populations=[1,100,10000],client_population_note="Independent fresh-client restart upper scenario multiplies whole-run work and completed queries by clients; no unmeasured shared-setup savings assumed.",
        update_rates=[0,.001,.01,.1,1]),indent=2))
    lines=["# Complete-work benchmark coverage","","CPU and GPU columns have different units and are never added. Engines and answer scopes are separate comparison lanes. A missing resource measurement is not zero. Smoke rows validate implementation, not scale or speedups.","",
        "| Family / case | Engine | Passed | CPU ms/query | GPU ms/query | Storage MiB | Client caps |",
        "|---|---|---:|---:|---:|---:|---|"]
    def fmt(v):return "—" if v is None else f"{v:.4f}"
    for r in rows:lines.append(f"| {r['family']} / {r['name']} | {r['engine']} | {r['passed']}/{r['attempts']} | {fmt(r['server_cpu_ms_per_query'])} | {fmt(r['gpu_active_ms_per_query'])} | {fmt(r['aggregate_storage_bytes']/2**20 if r['aggregate_storage_bytes'] is not None else None)} | {r['client_caps']} |")
    if plots:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        selected=[c for c in curves if c["name"] in ("cpu-dense","subset-2-cold-0","subset-4-cold-0","single-pass-2","single-pass-32")]
        if selected:
            dimensions={(c["rows"],c["row_bytes"]) for c in selected}
            if len(dimensions)!=1:raise ValueError("refuse cross-workload break-even plot")
            x=[10**(i/20) for i in range(81)]
            fig,ax=plt.subplots(figsize=(8,4.8))
            for c in selected:ax.plot(x,[c["setup_cpu_ms"]/q+c["online_cpu_ms"] for q in x],label=c["name"])
            ax.set(xscale="log",yscale="log",xlabel="Queries per generation (modeled, one client)",ylabel="Aggregate server CPU ms / complete row",title="Setup amortization — stationary projection from measured phases")
            ax.legend(fontsize=8);ax.grid(alpha=.2);fig.tight_layout();fig.savefig(output/"break-even.svg");fig.savefig(output/"break-even.png",dpi=150);plt.close(fig)
            for field,label,filename in (("aggregate_storage_bytes","Aggregate storage (MiB)","storage-work.svg"),("persistent_client_bytes","Persistent client state (MiB)","client-state-work.svg")):
                fig,ax=plt.subplots(figsize=(8,4.8))
                for c in selected:
                    at=c[field]/2**20;cost=c["setup_cpu_ms"]/100+c["online_cpu_ms"]
                    ax.scatter([at],[cost],label=c["name"])
                ax.set(xlabel=label,ylabel="Modeled CPU ms/query at 100 queries/generation");ax.legend(fontsize=8);ax.margins(x=.35,y=.25);ax.grid(alpha=.2);fig.tight_layout();fig.savefig(output/filename);fig.savefig((output/filename).with_suffix(".png"),dpi=150);plt.close(fig)
        updating=[c for c in curves if c["rebuild_cpu_ms"] is not None]
        if updating:
            fig,ax=plt.subplots(figsize=(8,4.8));rates=[.001,.01,.1,1]
            for c in updating:ax.plot(rates,[c["setup_cpu_ms"]/1000+c["online_cpu_ms"]+r*c["rebuild_cpu_ms"] for r in rates],marker="o",label=c["name"])
            ax.set(xscale="log",yscale="log",xlabel="Generation replacements / query (modeled)",ylabel="Aggregate server CPU ms/query",title="Full-generation replacement sensitivity")
            ax.legend(fontsize=8);ax.grid(alpha=.2);fig.tight_layout();fig.savefig(output/"update-sensitivity.svg");fig.savefig(output/"update-sensitivity.png",dpi=150);plt.close(fig)
        lines.extend(["","Plots are modeled sensitivity curves; they do not replace measured complete lifecycle runs."])
    (output/"comparison.md").write_text("\n".join(lines)+"\n")


if __name__=="__main__":
    p=argparse.ArgumentParser(description=__doc__);p.add_argument("input",type=Path);p.add_argument("output",type=Path);p.add_argument("--plots",action="store_true");args=p.parse_args();report(args.input,args.output,args.plots)
