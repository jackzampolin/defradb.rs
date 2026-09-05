"""Summarize matched use-case runs; never promote resource failures as winners."""
import argparse
import json
from pathlib import Path
import subprocess
import sys


def main():
    p=argparse.ArgumentParser();p.add_argument('root',type=Path);p.add_argument('--output',type=Path,required=True)
    p.add_argument('--additional',type=Path,action='append',default=[]);a=p.parse_args()
    roots=[a.root,*a.additional]
    attempted=0
    for root in roots:
        manifest=json.loads((root/'campaign/manifest.json').read_text());attempted+=len(manifest['matrix'])
        for repeat in range(manifest['repeats']):
            for index in range(len(manifest['matrix'])):
                case=root/'campaign'/f'{index:03d}-r{repeat}'
                if (case/'result.json').exists()==(case/'failure.txt').exists():
                    raise ValueError(f'case must have exactly one result or retained failure: {case}')
    subprocess.run([sys.executable,'analyze_cold_search.py',*[str(root/'campaign') for root in roots],'--output',str(a.output)],check=True)
    rows=json.loads(a.output.with_suffix('.json').read_text())
    canonical_cpu=max((r['external_canonical_fixture_cpu_ms'] for r in rows),default=0)/1000
    failures=[]
    for file in sorted(file for root in roots for file in (root/'campaign').glob('*-r*/failure.txt')):
        config=json.loads(file.parent.with_suffix('.json').read_text())
        failures.append(dict(campaign=file.parent.parent.parent.name,case=file.parent.name,config=config,error=file.read_text().strip().splitlines()[-1]))
    a.output.with_name(a.output.name+'_FAILURES').with_suffix('.json').write_text(json.dumps(failures,indent=2))
    lines=['# Indexed Dense across use cases: measured results','',
           f'{attempted} attempted configurations; {len(rows)} result-bearing configurations; {sum(r["verified_answers"] for r in rows):,} verified answers; '
           f'{len(failures)} case repetitions rejected before results.','',
           'Serving values are aggregate request-phase CPU milliseconds across replicas and the public metadata provider. '
           'Cold includes fresh-client setup delivery, but global build/publication is separate. '
           'Session averages charge that client setup once. These are synthetic 64-bit lookup keys and fixed payload projections, '
           'not imported production corpora. Bytes include the harness JSON/hex/base64 framing.','',
           '## Fresh clients: best qualified directory versus matched controls','',
           '| Workload | Source rows | Payload B / matches per key | Directory group | Directory CPU | XOR CPU | Hashed pages CPU | Build/publish + residual ms directory / XOR | Directory setup KB | Upload / download KB | Client setup / online ms | Client peak MB |',
           '|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|']
    keys=sorted({(r['workload'],r['rows']) for r in rows if r['family']=='directory'})
    def qualified(r):return not r['budget_failures'] and r['repeats']>=5
    def fmt(r):
        if not r:return 'gated'
        return f'{r["service_cpu_ms"]:.3f}'+('' if qualified(r) else ' (unqualified)')
    for name,n in keys:
        subset=[r for r in rows if r['workload']==name and r['rows']==n and r['queries_per_client']==1]
        directory=[r for r in subset if r['family']=='directory' and r['backend']=='dense' and qualified(r)]
        if not directory:
            lines.append(f'| {name} | {n:,} | — | — | no qualified directory | — | — | — | — | — | — | — |');continue
        best=min(directory,key=lambda r:r['service_cpu_ms'])
        controls={f:next((r for r in subset if r['family']==f and r['backend']=='dense'),None) for f in ('xor','binary-tags')}
        xor_build=f'{controls["xor"]["global_publish_build_cpu_ms"]:.1f}' if controls['xor'] else 'gated'
        lines.append(f'| {name} | {n:,} | {best["row_bytes"]} / {best["fanout"]} | {best["group"]} | {fmt(best)} | '
                     f'{fmt(controls["xor"])} | {fmt(controls["binary-tags"])} | {best["global_publish_build_cpu_ms"]:.1f} / {xor_build} | {best["setup_download_bytes"]/1000:.2f} | '
                     f'{best["query_upload_bytes"]/1000:.2f} / {best["query_download_bytes"]/1000:.2f} | '
                     f'{best["client_setup_cpu_ms"]:.2f} / {best["client_online_cpu_ms"]:.2f} | {best["max_client_rss_bytes"]/1e6:.2f} |')
    lines+=['','## Reused clients: matched directory layouts','',
            'Each entry reports setup-amortized service CPU / online-only server CPU per answer. '
            'Online-only is measured inside the session and excludes its setup; it is not a fresh-query figure.','',
            '| Workload | Rows | Group | Queries/client | Dense ms | SinglePass ms | Full campaign CPU/answer Dense / SP ms | Dense / SP setup download MB |',
            '|---|---:|---:|---:|---:|---:|---:|---:|']
    for r in sorted(rows,key=lambda r:(r['workload'],r['rows'],r['group'],r['queries_per_client'])):
        if r['backend']!='dense' or r['queries_per_client']==1 or r['family'] not in ('directory','canonical-directory'):continue
        sp=next((s for s in rows if s['backend']=='singlepass' and all(s[k]==r[k] for k in ('workload','rows','family','group','queries_per_client'))),None)
        if not sp:
            lines.append(f'| {r["workload"]} | {r["rows"]:,} | {r["group"]} | {r["queries_per_client"]} | '
                         f'{fmt(r)} / {r["server_online_cpu_ms"]:.3f} | gated | '
                         f'{r["full_campaign_server_cpu_per_answer_ms"]:.3f} / — | {r["setup_download_bytes"]/1e6:.3f} / — |')
        if sp:
            lines.append(f'| {r["workload"]} | {r["rows"]:,} | {r["group"]} | {r["queries_per_client"]} | '
                         f'{fmt(r)} / {r["server_online_cpu_ms"]:.3f} | {fmt(sp)} / {sp["server_online_cpu_ms"]:.3f} | '
                         f'{r["full_campaign_server_cpu_per_answer_ms"]:.3f} / {sp["full_campaign_server_cpu_per_answer_ms"]:.3f} | '
                         f'{r["setup_download_bytes"]/1e6:.3f} / {sp["setup_download_bytes"]/1e6:.3f} |')
    lines+=['','## Canonical witnesses and epoch presence','',
            '| Workload | Rows | Family | Backend | Group | Queries/client | Service CPU ms | Client online ms | Setup KB | Online up/down KB | Cap failures |',
            '|---|---:|---|---|---:|---:|---:|---:|---:|---:|---|']
    for r in sorted(rows,key=lambda r:(r['workload'],r['rows'],r['queries_per_client'],r['family'],r['backend'],r['group'])):
        if r['workload'] not in ('mizu-canonical-witness','shared-epoch-alerts'):continue
        lines.append(f'| {r["workload"]} | {r["rows"]:,} | {r["family"]} | {r["backend"]} | {r["group"]} | '
                     f'{r["queries_per_client"]} | {r["service_cpu_ms"]:.3f} | {r["client_online_cpu_ms"]:.3f} | '
                     f'{r["setup_download_bytes"]/1000:.2f} | {r["query_upload_bytes"]/1000:.2f} / {r["query_download_bytes"]/1000:.2f} | {r["budget_failures"] or "none"} |')
    lines+=['','## Reproduction and limits','',
            'Run `run_indexed_use_cases.py --output NEW_DIR --native NATIVE_STORE --bridge COLD_CANONICAL --repeats 5` '
            'from this directory in Linux/WSL. Then run `report_indexed_use_cases.py NEW_DIR --output OUTPUT_PREFIX`. '
            'The cold-search runner freezes source modules and hashes the native binary. The root contains the exact matrix, '
            'canonical corpus and wrong-root/tamper checks; each case retains its raw process phases and client measurements.','',
            'Add `--profile large-warm` with a new output directory for the 1,024-query larger-scope sessions. '
            'Use `--additional LARGE_WARM_ROOT` when generating this combined report.','',
            'The `witness-warm` profile takes `--canonical-corpus MAIN_ROOT/canonical-8192.json` and reuses that measured '
            'snapshot for 1,024-query witness sessions. Include its root with another `--additional`.','',
            '- Five repetitions alternate execution order over the same deterministic corpus for each shape. Client processes are fresh and sequential; no claim of a load-tested fleet.',
            '- Payload widths are benchmark projections (804/548/184/256/120 B), plus record framing. Tags have uniform multiplicity except the explicitly hot-value case (2,048 records).',
            '- Complete all-match retrieval is verified, including absent values. No result-driven extra network requests. A large hot group can force every answer over the wire cap.',
            '- Global fixtures search their entire stated scope. Their smaller resident sizes do not validate million/billion-row deployments or arbitrary compound predicates.',
            '- Canonical witnesses preserve the existing Poseidon depth-20 root and 2,008-byte witness. The fixture has 8,192 values plus the sentinel, sorted physical positions; live updates/root maintenance remain unmeasured.',
            f'- The canonical corpus builder used {canonical_cpu:.1f} seconds CPU once for this snapshot. This is included once per generation in the machine-readable totals. Precomputed full witnesses are not a measured replacement for the existing active base/delta predecessor and node-plane serving design.',
            '- Alert controls answer the same 65,536-bucket presence hint, including collisions. Payloads are separate. The public bitmap uses no native PIR endpoint and exposes no selected bucket. It must be distributed to all clients on a query-independent epoch schedule.',
            '- Public bitmap CPU is metadata-provider delivery CPU, not a claim that epoch construction, bandwidth, authentication or payload follow-ups are free.',
            '- Resource limits: 64 MiB logical index; 64 MiB client setup download/state; 128 MiB client RSS; 1 MiB per-direction online wire; 1 s client online CPU. Failures are retained, not dropped from qualification.',
            '- SinglePass uses this repository\'s show-and-shuffle adapter and four partitions; these are backend/layout comparisons, not a complete retuning of every SinglePass parameter.',
            '- Canonical client RSS adds the parent high-water mark and a conservative child-verifier high-water mark. A failure of this bound is a memory-qualification gate, not proof of simultaneous resident use over the cap.',
            '- Timings include harness serialization. Do not divide these CPU results by historical GPU/decoy wall-time projections. No production serving defaults were changed.','']
    lines+=['The machine-readable `full_campaign_server_cpu_ms` sums every native process CPU counter plus publisher build/publication/delivery '
            'and canonical construction, without dropping work outside request timers. `native_cpu_outside_request_phases_ms` exposes that residual '
            '(including input-line reading, startup and cleanup). The legacy `global_publish_build_cpu_ms` includes this residual; it is not a pure '
            'generation-build measurement. Do not treat its G projections as an independently measured generation-lifetime crossover.','']
    a.output.with_suffix('.md').write_text('\n'.join(lines))


if __name__=='__main__':main()
