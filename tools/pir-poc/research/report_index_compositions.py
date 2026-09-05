#!/usr/bin/env python3
"""Aggregate completed independent runs without mixing workloads or backends."""
import argparse
import csv
import json
import os
from pathlib import Path
import statistics


def percentile(values,p):
    ordered=sorted(values);return ordered[min(len(ordered)-1,int((len(ordered)-1)*p))]


def main():
    p=argparse.ArgumentParser();p.add_argument('inputs',type=Path,nargs='+');p.add_argument('--output',type=Path,required=True)
    a=p.parse_args();groups={};failures=[];count=0
    for root in a.inputs:
        for path in sorted(root.glob('*/result.json')):
            result=json.loads(path.read_text())
            if 'config' not in result:continue
            config=result['config'];key=json.dumps(config,sort_keys=True)
            groups.setdefault(key,[]).append((path,result));count+=1
        for path in sorted(root.glob('*/failure.json')):failures.append((str(path),json.loads(path.read_text())['error']))
    rows=[]
    for key,values in groups.items():
        c=json.loads(key);results=[v for _,v in values];samples=[s for r in results for s in r['samples']]
        # Legacy bridge phase samples stop before response serialization. Full
        # process CPU is still metered; do not silently treat these as exact
        # online measurements after introducing the final phase meter.
        phase_complete=all('value' not in phase for r in results for component in r['components'] for role in component['roles'] for phase in role['phases'])
        row=dict(family=c['family'],backend=c['backend'],rows=c['rows'],row_bytes=c['row_bytes'],group=c['group'],
            field_bits=c.get('field_bits',0),key_layout=c.get('key_layout','contiguous'),leaf_bits=c.get('radix_leaf_bits',0),
            partitions=c['partitions'],count_only=c['count_only'],distribution=c['distribution'],
            update_every=c['update_every'],mutation=c['mutation'],queries_per_run=c['queries'],runs=len(results),
            verified_answers=len(samples),phase_meter_complete=phase_complete,
            online_server_mean_ms=statistics.mean(s['all_server_cpu_ms'] for s in samples),
            online_server_p95_ms=percentile([s['all_server_cpu_ms'] for s in samples],.95),
            client_p95_ms=percentile([s['client_cpu_ms'] for s in samples],.95),
            total_server_plus_lifecycle_ms=statistics.median(r['server_plus_lifecycle_controller_per_answer_ms'] for r in results),
            all_participant_ms=statistics.median(r['all_participant_cpu_per_answer_ms'] for r in results),
            upload_max=max(s['upload_bytes'] for s in samples),download_max=max(s['download_bytes'] for s in samples),
            index_bytes=max(r['logical_index_bytes'] for r in results),
            caps=';'.join(sorted({f for r in results for f in r['budget_failures']})) or 'pass',
            raw=';'.join(os.path.relpath(path,a.output.parent) for path,_ in values))
        rows.append(row)
    a.output.parent.mkdir(parents=True,exist_ok=True)
    with a.output.with_suffix('.csv').open('w',newline='') as stream:
        writer=csv.DictWriter(stream,fieldnames=list(rows[0]));writer.writeheader();writer.writerows(rows)
    a.output.with_suffix('.json').write_text(json.dumps(dict(successful_runs=count,failures=failures,groups=rows),indent=2))
    lines=['# Private index composition measurements','',
        f'{count} completed runs; {sum(r["verified_answers"] for r in rows)} verified complete answers; {len(failures)} retained failures.',
        '', 'The primary total below sums every server process and the separately timed index construction/setup/update controller, amortized over the actual number of queries. This conservatively includes client-side setup work in the lifecycle term. Online client CPU is reported separately; all-participant CPU is in the CSV. Process startup and rebuild costs are not discarded.',
        '', 'These are local research prototypes with JSON serialization, public dimensions, one honest client/owner, and public writer update schedules. The controller also retains the publisher and correctness oracle; its RSS cap therefore conservatively covers those copies. Query methods receive a metadata-only view. This is not a production ranking or a malicious-security audit.',
        '', 'Rows may only be compared at matching payload width, predicate/output semantics, data layout, padding and lifecycle. Native store controls use a compiled set-bit XOR kernel; Path ORAM still has client-side Python cryptography. Ramen operates on 15-byte limbs. Bitmap cases include three extra MPC processes; wavelet count-only is a separate workload. Authenticated cases use SHA-256 and a trusted fresh root, not production Poseidon witness bytes.',
        '', 'Legacy Ramen bridge phase timings omit response serialization; those rows mark online CPU with `~`. Their full process/lifecycle totals remain measured. Final bridge timings include response serialization. Wall-clock results from overlapping local campaigns are not used for rankings.',
        '', '| Family | Backend | N / payload B | Variant | Runs | Online server ms | Client p95 ms | Total ms/answer | Caps |',
        '|---|---|---:|---|---:|---:|---:|---:|---|']
    for r in rows:
        variant=f'g={r["group"]}, leaf={r["leaf_bits"]}, Q={r["queries_per_run"]}, P={r["partitions"]}'
        if r['field_bits']:variant+=f', bits={r["field_bits"]}'
        if r['count_only']:variant+=', count'
        if r['update_every']:variant+=f', {r["mutation"]}/{r["update_every"]}'
        if r['distribution']!='shuffled':variant+=', '+r['distribution']
        prefix='' if r['phase_meter_complete'] else '~'
        lines.append(f'| {r["family"]} | {r["backend"]} | {r["rows"]} / {r["row_bytes"]} | {variant} | {r["runs"]} | {prefix}{r["online_server_mean_ms"]:.3f} | {r["client_p95_ms"]:.3f} | {r["total_server_plus_lifecycle_ms"]:.3f} | {r["caps"]} |')
    if failures:
        lines+=['','## Retained failures','']+[f'- `{path}`: {error}' for path,error in failures]
    lines+=['','Raw run paths, complete configuration columns, resource caps and per-run references are preserved in the adjacent CSV/JSON.']
    a.output.write_text('\n'.join(lines)+'\n')


if __name__=='__main__':main()
