#!/usr/bin/env python3
"""Run each private-index case in a fresh process, retain failures and raw data."""
import argparse
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import platform
import statistics
import subprocess
import sys
import time

from benchmarks.index_compositions import Config
from benchmarks.private_indexes import FAMILIES
from run_all_benchmarks import invoke


def matrix(profile):
    if profile=='all':
        for part in ('smoke','native','ramen','extras','maintenance','warm'):yield from matrix(part)
        return
    if profile in ('warm','frontier'):
        for n in (16384,65536):
            for backend in ('dense-native','singlepass-native'):
                for partitions in ((4,16,32) if profile=='frontier' and backend=='singlepass-native' else (4,)):
                    yield Config(family='posting',backend=backend,rows=n,row_bytes=32,queries=4096 if profile=='warm' else 128,partitions=partitions)
        return
    if profile=='maintenance':
        for family in FAMILIES:
            for backend in (('dense-native','singlepass-native') if family=='posting' else ('dense-native','path-native')):
                for mutation in ('value','key'):
                    yield Config(family=family,backend=backend,rows=128,row_bytes=32,queries=24,
                        fanout=1 if family=='authenticated' else 2,group=16 if family in ('bitmap','wavelet') else 4,
                        update_every=8,mutation=mutation)
        return
    if profile=='ramen':
        for n in (32,128):
            for family in FAMILIES:
                yield Config(family=family,backend='ramen',rows=n,row_bytes=16,queries=12,
                    fanout=1 if family=='authenticated' else 2,group=16 if family in ('bitmap','wavelet') else 4)
        for mutation in ('value','delete','insert'):
            yield Config(family='authenticated',backend='ramen',rows=32,row_bytes=16,queries=12,fanout=1,update_every=4,mutation=mutation)
        return
    if profile=='native':
        for n in (256,4096):
            for family in FAMILIES:
                for backend in ('dense-native','path-native','singlepass-native'):
                    if backend=='singlepass-native' and family not in ('posting','hash','radix'):continue
                    yield Config(family=family,backend=backend,rows=n,row_bytes=96,queries=32,
                        fanout=1 if family=='authenticated' else 2,group=32 if family in ('bitmap','wavelet') else 4)
        return
    if profile=='extras':
        for n in (256,1024):
            for backend in ('dense-native','path-native','singlepass-native'):
                for leaf in (0,8,16):
                    yield Config(family='radix',backend=backend,rows=n,row_bytes=32,queries=16,fanout=2,
                        field_bits=32,key_layout='scattered',radix_leaf_bits=leaf)
        return
    rows=[32] if profile=='smoke' else [256,1024,4096]
    for n in rows:
        for family in FAMILIES:
            for backend in ('dense','path','singlepass'):
                if backend=='singlepass' and family not in ('posting','hash'):continue
                groups=(1,2,4,8) if family=='radix' else (8,32) if family in ('bitmap','wavelet') else (4,)
                for group in groups:
                    yield Config(family=family,backend=backend,rows=n,group=group,
                        queries=12 if profile=='smoke' else 32,row_bytes=16 if profile=='smoke' else 96,
                        fanout=1 if family=='authenticated' else 2,slots=4)
        for kind in ('value','delete','insert'):
            yield Config(family='authenticated',backend='path',rows=n,queries=12,fanout=1,
                mutation=kind,update_every=4,row_bytes=16 if profile=='smoke' else 96)
        yield Config(family='posting',backend='singlepass',rows=n,queries=12,update_every=4)
        for family in ('bitmap','wavelet'):
            yield Config(family=family,backend='path',rows=n,group=16,distribution='clustered',
                count_only=family=='wavelet',queries=12)


def main():
    p=argparse.ArgumentParser();p.add_argument('--output',required=True,type=Path)
    p.add_argument('--profile',choices=['all','smoke','screen','ramen','native','extras','warm','frontier','maintenance'],default='smoke');p.add_argument('--repeats',type=int,default=3)
    p.add_argument('--family',choices=list(FAMILIES));p.add_argument('--backend');p.add_argument('--matrix',type=Path)
    p.add_argument('--ramen-binary');p.add_argument('--timeout',type=int,default=1800);p.add_argument('--dry-run',action='store_true')
    a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False)
    configs=[Config(**v) for v in json.loads(a.matrix.read_text())] if a.matrix else list(matrix(a.profile))
    configs=[c for c in configs if (not a.family or c.family==a.family) and (not a.backend or c.backend==a.backend)]
    (a.output/'matrix.json').write_text(json.dumps([asdict(c) for c in configs],indent=2))
    (a.output/'environment.json').write_text(json.dumps(dict(python=sys.version,platform=platform.platform(),
        cpu=Path('/proc/cpuinfo').read_text() if Path('/proc/cpuinfo').exists() else 'unavailable',
        source_hashes={str(path.name):hashlib.sha256(path.read_bytes()).hexdigest() for path in (Path(__file__).parent/'benchmarks').glob('*.py')}),indent=2))
    if a.dry_run:return
    summary=[]
    for i,c in enumerate(configs):
        for repeat in range(a.repeats):
            name=f'{i:03d}-{c.family}-{c.backend}-n{c.rows}-g{c.group}-r{repeat}'
            config=a.output/(name+'.json');config.write_text(json.dumps(asdict(c)))
            destination=a.output/name
            command=[sys.executable,'-m','benchmarks.index_compositions',str(config.resolve()),str(destination.resolve())]
            if a.ramen_binary:command+=['--ramen-binary',str(Path(a.ramen_binary).resolve())]
            start=time.time()
            with (a.output/(name+'.log')).open('w') as log:
                try:
                    code=invoke(command,log,a.timeout,cwd=Path(__file__).parent,
                        env=dict(os.environ,OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1'))
                    status='ok' if code==0 else 'failed'
                except subprocess.TimeoutExpired:status='timeout'
            entry=dict(name=name,status=status,elapsed_seconds=time.time()-start)
            if (destination/'result.json').exists():
                result=json.loads((destination/'result.json').read_text())
                entry.update(family=c.family,backend=c.backend,rows=c.rows,group=c.group,
                    mutation=c.mutation,update_every=c.update_every,count_only=c.count_only,
                    distribution=c.distribution,slots=c.slots,fanout=c.fanout,
                    field_bits=c.field_bits,key_layout=c.key_layout,radix_leaf_bits=c.radix_leaf_bits,
                    budget_failures=result['budget_failures'],
                    online_server_cpu_ms=statistics.mean(s['all_server_cpu_ms'] for s in result['samples']),
                    online_client_cpu_ms=statistics.mean(s['client_cpu_ms'] for s in result['samples']),
                    online_upload_max=max(s['upload_bytes'] for s in result['samples']),
                    online_download_max=max(s['download_bytes'] for s in result['samples']),
                    total_server_cpu_ms=result['server_plus_lifecycle_controller_per_answer_ms'],
                    all_participant_cpu_ms=result['all_participant_cpu_per_answer_ms'],
                    logical_index_bytes=result['logical_index_bytes'])
            summary.append(entry)
            (a.output/'summary.json').write_text(json.dumps(summary,indent=2))
            print(f'{name}: {status}',flush=True)
    if any(s['status']!='ok' for s in summary):sys.exit(1)


if __name__=='__main__':main()
