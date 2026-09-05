#!/usr/bin/env python3
"""Isolated, resumable cold predicate-search campaign with immutable case logs."""
import argparse
from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import subprocess
import signal
import shutil
import math
import sys
import time
from benchmarks.cold_search import Config


def matrix(profile):
    if profile=='reuse':
        for n in (1024,16384):
            for queries in (1,2,4,16):
                for backend in ('dense','singlepass'):
                    yield Config(family='binary-tags',rows=n,group=16,backend=backend,queries_per_client=queries,clients=8,key_layout='hashed')
        return
    if profile=='bit64':
        for n in (1024,16384):
            for family in ('bit-owners','packed-wavelet','binary-patricia','directory','binary-tags','xor'):
                for group in ((1,2,4,8) if family in ('bit-owners','packed-wavelet','binary-patricia') else (16,)):
                    yield Config(family=family,rows=n,group=group,clients=4,field_bits=64,key_layout='hashed',distribution='shuffled')
        return
    if profile=='frontier':
        for n in (65536,262144):
            for width in (32,96):
                for budget in (1024,16384,65536,262144):
                    group=math.ceil((n//2)*8*4/3/(budget-256))
                    yield Config(family='directory',rows=n,row_bytes=width,group=group,clients=8,key_layout='hashed')
                for family in ('xor','binary-tags'):
                    yield Config(family=family,rows=n,row_bytes=width,group=16,clients=8,key_layout='hashed')
        return
    if profile=='extensions':
        for n in (256,1024,16384):
            for family in ('xor','binary-tags','directory'):
                for width in (32,96):
                    yield Config(family=family,rows=n,row_bytes=width,group=16,clients=8)
        for n in (256,1024):
            for family in ('bit-owners','bit-owners-raw','packed-wavelet'):
                for group in (1,2,4,8):
                    for distribution in ('clustered','shuffled'):
                        yield Config(family=family,rows=n,group=group,clients=4,distribution=distribution)
        return
    if profile=='directory':
        for n in (1024,16384,65536):
            for family in ('directory','auth-directory'):
                for width in (32,96,2008):
                    for group in (4,16,64):
                        yield Config(family=family,rows=n,row_bytes=width,group=group,clients=8,
                                     fanout=1 if family=='auth-directory' else 2)
        return
    if profile=='finite':
        for n in (256,1024,4096):
            for family in ('binary-tags','json-tags'):
                for backend in ('dense','finite'):
                    yield Config(family=family,backend=backend,rows=n,clients=8)
        return
    sizes=(32,) if profile=='smoke' else (1024,16384,65536)
    for n in sizes:
        for width in ((32,) if profile=='smoke' else (32,96)):
            for group in ((4,) if profile=='smoke' else (2,4,16)):
                for backend in ('dense','singlepass'):
                    for family in ('binary-tags','json-tags'):
                        yield Config(family=family,backend=backend,rows=n,row_bytes=width,
                            clients=4 if profile=='smoke' else 16,group=group)
    for family in ('posting','hash','radix','authenticated'):
        for n in ((32,) if profile=='smoke' else (256,1024)):
            for backend in ('dense','singlepass'):
                yield Config(family=family,backend=backend,rows=n,clients=4 if profile=='smoke' else 16,
                    fanout=1 if family=='authenticated' else 2,leaf_bits=4)
    for n in ((32,) if profile=='smoke' else (1024,16384)):
        for group in (1,2,4,8):
            yield Config(family='binary-patricia',rows=n,group=group,clients=4 if profile=='smoke' else 16)


def main():
    p=argparse.ArgumentParser();p.add_argument('--output',type=Path,required=True)
    p.add_argument('--native',required=True);p.add_argument('--finite');p.add_argument('--profile',choices=['smoke','screen','finite','directory','extensions','frontier','reuse','bit64'],default='smoke')
    p.add_argument('--repeats',type=int,default=5);p.add_argument('--resume',action='store_true')
    p.add_argument('--matrix-from',type=Path);p.add_argument('--clients',type=int)
    a=p.parse_args();a.output.mkdir(parents=True,exist_ok=a.resume)
    a.native=str(Path(a.native).resolve())
    if a.finite:a.finite=str(Path(a.finite).resolve())
    configs=([Config(**c) for c in json.loads(a.matrix_from.read_text())['matrix']] if a.matrix_from else list(matrix(a.profile)))
    if a.clients:
        for c in configs:c.clients=a.clients
    manifest=dict(profile=a.profile,repeats=a.repeats,
        matrix=[asdict(c) for c in configs],native_sha256=hashlib.sha256(Path(a.native).read_bytes()).hexdigest(),
        finite_sha256=hashlib.sha256(Path(a.finite).read_bytes()).hexdigest() if a.finite else None,
        source_sha256={str(f):hashlib.sha256(f.read_bytes()).hexdigest() for f in [Path(__file__),*Path('benchmarks').glob('*.py')]})
    mf=a.output/'manifest.json'
    if mf.exists() and json.loads(mf.read_text())!=manifest:raise ValueError('resume manifest changed')
    mf.write_text(json.dumps(manifest,indent=2));outcomes=[]
    snapshot=a.output/'source'
    if not snapshot.exists():
        snapshot.mkdir();shutil.copy2(__file__,snapshot/Path(__file__).name)
        shutil.copytree('benchmarks',snapshot/'benchmarks',ignore=shutil.ignore_patterns('__pycache__'))
    # Child imports come from the frozen snapshot, even if development continues.
    a.output=a.output.resolve()
    snapshot=snapshot.resolve()
    for repeat in range(a.repeats):
        order=list(enumerate(configs))
        if repeat%2:order.reverse()
        for number,c in order:
            name=f'{number:03d}-r{repeat}';output=a.output/name;cfg=a.output/(name+'.json')
            cfg.write_text(json.dumps(asdict(c)))
            if (output/'result.json').exists():continue
            if output.exists():raise ValueError(f'failed prior case retained at {output}; use new campaign output')
            start=time.monotonic()
            with (a.output/(name+'.log')).open('w') as log:
                try:
                    binary=a.finite if c.backend=='finite' else a.native
                    if not binary:raise ValueError('--finite binary required')
                    r=subprocess.Popen([sys.executable,'-m','benchmarks.cold_search',str(cfg),str(output),'--native',binary],
                        stdout=log,stderr=subprocess.STDOUT,start_new_session=True,cwd=snapshot)
                    status=r.wait(timeout=600)
                except subprocess.TimeoutExpired:
                    os.killpg(r.pid,signal.SIGTERM)
                    try:r.wait(timeout=10)
                    except subprocess.TimeoutExpired:os.killpg(r.pid,signal.SIGKILL);r.wait()
                    status='timeout'
            item=dict(case=name,config=asdict(c),exit=status,wall_s=time.monotonic()-start)
            outcomes.append(item)
            with (a.output/'outcomes.jsonl').open('a') as f:f.write(json.dumps(item)+'\n')
            print(f'{name} {c.family} {c.backend} n={c.rows}: {status}',flush=True)
    print(f'Campaign complete: {a.output}',flush=True)


if __name__=='__main__':main()
