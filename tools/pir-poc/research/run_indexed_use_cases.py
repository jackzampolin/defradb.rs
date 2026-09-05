"""Matched application-shaped indexed-Dense campaign, including client reuse.

Run from this directory in Linux. All records are synthetic fixed projections;
the nullifier lane alone invokes the real Poseidon witness builder/verifier.
"""
import argparse
from dataclasses import asdict
import json
from pathlib import Path
import subprocess
import sys
import time
from benchmarks.cold_search import Config


def matrix(corpus,bridge):
    # name, payload bytes, matches/key, smaller and larger resident row counts.
    workloads=[('mizu-routing',804,4,(4096,65536)),
               ('shinzo-logs',548,4,(4096,65536)),
               ('shinzo-receipt',184,1,(1024,10000)),
               ('defra-document',256,1,(4096,65536)),
               ('defra-secondary',120,16,(4096,65536))]
    for name,width,fanout,sizes in workloads:
        for n in sizes:
            base=dict(workload=name,rows=n,row_bytes=width,fanout=fanout,
                      clients=2,key_layout='hashed',field_bits=64)
            for family,groups in [('directory',(1,4,16,64)),('xor',(16,)),('binary-tags',(16,))]:
                for group in groups:yield Config(**base,family=family,group=group)
            if n==sizes[0]:
                for backend in ('dense','singlepass'):
                    for q in (1,256):
                        if backend=='dense' and q==1:continue
                        for group in (1,16):
                            yield Config(**base,family='directory',group=group,backend=backend,queries_per_client=q)
    # Global unknown-partition lookups have the same private index, larger scope.
    for name,width,fanout in [('global-receipt',184,1),('global-document',256,1),('global-secondary',120,64)]:
        for family,group in [('directory',1),('directory',16),('xor',16),('binary-tags',16)]:
            yield Config(workload=name,rows=32768,row_bytes=width,fanout=fanout,clients=2,
                         key_layout='hashed',field_bits=64,family=family,group=group)
    # A hot value deliberately stresses complete-result padding, not only uniform tags.
    for family,group in [('directory',1),('directory',16),('xor',16),('binary-tags',16)]:
        yield Config(workload='skewed-secondary',rows=16384,row_bytes=120,fanout=4,hot_records=2048,
                     clients=4,key_layout='hashed',field_bits=64,family=family,group=group)
    for backend in ('dense','singlepass'):
        for group in (1,4,16):
            for q in (1,64):
                yield Config(workload='mizu-canonical-witness',family='canonical-directory',
                             rows=8192,row_bytes=2008,fanout=1,group=group,clients=2,
                             backend=backend,queries_per_client=q,canonical_file=str(corpus),verifier=str(bridge))
    # All three alert products share the same hash-bucket presence primitive.
    for n in (1024,16384):
        for q in (1,256):
            for family in ('packed-presence','directory-presence','public-presence'):
                yield Config(workload='shared-epoch-alerts',family=family,rows=n,row_bytes=8,
                             fanout=1,group=16,clients=4,queries_per_client=q,key_layout='hashed')


def main():
    p=argparse.ArgumentParser();p.add_argument('--output',type=Path,required=True)
    p.add_argument('--native',type=Path,required=True);p.add_argument('--bridge',type=Path,required=True)
    p.add_argument('--repeats',type=int,default=5)
    p.add_argument('--after-log',type=Path,help='Wait for a prior campaign completion marker to avoid overlapping timed runs')
    p.add_argument('--canonical-corpus',type=Path)
    p.add_argument('--profile',choices=('main','large-warm','witness-warm'),default='main');a=p.parse_args()
    if a.after_log:
        deadline=time.monotonic()+3600
        while 'Campaign complete:' not in a.after_log.read_text():
            if time.monotonic()>deadline:raise TimeoutError('prior campaign did not finish')
            time.sleep(5)
    a.output=a.output.resolve();a.native=a.native.resolve();a.bridge=a.bridge.resolve()
    a.output.mkdir(parents=True,exist_ok=False)
    if a.profile=='witness-warm':
        if not a.canonical_corpus:raise ValueError('--canonical-corpus required')
        corpus=a.output/'canonical-8192.json';corpus.write_bytes(a.canonical_corpus.read_bytes())
        configs=[Config(workload='mizu-canonical-witness',family='canonical-directory',rows=8192,
                        row_bytes=2008,fanout=1,group=1,clients=2,backend=backend,queries_per_client=1024,
                        canonical_file=str(corpus),verifier=str(a.bridge)) for backend in ('dense','singlepass')]
        launch(a,configs);return
    if a.profile=='large-warm':
        configs=[]
        for name,width,fanout,n in [('mizu-routing',804,4,65536),('shinzo-logs',548,4,65536),
                                    ('shinzo-receipt',184,1,10000),('defra-document',256,1,65536),
                                    ('defra-secondary',120,16,65536)]:
            for backend in ('dense','singlepass'):
                for group in (1,16):
                    configs.append(Config(workload=name,rows=n,row_bytes=width,fanout=fanout,clients=2,
                                          key_layout='hashed',field_bits=64,family='directory',group=group,
                                          backend=backend,queries_per_client=1024))
        launch(a,configs);return
    corpus=a.output/'canonical-8192.json'
    subprocess.run([str(a.bridge),'build','8192',str(corpus)],check=True)
    data=json.loads(corpus.read_text());r=data['data'][1];bad=bytearray.fromhex(r[3]);bad[-1]^=1
    checks=[]
    for payload,root,success in [(r[3],data['root'],True),(bad.hex(),data['root'],False),(r[3],'00'*32,False)]:
        result=subprocess.run([str(a.bridge),'verify',str(r[0]),root,payload],capture_output=True)
        assert (result.returncode==0)==success,'canonical verification check'
        checks.append(dict(expected_valid=success,returncode=result.returncode))
    (a.output/'canonical-checks.json').write_text(json.dumps(checks,indent=2))
    configs=list(matrix(corpus,a.bridge))
    launch(a,configs)


def launch(a,configs):
    (a.output/'matrix.json').write_text(json.dumps(dict(matrix=[asdict(c) for c in configs]),indent=2))
    print(f'{len(configs)} configurations, {a.repeats} repetitions',flush=True)
    subprocess.run([sys.executable,'run_cold_search.py','--matrix-from',str(a.output/'matrix.json'),
                    '--output',str(a.output/'campaign'),'--native',str(a.native),'--repeats',str(a.repeats)],check=True)


if __name__=='__main__':main()
