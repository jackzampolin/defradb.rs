"""Actual Dense base/delta accesses, tombstones, compaction and proof placement.

Maintenance is public-writer work. This isolated controller is the trusted client
and correctness oracle; it is not a deployed proxy. SHA-256 proof topology is a
separate synthetic experiment, never a canonical Mizu root.
"""
import argparse
import hashlib
import json
from pathlib import Path
import random
import struct
import time
from benchmarks.cold_search import BinaryTags
from benchmarks.private_memory import Memory


def base_delta(binary,threshold):
    source=[[i*1009+17,i,i%4,'ab'*32] for i in range(256)]
    live={r[0]:r for r in source};base=BinaryTags(source,16);mem=Memory(base.rows,'dense-native',binary)
    # Fixed-size operation table always read in full privately, even empty.
    width=1+8+16+32;zero=bytes(width);delta=Memory([zero]*threshold,'dense-native',binary)
    operations=[];samples=[];generations=[];global_cpu=0
    try:
        for step in range(32):
            start=time.process_time_ns();key=source[step][0]
            if step%3==0:
                live.pop(key);blob=struct.pack('<BQ',2,key)+bytes(width-9)
            else:
                if step%3==1:key=10_000_000+step
                row=[key,step+1000,(step+1000)%4,hashlib.shake_256(str(step).encode()).digest(32).hex()]
                live[key]=row;blob=struct.pack('<BQQQ',1,key,row[0],row[1])+bytes.fromhex(row[3])
            delta.write(len(operations),blob);operations.append(blob)
            mutation_client_cpu=(time.process_time_ns()-start)/1e6
            for query in (key,source[(step*7)%len(source)][0],key+1):
                start=time.process_time_ns();before=mem.wire();dbefore=delta.wire()
                found={r[0]:r for r in base.view().query(mem,query)}
                for at in range(threshold):
                    op=delta.read(at);kind=op[0];k=struct.unpack_from('<Q',op,1)[0]
                    if k!=query:continue
                    if kind==2:found.pop(k,None)
                    elif kind==1:
                        tag,identifier=struct.unpack_from('<QQ',op,9);found[k]=[tag,identifier,identifier%4,op[25:].hex()]
                expected=[live[query]] if query in live else []
                if list(found.values())!=expected:raise AssertionError('base/delta stale/deleted/inserted result')
                samples.append(dict(correct=True,client_cpu_ms=(time.process_time_ns()-start)/1e6,
                    wire=[a-b+c-d for a,b,c,d in zip(mem.wire(),before,delta.wire(),dbefore)],delta_private_reads=threshold,
                    mutation_client_cpu_ms=mutation_client_cpu))
            if len(operations)==threshold:
                generations.extend((mem.close(),delta.close()));start=time.process_time_ns()
                base=BinaryTags(list(live.values()),16);mem=Memory(base.rows,'dense-native',binary)
                delta=Memory([zero]*threshold,'dense-native',binary);operations=[]
                global_cpu+=(time.process_time_ns()-start)/1e6
    finally:generations.extend((mem.close(),delta.close()))
    return dict(threshold=threshold,correct=True,updates=32,answers=len(samples),samples=samples,
        role_reports=generations,aggregate_server_process_cpu_ms=sum(g['server_cpu_ms'] for g in generations),
        controller_rebuild_cpu_ms=global_cpu,
        qualification='real private base plus fixed delta, public writes, exact tombstones/upserts; all role lifecycle CPU charged; controller is trusted synthetic client; no SinglePass reused hints')


def proof_placement(n,group,scattered):
    depth=20;rng=random.Random(1);positions=list(range(n))
    if scattered:rng.shuffle(positions)
    leaves={positions[i]:hashlib.sha256(struct.pack('<QQ',i*1009+17,(i+1)*1009+17)).digest() for i in range(n)}
    default=[hashlib.sha256(b'empty').digest()];levels=[leaves]
    for level in range(depth):
        default.append(hashlib.sha256(default[-1]*4).digest())
        parents={at//4 for at in levels[-1]}
        levels.append({p:hashlib.sha256(b''.join(levels[-1].get(4*p+j,default[level]) for j in range(4))).digest() for p in parents})
    root=levels[-1][0];results=[]
    for start in range(0,min(n,256),group):
        chosen=positions[start:start+group];known={at:leaves[at] for at in chosen};proof=[]
        for level in range(depth):
            parents={at//4 for at in known};next_known={}
            for parent in parents:
                children=[]
                for j in range(4):
                    at=4*parent+j
                    if at in known:children.append(known[at])
                    else:
                        value=levels[level].get(at,default[level]);children.append(value);proof.append((level,at,value))
                next_known[parent]=hashlib.sha256(b''.join(children)).digest()
            known=next_known
        if known!={0:root}:raise AssertionError('multiproof root')
        results.append(dict(leaves=len(chosen),independent_sibling_bytes=len(chosen)*depth*3*32,
            multiproof_sibling_bytes=len(proof)*32,multiproof_addressed_bytes=len(proof)*(32+1+8)))
    return dict(rows=n,group=group,scattered=scattered,correct=True,samples=results,
        qualification='actual SHA256 quaternary depth-20 multiproof root reconstruction; synthetic topology, not Poseidon Mizu witness; groups padded to public worst size in any serving layout')


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--binary',required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
    a.output.mkdir(parents=True,exist_ok=False)
    for threshold in (4,8,16):
        (a.output/f'delta-{threshold}.json').write_text(json.dumps(base_delta(a.binary,threshold),indent=2))
    proofs=[proof_placement(n,g,s) for n in (1024,16384) for g in (1,4,16,64) for s in (False,True)]
    (a.output/'proof-placement.json').write_text(json.dumps(proofs,indent=2))
