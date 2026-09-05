"""Complete tag-query batch kernel with independent CSPRNG selectors.

The driver is the trusted synthetic client/oracle, not a proxy deployment.
Fresh OS-client RSS is qualified in cold_search separately. Each of two native
replicas receives only its own selectors. Public batch size and rounds are fixed.
"""
import argparse
import json
import os
from pathlib import Path
import time
from benchmarks.cold_search import BinaryTags
from benchmarks.native_memory import NativeEndpoint


def run(binary,n,batch,pad=False):
    data=[[i//2*2654435761+17,i,i%4,'ab'*32] for i in range(n)]
    index=BinaryTags(data,16);servers=[];samples=[]
    stored_width=((index.width+2047)//2048)*2048 if pad else index.width
    try:
        for i in range(2):
            e=NativeEndpoint(binary,f'batch-{i}');servers.append(e);e.call('publish',[r.ljust(stored_width,b'\0') for r in index.rows])
        queries=[data[(i*34)%n][0]+(i%4==0) for i in range(batch)]
        recovered=[[] for _ in queries];start=time.process_time_ns();wall=time.perf_counter_ns()
        for page in range(index.pages):
            first=[];second=[];width=(len(index.rows)+7)//8
            for key in queries:
                at=index.bucket(key)*index.pages+page;one=os.urandom(width)
                first.append(one);second.append((int.from_bytes(one,'little')^(1<<at)).to_bytes(width,'little'))
            replies=[e.call('batch-dense',q) for e,q in zip(servers,(first,second))]
            for i,(a,b) in enumerate(zip(*replies)):
                recovered[i].append((int.from_bytes(a,'little')^int.from_bytes(b,'little')).to_bytes(stored_width,'little')[:index.width])
        for i,key in enumerate(queries):
            class Retrieved:
                def read(self,address):return recovered[i][address-index.bucket(key)*index.pages]
            if sorted(index.view().query(Retrieved(),key))!=sorted(r for r in data if r[0]==key):raise AssertionError('complete batch answer')
        client_cpu=(time.process_time_ns()-start)/1e6;wall=(time.perf_counter_ns()-wall)/1e6
    finally:roles=[e.close() for e in servers]
    service=sum(p['cpu_ms'] for r in roles for p in r['phases'] if p['phase']=='batch-dense')
    return dict(rows=n,batch=batch,correct=True,server_batch_cpu_ms=service,server_cpu_per_answer_ms=service/batch,
        client_driver_cpu_ms=client_cpu,wall_ms=wall,logical_index_bytes=len(index.rows)*stored_width,padded_2048=pad,roles=roles,
        qualification=__doc__)


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--binary',required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
    a.output.mkdir(parents=True,exist_ok=False)
    for repeat in range(5):
        cases=[(n,b,pad) for n in (1024,16384) for b in (1,8,32,128) for pad in (False,True)]
        if repeat%2:cases.reverse()
        for n,b,pad in cases:(a.output/f'n{n}-b{b}-pad{int(pad)}-r{repeat}.json').write_text(json.dumps(run(a.binary,n,b,pad),indent=2))
