"""Real larger-memory encodings with fixed parameters, plus many-role formulas."""
from dataclasses import asdict
import importlib.util
import json
import math
from pathlib import Path
import subprocess
import sys
from functools import lru_cache
from benchmarks.cold_search import Config

root=Path('/mnt/c/src/defradb.rs/target/pir-cold-finite-frontier-v1')


def many_server_screen():
    path='/root/pir-cold-artifacts/finite-diffs/cost_calculations/costs_concrete.py'
    spec=importlib.util.spec_from_file_location('official_costs',path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    @lru_cache(None)
    def coefficient_count(total,degree,m):
        return sum((-1)**j*math.comb(m,j)*math.comb(total-j*(degree+1)+m-1,m-1)
                   for j in range(min(m,total//(degree+1))+1))
    # Extend the author's size-50 DP table with the exact bounded-composition
    # identity; cross-check its entire small slice before screening q=101.
    for m in range(2,8):
        for degree in range(1,8):
            for total in range(1,30):
                assert coefficient_count(total,degree,m)==int(module.count[total,degree,m])
    results=[]
    for n in (1024,16384,262144):
      for width in (32,196,2008):
       for servers,q in ((2,2),(4,5),(8,11),(16,17),(100,101)):
        best=None;by_budget={};lanes=math.ceil(width*8/math.floor(math.log2(q)));field_bits=math.ceil(math.log2(q))
        for m in range(2,30):
         storage=math.ceil(q**m*lanes*field_bits/8)*servers
         if storage>5<<30:continue
         for degree in range(1,q):
          for total in range(1,degree*m+1):
           if q==2 and total%2==0:continue
           if coefficient_count(total,degree,m)<n:continue
           count=sum(math.comb(m,i)*degree**i for i in range(min(total//servers,m)+1));download=math.ceil(count*lanes*field_bits/8)*servers
           candidate=dict(M=m,D=total,d=degree,q=q,roles=servers,encoded_total_bytes=storage,
               answer_total_bytes=download,upload_bytes=math.ceil(m*field_bits/8)*servers,
               passes_512MiB=storage<=512<<20,passes_128x_source=storage<=n*width*128)
           if best is None or (download,storage)<(best['answer_total_bytes'],best['encoded_total_bytes']):best=candidate
           for name,cap in (('512MiB',512<<20),('128x-source',n*width*128),('5GiB',5<<30)):
            old=by_budget.get(name)
            if storage<=cap and (old is None or (download,storage)<(old['answer_total_bytes'],old['encoded_total_bytes'])):by_budget[name]=candidate
        results.append(dict(records=n,record_bytes=width,servers=servers,best=best,best_by_memory_budget=by_budget))
    (root/'many-server-formulas.json').write_text(json.dumps(dict(cases=results,
        qualification='authors concrete homogeneous finite-difference cost formulas, t=1 no collusion; M<30 and all 1<=d<q,1<=D<=dm; exact bounded-composition identity cross-checked against author DP table; independent field lanes for packed records; theoretical bit-packed fields, not native JSON framing; candidate cost parameters, not a security proof, complete keyword search or measured CPU'),indent=2))


if __name__=='__main__':
    root.mkdir(parents=True,exist_ok=False);many_server_screen();matrix=[]
    for n,group,m,d in ((65536,6,17,5),(65536,16,16,5),(262144,6,22,5),(262144,8,21,5),(262144,16,18,5)):
        for backend in ('dense','finite'):
            matrix.append(asdict(Config(family='directory',backend=backend,rows=n,group=group,clients=8,key_layout='hashed',
                finite_m=m if backend=='finite' else 0,finite_d=d if backend=='finite' else 0)))
    (root/'matrix.json').write_text(json.dumps(dict(matrix=matrix),indent=2))
    subprocess.run([sys.executable,'run_cold_search.py','--matrix-from',str(root/'matrix.json'),'--output',str(root/'campaign'),
        '--native','/root/pir-ramen-build/release/examples/native_store','--finite','/root/pir-cold-artifacts/bin/finite-store-frontier','--repeats','5'],check=True)
