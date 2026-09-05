#!/usr/bin/env python3
"""Concrete arithmetic/storage gates; never reported as complete PIR timings."""
import argparse
import json
import math
import os
from pathlib import Path
import resource
import time
os.environ['OPENBLAS_NUM_THREADS']='1'
import numpy as np


def crt_tables(m,group,cap):
    # Enough pairwise-coprime moduli to reconstruct the exact lifted product.
    primes=[];product=1;candidate=2;bound=m*((1<<32)-1)**2
    while product<=bound:
        if all(candidate%p for p in primes if p*p<=candidate):primes.append(candidate);product*=candidate
        candidate+=1
    groups=math.ceil(m/group)
    size=sum(m*groups*p**group*8 for p in primes)
    result=dict(dimension=m,group=group,primes=primes,table_bytes=size)
    if size>cap:return result|{'status':'storage-preflight-rejected'}
    rng=np.random.default_rng(1701);a=rng.integers(0,1<<32,(m,m),dtype=np.uint64)
    tables=[];start=time.process_time_ns()
    for p in primes:
        choices=np.arange(p**group,dtype=np.uint64)
        digits=np.stack([(choices//p**j)%p for j in range(group)])
        padded=np.pad(a%p,((0,0),(0,groups*group-m)))
        tables.append([(padded[:,at*group:(at+1)*group]@digits)%p for at in range(groups)])
    result['build_cpu_ms']=(time.process_time_ns()-start)/1e6
    coeff=[(product//p)*pow(product//p,-1,p) for p in primes]
    samples=[]
    for _ in range(8):
        v=rng.integers(0,1<<32,m,dtype=np.uint64)
        start=time.process_time_ns();expected=(a@v)&np.uint64((1<<32)-1)
        dense=(time.process_time_ns()-start)/1e6
        start=time.process_time_ns();residues=[]
        for p,blocks in zip(primes,tables):
            vector=np.pad(v%p,(0,groups*group-m));res=np.zeros(m,dtype=np.uint64)
            for at,table in enumerate(blocks):
                code=sum(int(vector[at*group+j])*p**j for j in range(group))
                res+=table[:,code]
            residues.append(res%p)
        actual=[(sum(int(r[i])*c for r,c in zip(residues,coeff))%product)% (1<<32) for i in range(m)]
        elapsed=(time.process_time_ns()-start)/1e6
        if actual!=expected.tolist():raise AssertionError('CRT kernel mismatch')
        samples.append(dict(dense_cpu_ms=dense,table_crt_cpu_ms=elapsed,correct=True))
    return result|dict(status='kernel-verified',samples=samples,
        qualification='one-level CRT lookup kernel, not the full two-level Williams algorithm or complete DEPIR')


def main():
    p=argparse.ArgumentParser();p.add_argument('--output',type=Path,required=True);p.add_argument('--kernel',action='store_true');a=p.parse_args()
    a.output.mkdir(parents=True,exist_ok=False)
    hints=[]
    for rows in (1024,16384,65536,262144):
      for width in (32,96,2008):
       bits=rows*width*8;m=math.ceil(math.sqrt(bits/math.ceil(math.log2(bits))))
       for secret in (512,1024,1400):
        for modulus_bits in (32,64):
         h=math.ceil(m*secret*modulus_bits/8)
         hints.append(dict(rows=rows,row_bytes=width,secret_dimension=secret,modulus_bits=modulus_bits,m=m,
             answer_H_bytes=h,download_cap_pass_for_H_alone=h<=1<<20))
    (a.output/'barely-H-frontier.json').write_text(json.dumps(dict(cases=hints,
        qualification='candidate parameter byte calculation for Fig. 2; not security-validated parameters or a universal lower bound'),indent=2))
    gates=[
      dict(experiment=11,variant='CHOO-SS published 1GB',online_communication_KB=128624,cap_note='far beyond even combined 2MiB upload/download caps'),
      dict(experiment=11,variant='CHOO-SS published 8GB',online_communication_KB=263472,cap_note='far beyond even combined 2MiB upload/download caps'),
      dict(experiment=14,variant='2026 DEPIR smallest reported',server_storage_GB=.39,batch_items=5461,batch_seconds=10,
           qualification='published batch, not measured singleton; no full code artifact found'),
      dict(experiment=15,variant='secret-key representative bounded-query setting',encoded_GB=155,source_GB=36.6,
           qualification='published encoding estimates; exceeds local RAM, artifact URL/API returns 404'),
      dict(experiment=24,variant='SGX',available='sgx' in Path('/proc/cpuinfo').read_text().split(),
           qualification='local AMD Ryzen platform; no SGX execution attempted without hardware')]
    (a.output/'gates.json').write_text(json.dumps(gates,indent=2))
    if a.kernel:
        results=[crt_tables(m,g,128<<20) for m in (32,128,256) for g in (1,2,3)]
        (a.output/'crt-kernels.json').write_text(json.dumps(results,indent=2))


if __name__=='__main__':main()
