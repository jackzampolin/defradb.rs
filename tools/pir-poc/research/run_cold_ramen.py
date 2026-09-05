#!/usr/bin/env python3
"""Persistent three-party Ramen serving independent cold predicate clients."""
import argparse
import hashlib
import io
import json
import multiprocessing as mp
from multiprocessing.reduction import DupFd
import os
from pathlib import Path
import time
from types import SimpleNamespace
from benchmarks.cold_search import BinaryTags,BinaryPatricia,fresh_process_stats
from benchmarks.private_memory import Ramen
from benchmarks.transport import socket_pair,send,receive,process_stats


def query_client(descriptors,control,view,width,key):
    control.settimeout(120)
    handles=[]
    try:
        processes=[]
        for writer,reader in descriptors:
            a=os.fdopen(writer.detach(),'w',buffering=1);b=os.fdopen(reader.detach(),'r',buffering=1)
            handles.extend((a,b));processes.append(SimpleNamespace(stdin=a,stdout=b,stderr=io.StringIO('replica disconnected')))
        memory=object.__new__(Ramen);memory.width=width;memory.limbs=(width+14)//15
        memory.processes=processes;memory.phases=[[] for _ in processes];memory.sent=memory.received=0
        memory.read=memory.access
        start=time.process_time_ns();wall=time.perf_counter_ns()
        answer=view.query(memory,key)
        send(control,dict(answer=answer,client_cpu_ms=(time.process_time_ns()-start)/1e6,
            wall_ms=(time.perf_counter_ns()-wall)/1e6,upload_bytes=memory.sent,download_bytes=memory.received,
            phases=memory.phases,client_process=fresh_process_stats()))
    except Exception as exc:send(control,dict(error=str(exc)))
    finally:
        for handle in handles:handle.close()
        control.close()


def main():
    p=argparse.ArgumentParser();p.add_argument('--binary',required=True);p.add_argument('--output',type=Path,required=True)
    p.add_argument('--rows',type=int,default=16);p.add_argument('--clients',type=int,default=4)
    p.add_argument('--family',choices=['tags','patricia'],default='tags');a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False)
    data=[[i//2*2654435761+17,i,i%4,'ac'*8] for i in range(a.rows)]
    index=(BinaryTags if a.family=='tags' else BinaryPatricia)(data,2)
    start=time.process_time_ns();memory=Ramen(index.rows,a.binary);publication_client_cpu=(time.process_time_ns()-start)/1e6
    samples=[]
    try:
        for number in range(a.clients):
            key=(number*7%(a.rows//2))*2654435761+17
            if number%4==0:key+=1
            descriptors=[(DupFd(p.stdin.fileno()),DupFd(p.stdout.fileno())) for p in memory.processes]
            parent,child=socket_pair();proc=mp.get_context('spawn').Process(target=query_client,args=(descriptors,child,index.view(),index.width,key))
            proc.start();child.close()
            try:
                result,_=receive(parent);proc.join(10)
                if proc.exitcode!=0 or 'error' in result:raise RuntimeError(result)
                if sorted(result.pop('answer'))!=sorted(r for r in data if r[0]==key):raise AssertionError('complete query mismatch')
                result.update(client=number,pid=proc.pid,correct=True)
                memory.sent+=result['upload_bytes'];memory.received+=result['download_bytes']
                samples.append(result)
            finally:
                if proc.is_alive():proc.terminate();proc.join(10)
                parent.close()
    finally:roles=memory.close()
    for sample in samples:
        sample['server_phase_cpu_ms']=sum(p['cpu_ms'] for role in sample['phases'] for p in role)
        sample['client_caps_pass']=max(sample['upload_bytes'],sample['download_bytes'])<=1<<20 and sample['client_process']['peak_rss_bytes']<=128<<20 and sample['client_cpu_ms']<=1000
    (a.output/'result.json').write_text(json.dumps(dict(samples=samples,roles=roles,publication_client_cpu_ms=publication_client_cpu,
        rows=a.rows,family=a.family,record_bytes=index.width,field_limbs=memory.limbs,index_records=len(index.rows),
        qualification='actual persistent three-party Ramen with fresh clients; scalar-field values; current response phase timers exclude final framing; full lifecycle role CPU also reported; synthetic complete tag search'),indent=2))


if __name__=='__main__':main()
