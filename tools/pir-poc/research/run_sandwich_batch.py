"""Independent native clients over HTTP; public GPU batching windows.

Unlike instantiating several native clients in one process, separate processes
do not share the upstream global secret-key state. Per-child CPU/RSS come from
GNU time; total server CPU is sampled once per group, not charged N times.
"""
import argparse
from concurrent.futures import ProcessPoolExecutor
import multiprocessing as mp
import hashlib
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import time
import urllib.request
from benchmarks.cold_search import BinaryTags,fresh_process_stats
from run_sandwich_cold import server_cpu


def independent_query(job):
    a,address,env,client,key,metadata_size=job
    time.sleep(client*a.spacing/1000)
    wall=time.perf_counter_ns()
    with urllib.request.urlopen(f'http://{address}/cold-meta.json',timeout=10) as r:nav=r.read()
    index=object.__new__(BinaryTags);index.__dict__=json.loads(nav)
    answers=[];cpu=0;peak=0;up=0;down=len(nav)
    for page in range(index.pages):
        at=index.bucket(key)*index.pages+page;prefix=a.output/f'c{client}-p{page}'
        result=subprocess.run(['/usr/bin/time','-f','%U %S %M','-o',str(prefix)+'.time',str(a.binaries/'pir-query'),
            '--server',address,'--row',str(at),'--output',str(prefix)+'.bin'],env=env,capture_output=True,text=True,timeout=120)
        Path(str(prefix)+'.log').write_text(result.stdout+result.stderr)
        if result.returncode:raise RuntimeError('native client failed')
        user,system,rss=map(float,Path(str(prefix)+'.time').read_text().split());cpu+=(user+system)*1000;peak=max(peak,rss*1024)
        answers.append(Path(str(prefix)+'.bin').read_bytes()[:index.width])
        up+=int(re.search(r'Query generated.*\((\d+) bytes\)',result.stderr)[1])
        down+=int(re.search(r'Response received.*\((\d+) bytes\)',result.stderr)[1])+metadata_size
    class Retrieved:
        def read(self,address):return answers[address-index.bucket(key)*index.pages]
    answer=index.view().query(Retrieved(),key)
    own=fresh_process_stats()
    # GNU time prints hundredths for each user/system counter. Retain a
    # conservative interval instead of treating rounded zeros as free clients.
    upper=cpu+20*index.pages+own['cpu_ms'];memory_bound=peak+own['peak_rss_bytes']
    return dict(answer=answer,pid=os.getpid(),client_cpu_ms=cpu+own['cpu_ms'],client_cpu_upper_bound_ms=upper,
        client_peak_rss_bytes=memory_bound,client_wrapper=own,wall_ms=(time.perf_counter_ns()-wall)/1e6,
        upload_bytes=up,download_bytes=down,private_pages=index.pages,
        caps_pass=memory_bound<=128<<20 and upper<=1000 and max(up,down)+(index.pages+1)*4096<=1<<20)


def run(a):
    a.output.mkdir(parents=True,exist_ok=False)
    data=[[i//2*2654435761+17,i,i%4,hashlib.shake_256(str(i).encode()).digest(a.payload).hex()] for i in range(a.rows)]
    index=BinaryTags(data,16);width=((index.width+2047)//2048)*2048
    (a.output/'cold-meta.json').write_text(json.dumps(vars(index.view())))
    db=a.output/'database.bin';db.write_bytes(b''.join(r.ljust(width,b'\0') for r in index.rows))
    with socket.socket() as sock:sock.bind(('127.0.0.1',0));port=sock.getsockname()[1]
    address=f'127.0.0.1:{port}'
    env=dict(os.environ,LD_LIBRARY_PATH='/usr/local/cuda/lib64:/usr/lib/wsl/lib',VERBOSE='1',COLD_BATCH_WINDOW_MS=str(a.window))
    with (a.output/'server.log').open('w') as log:
        server=subprocess.Popen([str(a.binaries/'pir-serve'),'--db',str(db),'--num-items',str(len(index.rows)),
            '--item-size-bits',str(width*8),'--listen',address,'--verbose','--web-dir',str(a.output)],env=env,stdout=log,stderr=subprocess.STDOUT)
        try:
            deadline=time.monotonic()+120
            while True:
                if server.poll() is not None:raise RuntimeError('server startup failed')
                try:
                    with urllib.request.urlopen(f'http://{address}/api/info',timeout=1) as r:metadata=r.read()
                    break
                except OSError:
                    if time.monotonic()>deadline:raise TimeoutError('startup')
                    time.sleep(.1)
            publication=server_cpu(server.pid)
            keys=[(client*17%(a.rows//2))*2654435761+17+(client%4==0) for client in range(a.clients)]
            start=server_cpu(server.pid);wall=time.perf_counter_ns()
            with ProcessPoolExecutor(max_workers=a.clients,mp_context=mp.get_context('spawn'),max_tasks_per_child=1) as pool:
                samples=list(pool.map(independent_query,[(a,address,env,i,k,len(metadata)) for i,k in enumerate(keys)]))
            if len({s['pid'] for s in samples})!=a.clients:raise AssertionError('client process reuse')
            for key,sample in zip(keys,samples):
                if sorted(sample.pop('answer'))!=sorted(r for r in data if r[0]==key):raise AssertionError('complete result')
                sample['correct']=True
            report=dict(config={k:str(v) if isinstance(v,Path) else v for k,v in vars(a).items()},samples=samples,
                publication_server_cpu_ms=publication,batch_server_cpu_ms=server_cpu(server.pid)-start,
                batch_wall_ms=(time.perf_counter_ns()-wall)/1e6,physical_index_bytes=db.stat().st_size,
                qualification='complete synthetic tag search; fresh isolated Python client fetches public navigation over HTTP, then fresh native process per page; no source records given to clients; GPU timers and actual batch sizes in server log; CPU counters have 10ms resolution, client CPU upper bounds retained; payload wire plus 4KiB header allowance')
            (a.output/'result.json').write_text(json.dumps(report,indent=2))
        finally:
            server.terminate()
            try:server.wait(10)
            except subprocess.TimeoutExpired:server.kill();server.wait()


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--binaries',type=Path,required=True);p.add_argument('--output',type=Path,required=True)
    p.add_argument('--rows',type=int,default=1024);p.add_argument('--payload',type=int,default=32)
    p.add_argument('--clients',type=int,default=8);p.add_argument('--window',type=int,default=5);p.add_argument('--spacing',type=int,default=0)
    run(p.parse_args())
