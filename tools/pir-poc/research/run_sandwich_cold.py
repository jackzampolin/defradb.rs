#!/usr/bin/env python3
"""Complete tag-search diagnostic using the author's HTTP GPU service.

Each page uses a fresh native client, conservatively repeating key generation
within a multi-page logical search. Metrics distinguish CPU and GPU timers.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import resource
import socket
import subprocess
import time
import urllib.request
from benchmarks.cold_search import BinaryTags


def server_cpu(pid):
    fields=Path(f'/proc/{pid}/stat').read_text().split()
    return (int(fields[13])+int(fields[14]))*1000/os.sysconf('SC_CLK_TCK')


def main():
    p=argparse.ArgumentParser();p.add_argument('--binaries',type=Path,required=True);p.add_argument('--output',type=Path,required=True)
    p.add_argument('--rows',type=int,default=1024);p.add_argument('--payload',type=int,default=32);p.add_argument('--clients',type=int,default=8)
    a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False)
    data=[[i//2*2654435761+17,i,i%4,hashlib.shake_256(str(i).encode()).digest(a.payload).hex()] for i in range(a.rows)]
    index=BinaryTags(data,16)
    # The author's HE layout pads internally to 2048 columns. Explicit file
    # padding makes the corresponding Dense control unambiguous.
    width=((index.width+2047)//2048)*2048
    db=a.output/'database.bin';db.write_bytes(b''.join(row.ljust(width,b'\0') for row in index.rows))
    with socket.socket() as sock:sock.bind(('127.0.0.1',0));port=sock.getsockname()[1]
    address=f'127.0.0.1:{port}';log=(a.output/'server.log').open('w')
    env=dict(os.environ,LD_LIBRARY_PATH='/usr/local/cuda/lib64:/usr/lib/wsl/lib',VERBOSE='1')
    server=subprocess.Popen([str(a.binaries/'pir-serve'),'--db',str(db),'--num-items',str(len(index.rows)),
        '--item-size-bits',str(width*8),'--listen',address],env=env,stdout=log,stderr=subprocess.STDOUT)
    samples=[]
    try:
        deadline=time.monotonic()+120
        while True:
            if server.poll() is not None:raise RuntimeError('server exited; see server.log')
            try:
                with urllib.request.urlopen(f'http://{address}/api/info',timeout=1) as response:metadata=response.read()
                break
            except OSError:
                if time.monotonic()>deadline:raise TimeoutError('server startup')
                time.sleep(.1)
        publication_cpu=server_cpu(server.pid)
        for client in range(a.clients):
            key=(client*17%(a.rows//2))*2654435761+17
            if client%4==0:key+=1
            wall=time.perf_counter_ns();start_cpu=server_cpu(server.pid);own=time.process_time_ns();before=resource.getrusage(resource.RUSAGE_CHILDREN)
            answers=[];upload=download=0
            for page in range(index.pages):
                at=index.bucket(key)*index.pages+page;output=a.output/f'client-{client}-page-{page}.bin'
                result=subprocess.run([str(a.binaries/'pir-query'),'--server',address,'--row',str(at),'--output',str(output)],
                    env=env,capture_output=True,text=True,timeout=60)
                (a.output/f'client-{client}-page-{page}.log').write_text(result.stdout+result.stderr)
                if result.returncode:raise RuntimeError('client failed')
                raw=output.read_bytes()
                if raw[:index.width]!=index.rows[at]:raise AssertionError('private page recovery mismatch')
                answers.append(raw[:index.width])
                match=re.search(r'Query generated.*\((\d+) bytes\)',result.stderr)
                if not match:raise ValueError('missing upload measurement')
                upload+=int(match[1])
                match=re.search(r'Response received.*\((\d+) bytes\)',result.stderr)
                if not match:raise ValueError('missing response measurement')
                download+=int(match[1])+len(metadata)
            after=resource.getrusage(resource.RUSAGE_CHILDREN)
            child_cpu=((after.ru_utime+after.ru_stime)-(before.ru_utime+before.ru_stime))*1000
            # Verify complete filtering with a memory view containing only the
            # privately recovered pages, not the original database.
            class Retrieved:
                def read(self,address):return answers[address-index.bucket(key)*index.pages]
            actual=index.view().query(Retrieved(),key)
            expected=[r for r in data if r[0]==key]
            if sorted(actual)!=sorted(expected):raise AssertionError('complete search mismatch')
            samples.append(dict(client=client,correct=True,matches=len(expected),private_pages=index.pages,
                server_cpu_ms=server_cpu(server.pid)-start_cpu,
                native_client_process_cpu_ms=child_cpu,controller_cpu_ms=(time.process_time_ns()-own)/1e6,
                wall_ms=(time.perf_counter_ns()-wall)/1e6,upload_payload_bytes=upload,download_payload_bytes=download,
                wire_cap_pass_with_4KiB_header_allowance=max(upload,download)+4096*index.pages<=1<<20))
        report=dict(config=vars(a)|{'binaries':str(a.binaries),'output':str(a.output)},samples=samples,
            publication_server_cpu_ms=publication_cpu,physical_index_bytes=db.stat().st_size,record_bytes=width,
            source_record_bytes=a.payload,index_records=len(index.rows),metadata_bytes=len(metadata),
            qualification='synthetic tag search; fresh native process per continuation page; HTTP payload sizes plus conservative header allowance; server CPU has OS tick resolution; GPU timing in raw server log; no production proofs')
        (a.output/'result.json').write_text(json.dumps(report,indent=2))
    finally:
        server.terminate()
        try:server.wait(10)
        except subprocess.TimeoutExpired:server.kill();server.wait()
        log.close()


if __name__=='__main__':main()
