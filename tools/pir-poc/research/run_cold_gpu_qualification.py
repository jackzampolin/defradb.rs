"""Repeat isolated complete GPU clients, including 128 arrivals and spacing."""
import argparse
import os
from pathlib import Path
import subprocess
import sys
import time


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--wait-pid',type=int);a=p.parse_args()
    if a.wait_pid:
        while Path(f'/proc/{a.wait_pid}').exists():time.sleep(2)
    root=Path('/root/pir-cold-artifacts');target=Path('/mnt/c/src/defradb.rs/target')
    binary=root/'zippir/build/zippir'
    if binary.exists():
        for n in (65536,1048576):
            with (root/f'zippir-full-v2-{n}.log').open('w') as log:
                try:
                    r=subprocess.run([str(binary),'--N',str(n),'--output',str(target/f'pir-cold-zippir-{n}.json')],
                        env=dict(os.environ,OMP_NUM_THREADS='1'),stdout=log,stderr=subprocess.STDOUT,timeout=180)
                    print('zippir',n,r.returncode,flush=True)
                except subprocess.TimeoutExpired:print('zippir timeout',n,flush=True)
    for repeat in range(5):
        cases=[(b,w,0) for b in (1,8,32,128) for w in (0,5,20)]+[(32,5,5)]
        if repeat%2:cases.reverse()
        for batch,window,spacing in cases:
            name=f'pir-cold-gpu-v2-b{batch}-w{window}-s{spacing}-r{repeat}'
            with (root/(name+'.log')).open('w') as log:
                r=subprocess.run([sys.executable,'run_sandwich_batch.py','--binaries',str(root/'sandwichpir/target/release'),
                    '--output',str(target/name),'--clients',str(batch),'--window',str(window),'--spacing',str(spacing)],
                    stdout=log,stderr=subprocess.STDOUT,timeout=300)
            print(name,r.returncode,flush=True)
