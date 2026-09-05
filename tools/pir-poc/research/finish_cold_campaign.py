"""Bounded sequential tail of the cold experiment queue; retain every failure."""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time


def main():
    p=argparse.ArgumentParser();p.add_argument('--wait-pid',type=int);a=p.parse_args()
    if a.wait_pid:
        while Path(f'/proc/{a.wait_pid}').exists():time.sleep(2)
    root=Path('/root/pir-cold-artifacts');target=Path('/mnt/c/src/defradb.rs/target')
    research=Path(__file__).resolve().parent;native='/root/pir-ramen-build/release/examples/native_store'
    log=root/'tail-outcomes.jsonl'
    def call(name,argv,cwd=research,env=None,timeout=1200):
        start=time.monotonic()
        with (root/(name+'.log')).open('w') as output:
            try:
                proc=subprocess.Popen(list(map(str,argv)),cwd=cwd,env=env,stdout=output,stderr=subprocess.STDOUT,start_new_session=True)
                code=proc.wait(timeout)
            except FileNotFoundError as exc:
                output.write(str(exc));code='missing-executable'
            except subprocess.TimeoutExpired:
                os.killpg(proc.pid,signal.SIGTERM)
                try:proc.wait(10)
                except subprocess.TimeoutExpired:os.killpg(proc.pid,signal.SIGKILL);proc.wait()
                code='timeout'
        with log.open('a') as f:f.write(json.dumps(dict(name=name,argv=list(map(str,argv)),exit=code,wall_s=time.monotonic()-start))+'\n')
        print(name,code,flush=True);return code
    call('zippir-configure',['cmake','-S',root/'zippir','-B',root/'zippir/build','-DCMAKE_BUILD_TYPE=Release'])
    if call('zippir-build',['cmake','--build',root/'zippir/build','-j2'])==0:
        for n in (65536,1048576):
            call(f'zippir-full-{n}',[root/'zippir/build/zippir','--N',n,'--output',target/f'pir-cold-zippir-{n}.json'],
                env=dict(os.environ,OMP_NUM_THREADS='1'),timeout=180)
    hint=root/'hintless/bazel-bin/hintless_simplepir/hintless_simplepir_test'
    call('hintless-cold-opt',[hint,'--gtest_filter=HintlessColdSearch.CompleteTagRecords'],cwd=root/'hintless',
        env=dict(os.environ,PIR_COLD_HINTLESS_OUTPUT=str(target/'pir-cold-hintless-opt.jsonl')))
    if not (target/'pir-cold-frontiers-v1/crt-kernels.json').exists():
        call('frontiers-tail',[sys.executable,'screen_cold_frontiers.py','--kernel','--output',target/'pir-cold-frontiers-v2'])
    call('cold-maintenance',[sys.executable,'run_cold_maintenance.py','--binary',native,'--output',target/'pir-cold-maintenance-v1'])
    call('cold-large-frontier',[sys.executable,'run_cold_search.py','--profile','frontier','--output',target/'pir-cold-large-frontier-v1','--native',native,'--repeats','5'],timeout=2400)
    call('cold-finite-rss',[sys.executable,'run_cold_search.py','--profile','finite','--output',target/'pir-cold-finite-rss-v1',
        '--native',native,'--finite',root/'bin/finite-store','--clients','4','--repeats','1'])
    for repeat in range(5):
        cases=[(clients,window) for clients in (1,8,32) for window in (0,5,20)]
        if repeat%2:cases.reverse()
        for clients,window in cases:
            call(f'sandwich-batch-{clients}-{window}-r{repeat}',[sys.executable,'run_sandwich_batch.py','--binaries',root/'sandwichpir/target/release',
                '--output',target/f'pir-cold-gpu-b{clients}-w{window}-r{repeat}','--clients',clients,'--window',window],timeout=180)
    print('Tail complete',flush=True)


if __name__=='__main__':main()
