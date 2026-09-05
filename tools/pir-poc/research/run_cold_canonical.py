"""Build canonical Poseidon corpora and serve unchanged witnesses privately."""
import argparse
from dataclasses import asdict
import json
from pathlib import Path
import subprocess
import sys
from benchmarks.cold_search import Config


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--bridge',required=True);p.add_argument('--native',required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
    a.bridge=str(Path(a.bridge).resolve());a.native=str(Path(a.native).resolve());a.output=a.output.resolve()
    a.output.mkdir(parents=True,exist_ok=False);matrix=[]
    for n in (256,1024):
        corpus=a.output/f'corpus-{n}.json'
        subprocess.run([a.bridge,'build',str(n),str(corpus)],check=True)
        data=json.loads(corpus.read_text());root=data['root'];row=data['data'][1]
        valid=subprocess.run([a.bridge,'verify',str(row[0]),root,row[3]],capture_output=True)
        bad=bytearray.fromhex(row[3]);bad[-1]^=1
        tamper=subprocess.run([a.bridge,'verify',str(row[0]),root,bad.hex()],capture_output=True)
        wrong_root=subprocess.run([a.bridge,'verify',str(row[0]),'00'*32,row[3]],capture_output=True)
        if valid.returncode or not tamper.returncode or not wrong_root.returncode:raise AssertionError('canonical verification negative checks')
        for group in (1,4,16,64):
            matrix.append(asdict(Config(family='canonical-directory',rows=n,row_bytes=2008,fanout=1,group=group,
                clients=4,canonical_file=str(corpus),verifier=a.bridge)))
        matrix.append(asdict(Config(family='canonical-directory',rows=n,row_bytes=2008,fanout=1,group=16,
            clients=4,backend='singlepass',canonical_file=str(corpus),verifier=a.bridge)))
    (a.output/'matrix.json').write_text(json.dumps(dict(matrix=matrix),indent=2))
    (a.output/'negative-checks.json').write_text(json.dumps(dict(tampered_witness_rejected=True,wrong_root_rejected=True)))
    subprocess.run([sys.executable,'run_cold_search.py','--matrix-from',str(a.output/'matrix.json'),
        '--output',str(a.output/'campaign'),'--native',a.native,'--repeats','5'],check=True)
