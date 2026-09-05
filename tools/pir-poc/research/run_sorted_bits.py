"""Sorted placement and one private prefix owner, followed by exact local filtering."""
from dataclasses import asdict
import json
from pathlib import Path
import subprocess
import sys
from benchmarks.cold_search import Config

if __name__=='__main__':
    output=Path('/mnt/c/src/defradb.rs/target/pir-cold-sorted-bits-v1');output.mkdir(parents=True,exist_ok=False)
    matrix=[]
    for n in (16384,262144):
        for family,groups in (('prefix-owner',(8,12,16)),('directory',(22,)),('xor',(16,))):
            for group in groups:
                matrix.append(asdict(Config(family=family,rows=n,group=group,field_bits=64,key_layout='hashed',distribution='sorted',clients=8)))
    for family in ('bit-owners','bit-owners-raw'):
        for group in (1,4,8):
            matrix.append(asdict(Config(family=family,rows=16384,group=group,field_bits=64,key_layout='hashed',distribution='sorted',clients=4)))
    (output/'matrix.json').write_text(json.dumps(dict(matrix=matrix),indent=2))
    subprocess.run([sys.executable,'run_cold_search.py','--matrix-from',str(output/'matrix.json'),'--output',str(output/'campaign'),
        '--native','/root/pir-ramen-build/release/examples/native_store','--repeats','5'],check=True)
