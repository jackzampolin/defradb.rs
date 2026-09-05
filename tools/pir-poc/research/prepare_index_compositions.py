#!/usr/bin/env python3
"""Fetch exact Ramen revision and build only our metered bridge, with two jobs."""
import argparse
from pathlib import Path
import shutil
import subprocess
import os

from benchmarks.private_memory import RAMEN_REVISION


def main():
    p=argparse.ArgumentParser();p.add_argument('output',type=Path);p.add_argument('--target-dir',type=Path,required=True)
    a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False)
    source=a.output/'ramen'
    subprocess.run(['git','clone','https://github.com/AarhusCrypto/Ramen.git',str(source)],check=True)
    subprocess.run(['git','-C',str(source),'checkout','--detach',RAMEN_REVISION],check=True)
    here=Path(__file__).parent/'benchmarks'
    shutil.copyfile(here/'ramen_bridge.rs',source/'oram/examples/private_index_bridge.rs')
    shutil.copyfile(here/'native_store.rs',source/'oram/examples/native_store.rs')
    shutil.copyfile(here/'ramen.Cargo.lock',source/'Cargo.lock')
    subprocess.run(['cargo','build','--locked','--release','--example','private_index_bridge','--example','native_store'],cwd=source,
        env=dict(os.environ,CARGO_BUILD_JOBS='2',CARGO_TARGET_DIR=str(a.target_dir.resolve())),check=True)
    (a.output/'binary.txt').write_text(str(a.target_dir.resolve()/'release/examples/private_index_bridge'))


if __name__=='__main__':main()
