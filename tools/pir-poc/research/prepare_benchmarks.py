"""Fetch exact research artifacts into a new user-selected directory."""
import argparse
import json
from pathlib import Path
import subprocess
from benchmarks.zelda import REVISION as ZELDA_REVISION, SOURCE as ZELDA_SOURCE
from benchmarks.gpu import REVISION as GPU_REVISION


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument("output",type=Path);args=p.parse_args()
    args.output.mkdir(parents=True,exist_ok=False)
    pins=[("zelda",ZELDA_SOURCE,ZELDA_REVISION),("gpu-dpf","https://github.com/facebookresearch/GPU-DPF.git",GPU_REVISION)]
    for name,url,revision in pins:
        path=args.output/name
        subprocess.run(["git","clone","--no-checkout",url,str(path)],check=True)
        subprocess.run(["git","-C",str(path),"checkout","--detach",revision],check=True)
        if subprocess.check_output(["git","-C",str(path),"rev-parse","HEAD"],text=True).strip()!=revision:raise ValueError("pin verification failed")
    (args.output/"pins.json").write_text(json.dumps(pins,indent=2))


if __name__=="__main__":main()
