"""Subprocess boundary for pinned external implementations."""
import json
from pathlib import Path
import sys


if __name__=="__main__":
    engine,config,output,source=sys.argv[1:]
    if engine=="zelda":from .zelda import run
    elif engine=="gpu":from .gpu import run
    else:raise ValueError(engine)
    run(Path(source),Path(output),**json.loads(Path(config).read_text()))
