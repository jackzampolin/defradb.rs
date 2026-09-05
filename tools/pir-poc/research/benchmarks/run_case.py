"""Isolated case entry point. All output is written into a fresh directory."""
import argparse
import json
from pathlib import Path
import traceback

from .cases import Case, run


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("config",type=Path)
    parser.add_argument("output",type=Path)
    args=parser.parse_args()
    config=json.loads(args.config.read_text())
    args.output.mkdir(parents=True,exist_ok=False)
    try:
        result=run(Case(**config),args.output)
        (args.output/"result.json").write_text(json.dumps(result,indent=2))
    except Exception as error:
        (args.output/"failure.json").write_text(json.dumps(dict(error=str(error),traceback=traceback.format_exc()),indent=2))
        raise


if __name__=="__main__":main()
