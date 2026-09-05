#!/usr/bin/env python3
"""Decide whether this host is quiet enough for a timing to mean anything.

Run *before* the benchmarks, never after: a guard that samples load once the
suite has finished is measuring the suite. Its verdict is written to a file
that `collect` folds into the platform document, and a missing or unreadable
verdict is treated as "not certified quiet" rather than as a pass.

The rule is the one-minute load average against the core count. A shared CI
runner with other tenants on the box shows up here; a quiet one does not.
Anything the platform cannot report leaves the guard failing closed, because
"we could not tell" and "it was quiet" are different answers and only one of
them is honest.
"""

import argparse
import json
import os
import sys


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", help="write the verdict here as well as to stdout")
    ap.add_argument(
        "--max-load-per-core",
        type=float,
        default=0.5,
        help="one-minute load average per core above which the host is busy (default 0.5)",
    )
    args = ap.parse_args()

    cores = os.cpu_count()
    try:
        load = os.getloadavg()
    except (OSError, AttributeError) as e:
        verdict = {
            "passed": False,
            "cores": cores,
            "note": f"the load average is not available on this host ({e}), so it cannot be "
            "certified quiet. Timing families are marked contaminated; deterministic "
            "families stay comparable.",
        }
    else:
        ceiling = (cores or 1) * args.max_load_per_core
        passed = load[0] <= ceiling
        verdict = {
            "passed": passed,
            "cores": cores,
            "loadavg": list(load),
            "ceiling": ceiling,
            "note": (
                f"one-minute load {load[0]:.2f} on {cores} core(s), at or under the "
                f"{ceiling:.2f} ceiling: the host was quiet."
                if passed
                else f"one-minute load {load[0]:.2f} on {cores} core(s) exceeds the "
                f"{ceiling:.2f} ceiling, so timing families are marked contaminated. "
                "Binary size, allocation counts and other deterministic families stay "
                "comparable."
            ),
        }

    text = json.dumps(verdict, indent=1)
    print(text)
    if args.out:
        with open(args.out, "w") as f:
            f.write(text + "\n")
    # Always exits 0: the verdict is data for the collector, not a gate. A
    # busy runner should still produce a document that says it was busy.
    return 0


if __name__ == "__main__":
    sys.exit(main())
