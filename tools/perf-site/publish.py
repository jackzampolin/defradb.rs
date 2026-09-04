#!/usr/bin/env python3
"""Merge this run's platform documents into the published history.

The history is the point. A single rendered page per CI run answers "is this
run fast" and nothing else: the moment it is replaced, the comparison it
supported is gone. Keeping every run document and rendering from the whole set
answers "when did this regress", which is the question a performance gate
actually has to answer.

Layout under the site root:

    index.html          the dashboard, self-contained, reads the JSON below
    runs/index.json     manifest, newest first
    runs/<commit>.json  one document per run, every platform merged into it

One `collect` invocation produces one *platform* document. This merges the
platforms measured for a commit into the single run document the dashboard
reads, so a platform that was added later shows up beside the ones that were
always there rather than replacing them.

The manifest is regenerated from the directory on every publish rather than
appended to, so a run file that was removed cannot leave a dangling row behind
and a run file added out of band is still picked up.
"""

import argparse
import json
import pathlib
import sys

KEEP = 200


def load(path):
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as e:
        print(f"publish: cannot read {path}: {e}", file=sys.stderr)
        return None


def merge_platforms(docs):
    """One run document from several platform documents of the same commit."""
    commits = {d.get("commit") for d in docs}
    if len(commits) != 1:
        print(
            "publish: the platform documents disagree about which commit they "
            f"measured ({', '.join(sorted(str(c) for c in commits))}). Refusing to "
            "merge them into one run: a run document that mixes commits cannot be "
            "compared with anything.",
            file=sys.stderr,
        )
        return None

    platforms = {}
    for doc in docs:
        target = doc.get("target")
        if not target:
            print("publish: a platform document names no target, skipping it", file=sys.stderr)
            continue
        if target in platforms:
            print(
                f"publish: {target} was collected twice for this commit. Refusing to "
                "pick one: re-run the collection against a fresh output directory.",
                file=sys.stderr,
            )
            return None
        platforms[target] = {
            "timestamp": doc.get("timestamp", ""),
            "toolchain": doc.get("toolchain", ""),
            "host": doc.get("host", {}),
            "loadguard": doc.get("loadguard", {}),
            "families": doc.get("families", {}),
        }

    if not platforms:
        print("publish: no usable platform document, nothing to file", file=sys.stderr)
        return None

    return {
        "schema_version": 1,
        "commit": docs[0].get("commit", ""),
        "label": docs[0].get("label", ""),
        # The run is as old as its earliest platform: a macOS runner that
        # started twenty minutes late did not make the run newer.
        "timestamp": min(p["timestamp"] for p in platforms.values() if p["timestamp"])
        if any(p["timestamp"] for p in platforms.values())
        else "",
        "platforms": platforms,
    }


def summarize(doc, name):
    """The manifest row for one run: enough to populate the picker without
    fetching the run itself."""
    platforms = doc.get("platforms", {})
    trust = {}
    for target, p in platforms.items():
        for fam, body in (p.get("families") or {}).items():
            level = (body or {}).get("trust", "absent")
            # Worst level across platforms: a family clean on one runner and
            # contaminated on another is not a clean family.
            rank = {"clean": 0, "contaminated": 1, "absent": 2}
            if rank.get(level, 2) >= rank.get(trust.get(fam, "clean"), 0):
                trust[fam] = level
    return {
        "file": name,
        "commit": doc.get("commit", ""),
        "label": doc.get("label", ""),
        "timestamp": doc.get("timestamp", ""),
        "platforms": sorted(platforms),
        "quiet_hosts": all(
            (p.get("loadguard") or {}).get("passed") is True for p in platforms.values()
        ),
        "trust": trust,
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("platform_docs", nargs="+", help="platform documents from `collect`")
    ap.add_argument("--site", required=True, help="site root to publish into")
    ap.add_argument("--keep", type=int, default=KEEP, help=f"runs to retain (default {KEEP})")
    args = ap.parse_args()

    docs = [d for d in (load(pathlib.Path(p)) for p in args.platform_docs) if d is not None]
    if not docs:
        print("publish: none of the platform documents could be read", file=sys.stderr)
        return 1

    run = merge_platforms(docs)
    if run is None:
        return 1
    commit = run["commit"]
    if not commit:
        print(
            "publish: the merged run carries no commit, refusing to file it",
            file=sys.stderr,
        )
        return 1

    root = pathlib.Path(args.site)
    runs = root / "runs"
    runs.mkdir(parents=True, exist_ok=True)

    # A re-run of the same commit merges into what is already on file rather
    # than replacing it, so re-measuring one platform does not drop the others.
    target = runs / f"{commit}.json"
    existing = load(target) if target.exists() else None
    if existing and existing.get("platforms"):
        merged = dict(existing["platforms"])
        merged.update(run["platforms"])
        run["platforms"] = merged
        print(f"publish: merged {len(run['platforms'])} platform(s) into the existing {commit[:12]}")
    target.write_text(json.dumps(run, indent=1, sort_keys=True) + "\n")

    entries = []
    for f in sorted(runs.glob("*.json")):
        if f.name == "index.json":
            continue
        doc = load(f)
        if doc is not None:
            entries.append(summarize(doc, f.name))
    entries.sort(key=lambda e: e.get("timestamp") or "", reverse=True)

    # Bounded, and the drop is reported rather than silent.
    if len(entries) > args.keep:
        for e in entries[args.keep:]:
            (runs / e["file"]).unlink(missing_ok=True)
        print(f"publish: pruned {len(entries) - args.keep} run(s) beyond the newest {args.keep}")
        entries = entries[: args.keep]

    (runs / "index.json").write_text(json.dumps({"runs": entries}, indent=1) + "\n")
    newest = entries[0]
    print(
        f"publish: {len(entries)} run(s) on file, newest {newest['commit'][:12]} "
        f"on {', '.join(newest['platforms']) or 'no platform'}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
