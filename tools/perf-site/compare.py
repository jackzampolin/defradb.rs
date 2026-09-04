#!/usr/bin/env python3
"""Compare two run documents and decide what actually regressed.

The hard part is not computing a percentage, it is refusing to report one that
means nothing. A run recorded while the host was busy can shift a figure by a
quarter with no code change, so a comparison that treats every delta as signal
produces alerts nobody trusts, and an alert nobody trusts is worse than none.

Three rules keep a verdict honest:

* **Both sides must be trusted.** If either run marked a family
  ``contaminated`` or ``absent``, the comparison is reported as ``unverified``
  and never as a pass or a regression.
* **Ranges must separate.** A row carries the ``min`` and ``max`` seen across
  repetitions. A delta counts only when the two ranges do not overlap: if the
  new ``max`` still reaches the old ``min``, the runs did not measure a
  difference, whatever their medians say. A row without a range skips this
  test, which is correct for a deterministic count and is why a bench that
  repeats a timing should always report one.
* **A floor on top of that.** Non-overlapping ranges can still be a fraction of
  a percent apart. A change under ``--threshold`` is noise regardless.

Comparisons are per platform. A metric measured on Linux is not a baseline for
the same metric measured in a browser, and pretending otherwise would produce a
regression report on every run.
"""

import argparse
import json
import pathlib
import sys

PASS, REGRESSED, IMPROVED, NOISE, UNVERIFIED = (
    "pass",
    "regressed",
    "improved",
    "noise",
    "unverified",
)
TRUSTED = "clean"


def rows_of(doc, platform):
    """Every comparable row on one platform, keyed the way the dashboard keys
    them, carrying everything a verdict needs."""
    out = {}
    families = ((doc.get("platforms") or {}).get(platform) or {}).get("families") or {}
    for fname, fam in families.items():
        trust = (fam or {}).get("trust", "absent")
        for group in (fam or {}).get("groups") or []:
            unit = group.get("unit", "")
            lower = bool(group.get("lower_is_better"))
            for row in group.get("rows") or []:
                value = row.get("value")
                if not isinstance(value, (int, float)):
                    continue
                key = f"{fname}/{group.get('name')}/{row.get('name')}"
                out[key] = {
                    "value": float(value),
                    "min": row.get("min"),
                    "max": row.get("max"),
                    "unit": unit,
                    "lower": lower,
                    "trust": trust,
                    "family": fname,
                }
    return out


def classify(before, after, threshold):
    """Verdict, percentage and reason for one pair of measurements."""
    if before["trust"] != TRUSTED:
        return UNVERIFIED, None, f"the baseline's {before['family']} is {before['trust']}"
    if after["trust"] != TRUSTED:
        return UNVERIFIED, None, f"this run's {after['family']} is {after['trust']}"
    if before["value"] == 0:
        return UNVERIFIED, None, "the baseline measured zero, so there is no ratio to take"

    pct = (after["value"] - before["value"]) / abs(before["value"]) * 100.0
    if before["lower"]:
        pct = -pct

    if abs(pct) < threshold:
        return NOISE, pct, f"within the {threshold:g}% threshold"

    have_ranges = all(
        isinstance(side[k], (int, float)) for side in (before, after) for k in ("min", "max")
    )
    if have_ranges and not (after["max"] < before["min"] or after["min"] > before["max"]):
        return (
            NOISE,
            pct,
            "the two runs' measured ranges overlap, so they did not measure a difference",
        )

    return (REGRESSED if pct < 0 else IMPROVED), pct, ""


def compare(base, cur, threshold):
    platforms = sorted(
        set((base.get("platforms") or {})) & set((cur.get("platforms") or {}))
    )
    only_cur = sorted(set(cur.get("platforms") or {}) - set(base.get("platforms") or {}))
    deltas = []
    for platform in platforms:
        b, c = rows_of(base, platform), rows_of(cur, platform)
        for key in sorted(set(b) | set(c)):
            if key not in b or key not in c:
                deltas.append(
                    {
                        "platform": platform,
                        "key": key,
                        "verdict": UNVERIFIED,
                        "pct": None,
                        "reason": "only one of the two runs measured this",
                        "before": (b.get(key) or {}).get("value"),
                        "after": (c.get(key) or {}).get("value"),
                        "unit": (b.get(key) or c.get(key) or {}).get("unit", ""),
                    }
                )
                continue
            verdict, pct, reason = classify(b[key], c[key], threshold)
            deltas.append(
                {
                    "platform": platform,
                    "key": key,
                    "verdict": verdict,
                    "pct": pct,
                    "reason": reason,
                    "before": b[key]["value"],
                    "after": c[key]["value"],
                    "unit": c[key]["unit"],
                }
            )
    return deltas, platforms, only_cur


def fmt(v, unit):
    if not isinstance(v, (int, float)):
        return "n/a"
    if unit in ("B", "bytes"):
        for limit, suffix in ((1 << 30, "GiB"), (1 << 20, "MiB"), (1024, "KiB")):
            if abs(v) >= limit:
                return f"{v / limit:.2f} {suffix}"
        return f"{v:.0f} B"
    a = abs(v)
    if a >= 1e6:
        return f"{v / 1e6:.2f}M"
    if a >= 1e4:
        return f"{v / 1e3:.1f}k"
    if a >= 1:
        return f"{v:,.2f}".rstrip("0").rstrip(".")
    if a == 0:
        return "0"
    # Two decimals turn every sub-second timing into "0.01", which is the same
    # string for a value and the value it regressed from. Significant figures
    # keep the two distinguishable.
    return f"{v:.4g}"


def markdown(deltas, platforms, only_cur, base, cur, threshold, note=""):
    regressed = [d for d in deltas if d["verdict"] == REGRESSED]
    improved = [d for d in deltas if d["verdict"] == IMPROVED]
    unverified = [d for d in deltas if d["verdict"] == UNVERIFIED]
    compared = [d for d in deltas if d["verdict"] != UNVERIFIED]

    out = ["## Performance", ""]
    out.append(
        f"`{(cur.get('commit') or '')[:12]}` ({cur.get('label') or 'this run'}) "
        f"against `{(base.get('commit') or '')[:12]}` ({base.get('label') or 'baseline'}), "
        f"threshold {threshold:g}%."
    )
    if note:
        out.append("")
        out.append(note)
    out.append("")
    if not platforms:
        out.append(
            "The two runs share no platform, so nothing was compared. This is a gap in "
            "collection, not a result."
        )
        return "\n".join(out)
    out.append(f"Platforms compared: {', '.join(f'`{p}`' for p in platforms)}.")
    if only_cur:
        out.append(
            f"Measured only in this run, so not compared: {', '.join(f'`{p}`' for p in only_cur)}."
        )
    out.append("")

    def table(title, rows):
        if not rows:
            return []
        body = [f"### {title}", "", "| platform | metric | baseline | this run | change |", "|---|---|---:|---:|---:|"]
        for d in sorted(rows, key=lambda r: abs(r["pct"] or 0), reverse=True):
            body.append(
                f"| `{d['platform']}` | {d['key']} | {fmt(d['before'], d['unit'])} | "
                f"{fmt(d['after'], d['unit'])} | {d['pct']:+.1f}% |"
            )
        body.append("")
        return body

    out += table(f"Regressed ({len(regressed)})", regressed)
    out += table(f"Improved ({len(improved)})", improved)

    out.append(
        f"{len(compared)} metric(s) compared, {len(unverified)} not comparable."
    )
    if unverified:
        reasons = {}
        for d in unverified:
            reasons[d["reason"]] = reasons.get(d["reason"], 0) + 1
        out.append("")
        out.append("Not compared, and why:")
        for reason, n in sorted(reasons.items(), key=lambda kv: -kv[1]):
            out.append(f"- {n} × {reason}")
    out.append("")
    if regressed:
        out.append(f"**Verdict: regressed** on {len(regressed)} metric(s).")
    elif not compared:
        out.append(
            "**Verdict: unverified.** Nothing was comparable between these two runs, so this "
            "is not a pass."
        )
    else:
        out.append("**Verdict: no regression.**")
    return "\n".join(out)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--baseline", required=True)
    ap.add_argument("--current", required=True)
    ap.add_argument(
        "--threshold",
        type=float,
        default=5.0,
        help="percent change below which a delta is noise (default 5)",
    )
    ap.add_argument("--markdown", help="write the report here as well as to stdout")
    ap.add_argument(
        "--note",
        default="",
        help="what this comparison did and did not measure, printed with the report",
    )
    ap.add_argument("--fail-on-regression", action="store_true")
    args = ap.parse_args()

    base = json.loads(pathlib.Path(args.baseline).read_text())
    cur = json.loads(pathlib.Path(args.current).read_text())
    deltas, platforms, only_cur = compare(base, cur, args.threshold)
    report = markdown(deltas, platforms, only_cur, base, cur, args.threshold, args.note)
    print(report)
    if args.markdown:
        pathlib.Path(args.markdown).write_text(report + "\n")

    regressed = [d for d in deltas if d["verdict"] == REGRESSED]
    if args.fail_on_regression and regressed:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
