//! Criterion's own estimates, folded into the run document.
//!
//! Every `bench_function` in the suite already produces a per-iteration
//! estimate; harvesting the directory is what puts all of them on the
//! dashboard without a second copy of the measurement living anywhere. A bench
//! target therefore never reports its criterion timings itself: the two places
//! that would have to agree instead read the one place criterion wrote.
//!
//! Criterion records a benchmark under its group name and nothing else, so the
//! bench target that produced it is lost the moment two targets' output share
//! a directory. The harvest reads a tree where each immediate child is a bench
//! target, which is the layout the runner and `just perf` both hand it, and
//! gives every target its own family so every target gets its own section on
//! the page. A flat tree, which is what a bare `cargo bench` leaves behind, is
//! still read: it becomes one family, named for being unattributed rather than
//! pretending otherwise.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use crate::emit::{Family, Group, Row, Trust};

/// Where criterion left its sample sets for this run.
pub fn criterion_root() -> PathBuf {
    PathBuf::from(std::env::var("CARGO_TARGET_DIR").unwrap_or_else(|_| "target".into()))
        .join("criterion")
}

/// One family per bench target found under `root`, keyed by the target's name.
///
/// Empty when criterion recorded nothing, which the collector renders as an
/// explicit gap rather than as a set of zeroes.
pub fn harvest(root: &Path, trust: Trust) -> BTreeMap<String, Family> {
    let mut out = BTreeMap::new();
    for (target, dir) in targets(root) {
        if let Some(family) = one_target(&target, &dir, trust) {
            out.insert(format!("criterion_{target}"), family);
        }
    }
    out
}

/// The bench targets under `root`. A directory holding a `new/estimates.json`
/// anywhere beneath it is a criterion group, not a target, so a flat tree is
/// reported as the single unattributed target it is.
fn targets(root: &Path) -> Vec<(String, PathBuf)> {
    let Ok(entries) = fs::read_dir(root) else {
        return Vec::new();
    };
    let mut dirs: Vec<PathBuf> = entries
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.is_dir() && p.file_name().is_some_and(|n| n != "report"))
        .collect();
    dirs.sort();

    // A per-target tree nests one more level than a flat one: `<target>/<group>/…`
    // against `<group>/…`. Deciding on the shape rather than on a flag means a
    // directory assembled either way still reads correctly.
    let flat = dirs
        .iter()
        .any(|d| d.join("new").join("estimates.json").is_file());
    if flat || dirs.is_empty() {
        return vec![("unattributed".to_string(), root.to_path_buf())];
    }
    dirs.into_iter()
        .filter_map(|d| {
            let name = d.file_name()?.to_string_lossy().into_owned();
            Some((name, d))
        })
        .collect()
}

fn one_target(target: &str, dir: &Path, trust: Trust) -> Option<Family> {
    let mut found: Vec<(String, Estimate)> = Vec::new();
    walk(dir, dir, 0, &mut found);
    if found.is_empty() {
        return None;
    }
    found.sort_by(|a, b| a.0.cmp(&b.0));

    let mut groups: BTreeMap<String, Vec<Row>> = BTreeMap::new();
    for (id, estimate) in found {
        let (group, rest) = match id.split_once('/') {
            Some((g, r)) => (g.to_string(), r.to_string()),
            None => ("ungrouped".to_string(), id),
        };
        groups.entry(group).or_default().push(row(rest, estimate));
    }

    // How criterion was driven is part of what the number means: a run taken at
    // a reduced sample size is comparable with another run taken the same way
    // and not with a full one, so the configuration travels with the
    // measurement instead of being implied by the job that produced it.
    let sampling = match std::env::var("DEFRA_BENCH_CRITERION_ARGS") {
        Ok(args) if !args.trim().is_empty() => {
            format!(" Criterion was driven with `{}`.", args.trim())
        }
        _ => " Criterion ran with its default sampling.".to_string(),
    };
    let title = if target == "unattributed" {
        "Criterion benchmarks".to_string()
    } else {
        format!("{target} benchmarks")
    };
    let family = Family::new(
        title,
        format!(
            "Median wall time per iteration, harvested from the sample sets criterion wrote for \
             the `{target}` bench target. Lower is better.{sampling}"
        ),
    )
    .trust(trust);
    Some(groups.into_iter().fold(family, |f, (name, rows)| {
        f.group(Group::lower_better(name, "ns/iter").rows(rows))
    }))
}

/// Criterion's median for one benchmark, with the interval it is confident the
/// true median lies in.
struct Estimate {
    median_ns: f64,
    lower_ns: f64,
    upper_ns: f64,
}

/// A criterion benchmark id is a string, so `16` sorts before `4`. When every
/// name in a group is a number the row carries it as its x, which both orders
/// the table and lets the group draw as a curve instead of a list.
///
/// The confidence interval is carried as the row's range, and that is what
/// makes a criterion row comparable honestly: a comparison refuses to call a
/// delta real when the two runs' ranges overlap, and a row with no range gets
/// judged on the threshold alone. Discarding the interval turned every noisy
/// benchmark on a shared runner into a reported regression.
fn row(name: String, estimate: Estimate) -> Row {
    let row = Row::new(&name, estimate.median_ns).range(estimate.lower_ns, estimate.upper_ns);
    match name.parse::<f64>() {
        Ok(x) if x.is_finite() => row.at(x),
        _ => row,
    }
}

fn walk(dir: &Path, root: &Path, depth: usize, out: &mut Vec<(String, Estimate)>) {
    // Criterion nests one directory per benchmark id segment. Eight is deeper
    // than any id this suite produces and stops a symlink loop cheaply.
    if depth > 8 {
        return;
    }
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    let mut subdirs: Vec<PathBuf> = entries
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.is_dir())
        .collect();
    subdirs.sort();
    for sub in subdirs {
        // The newest sample set is under `new/`; `base/` and any saved baseline
        // beside it are older copies of the same benchmark.
        if sub.file_name().is_some_and(|n| n == "new") {
            let estimates = sub.join("estimates.json");
            let id = sub.parent().and_then(|p| p.strip_prefix(root).ok());
            if let (Some(estimate), Some(id)) = (estimate(&estimates), id) {
                out.push((id.to_string_lossy().replace('\\', "/"), estimate));
            }
            continue;
        }
        walk(&sub, root, depth + 1, out);
    }
}

fn estimate(path: &Path) -> Option<Estimate> {
    let text = fs::read_to_string(path).ok()?;
    let value: serde_json::Value = serde_json::from_str(&text).ok()?;
    let median = value.get("median")?;
    let point = median
        .get("point_estimate")?
        .as_f64()
        .filter(|v| v.is_finite())?;
    let interval = median.get("confidence_interval");
    let bound = |name: &str| {
        interval
            .and_then(|i| i.get(name))
            .and_then(serde_json::Value::as_f64)
            .filter(|v| v.is_finite())
    };
    // An older sample set without an interval still reports its median; it just
    // has no range, and is then compared on the threshold alone.
    let (lower, upper) = match (bound("lower_bound"), bound("upper_bound")) {
        (Some(lower), Some(upper)) if lower <= upper => (lower, upper),
        _ => (point, point),
    };
    Some(Estimate {
        median_ns: point,
        lower_ns: lower,
        upper_ns: upper,
    })
}
