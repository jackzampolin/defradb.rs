//! Assemble one platform document from every metric family the benches wrote.
//!
//! ```text
//! cargo run -p defra-perf --bin collect -- \
//!     --out platform-x86_64-unknown-linux-gnu.json \
//!     --commit <sha> --label main --target x86_64-unknown-linux-gnu \
//!     [--loadguard loadguard.json]
//! ```
//!
//! Families come from `$DEFRA_BENCH_OUT` (JSON Lines) when it is set and from
//! `./bench-out/*.json` otherwise. Criterion's own estimates are folded in as
//! one more family, so every benchmark in the suite reaches the dashboard
//! without a second copy of its timing living anywhere.
//!
//! Two rules this exists to enforce. A family that was not collected is
//! written `trust: "absent"` and never as a zero, so a gap in collection
//! renders as a gap. And a document is append-only once published: an existing
//! `--out` is an error, never an overwrite.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use defra_perf::criterion;
use defra_perf::emit::{Family, Trust};
use defra_perf::run_meta;
use serde_json::{json, Value};

const USAGE: &str = "collect: assemble one platform document from every bench family.

  cargo run -p defra-perf --bin collect -- --out <file.json> --commit <sha> \\
      --label <label> --target <triple> [--loadguard <verdict.json>] \\
      [--criterion-root <dir>]

Families are read from $DEFRA_BENCH_OUT (JSON Lines) when set, else ./bench-out/*.json.";

fn main() {
    let Some(args) = parse_args() else { return };

    let files = record_files();
    let mut families = read_records(&files);
    if families.is_empty() {
        eprintln!(
            "collect: no family records found in {}. Only the criterion family can be reported.",
            describe(&files)
        );
    }

    let guard = run_meta::load_guard(args.loadguard.as_deref());
    let timing_trust = if guard.passed {
        Trust::Clean
    } else {
        Trust::Contaminated
    };

    let criterion_root = args
        .criterion_root
        .clone()
        .unwrap_or_else(criterion::criterion_root);
    let harvested = criterion::harvest(&criterion_root, timing_trust);
    if harvested.is_empty() {
        // The same refusal the harvested families get below: silently replacing
        // a bench's own family with a stub saying nothing was collected would
        // report a gap over data that exists.
        if families.contains_key("criterion") {
            die("a bench emitted a family named 'criterion', which the criterion harvester owns");
        }
        families.insert(
            "criterion".to_string(),
            absent(
                "Criterion benchmarks",
                "No criterion sample sets were found under the target directory for this run.",
            ),
        );
    }
    for (name, family) in harvested {
        let value = serde_json::to_value(family).unwrap_or_else(|e| die(&format!("{e}")));
        if families.insert(name.clone(), value).is_some() {
            die(&format!(
                "a bench emitted a family named '{name}', which the criterion harvester owns"
            ));
        }
    }

    for (name, family) in families.iter_mut() {
        apply_guard(name, family, timing_trust);
    }

    let document = json!({
        "schema_version": 1,
        "commit": args.commit,
        "label": args.label,
        "timestamp": run_meta::now_iso8601(),
        "target": args.target,
        "toolchain": run_meta::toolchain(),
        "host": run_meta::host(),
        "loadguard": guard,
        "families": families,
    });

    write_new(&args.out, &format!("{:#}\n", document));
    summarize(&args.out, &families, guard.passed);
}

/// Trust is only ever downgraded. A bench that declared its own doubt keeps
/// it, and a deterministic family is immune to how busy the host was.
fn apply_guard(name: &str, family: &mut Value, timing_trust: Trust) {
    let deterministic = family
        .get("deterministic")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let declared = match family.get("trust").and_then(Value::as_str) {
        Some("clean") => Trust::Clean,
        Some("contaminated") => Trust::Contaminated,
        Some("absent") => Trust::Absent,
        Some(other) => die(&format!(
            "family '{name}': trust {other:?} is not one of clean, contaminated, absent"
        )),
        None => die(&format!("family '{name}' did not declare a trust level")),
    };
    let effective = if deterministic || declared == Trust::Absent {
        declared
    } else {
        worst(declared, timing_trust)
    };
    family["trust"] = json!(match effective {
        Trust::Clean => "clean",
        Trust::Contaminated => "contaminated",
        Trust::Absent => "absent",
    });
}

fn worst(a: Trust, b: Trust) -> Trust {
    let rank = |t| match t {
        Trust::Clean => 0,
        Trust::Contaminated => 1,
        Trust::Absent => 2,
    };
    if rank(a) >= rank(b) {
        a
    } else {
        b
    }
}

fn absent(title: &str, note: &str) -> Value {
    serde_json::to_value(Family::new(title, note).trust(Trust::Absent))
        .unwrap_or_else(|e| die(&format!("{e}")))
}

struct Args {
    out: PathBuf,
    commit: String,
    label: String,
    target: String,
    loadguard: Option<PathBuf>,
    /// A tree whose immediate children are bench-target names. Defaults to
    /// `$CARGO_TARGET_DIR/criterion`, which is flat and therefore reports one
    /// unattributed family.
    criterion_root: Option<PathBuf>,
}

/// `None` means `--help` was asked for and printed.
fn parse_args() -> Option<Args> {
    let argv: Vec<String> = std::env::args().skip(1).collect();

    let mut out = None;
    let mut commit = None;
    let mut label = None;
    let mut target = None;
    let mut loadguard = None;
    let mut criterion_root = None;
    let mut foreign = Vec::new();

    let mut i = 0;
    while i < argv.len() {
        let value = |i: &mut usize, flag: &str| -> String {
            *i += 1;
            argv.get(*i)
                .cloned()
                .unwrap_or_else(|| die(&format!("{flag} needs a value")))
        };
        match argv[i].as_str() {
            "--out" => out = Some(PathBuf::from(value(&mut i, "--out"))),
            "--commit" => commit = Some(value(&mut i, "--commit")),
            "--label" => label = Some(value(&mut i, "--label")),
            "--target" => target = Some(value(&mut i, "--target")),
            "--loadguard" => loadguard = Some(PathBuf::from(value(&mut i, "--loadguard"))),
            "--criterion-root" => {
                criterion_root = Some(PathBuf::from(value(&mut i, "--criterion-root")))
            }
            "-h" | "--help" => {
                println!("{USAGE}");
                return None;
            }
            other => foreign.push(other.to_string()),
        }
        i += 1;
    }

    if !foreign.is_empty() {
        die(&format!("unrecognized arguments: {}", foreign.join(" ")));
    }
    let Some(out) = out else {
        eprintln!("{USAGE}");
        std::process::exit(2);
    };

    Some(Args {
        out,
        commit: commit.unwrap_or_else(|| die("--commit is required")),
        label: label.unwrap_or_else(|| die("--label is required")),
        target: target.unwrap_or_else(run_meta::derived_target),
        loadguard,
        criterion_root,
    })
}

fn record_files() -> Vec<PathBuf> {
    match std::env::var_os("DEFRA_BENCH_OUT") {
        Some(p) if !p.is_empty() => vec![PathBuf::from(p)],
        _ => {
            let mut files: Vec<PathBuf> = std::fs::read_dir("bench-out")
                .into_iter()
                .flatten()
                .filter_map(|e| e.ok().map(|e| e.path()))
                .filter(|p| p.extension().is_some_and(|x| x == "json"))
                .collect();
            files.sort();
            files
        }
    }
}

fn describe(files: &[PathBuf]) -> String {
    if files.is_empty() {
        "$DEFRA_BENCH_OUT (unset) and ./bench-out/*.json (empty or missing)".to_string()
    } else {
        files
            .iter()
            .map(|p| p.display().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    }
}

fn read_records(files: &[PathBuf]) -> BTreeMap<String, Value> {
    let mut out = BTreeMap::new();
    for file in files {
        let text = std::fs::read_to_string(file)
            .unwrap_or_else(|e| die(&format!("read {}: {e}", file.display())));
        for (n, line) in text.lines().enumerate() {
            if line.trim().is_empty() {
                continue;
            }
            let at = format!("{}:{}", file.display(), n + 1);
            let record: Value = serde_json::from_str(line)
                .unwrap_or_else(|e| die(&format!("{at}: malformed JSON: {e}")));
            let Some(name) = record.get("family").and_then(Value::as_str) else {
                die(&format!("{at}: record has no \"family\" string"))
            };
            let Some(data) = record.get("data") else {
                die(&format!("{at}: family '{name}' has no \"data\""))
            };
            if out.insert(name.to_string(), data.clone()).is_some() {
                die(&format!(
                    "{at}: family '{name}' was emitted twice; a document records one measurement \
                     per family. Collect against a fresh output file."
                ));
            }
        }
    }
    out
}

/// A platform document is append-only once published, so an existing path is
/// an error rather than an overwrite.
fn write_new(path: &Path, body: &str) {
    match path.parent() {
        Some(dir) if !dir.as_os_str().is_empty() => std::fs::create_dir_all(dir)
            .unwrap_or_else(|e| die(&format!("create {}: {e}", dir.display()))),
        _ => {}
    }
    match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
    {
        Ok(mut f) => {
            use std::io::Write;
            f.write_all(body.as_bytes())
                .unwrap_or_else(|e| die(&format!("write {}: {e}", path.display())));
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => die(&format!(
            "{} already exists. A platform document is append-only once published: re-measuring \
             produces a new file, so a comparison can always be reproduced.",
            path.display()
        )),
        Err(e) => die(&format!("create {}: {e}", path.display())),
    }
}

fn summarize(path: &Path, families: &BTreeMap<String, Value>, quiet_host: bool) {
    println!("collect: wrote {}", path.display());
    for (name, family) in families {
        let trust = family.get("trust").and_then(Value::as_str).unwrap_or("?");
        let groups = family
            .get("groups")
            .and_then(Value::as_array)
            .map(|g| g.len())
            .unwrap_or(0);
        let rows: usize = family
            .get("groups")
            .and_then(Value::as_array)
            .map(|gs| {
                gs.iter()
                    .filter_map(|g| g.get("rows").and_then(Value::as_array).map(Vec::len))
                    .sum()
            })
            .unwrap_or(0);
        println!("  {name:<20} trust={trust:<13} {groups} group(s), {rows} row(s)");
    }
    println!("  {:<20} passed={quiet_host}", "loadguard");
}

fn die(why: &str) -> ! {
    eprintln!("collect: {why}");
    std::process::exit(1)
}
