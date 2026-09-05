//! Peak resident memory, per store profile and per workload.
//!
//! ```text
//! cargo bench -p benches --bench memory
//! ```
//!
//! Every case runs in its own child process, and that is the whole design.
//! Peak RSS is a process-wide high-water mark: it only ever rises, so a parent
//! that ran the embedded profile after the server one would report the server
//! profile's peak twice and call the second number "embedded". Re-executing
//! this binary once per case is what makes each figure belong to the case it
//! is labelled with.
//!
//! A `baseline` case does nothing but start, so the process floor can be
//! subtracted from every other row rather than silently inflating it.
//!
//! Not a criterion target: peak memory is a high-water mark, not a rate, and
//! a sampling loop would measure the loop.

use std::process::Command;
use std::sync::Arc;

use db::DB;
use defra_perf::emit::{Family, Group, Row};
use defra_perf::measure::peak_rss_bytes;
use document::{Document, NormalValue};
use query::mutator::DocMutator;
use query::DocFetcher;
use schema::{CollectionVersion, FieldDescription, FieldKind};
use storage::{RegolithStore, RegolithStoreOptions};
use tempfile::TempDir;

const CASE_VAR: &str = "DEFRA_MEMORY_CASE";
const MARKER: &str = "DEFRA_PEAK_RSS_BYTES ";
const COLLECTION: &str = "Users";
const DOCS: usize = 1_000;

const PROFILES: [&str; 3] = ["server", "embedded", "memory"];
/// What each row's name means is said once in the family note rather than
/// repeated into every row: a row name is a column heading, not a sentence.
const WORKLOADS: [&str; 4] = ["baseline", "open", "seed", "scan"];

fn options(profile: &str) -> RegolithStoreOptions {
    match profile {
        "embedded" => RegolithStoreOptions::embedded(),
        _ => RegolithStoreOptions::new(),
    }
}

fn collection_version() -> CollectionVersion {
    CollectionVersion::new(
        COLLECTION,
        "bafkmemory",
        "memory",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
            FieldDescription::new("3", "note", FieldKind::string()),
        ],
    )
}

async fn run_case(profile: &str, workload: &str) {
    if workload == "baseline" {
        return;
    }
    let dir = TempDir::new().expect("a scratch directory");
    let store = Arc::new(if profile == "memory" {
        RegolithStore::in_memory().expect("an in-memory store")
    } else {
        RegolithStore::open_with_options(dir.path().join("db"), options(profile))
            .expect("a store on disk")
    });
    let db = Arc::new(DB::open_from_arc(store).await.expect("a database"));
    if workload == "open" {
        db.close().await.expect("a clean close");
        return;
    }

    db.create_collection(collection_version())
        .await
        .expect("the collection to register");
    let mutator = db::write::autocommit::AutoCommitMutator::new(db.clone());
    for seq in 0..DOCS {
        let mut doc = Document::new();
        doc.set("name", NormalValue::String(format!("name-{seq}")));
        doc.set("note", NormalValue::String(format!("note-{seq}")));
        mutator
            .create(COLLECTION, doc)
            .await
            .expect("the create to succeed");
    }

    if workload == "scan" {
        let fetcher = db::AutoCommitFetcher::new(db.clone());
        let docs = fetcher
            .get_all(COLLECTION)
            .await
            .expect("the scan to succeed");
        assert_eq!(docs.len(), DOCS, "the scan must see every document written");
    }
    db.close().await.expect("a clean close");
}

/// Re-execute this binary for one case and read back the peak it reached.
///
/// `None` when the child could not report one, which is drawn as a gap. A
/// platform without `getrusage` is a platform this cannot measure, and saying
/// so is the only honest answer.
fn measure(profile: &str, workload: &str) -> Option<f64> {
    let exe = std::env::current_exe().expect("this binary's own path");
    let output = Command::new(exe)
        .env(CASE_VAR, format!("{profile}/{workload}"))
        .output()
        .expect("to re-execute this binary");
    if !output.status.success() {
        eprintln!(
            "memory: the {profile}/{workload} child exited {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
        return None;
    }
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .find_map(|l| l.strip_prefix(MARKER))
        .and_then(|v| v.trim().parse::<f64>().ok())
}

fn main() {
    if let Ok(case) = std::env::var(CASE_VAR) {
        let (profile, workload) = case.split_once('/').expect("a profile/workload case");
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("a runtime")
            .block_on(run_case(profile, workload));
        match peak_rss_bytes() {
            Some(bytes) => println!("{MARKER}{bytes}"),
            None => eprintln!("memory: peak RSS is not available on this platform"),
        }
        return;
    }

    let mut family = Family::new(
        "Resident memory",
        format!(
            "Peak resident set size of a process that ran one workload and nothing else. Each \
             row is its own child process, because peak RSS only ever rises and cases sharing a \
             process would all report the largest of them. baseline is the process floor, to be \
             subtracted from the rest; open is an empty database opened and closed; seed writes \
             {DOCS} documents; scan writes them and reads every one back."
        ),
    )
    // RSS for a given configuration does not depend on how busy the host was,
    // so it stays comparable on a runner the load guard failed.
    .deterministic();

    let mut unmeasured = Vec::new();
    for profile in PROFILES {
        let mut group = Group::lower_better(format!("{profile} profile"), "B");
        for workload in WORKLOADS {
            match measure(profile, workload) {
                Some(bytes) => group = group.row(Row::new(workload, bytes)),
                None => unmeasured.push(format!("{profile}/{workload}")),
            }
        }
        if !group.rows.is_empty() {
            family = family.group(group);
        }
    }

    if !unmeasured.is_empty() {
        eprintln!(
            "memory: {} case(s) reported no peak and are absent from the family: {}",
            unmeasured.len(),
            unmeasured.join(", ")
        );
    }
    if family.groups.is_empty() {
        eprintln!("memory: nothing could be measured on this platform");
        family = family.trust(defra_perf::emit::Trust::Absent);
    }
    family.emit("memory");
    println!("memory: recorded {} profile group(s)", PROFILES.len());
}
