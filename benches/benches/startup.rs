//! What it costs to open a database, and what it leaves on disk.
//!
//! ```text
//! cargo bench -p benches --bench startup
//! ```
//!
//! Startup is the first thing a user feels and the last thing anything here
//! measured. A cold open replays whatever the last process left behind and
//! then loads every collection, so its cost grows with both, and neither
//! growth was visible before this existed.
//!
//! Reported as two families because they answer to different things. Open
//! times are wall clock and a busy host moves them; the footprint a database
//! leaves on disk is a byte count that a busy host does not touch, so it stays
//! comparable on a runner that was not quiet.

use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use criterion::{criterion_group, criterion_main, Criterion};
use db::DB;
use defra_perf::emit::{Family, Group, Row};
use defra_perf::measure::repeat;
use document::{Document, NormalValue};
use query::mutator::DocMutator;
use schema::{CollectionVersion, FieldDescription, FieldKind};
use storage::{RegolithStore, RegolithStoreOptions};
use tempfile::TempDir;

mod common;

const COLLECTION_COUNTS: [usize; 4] = [0, 1, 10, 100];
const SEEDED_DOCS: usize = 200;
const REPS: usize = 7;

/// A named store profile and the options it opens with.
type Profile = (&'static str, fn() -> RegolithStoreOptions);

/// The profiles a deployment actually runs under. There is one engine, so what
/// varies is the memory budget it was opened with.
fn profiles() -> Vec<Profile> {
    vec![
        ("server", RegolithStoreOptions::new as fn() -> _),
        ("embedded", RegolithStoreOptions::embedded as fn() -> _),
    ]
}

fn collection_version(index: usize) -> CollectionVersion {
    let fields = vec![
        FieldDescription::new("1", "_docID", FieldKind::doc_id()),
        FieldDescription::new("2", "name", FieldKind::string()),
        FieldDescription::new("3", "note", FieldKind::string()),
    ];
    CollectionVersion::new(
        format!("Collection{index}"),
        format!("bafkstartup{index:04}"),
        format!("startup{index:04}"),
        fields,
    )
}

/// Open a store at `path`, build a database over it, and register `count`
/// collections. Returns how long the open itself took.
async fn seed(path: &Path, options: RegolithStoreOptions, count: usize, docs: usize) {
    let store = Arc::new(RegolithStore::open_with_options(path, options).expect("a store"));
    let db = Arc::new(DB::open_from_arc(store).await.expect("a database"));
    for index in 0..count {
        db.create_collection(collection_version(index))
            .await
            .expect("the collection to register");
    }
    if count > 0 && docs > 0 {
        let mutator = db::write::autocommit::AutoCommitMutator::new(db.clone());
        let name = collection_version(0).name.clone();
        for seq in 0..docs {
            let mut doc = Document::new();
            doc.set("name", NormalValue::String(format!("name-{seq}")));
            doc.set("note", NormalValue::String(format!("note-{seq}")));
            mutator
                .create(&name, doc)
                .await
                .expect("the seed create to succeed");
        }
    }
    db.close().await.expect("a clean close");
}

/// Wall time for a cold open of a database already on disk: the store's own
/// recovery plus loading every collection.
async fn cold_open_secs(path: &Path, options: RegolithStoreOptions) -> f64 {
    let start = Instant::now();
    let store = Arc::new(RegolithStore::open_with_options(path, options).expect("a store"));
    // `open_from_arc`, not `from_arc`: the latter starts with an empty
    // collection map and defers the load, so timing it would report a constant
    // and call it a cold start.
    let db = Arc::new(DB::open_from_arc(store).await.expect("a database"));
    let elapsed = start.elapsed().as_secs_f64();
    db.close().await.expect("a clean close");
    elapsed
}

fn dir_bytes(path: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(path) else {
        return 0;
    };
    entries
        .filter_map(Result::ok)
        .map(|entry| match entry.file_type() {
            Ok(t) if t.is_dir() => dir_bytes(&entry.path()),
            Ok(_) => entry.metadata().map(|m| m.len()).unwrap_or(0),
            Err(_) => 0,
        })
        .sum()
}

/// The families the dashboard draws. Runs once, outside criterion's sampling,
/// because an open is a one-shot cost and criterion measures a loop.
fn report(rt: &tokio::runtime::Runtime) {
    let mut open = Family::new(
        "Startup",
        format!(
            "Cold open of a database already on disk: the store's recovery plus loading every \
             collection. The largest collection carries {SEEDED_DOCS} documents, so the open \
             replays real content rather than an empty log."
        ),
    );
    let mut footprint = Family::new(
        "Disk footprint",
        format!(
            "What a database occupies after {SEEDED_DOCS} documents, per store profile and \
             collection count. Blocks, heads, indexes and the log, everything the directory \
             holds."
        ),
    )
    .deterministic();

    for (profile, options) in profiles() {
        let mut open_group = Group::lower_better(format!("cold open, {profile}"), "s")
            .over("collections")
            .note("Lower is better. The x axis is how many collections the open has to load.");
        let mut size_group =
            Group::lower_better(format!("on disk, {profile}"), "B").over("collections");

        for count in COLLECTION_COUNTS {
            let dir = TempDir::new().expect("a scratch directory");
            let path = dir.path().join("db");
            rt.block_on(seed(&path, options(), count, SEEDED_DOCS));

            let row = repeat(format!("{count} collections"), REPS, || {
                rt.block_on(cold_open_secs(&path, options()))
            })
            .at(count as f64);
            open_group = open_group.row(row);
            size_group = size_group.row(
                Row::new(format!("{count} collections"), dir_bytes(&path) as f64).at(count as f64),
            );
        }
        open = open.group(open_group);
        footprint = footprint.group(size_group);
    }

    open.emit("startup");
    footprint.emit("disk_footprint");
}

/// Criterion's own view of the same open, so the trend chart has a sampled
/// series beside the one-shot numbers above.
fn cold_open(c: &mut Criterion) {
    let rt = common::owned_runtime();
    report(&rt);

    let mut group = c.benchmark_group("startup");
    for (profile, options) in profiles() {
        let dir = TempDir::new().expect("a scratch directory");
        let path = dir.path().join("db");
        rt.block_on(seed(&path, options(), 10, SEEDED_DOCS));
        group.bench_function(format!("cold_open/{profile}/10_collections"), |b| {
            b.iter(|| rt.block_on(cold_open_secs(&path, options())))
        });
    }
    group.finish();
}

criterion_group!(benches, cold_open);
criterion_main!(benches);
