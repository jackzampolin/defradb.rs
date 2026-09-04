//! What DefraDB costs in a browser.
//!
//! ```text
//! CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER=wasm-bindgen-test-runner \
//!   cargo test -p defra-wasm --target wasm32-unknown-unknown --release \
//!   --test perf -- --nocapture
//! ```
//!
//! The browser is a first-class target for this database, and every other
//! number in the suite is a native one. Inferring browser cost from a native
//! figure would be a guess presented as a measurement, so this measures it
//! where it actually runs, under the same `wasm-bindgen-test` harness the WASM
//! tests already use.
//!
//! The families here carry the same names, groups and rows as their native
//! counterparts wherever the operation is genuinely the same one, which is what
//! lets the dashboard line a browser figure up beside the Linux and macOS ones
//! rather than showing three unrelated tables.
//!
//! A browser cannot write a file, so each family is printed behind a marker the
//! workflow greps out of the log. `Family::record` builds that line, and the
//! native emitter builds its own from the same function, so the two transports
//! cannot drift apart.

#![cfg(target_arch = "wasm32")]

use std::sync::Arc;

use db::DB;
use defra_perf::emit::{Family, Group, Row, Trust};
use document::{Document, NormalValue};
use query::mutator::DocMutator;
use query::DocFetcher;
use schema::{CollectionVersion, FieldDescription, FieldKind};
use storage::corekv::{Reader, Store, Writer};
use storage::RegolithStore;
use wasm_bindgen::prelude::*;
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

#[wasm_bindgen]
extern "C" {
    /// `performance.now()`, a monotonic clock in fractional milliseconds.
    /// `Date.now` is whole milliseconds and would round most of these to zero.
    #[wasm_bindgen(js_namespace = performance, js_name = now)]
    fn now() -> f64;
}

const COLLECTION: &str = "User";
const FIELD_COUNTS: [usize; 3] = [4, 16, 64];
const OPS: usize = 200;

/// Print one family where the workflow can find it.
fn report(family: &Family, name: &str) {
    web_sys::console::log_1(&JsValue::from_str(&format!(
        "DEFRA_BENCH_FAMILY {}",
        family.record(name)
    )));
}

/// Operations per second for `ops` operations that took `elapsed_ms`.
fn ops_per_s(ops: usize, elapsed_ms: f64) -> f64 {
    if elapsed_ms <= 0.0 {
        return f64::NAN;
    }
    ops as f64 / (elapsed_ms / 1000.0)
}

fn field_names(count: usize) -> Vec<String> {
    (0..count).map(|i| format!("field_{i}")).collect()
}

fn collection_version(field_count: usize) -> CollectionVersion {
    let mut fields = vec![FieldDescription::new("1", "_docID", FieldKind::doc_id())];
    for (index, name) in field_names(field_count).iter().enumerate() {
        fields.push(FieldDescription::new(
            (index + 2).to_string(),
            name,
            FieldKind::string(),
        ));
    }
    CollectionVersion::new(COLLECTION, "bafkwasmperf", "wasmperf", fields)
}

fn document(field_count: usize, seq: usize) -> Document {
    let mut doc = Document::new();
    for name in field_names(field_count) {
        doc.set(&name, NormalValue::String(format!("{name}-{seq}")));
    }
    doc
}

/// The same document operations the native suite measures, in a browser.
///
/// Group and row names match `document_ops` on every other platform on
/// purpose: that is what puts a browser column beside the native ones instead
/// of a separate table nothing lines up with.
#[wasm_bindgen_test]
#[allow(clippy::arc_with_non_send_sync)]
async fn document_ops() {
    let mut create = Group::higher_better("create", "ops/s").over("fields");
    let mut read = Group::higher_better("read", "ops/s").over("fields");

    for fields in FIELD_COUNTS {
        let store = Arc::new(RegolithStore::in_memory().expect("an in-memory store"));
        let db = Arc::new(DB::open_from_arc(store).await.expect("a database"));
        db.create_collection(collection_version(fields))
            .await
            .expect("the collection to register");
        let mutator = db::write::autocommit::AutoCommitMutator::new(db.clone());

        let started = now();
        let mut ids = Vec::with_capacity(OPS);
        for seq in 0..OPS {
            ids.push(
                mutator
                    .create(COLLECTION, document(fields, seq))
                    .await
                    .expect("the create to succeed")
                    .doc_id
                    .to_string(),
            );
        }
        create = create.row(
            Row::new(format!("{fields} fields"), ops_per_s(OPS, now() - started)).at(fields as f64),
        );

        let fetcher = db::AutoCommitFetcher::new(db.clone());
        let started = now();
        for id in &ids {
            fetcher
                .get_by_ids(COLLECTION, std::slice::from_ref(id))
                .await
                .expect("the read to succeed");
        }
        read = read.row(
            Row::new(
                format!("{fields} fields"),
                ops_per_s(ids.len(), now() - started),
            )
            .at(fields as f64),
        );
    }

    let family = Family::new(
        "Document operations",
        format!(
            "Documents created and read back one at a time, {OPS} of each, through the same \
             mutator and fetcher every platform uses. Measured with `performance.now()` in the \
             browser and with a wall clock natively, so the two are the same operation counted \
             the same way."
        ),
    )
    .group(create)
    .group(read);
    report(&family, "document_ops");
}

/// The storage engine underneath, in the browser. Regolith is the same engine
/// on every target, so this is where a browser-only cost would show up first.
#[wasm_bindgen_test]
async fn browser_storage() {
    let store = RegolithStore::in_memory().expect("an in-memory store");
    let value = vec![0xa5u8; 256];

    let started = now();
    let mut txn = store.new_txn(false).await.expect("a write transaction");
    for seq in 0..OPS {
        txn.set(format!("key{seq:08}").as_bytes(), &value)
            .await
            .expect("the write to succeed");
    }
    txn.commit().await.expect("the commit to succeed");
    let write = ops_per_s(OPS, now() - started);

    let started = now();
    let txn = store.new_txn(true).await.expect("a read transaction");
    for seq in 0..OPS {
        let got = txn
            .get(format!("key{seq:08}").as_bytes())
            .await
            .expect("the read to succeed");
        assert!(got.is_some(), "every key written must be readable");
    }
    let read = ops_per_s(OPS, now() - started);

    // A commit per write rather than one for all of them: the difference
    // between the two is what batching is worth in a browser.
    let started = now();
    for seq in 0..OPS {
        let mut txn = store.new_txn(false).await.expect("a write transaction");
        txn.set(format!("solo{seq:08}").as_bytes(), &value)
            .await
            .expect("the write to succeed");
        txn.commit().await.expect("the commit to succeed");
    }
    let per_commit = ops_per_s(OPS, now() - started);

    let family = Family::new(
        "Browser storage",
        format!(
            "The regolith key/value path in a browser tab: {OPS} operations of a 256 byte value. \
             `batched write` puts every write in one transaction; `write per commit` opens and \
             commits one per write, and the gap between them is what batching buys."
        ),
    )
    .trust(Trust::Clean)
    .group(
        Group::higher_better("key/value", "ops/s")
            .row(Row::new("batched write", write))
            .row(Row::new("read", read))
            .row(Row::new("write per commit", per_commit)),
    );
    report(&family, "browser_storage");
}
