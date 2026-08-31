//! A partial `_in` over a composite index scans a prefix, so one `_in` value
//! covers several distinct full index keys. Index order across those keys is
//! the secondary field's order; the public-DocID tie-break (#1602) applies
//! only within one full key.

use db::database::DB;
use db::DbDocFetcher;
use db::DbDocMutator;
use document::Document;
use document::NormalValue;
use query::mutator::DocMutator;
use query::planner::index_selection::IndexScanParams;
use query::planner::index_selection::IndexScanType;
use query::runner::DocFetcher;
use schema::CollectionVersion;
use schema::FieldDescription;
use schema::FieldKind;
use schema::IndexDescription;
use schema::IndexedFieldDescription;
use std::sync::Arc;
use storage::RegolithStore;

const COLLECTION: &str = "products";
const INDEX: &str = "by_category_rank";
const CATEGORY: &str = "a";

/// Ranks repeat so one scan covers both the across-key order and the
/// within-key tie-break.
const SEED: [(&str, i64); 5] = [("p0", 1), ("p1", 1), ("p2", 2), ("p3", 2), ("p4", 3)];

fn schema() -> CollectionVersion {
    let mut version = CollectionVersion::new(
        COLLECTION,
        "v1",
        "col-products",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "category", FieldKind::string()),
            FieldDescription::new("3", "rank", FieldKind::int()),
            FieldDescription::new("4", "name", FieldKind::string()),
        ],
    );
    version.indexes = vec![IndexDescription {
        name: INDEX.to_string(),
        id: 0,
        fields: vec![
            IndexedFieldDescription {
                name: "category".to_string(),
                descending: false,
            },
            IndexedFieldDescription {
                name: "rank".to_string(),
                descending: false,
            },
        ],
        unique: false,
        kind: None,
        auto_generated: false,
    }];
    version
}

/// Returns (rank, doc_id) per seeded document, in insertion order.
async fn populated() -> (Arc<DB<RegolithStore>>, Vec<(i64, String)>) {
    let db = Arc::new(DB::new(RegolithStore::in_memory().unwrap()).expect("a database"));
    db.create_collection(schema())
        .await
        .expect("the collection must register");

    let txn = db.new_txn(false).await.unwrap();
    let mutator = DbDocMutator::new(db.clone(), txn);
    let mut seeded = Vec::new();
    for (name, rank) in SEED {
        let mut doc = Document::new();
        doc.set("category", NormalValue::String(CATEGORY.to_string()));
        doc.set("rank", NormalValue::Int(rank));
        doc.set("name", NormalValue::String(name.to_string()));
        let created = mutator
            .create(COLLECTION, doc)
            .await
            .expect("the document must be created");
        seeded.push((rank, created.doc_id.to_string()));
    }
    mutator
        .take_txn()
        .await
        .expect("the mutator still holds its transaction")
        .commit()
        .await
        .unwrap();

    (db, seeded)
}

/// Index order: rank ascending, public DocID ascending within one rank.
fn index_order(seeded: &[(i64, String)]) -> Vec<String> {
    let mut ordered = seeded.to_vec();
    ordered.sort();
    ordered.into_iter().map(|(_, id)| id).collect()
}

#[tokio::test]
async fn partial_in_over_a_composite_index_keeps_secondary_field_order() {
    let (db, seeded) = populated().await;

    let expected = index_order(&seeded);
    let doc_id_only: Vec<String> = {
        let mut ids: Vec<String> = seeded.iter().map(|(_, id)| id.clone()).collect();
        ids.sort();
        ids
    };
    assert_ne!(
        expected, doc_id_only,
        "the seed must distinguish index order from a flat DocID sort, \
         otherwise this test cannot fail"
    );

    let txn = db.new_txn(true).await.unwrap();
    let got = DbDocFetcher::new(txn)
        .get_by_index_scan(
            COLLECTION,
            &IndexScanParams {
                index_name: INDEX.to_string(),
                scan_type: IndexScanType::InScan {
                    values: vec![NormalValue::String(CATEGORY.to_string())],
                    suffix_values: vec![],
                },
                limit: None,
                offset: 0,
                value_filter: None,
                cursor_seek: None,
            },
        )
        .await
        .expect("the prefix scan must run")
        .into_doc_ids();

    assert_eq!(
        got, expected,
        "a partial `_in` must return index order (rank, then DocID), not a \
         DocID sort spanning distinct ranks"
    );
}
