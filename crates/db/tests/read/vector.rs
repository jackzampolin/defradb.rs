//! Reading documents by short id without scanning the collection.

use cid::Cid;
use db::collection::Collection;
use db::database::DB;
use defra_core::SHA2_256_CODE;
use document::Document;
use document::NormalValue;
use multihash::Multihash;
use schema::CollectionVersion;
use schema::FieldDescription;
use schema::FieldKind;
use sha2::Digest;
use sha2::Sha256;
use storage::backends::MemoryStore;

/// A distinct, reproducible document id per index.
fn doc_id(index: usize) -> document::DocID {
    let mut hasher = Sha256::new();
    hasher.update(format!("seek-path-doc-{index}").as_bytes());
    let mh: Multihash<64> = Multihash::wrap(SHA2_256_CODE, &hasher.finalize()).unwrap();
    document::DocID::new_v0(Cid::new_v1(0x55, mh))
}

const COLLECTION_SHORT_ID: u32 = 1;

fn collection() -> Collection {
    Collection::new(schema())
}

fn schema() -> CollectionVersion {
    CollectionVersion::new(
        "docs",
        "v1",
        "col-docs",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "title", FieldKind::string()),
        ],
    )
}

/// Writes `count` documents and returns their short ids paired with titles.
async fn populate(db: &DB<MemoryStore>, count: usize) -> Vec<(u64, String)> {
    let txn = db.new_txn(false).await.unwrap();
    let mut written = Vec::new();
    {
        let datastore = txn.datastore().unwrap();
        let systemstore = txn.systemstore().unwrap();
        let collection = collection();
        for i in 0..count {
            let title = format!("doc-{i}");
            let mut doc = Document::new();
            doc.set("title", NormalValue::String(title.clone()));
            // `create_with_datastore` expects the document to carry its id
            // already; a fixed uuid per index keeps the corpus reproducible.
            doc.set_id(doc_id(i));

            let short_id = db::docid::map::next_doc_short_id(&systemstore)
                .await
                .expect("short id");
            let doc_id = collection
                .create_with_datastore(&datastore, &doc, short_id)
                .await
                .expect("create");
            db::docid::map::set_doc_id_mapping(
                &systemstore,
                COLLECTION_SHORT_ID,
                short_id,
                &doc_id.to_string(),
            )
            .await
            .expect("map short id to doc id");
            written.push((short_id, title));
        }
    }
    txn.commit().await.unwrap();
    written
}

#[tokio::test]
async fn a_seek_reads_only_what_was_asked_for() {
    let db = DB::new(MemoryStore::new()).unwrap();
    let written = populate(&db, 20).await;

    let txn = db.new_txn(false).await.unwrap();
    let datastore = txn.datastore().unwrap();
    let systemstore = txn.systemstore().unwrap();
    let collection = collection();

    let wanted: Vec<u64> = written.iter().step_by(4).map(|(id, _)| *id).collect();
    let got = collection
        .get_by_short_ids(&datastore, &systemstore, &wanted, false)
        .await
        .unwrap();

    assert_eq!(got.len(), wanted.len());
    assert_eq!(
        got.iter().map(|(id, _, _)| *id).collect::<Vec<_>>(),
        wanted,
        "the result must follow the order asked for"
    );
    for (short_id, doc, deleted) in &got {
        assert!(!deleted);
        let expected = &written.iter().find(|(id, _)| id == short_id).unwrap().1;
        assert_eq!(
            doc.get("title"),
            Some(&NormalValue::String(expected.clone())),
            "a seek returned the wrong document"
        );
        assert!(doc.id().is_some(), "the document id must be hydrated");
    }
}

/// An id whose document is gone is skipped, not reported: a caller holding an
/// id from an index may hold a stale one.
#[tokio::test]
async fn absent_ids_are_skipped() {
    let db = DB::new(MemoryStore::new()).unwrap();
    let written = populate(&db, 5).await;

    let txn = db.new_txn(false).await.unwrap();
    let datastore = txn.datastore().unwrap();
    let systemstore = txn.systemstore().unwrap();
    let collection = collection();

    let mut wanted: Vec<u64> = written.iter().map(|(id, _)| *id).collect();
    wanted.push(9_999_999);
    let got = collection
        .get_by_short_ids(&datastore, &systemstore, &wanted, false)
        .await
        .unwrap();
    assert_eq!(got.len(), written.len(), "the absent id must be skipped");

    assert!(collection
        .get_by_short_ids(&datastore, &systemstore, &[], false)
        .await
        .unwrap()
        .is_empty());
}

/// A seek and a scan must agree about every document they both return, or the
/// two paths have diverged about what a loaded document looks like.
#[tokio::test]
async fn a_seek_agrees_with_a_scan() {
    let db = DB::new(MemoryStore::new()).unwrap();
    populate(&db, 30).await;

    let txn = db.new_txn(false).await.unwrap();
    let datastore = txn.datastore().unwrap();
    let systemstore = txn.systemstore().unwrap();
    let collection = collection();

    let scanned = collection
        .get_all_with_datastore_short_ids(&datastore, &systemstore, false)
        .await
        .unwrap();
    let ids: Vec<u64> = scanned.iter().map(|(id, _, _)| *id).collect();
    let sought = collection
        .get_by_short_ids(&datastore, &systemstore, &ids, false)
        .await
        .unwrap();

    assert_eq!(sought.len(), scanned.len());
    for ((sid, sdoc, sdel), (gid, gdoc, gdel)) in scanned.iter().zip(&sought) {
        assert_eq!(sid, gid);
        assert_eq!(sdel, gdel);
        assert_eq!(sdoc.id(), gdoc.id(), "document id differs");
        assert_eq!(
            sdoc.get("title"),
            gdoc.get("title"),
            "field value differs between seek and scan"
        );
    }
}
